package floxy

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// assertRollbackInvariant checks the invariant: when workflow is failed and compensations succeeded,
// all steps after the savepoint should be in `rolled_back` or `skipped` status only.
// The failingStep is allowed to be in `failed` or `rolled_back` status.
// Steps before savepoint (or all steps if no savepoint) that were completed should be `rolled_back`.
// Steps that didn't execute should be `skipped`.
// Fork/Join steps are also checked.
func assertRollbackInvariant(
	t *testing.T,
	stepStatuses map[string]StepStatus,
	failingStep string,
	stepsBeforeSavepoint []string, // steps that should remain completed (before savepoint)
) {
	t.Helper()

	beforeSavepointSet := make(map[string]bool)
	for _, s := range stepsBeforeSavepoint {
		beforeSavepointSet[s] = true
	}

	for stepName, status := range stepStatuses {
		// Steps before savepoint should remain completed
		if beforeSavepointSet[stepName] {
			assert.Equal(t, StepStatusCompleted, status,
				"Step %s (before savepoint) should remain completed, got: %s", stepName, status)
			continue
		}

		// The failing step can be failed or rolled_back
		if stepName == failingStep {
			assert.True(t, status == StepStatusFailed || status == StepStatusRolledBack,
				"Failing step %s should be failed or rolled_back, got: %s", stepName, status)
			continue
		}

		// All other steps after savepoint must be rolled_back or skipped
		allowedStatuses := []StepStatus{StepStatusRolledBack, StepStatusSkipped}
		isAllowed := false
		for _, allowed := range allowedStatuses {
			if status == allowed {
				isAllowed = true
				break
			}
		}

		assert.True(t, isAllowed,
			"Step %s should be rolled_back or skipped after rollback, got: %s (invariant violation!)", stepName, status)
	}
}

// nestedForkFailingHandler is a handler that fails for specific step names
type nestedForkFailingHandler struct {
	failSteps map[string]bool
	callCount map[string]int
}

func newNestedForkFailingHandler(failSteps ...string) *nestedForkFailingHandler {
	fs := make(map[string]bool)
	for _, s := range failSteps {
		fs[s] = true
	}

	return &nestedForkFailingHandler{
		failSteps: fs,
		callCount: make(map[string]int),
	}
}

func (h *nestedForkFailingHandler) Name() string {
	return "nested-fork-handler"
}

func (h *nestedForkFailingHandler) Execute(ctx context.Context, stepCtx StepContext, input json.RawMessage) (json.RawMessage, error) {
	stepName := stepCtx.StepName()
	h.callCount[stepName]++

	if h.failSteps[stepName] {
		return nil, fmt.Errorf("intentional failure in step %s", stepName)
	}

	var data map[string]any
	_ = json.Unmarshal(input, &data)
	if data == nil {
		data = make(map[string]any)
	}
	data["executed_"+stepName] = true

	return json.Marshal(data)
}

// nestedForkCompensationHandler handles compensation for nested fork tests
type nestedForkCompensationHandler struct {
	compensated map[string]bool
}

func newNestedForkCompensationHandler() *nestedForkCompensationHandler {
	return &nestedForkCompensationHandler{
		compensated: make(map[string]bool),
	}
}

func (h *nestedForkCompensationHandler) Name() string {
	return "nested-compensation"
}

func (h *nestedForkCompensationHandler) Execute(ctx context.Context, stepCtx StepContext, input json.RawMessage) (json.RawMessage, error) {
	stepName := stepCtx.StepName()
	h.compensated[stepName] = true

	return input, nil
}

// TestNestedForkRollback_Level2 tests rollback when a step fails in a level-2 nested fork
func TestNestedForkRollback_Level2(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	store, txManager, cleanup := setupTestStore(t)
	t.Cleanup(cleanup)

	ctx := context.Background()
	engine := NewEngine(nil,
		WithEngineStore(store),
		WithEngineTxManager(txManager),
		WithEngineCancelInterval(time.Minute),
	)
	defer func() { _ = engine.Shutdown(time.Second) }()

	// Create handlers
	handler := newNestedForkFailingHandler("nested-step-2a") // Fail in nested fork
	compensationHandler := newNestedForkCompensationHandler()
	engine.RegisterHandler(handler)
	engine.RegisterHandler(compensationHandler)

	// Build workflow with nested fork:
	// start -> Fork1 (outer-fork)
	//            |-- branch1: step-1a -> Fork2 (inner-fork)
	//            |                         |-- branch2a: nested-step-2a (FAILS)
	//            |                         |-- branch2b: nested-step-2b
	//            |                       Join2 (inner-join)
	//            |                       -> step-1a-after
	//            |-- branch2: step-1b
	//          Join1 (outer-join)
	//          -> final-step
	workflowDef, err := NewBuilder("nested-fork-rollback-test", 1).
		Fork("outer-fork",
			func(b *Builder) {
				b.Step("step-1a", "nested-fork-handler").
					OnFailure("comp-step-1a", "nested-compensation").
					Fork("inner-fork",
						func(ib *Builder) {
							ib.Step("nested-step-2a", "nested-fork-handler").
								OnFailure("comp-nested-step-2a", "nested-compensation")
						},
						func(ib *Builder) {
							ib.Step("nested-step-2b", "nested-fork-handler").
								OnFailure("comp-nested-step-2b", "nested-compensation")
						},
					).
					Join("inner-join", JoinStrategyAll).
					Then("step-1a-after", "nested-fork-handler").
					OnFailure("comp-step-1a-after", "nested-compensation")
			},
			func(b *Builder) {
				b.Step("step-1b", "nested-fork-handler").
					OnFailure("comp-step-1b", "nested-compensation")
			},
		).
		Join("outer-join", JoinStrategyAll).
		Then("final-step", "nested-fork-handler").
		Build()

	require.NoError(t, err)
	err = engine.RegisterWorkflow(ctx, workflowDef)
	require.NoError(t, err)

	input := json.RawMessage(`{"test": "nested-fork-rollback"}`)
	instanceID, err := engine.Start(ctx, "nested-fork-rollback-test-v1", input)
	require.NoError(t, err)

	// Process workflow until completion or failure
	for i := 0; i < 100; i++ {
		empty, err := engine.ExecuteNext(ctx, "worker1")
		require.NoError(t, err)
		if empty {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Check final status
	status, err := engine.GetStatus(ctx, instanceID)
	require.NoError(t, err)

	// Get all steps
	steps, err := engine.GetSteps(ctx, instanceID)
	require.NoError(t, err)

	stepStatuses := make(map[string]StepStatus)
	for _, step := range steps {
		stepStatuses[step.StepName] = step.Status
	}

	t.Logf("Final workflow status: %s", status)
	t.Logf("Step statuses: %v", stepStatuses)

	// Workflow should be failed
	assert.Equal(t, StatusFailed, status, "Workflow should be failed")

	// Check rollback invariant: all steps should be rolled_back or skipped (no completed steps after rollback)
	// No savepoint in this test, so stepsBeforeSavepoint is empty
	assertRollbackInvariant(t, stepStatuses, "nested-step-2a", nil)
}

// TestNestedForkRollback_NoDoubleRollback verifies that steps are not rolled back twice
func TestNestedForkRollback_NoDoubleRollback(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	store, txManager, cleanup := setupTestStore(t)
	t.Cleanup(cleanup)

	ctx := context.Background()
	engine := NewEngine(nil,
		WithEngineStore(store),
		WithEngineTxManager(txManager),
		WithEngineCancelInterval(time.Minute),
	)
	defer func() { _ = engine.Shutdown(time.Second) }()

	// Track compensation calls
	compensationCalls := make(map[string]int)

	// Custom compensation handler that tracks calls
	trackingCompHandler := &trackingCompensationHandler{
		calls: compensationCalls,
	}

	handler := newNestedForkFailingHandler("step-fail")
	engine.RegisterHandler(handler)
	engine.RegisterHandler(trackingCompHandler)

	// Build workflow:
	// start -> step-before -> Fork (fork-1)
	//                           |-- branch1: step-a -> step-fail (FAILS)
	//                           |-- branch2: step-b
	//                         Join (join-1)
	//                         -> step-after
	workflowDef, err := NewBuilder("no-double-rollback-test", 1).
		Step("step-before", "nested-fork-handler").
		OnFailure("comp-step-before", "tracking-compensation").
		Fork("fork-1",
			func(b *Builder) {
				b.Step("step-a", "nested-fork-handler").
					OnFailure("comp-step-a", "tracking-compensation").
					Then("step-fail", "nested-fork-handler").
					OnFailure("comp-step-fail", "tracking-compensation")
			},
			func(b *Builder) {
				b.Step("step-b", "nested-fork-handler").
					OnFailure("comp-step-b", "tracking-compensation")
			},
		).
		Join("join-1", JoinStrategyAll).
		Then("step-after", "nested-fork-handler").
		OnFailure("comp-step-after", "tracking-compensation").
		Build()

	require.NoError(t, err)
	err = engine.RegisterWorkflow(ctx, workflowDef)
	require.NoError(t, err)

	input := json.RawMessage(`{"test": "no-double-rollback"}`)
	instanceID, err := engine.Start(ctx, "no-double-rollback-test-v1", input)
	require.NoError(t, err)

	// Process workflow
	for i := 0; i < 100; i++ {
		empty, err := engine.ExecuteNext(ctx, "worker1")
		require.NoError(t, err)
		if empty {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	status, err := engine.GetStatus(ctx, instanceID)
	require.NoError(t, err)

	t.Logf("Final status: %s", status)
	t.Logf("Compensation calls: %v", compensationCalls)

	// Verify no step was compensated more than once
	for stepName, count := range compensationCalls {
		assert.LessOrEqual(t, count, 1,
			"Step %s was compensated %d times (should be at most 1)", stepName, count)
	}
}

// trackingCompensationHandler tracks how many times each step's compensation was called
type trackingCompensationHandler struct {
	calls map[string]int
}

func (h *trackingCompensationHandler) Name() string {
	return "tracking-compensation"
}

func (h *trackingCompensationHandler) Execute(ctx context.Context, stepCtx StepContext, input json.RawMessage) (json.RawMessage, error) {
	stepName := stepCtx.StepName()
	h.calls[stepName]++
	return input, nil
}

// TestNestedForkRollback_DeepNesting tests rollback with 3 levels of nested forks
func TestNestedForkRollback_DeepNesting(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	store, txManager, cleanup := setupTestStore(t)
	t.Cleanup(cleanup)

	ctx := context.Background()
	engine := NewEngine(nil,
		WithEngineStore(store),
		WithEngineTxManager(txManager),
		WithEngineCancelInterval(time.Minute),
	)
	defer func() { _ = engine.Shutdown(time.Second) }()

	// Fail at the deepest level
	handler := newNestedForkFailingHandler("deep-step")
	compensationHandler := newNestedForkCompensationHandler()
	engine.RegisterHandler(handler)
	engine.RegisterHandler(compensationHandler)

	// Build workflow with 3 levels of nesting:
	// Fork1
	//   |-- branch1a: step-l1-a -> Fork2
	//   |                           |-- branch2a: step-l2-a -> Fork3
	//   |                           |                           |-- branch3a: deep-step (FAILS)
	//   |                           |                           |-- branch3b: step-l3-b
	//   |                           |                         Join3
	//   |                           |-- branch2b: step-l2-b
	//   |                         Join2
	//   |-- branch1b: step-l1-b
	// Join1
	workflowDef, err := NewBuilder("deep-nesting-rollback-test", 1).
		Fork("fork-l1",
			func(b *Builder) {
				b.Step("step-l1-a", "nested-fork-handler").
					OnFailure("comp-step-l1-a", "nested-compensation").
					Fork("fork-l2",
						func(b2 *Builder) {
							b2.Step("step-l2-a", "nested-fork-handler").
								OnFailure("comp-step-l2-a", "nested-compensation").
								Fork("fork-l3",
									func(b3 *Builder) {
										b3.Step("deep-step", "nested-fork-handler").
											OnFailure("comp-deep-step", "nested-compensation")
									},
									func(b3 *Builder) {
										b3.Step("step-l3-b", "nested-fork-handler").
											OnFailure("comp-step-l3-b", "nested-compensation")
									},
								).
								Join("join-l3", JoinStrategyAll)
						},
						func(b2 *Builder) {
							b2.Step("step-l2-b", "nested-fork-handler").
								OnFailure("comp-step-l2-b", "nested-compensation")
						},
					).
					Join("join-l2", JoinStrategyAll)
			},
			func(b *Builder) {
				b.Step("step-l1-b", "nested-fork-handler").
					OnFailure("comp-step-l1-b", "nested-compensation")
			},
		).
		Join("join-l1", JoinStrategyAll).
		Build()

	require.NoError(t, err)
	err = engine.RegisterWorkflow(ctx, workflowDef)
	require.NoError(t, err)

	input := json.RawMessage(`{"test": "deep-nesting"}`)
	instanceID, err := engine.Start(ctx, "deep-nesting-rollback-test-v1", input)
	require.NoError(t, err)

	// Process workflow
	for i := 0; i < 150; i++ {
		empty, err := engine.ExecuteNext(ctx, "worker1")
		require.NoError(t, err)
		if empty {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	status, err := engine.GetStatus(ctx, instanceID)
	require.NoError(t, err)

	steps, err := engine.GetSteps(ctx, instanceID)
	require.NoError(t, err)

	stepStatuses := make(map[string]StepStatus)
	for _, step := range steps {
		stepStatuses[step.StepName] = step.Status
	}

	t.Logf("Final workflow status: %s", status)
	t.Logf("Step statuses: %v", stepStatuses)

	// Workflow should be failed
	assert.Equal(t, StatusFailed, status, "Workflow should be failed")

	// Check rollback invariant: all steps should be rolled_back or skipped (no completed steps)
	// No savepoint in this test
	assertRollbackInvariant(t, stepStatuses, "deep-step", nil)
}

// TestNestedForkRollback_WithSavePoint tests that rollback stops at savepoint even with nested forks
func TestNestedForkRollback_WithSavePoint(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	store, txManager, cleanup := setupTestStore(t)
	t.Cleanup(cleanup)

	ctx := context.Background()
	engine := NewEngine(nil,
		WithEngineStore(store),
		WithEngineTxManager(txManager),
		WithEngineCancelInterval(time.Minute),
	)
	defer func() { _ = engine.Shutdown(time.Second) }()

	handler := newNestedForkFailingHandler("step-after-fork-fail")
	compensationHandler := newNestedForkCompensationHandler()
	engine.RegisterHandler(handler)
	engine.RegisterHandler(compensationHandler)

	// Build workflow:
	// step-before-savepoint -> SavePoint -> Fork
	//                                         |-- branch1: step-a -> step-after-fork-fail (FAILS)
	//                                         |-- branch2: step-b
	//                                       Join
	workflowDef, err := NewBuilder("savepoint-nested-fork-test", 1).
		Step("step-before-savepoint", "nested-fork-handler").
		OnFailure("comp-step-before-savepoint", "nested-compensation").
		SavePoint("checkpoint").
		Fork("fork-after-savepoint",
			func(b *Builder) {
				b.Step("step-a", "nested-fork-handler").
					OnFailure("comp-step-a", "nested-compensation").
					Then("step-after-fork-fail", "nested-fork-handler").
					OnFailure("comp-step-after-fork-fail", "nested-compensation")
			},
			func(b *Builder) {
				b.Step("step-b", "nested-fork-handler").
					OnFailure("comp-step-b", "nested-compensation")
			},
		).
		Join("join-after-savepoint", JoinStrategyAll).
		Build()

	require.NoError(t, err)
	err = engine.RegisterWorkflow(ctx, workflowDef)
	require.NoError(t, err)

	input := json.RawMessage(`{"test": "savepoint-nested-fork"}`)
	instanceID, err := engine.Start(ctx, "savepoint-nested-fork-test-v1", input)
	require.NoError(t, err)

	// Process workflow
	for i := 0; i < 100; i++ {
		empty, err := engine.ExecuteNext(ctx, "worker1")
		require.NoError(t, err)
		if empty {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	status, err := engine.GetStatus(ctx, instanceID)
	require.NoError(t, err)

	steps, err := engine.GetSteps(ctx, instanceID)
	require.NoError(t, err)

	stepStatuses := make(map[string]StepStatus)
	for _, step := range steps {
		stepStatuses[step.StepName] = step.Status
	}

	t.Logf("Final workflow status: %s", status)
	t.Logf("Step statuses: %v", stepStatuses)

	// Workflow should be failed
	assert.Equal(t, StatusFailed, status, "Workflow should be failed")

	// Check rollback invariant with savepoint:
	// Steps before savepoint should remain completed
	// Steps after savepoint should be rolled_back or skipped
	stepsBeforeSavepoint := []string{"step-before-savepoint", "checkpoint"}
	assertRollbackInvariant(t, stepStatuses, "step-after-fork-fail", stepsBeforeSavepoint)
}

// TestNestedForkRollback_ParallelNestedForks tests rollback when each parallel branch has its own nested fork.
// Structure:
// Fork1 (outer-fork)
//
//	|-- branch1: step-1a -> Fork1A (inner-fork-1)
//	|                         |-- branch1a: step-1a-1
//	|                         |-- branch1b: step-1a-2 (FAILS)
//	|                       Join1A (inner-join-1)
//	|-- branch2: step-1b -> Fork1B (inner-fork-2)
//	|                         |-- branch2a: step-1b-1
//	|                         |-- branch2b: step-1b-2
//	|                       Join1B (inner-join-2)
//
// Join1 (outer-join)
func TestNestedForkRollback_ParallelNestedForks(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	store, txManager, cleanup := setupTestStore(t)
	t.Cleanup(cleanup)

	ctx := context.Background()
	engine := NewEngine(nil,
		WithEngineStore(store),
		WithEngineTxManager(txManager),
		WithEngineCancelInterval(time.Minute),
	)
	defer func() { _ = engine.Shutdown(time.Second) }()

	// Fail in the first branch's nested fork
	handler := newNestedForkFailingHandler("step-1a-2")
	compensationHandler := newNestedForkCompensationHandler()
	engine.RegisterHandler(handler)
	engine.RegisterHandler(compensationHandler)

	// Build workflow with parallel nested forks
	workflowDef, err := NewBuilder("parallel-nested-forks-test", 1).
		Fork("outer-fork",
			func(b *Builder) {
				b.Step("step-1a", "nested-fork-handler").
					OnFailure("comp-step-1a", "nested-compensation").
					Fork("inner-fork-1",
						func(ib *Builder) {
							ib.Step("step-1a-1", "nested-fork-handler").
								OnFailure("comp-step-1a-1", "nested-compensation")
						},
						func(ib *Builder) {
							ib.Step("step-1a-2", "nested-fork-handler"). // This one will FAIL
													OnFailure("comp-step-1a-2", "nested-compensation")
						},
					).
					Join("inner-join-1", JoinStrategyAll)
			},
			func(b *Builder) {
				b.Step("step-1b", "nested-fork-handler").
					OnFailure("comp-step-1b", "nested-compensation").
					Fork("inner-fork-2",
						func(ib *Builder) {
							ib.Step("step-1b-1", "nested-fork-handler").
								OnFailure("comp-step-1b-1", "nested-compensation")
						},
						func(ib *Builder) {
							ib.Step("step-1b-2", "nested-fork-handler").
								OnFailure("comp-step-1b-2", "nested-compensation")
						},
					).
					Join("inner-join-2", JoinStrategyAll)
			},
		).
		Join("outer-join", JoinStrategyAll).
		Build()

	require.NoError(t, err)
	err = engine.RegisterWorkflow(ctx, workflowDef)
	require.NoError(t, err)

	input := json.RawMessage(`{"test": "parallel-nested-forks"}`)
	instanceID, err := engine.Start(ctx, "parallel-nested-forks-test-v1", input)
	require.NoError(t, err)

	// Process workflow until completion or failure
	for i := 0; i < 100; i++ {
		empty, err := engine.ExecuteNext(ctx, "worker1")
		require.NoError(t, err)
		if empty {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Check final status
	status, err := engine.GetStatus(ctx, instanceID)
	require.NoError(t, err)

	// Get all steps
	steps, err := engine.GetSteps(ctx, instanceID)
	require.NoError(t, err)

	stepStatuses := make(map[string]StepStatus)
	for _, step := range steps {
		stepStatuses[step.StepName] = step.Status
	}

	t.Logf("Final workflow status: %s", status)
	t.Logf("Step statuses: %v", stepStatuses)

	// Workflow should be failed
	assert.Equal(t, StatusFailed, status, "Workflow should be failed")

	// Check rollback invariant: all steps should be rolled_back or skipped (no completed steps)
	// No savepoint in this test
	assertRollbackInvariant(t, stepStatuses, "step-1a-2", nil)
}

// TestNestedForkRollback_ParallelNestedForks_SecondBranchFails tests when the second parallel branch's nested fork fails
func TestNestedForkRollback_ParallelNestedForks_SecondBranchFails(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	store, txManager, cleanup := setupTestStore(t)
	t.Cleanup(cleanup)

	ctx := context.Background()
	engine := NewEngine(nil,
		WithEngineStore(store),
		WithEngineTxManager(txManager),
		WithEngineCancelInterval(time.Minute),
	)
	defer func() { _ = engine.Shutdown(time.Second) }()

	// Fail in the SECOND branch's nested fork
	handler := newNestedForkFailingHandler("step-1b-2")
	compensationHandler := newNestedForkCompensationHandler()
	engine.RegisterHandler(handler)
	engine.RegisterHandler(compensationHandler)

	// Build workflow with parallel nested forks - same structure as above
	workflowDef, err := NewBuilder("parallel-nested-forks-test-2", 1).
		Fork("outer-fork",
			func(b *Builder) {
				b.Step("step-1a", "nested-fork-handler").
					OnFailure("comp-step-1a", "nested-compensation").
					Fork("inner-fork-1",
						func(ib *Builder) {
							ib.Step("step-1a-1", "nested-fork-handler").
								OnFailure("comp-step-1a-1", "nested-compensation")
						},
						func(ib *Builder) {
							ib.Step("step-1a-2", "nested-fork-handler").
								OnFailure("comp-step-1a-2", "nested-compensation")
						},
					).
					Join("inner-join-1", JoinStrategyAll)
			},
			func(b *Builder) {
				b.Step("step-1b", "nested-fork-handler").
					OnFailure("comp-step-1b", "nested-compensation").
					Fork("inner-fork-2",
						func(ib *Builder) {
							ib.Step("step-1b-1", "nested-fork-handler").
								OnFailure("comp-step-1b-1", "nested-compensation")
						},
						func(ib *Builder) {
							ib.Step("step-1b-2", "nested-fork-handler"). // This one will FAIL
													OnFailure("comp-step-1b-2", "nested-compensation")
						},
					).
					Join("inner-join-2", JoinStrategyAll)
			},
		).
		Join("outer-join", JoinStrategyAll).
		Build()

	require.NoError(t, err)
	err = engine.RegisterWorkflow(ctx, workflowDef)
	require.NoError(t, err)

	input := json.RawMessage(`{"test": "parallel-nested-forks-2"}`)
	instanceID, err := engine.Start(ctx, "parallel-nested-forks-test-2-v1", input)
	require.NoError(t, err)

	// Process workflow
	for i := 0; i < 100; i++ {
		empty, err := engine.ExecuteNext(ctx, "worker1")
		require.NoError(t, err)
		if empty {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	status, err := engine.GetStatus(ctx, instanceID)
	require.NoError(t, err)

	steps, err := engine.GetSteps(ctx, instanceID)
	require.NoError(t, err)

	stepStatuses := make(map[string]StepStatus)
	for _, step := range steps {
		stepStatuses[step.StepName] = step.Status
	}

	t.Logf("Final workflow status: %s", status)
	t.Logf("Step statuses: %v", stepStatuses)

	// Workflow should be failed
	assert.Equal(t, StatusFailed, status, "Workflow should be failed")

	// Check rollback invariant: all steps should be rolled_back or skipped (no completed steps)
	// No savepoint in this test
	assertRollbackInvariant(t, stepStatuses, "step-1b-2", nil)
}

// TestFindForkStepForStepInBranch tests the helper function
func TestFindForkStepForStepInBranch(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	store, txManager, cleanup := setupTestStore(t)
	t.Cleanup(cleanup)

	engine := NewEngine(nil,
		WithEngineStore(store),
		WithEngineTxManager(txManager),
	)
	defer func() { _ = engine.Shutdown(time.Second) }()

	handler := newNestedForkFailingHandler()
	engine.RegisterHandler(handler)

	// Build a workflow with nested forks
	workflowDef, err := NewBuilder("find-fork-test", 1).
		Step("start-step", "nested-fork-handler").
		Fork("outer-fork",
			func(b *Builder) {
				b.Step("outer-branch-1-step-1", "nested-fork-handler").
					Fork("inner-fork",
						func(ib *Builder) {
							ib.Step("inner-branch-1-step", "nested-fork-handler")
						},
						func(ib *Builder) {
							ib.Step("inner-branch-2-step", "nested-fork-handler")
						},
					).
					Join("inner-join", JoinStrategyAll)
			},
			func(b *Builder) {
				b.Step("outer-branch-2-step", "nested-fork-handler")
			},
		).
		Join("outer-join", JoinStrategyAll).
		Build()

	require.NoError(t, err)

	// Test finding fork for various steps
	tests := []struct {
		stepName     string
		expectedFork string
	}{
		{"start-step", ""},                      // Not in a fork
		{"outer-branch-1-step-1", "outer-fork"}, // Direct child of outer fork
		{"outer-branch-2-step", "outer-fork"},   // Direct child of outer fork
		{"inner-branch-1-step", "inner-fork"},   // Direct child of inner fork
		{"inner-branch-2-step", "inner-fork"},   // Direct child of inner fork
		{"outer-join", ""},                      // Join step at outer level, not in a branch
		{"inner-join", "outer-fork"},            // Inner join is within outer fork's branch
	}

	for _, tt := range tests {
		t.Run(tt.stepName, func(t *testing.T) {
			result := engine.findForkStepForStepInBranch(tt.stepName, workflowDef)
			assert.Equal(t, tt.expectedFork, result,
				"findForkStepForStepInBranch(%s) = %s, want %s", tt.stepName, result, tt.expectedFork)
		})
	}
}
