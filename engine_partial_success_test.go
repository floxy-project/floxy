package floxy

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type partialItemHandler struct {
	fail      map[string]bool
	failFirst map[string]bool
	sleep     map[string]time.Duration
}

func (h *partialItemHandler) Name() string { return "partial-item" }

func (h *partialItemHandler) Execute(_ context.Context, stepCtx StepContext, _ json.RawMessage) (json.RawMessage, error) {
	name := stepCtx.StepName()
	time.Sleep(h.sleep[name])
	if h.fail[name] || (h.failFirst[name] && stepCtx.RetryCount() == 0) {
		return nil, fmt.Errorf("item %s failed", name)
	}

	return json.Marshal(map[string]any{"item": name, "count": 1})
}

type partialCompensateHandler struct {
	calls atomic.Int32
}

func (h *partialCompensateHandler) Name() string { return "partial-compensate" }

func (h *partialCompensateHandler) Execute(context.Context, StepContext, json.RawMessage) (json.RawMessage, error) {
	h.calls.Add(1)

	return json.RawMessage(`{}`), nil
}

func runPartialWorkflow(
	t *testing.T,
	def *WorkflowDefinition,
	handlers ...StepHandler,
) (*MemoryStore, *Engine, *StartAwaitResult) {
	t.Helper()

	store := NewMemoryStore()
	engine := NewEngine(nil,
		WithEngineStore(store),
		WithEngineTxManager(NewMemoryTxManager()),
	)
	t.Cleanup(func() { _ = engine.Shutdown() })

	for _, h := range handlers {
		engine.RegisterHandler(h)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)

	require.NoError(t, engine.RegisterWorkflow(ctx, def))

	pool := NewWorkerPool(engine, 3, 20*time.Millisecond)
	pool.Start(ctx)
	t.Cleanup(pool.Stop)

	res, err := engine.StartAwait(ctx, def.ID, json.RawMessage(`{"count":1}`))
	require.NoError(t, err)

	return store, engine, res
}

func partialStepsByName(t *testing.T, store *MemoryStore, instanceID int64) map[string]WorkflowStep {
	t.Helper()

	steps, err := store.GetStepsByInstance(context.Background(), instanceID)
	require.NoError(t, err)

	out := make(map[string]WorkflowStep, len(steps))
	for _, s := range steps {
		out[s.StepName] = s
	}

	return out
}

func partialJoinOutput(t *testing.T, step WorkflowStep) map[string]any {
	t.Helper()

	var out map[string]any
	require.NoError(t, json.Unmarshal(step.Output, &out))

	return out
}

func TestPartialSuccess_BranchFailureKeepsSiblings(t *testing.T) {
	def, err := NewBuilder("batch", 1,
		WithBuilderMaxRetries(0),
		WithFailurePolicy(FailurePolicyPartialSuccess),
	).
		Fork("process",
			func(b *Builder) {
				b.Step("item1", "partial-item").
					OnFailure("item1-comp", "partial-compensate", WithStepMaxRetries(1))
			},
			func(b *Builder) { b.Step("item2", "partial-item") },
			func(b *Builder) { b.Step("item3", "partial-item") },
		).
		Join("collect", JoinStrategyAll).
		Then("report", "partial-item").
		Build()
	require.NoError(t, err)

	comp := &partialCompensateHandler{}
	store, engine, res := runPartialWorkflow(t, def,
		&partialItemHandler{fail: map[string]bool{"item2": true}},
		comp,
	)

	require.Equal(t, StatusCompletedWithErrors, res.Status)
	require.NotNil(t, res.Error)
	assert.Contains(t, *res.Error, "item2")

	steps := partialStepsByName(t, store, res.InstanceID)
	assert.Equal(t, StepStatusCompleted, steps["item1"].Status)
	assert.Equal(t, StepStatusFailed, steps["item2"].Status)
	assert.Equal(t, StepStatusCompleted, steps["item3"].Status)
	assert.Equal(t, StepStatusCompleted, steps["collect"].Status)
	assert.Equal(t, StepStatusCompleted, steps["report"].Status)
	assert.Equal(t, int32(0), comp.calls.Load())

	joinOut := partialJoinOutput(t, steps["collect"])
	assert.Equal(t, string(StatusCompletedWithErrors), joinOut[KeyStatus])
	assert.ElementsMatch(t, []any{"item2"}, joinOut[KeyFailed])
	outputs, ok := joinOut[KeyOutputs].(map[string]any)
	require.True(t, ok)
	assert.Contains(t, outputs, "item1")
	assert.Contains(t, outputs, "item3")
	assert.NotContains(t, outputs, "item2")

	require.Error(t, engine.CancelWorkflow(context.Background(), res.InstanceID, "test", "late"))
}

func TestPartialSuccess_MidBranchFailureReleasesJoin(t *testing.T) {
	def, err := NewBuilder("batch-mid", 1,
		WithBuilderMaxRetries(0),
		WithFailurePolicy(FailurePolicyPartialSuccess),
	).
		Fork("process",
			func(b *Builder) { b.Step("item1", "partial-item") },
			func(b *Builder) {
				b.Step("item2a", "partial-item").
					Then("item2b", "partial-item")
			},
		).
		Join("collect", JoinStrategyAll).
		Build()
	require.NoError(t, err)

	store, _, res := runPartialWorkflow(t, def,
		&partialItemHandler{fail: map[string]bool{"item2a": true}},
	)

	require.Equal(t, StatusCompletedWithErrors, res.Status)

	steps := partialStepsByName(t, store, res.InstanceID)
	assert.Equal(t, StepStatusCompleted, steps["item1"].Status)
	assert.Equal(t, StepStatusFailed, steps["item2a"].Status)
	assert.NotContains(t, steps, "item2b")
	require.Contains(t, steps, "collect")
	assert.Equal(t, StepStatusCompleted, steps["collect"].Status)

	joinOut := partialJoinOutput(t, steps["collect"])
	assert.ElementsMatch(t, []any{"item2a"}, joinOut[KeyFailed])
}

func TestPartialSuccess_FailureInsideConditionBranch(t *testing.T) {
	for _, failing := range []string{"item2a", "item2-then"} {
		t.Run(failing, func(t *testing.T) {
			def, err := NewBuilder("batch-cond-"+failing, 1,
				WithBuilderMaxRetries(0),
				WithFailurePolicy(FailurePolicyPartialSuccess),
			).
				Fork("process",
					func(b *Builder) { b.Step("item1", "partial-item") },
					func(b *Builder) {
						b.Step("item2a", "partial-item").
							Condition("check", "{{ gt .count 0 }}", func(e *Builder) {
								e.Step("item2-else", "partial-item")
							}).
							Then("item2-then", "partial-item")
					},
				).
				Join("collect", JoinStrategyAll).
				Build()
			require.NoError(t, err)

			store, _, res := runPartialWorkflow(t, def,
				&partialItemHandler{fail: map[string]bool{failing: true}},
			)

			require.Equal(t, StatusCompletedWithErrors, res.Status)

			steps := partialStepsByName(t, store, res.InstanceID)
			assert.Equal(t, StepStatusCompleted, steps["item1"].Status)
			assert.Equal(t, StepStatusFailed, steps[failing].Status)
			require.Contains(t, steps, "collect")
			assert.Equal(t, StepStatusCompleted, steps["collect"].Status)

			joinOut := partialJoinOutput(t, steps["collect"])
			assert.ElementsMatch(t, []any{failing}, joinOut[KeyFailed])
		})
	}
}

func TestPartialSuccess_RetriesSucceed(t *testing.T) {
	def, err := NewBuilder("batch-retry", 1,
		WithBuilderMaxRetries(0),
		WithFailurePolicy(FailurePolicyPartialSuccess),
	).
		Fork("process",
			func(b *Builder) {
				b.Step("item1", "partial-item").
					OnFailure("item1-comp", "partial-compensate", WithStepMaxRetries(1))
			},
			func(b *Builder) {
				b.Step("item2", "partial-item",
					WithStepMaxRetries(2),
					WithStepDelay(300*time.Millisecond),
				)
			},
		).
		Join("collect", JoinStrategyAll).
		Build()
	require.NoError(t, err)

	comp := &partialCompensateHandler{}
	store, _, res := runPartialWorkflow(t, def,
		&partialItemHandler{
			failFirst: map[string]bool{"item2": true},
			sleep:     map[string]time.Duration{"item1": 450 * time.Millisecond},
		},
		comp,
	)

	require.Equal(t, StatusCompleted, res.Status)
	assert.Nil(t, res.Error)
	assert.Equal(t, int32(0), comp.calls.Load())

	steps := partialStepsByName(t, store, res.InstanceID)
	assert.Equal(t, StepStatusCompleted, steps["item2"].Status)
}

func TestPartialSuccess_FailureOutsideBranch(t *testing.T) {
	def, err := NewBuilder("batch-seq", 1,
		WithBuilderMaxRetries(0),
		WithFailurePolicy(FailurePolicyPartialSuccess),
	).
		Step("prepare", "partial-item").
		OnFailure("prepare-comp", "partial-compensate", WithStepMaxRetries(1)).
		Then("broken", "partial-item").
		Then("never", "partial-item").
		Build()
	require.NoError(t, err)

	comp := &partialCompensateHandler{}
	store, _, res := runPartialWorkflow(t, def,
		&partialItemHandler{fail: map[string]bool{"broken": true}},
		comp,
	)

	require.Equal(t, StatusFailed, res.Status)
	assert.Equal(t, int32(0), comp.calls.Load())

	steps := partialStepsByName(t, store, res.InstanceID)
	assert.Equal(t, StepStatusCompleted, steps["prepare"].Status)
	assert.Equal(t, StepStatusFailed, steps["broken"].Status)
	assert.NotContains(t, steps, "never")
}
