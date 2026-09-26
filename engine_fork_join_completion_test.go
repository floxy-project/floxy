package floxy

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type delayedCreateStepStore struct {
	*MemoryStore
	delays map[string]time.Duration
}

func (s *delayedCreateStepStore) CreateStep(ctx context.Context, step *WorkflowStep) error {
	time.Sleep(s.delays[step.StepName])

	return s.MemoryStore.CreateStep(ctx, step)
}

type delayedCreateJoinStateStore struct {
	*MemoryStore
	delay time.Duration
}

func (s *delayedCreateJoinStateStore) CreateJoinState(
	ctx context.Context,
	instanceID int64,
	joinStepName string,
	waitingFor []string,
	strategy JoinStrategy,
) error {
	time.Sleep(s.delay)

	return s.MemoryStore.CreateJoinState(ctx, instanceID, joinStepName, waitingFor, strategy)
}

func conditionInForkDefinition(t *testing.T, name string, opts ...BuilderOption) *WorkflowDefinition {
	t.Helper()

	def, err := NewBuilder(name, 1, append([]BuilderOption{WithBuilderMaxRetries(0)}, opts...)...).
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

	return def
}

func delayedItem2ThenStore() *delayedCreateStepStore {
	return &delayedCreateStepStore{
		MemoryStore: NewMemoryStore(),
		delays:      map[string]time.Duration{"item2-then": 400 * time.Millisecond},
	}
}

func TestForkJoin_BranchTerminalDoesNotCompleteWorkflowBeforeJoin_Saga(t *testing.T) {
	store := delayedItem2ThenStore()
	def := conditionInForkDefinition(t, "fork-join-early-saga")

	_, res := runWorkflowOnStore(t, store, def,
		&partialItemHandler{sleep: map[string]time.Duration{"item1": 150 * time.Millisecond}},
	)

	require.Equal(t, StatusCompleted, res.Status)

	steps := partialStepsByName(t, store, res.InstanceID)
	assert.Equal(t, StepStatusCompleted, steps["item1"].Status)
	assert.Equal(t, StepStatusCompleted, steps["item2-then"].Status)
	require.Contains(t, steps, "collect")
	assert.Equal(t, StepStatusCompleted, steps["collect"].Status)
}

func TestForkJoin_BranchTerminalDoesNotCompleteWorkflowBeforeJoin_PartialSuccess(t *testing.T) {
	store := delayedItem2ThenStore()
	def := conditionInForkDefinition(t, "fork-join-early-partial", WithFailurePolicy(FailurePolicyPartialSuccess))

	_, res := runWorkflowOnStore(t, store, def,
		&partialItemHandler{
			fail:  map[string]bool{"item2-then": true},
			sleep: map[string]time.Duration{"item1": 150 * time.Millisecond},
		},
	)

	require.Equal(t, StatusCompletedWithErrors, res.Status)

	steps := partialStepsByName(t, store, res.InstanceID)
	assert.Equal(t, StepStatusFailed, steps["item2-then"].Status)
	require.Contains(t, steps, "collect")
	assert.Equal(t, StepStatusCompleted, steps["collect"].Status)
}

func TestForkJoin_ForkWithoutJoinCompletesOnBranchTerminals(t *testing.T) {
	def, err := NewBuilder("fork-no-join", 1, WithBuilderMaxRetries(0)).
		Fork("process",
			func(b *Builder) { b.Step("item1", "partial-item") },
			func(b *Builder) { b.Step("item2", "partial-item") },
		).
		Build()
	require.NoError(t, err)

	store, _, res := runPartialWorkflow(t, def,
		&partialItemHandler{sleep: map[string]time.Duration{"item2": 100 * time.Millisecond}},
	)

	require.Equal(t, StatusCompleted, res.Status)

	steps := partialStepsByName(t, store, res.InstanceID)
	assert.Equal(t, StepStatusCompleted, steps["item1"].Status)
	assert.Equal(t, StepStatusCompleted, steps["item2"].Status)
}

func TestForkJoin_NestedJoinInsideForkWithoutJoinCompletes(t *testing.T) {
	def, err := NewBuilder("fork-nested-no-outer-join", 1, WithBuilderMaxRetries(0)).
		Fork("outer",
			func(b *Builder) { b.Step("item1", "partial-item") },
			func(b *Builder) {
				b.Step("item2", "partial-item").
					Fork("inner",
						func(ib *Builder) { ib.Step("inner1", "partial-item") },
						func(ib *Builder) { ib.Step("inner2", "partial-item") },
					).
					Join("inner-join", JoinStrategyAll)
			},
		).
		Build()
	require.NoError(t, err)

	store, _, res := runPartialWorkflow(t, def,
		&partialItemHandler{sleep: map[string]time.Duration{"item1": 50 * time.Millisecond}},
	)

	require.Equal(t, StatusCompleted, res.Status)

	steps := partialStepsByName(t, store, res.InstanceID)
	assert.Equal(t, StepStatusCompleted, steps["item1"].Status)
	assert.Equal(t, StepStatusCompleted, steps["inner1"].Status)
	assert.Equal(t, StepStatusCompleted, steps["inner2"].Status)
	assert.Equal(t, StepStatusCompleted, steps["inner-join"].Status)
}

func TestForkJoin_SiblingCompletedAfterFailureIsCompensated_Saga(t *testing.T) {
	def, err := NewBuilder("fork-join-late-sibling", 1, WithBuilderMaxRetries(0)).
		Fork("process",
			func(b *Builder) {
				b.Step("item1", "partial-item").
					OnFailure("item1-comp", "partial-compensate", WithStepMaxRetries(1))
			},
			func(b *Builder) { b.Step("item2", "partial-item") },
		).
		Join("collect", JoinStrategyAll).
		Build()
	require.NoError(t, err)

	comp := &partialCompensateHandler{}
	store, _, res := runPartialWorkflow(t, def,
		&partialItemHandler{
			fail:  map[string]bool{"item2": true},
			sleep: map[string]time.Duration{"item1": 300 * time.Millisecond},
		},
		comp,
	)

	require.Equal(t, StatusFailed, res.Status)
	assert.Eventually(t, func() bool { return comp.calls.Load() == 1 }, 5*time.Second, 50*time.Millisecond)
	assert.Eventually(t, func() bool {
		return partialStepsByName(t, store, res.InstanceID)["item1"].Status == StepStatusRolledBack
	}, 5*time.Second, 50*time.Millisecond)
}

func TestForkJoin_JoinStateCreatedBeforeBranchesRun(t *testing.T) {
	for _, policy := range []FailurePolicy{FailurePolicySaga, FailurePolicyPartialSuccess} {
		t.Run(string(policy), func(t *testing.T) {
			store := &delayedCreateJoinStateStore{MemoryStore: NewMemoryStore(), delay: 200 * time.Millisecond}
			def := conditionInForkDefinition(t, "fork-join-state-"+string(policy), WithFailurePolicy(policy))

			_, res := runWorkflowOnStore(t, store, def, &partialItemHandler{})

			require.Equal(t, StatusCompleted, res.Status)

			steps := partialStepsByName(t, store, res.InstanceID)
			require.Contains(t, steps, "collect")
			assert.Equal(t, StepStatusCompleted, steps["collect"].Status)

			joinState, err := store.GetJoinState(context.Background(), res.InstanceID, "collect")
			require.NoError(t, err)
			assert.ElementsMatch(t, []string{"item1", "item2-then"}, joinState.WaitingFor)
		})
	}
}
