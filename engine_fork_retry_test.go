package floxy

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSaga_ForkBranchSucceedsAfterRetry(t *testing.T) {
	def, err := NewBuilder("saga-fork-retry", 1, WithBuilderMaxRetries(0)).
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
	assert.Equal(t, int32(0), comp.calls.Load())

	steps := partialStepsByName(t, store, res.InstanceID)
	assert.Equal(t, StepStatusCompleted, steps["item1"].Status)
	assert.Equal(t, StepStatusCompleted, steps["item2"].Status)
	assert.Equal(t, StepStatusCompleted, steps["collect"].Status)
}

func TestSaga_ForkBranchFailsAfterRetries(t *testing.T) {
	def, err := NewBuilder("saga-fork-retry-fail", 1, WithBuilderMaxRetries(0)).
		Fork("process",
			func(b *Builder) {
				b.Step("item1", "partial-item").
					OnFailure("item1-comp", "partial-compensate", WithStepMaxRetries(1))
			},
			func(b *Builder) {
				b.Step("item2", "partial-item",
					WithStepMaxRetries(1),
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
			fail:  map[string]bool{"item2": true},
			sleep: map[string]time.Duration{"item1": 450 * time.Millisecond},
		},
		comp,
	)

	require.Equal(t, StatusFailed, res.Status)
	assert.Eventually(t, func() bool { return comp.calls.Load() == 1 }, 5*time.Second, 50*time.Millisecond)

	assert.Eventually(t, func() bool {
		return partialStepsByName(t, store, res.InstanceID)["item1"].Status == StepStatusRolledBack
	}, 5*time.Second, 50*time.Millisecond)
}
