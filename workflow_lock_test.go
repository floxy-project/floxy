package floxy

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMemoryStoreDequeueStep_LocksWorkflowInstance(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	workflow, err := NewBuilder("locked", 1, WithWorkflowLockTimeout(time.Minute)).
		Step("start", "handler").
		Build()
	require.NoError(t, err)
	require.NoError(t, store.SaveWorkflowDefinition(ctx, workflow))
	instance, err := store.CreateInstance(ctx, workflow.ID, nil)
	require.NoError(t, err)
	require.NoError(t, store.EnqueueStep(ctx, instance.ID, nil, PriorityNormal, 0))

	item, err := store.DequeueStep(ctx, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, item)

	locked, err := store.GetInstance(ctx, instance.ID)
	require.NoError(t, err)
	require.Equal(t, StatusRunning, locked.Status)
	require.NotNil(t, locked.LockedUntil)
	require.True(t, locked.LockedUntil.After(time.Now()))

	item, err = store.DequeueStep(ctx, "worker-2")
	require.NoError(t, err)
	require.Nil(t, item)
}

func TestMemoryStoreDequeueStep_AllowsExpiredWorkflowLock(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	workflow, err := NewBuilder("expired-lock", 1, WithWorkflowLockTimeout(time.Minute)).
		Step("start", "handler").
		Build()
	require.NoError(t, err)
	require.NoError(t, store.SaveWorkflowDefinition(ctx, workflow))
	instance, err := store.CreateInstance(ctx, workflow.ID, nil)
	require.NoError(t, err)
	require.NoError(t, store.EnqueueStep(ctx, instance.ID, nil, PriorityNormal, 0))

	firstItem, err := store.DequeueStep(ctx, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, firstItem)

	store.mu.Lock()
	expired := time.Now().Add(-time.Second)
	store.instances[instance.ID].LockedUntil = &expired
	store.mu.Unlock()

	secondItem, err := store.DequeueStep(ctx, "worker-2")
	require.NoError(t, err)
	require.NotNil(t, secondItem)
	require.Equal(t, firstItem.ID, secondItem.ID)
}
