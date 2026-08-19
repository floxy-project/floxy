package floxy

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMemoryStoreDequeueStep_LocksQueueItem(t *testing.T) {
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
	require.NotNil(t, item.LockedUntil)
	require.True(t, item.LockedUntil.After(time.Now()))

	running, err := store.GetInstance(ctx, instance.ID)
	require.NoError(t, err)
	require.Equal(t, StatusRunning, running.Status)

	item, err = store.DequeueStep(ctx, "worker-2")
	require.NoError(t, err)
	require.Nil(t, item)
}

func TestMemoryStoreDequeueStep_AllowsExpiredQueueLock(t *testing.T) {
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
	store.queue[firstItem.ID].LockedUntil = &expired
	store.mu.Unlock()

	secondItem, err := store.DequeueStep(ctx, "worker-2")
	require.NoError(t, err)
	require.NotNil(t, secondItem)
	require.Equal(t, firstItem.ID, secondItem.ID)
}

func TestMemoryStoreDequeueStep_AllowsParallelQueueItemsForSameInstance(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	workflow, err := NewBuilder("parallel-lock", 1, WithWorkflowLockTimeout(time.Minute)).
		Step("start", "handler").
		Build()
	require.NoError(t, err)
	require.NoError(t, store.SaveWorkflowDefinition(ctx, workflow))
	instance, err := store.CreateInstance(ctx, workflow.ID, nil)
	require.NoError(t, err)

	stepID1 := int64(101)
	stepID2 := int64(102)
	require.NoError(t, store.EnqueueStep(ctx, instance.ID, &stepID1, PriorityNormal, 0))
	require.NoError(t, store.EnqueueStep(ctx, instance.ID, &stepID2, PriorityNormal, 0))

	item1, err := store.DequeueStep(ctx, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, item1)

	item2, err := store.DequeueStep(ctx, "worker-2")
	require.NoError(t, err)
	require.NotNil(t, item2)
	require.NotEqual(t, item1.ID, item2.ID)
	require.Equal(t, instance.ID, item1.InstanceID)
	require.Equal(t, instance.ID, item2.InstanceID)
}

func TestSQLiteStoreDequeueStep_AllowsParallelQueueItemsForSameInstance(t *testing.T) {
	ctx := context.Background()
	store, err := NewSQLiteInMemoryStore()
	require.NoError(t, err)
	defer store.Close()

	workflow, err := NewBuilder("sqlite-parallel-lock", 1, WithWorkflowLockTimeout(time.Minute)).
		Step("start", "handler").
		Build()
	require.NoError(t, err)
	require.NoError(t, store.SaveWorkflowDefinition(ctx, workflow))
	instance, err := store.CreateInstance(ctx, workflow.ID, nil)
	require.NoError(t, err)

	stepID1 := int64(101)
	stepID2 := int64(102)
	require.NoError(t, store.EnqueueStep(ctx, instance.ID, &stepID1, PriorityNormal, 0))
	require.NoError(t, store.EnqueueStep(ctx, instance.ID, &stepID2, PriorityNormal, 0))

	item1, err := store.DequeueStep(ctx, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, item1)
	require.NotNil(t, item1.LockedUntil)

	item2, err := store.DequeueStep(ctx, "worker-2")
	require.NoError(t, err)
	require.NotNil(t, item2)
	require.NotNil(t, item2.LockedUntil)
	require.NotEqual(t, item1.ID, item2.ID)
	require.Equal(t, instance.ID, item1.InstanceID)
	require.Equal(t, instance.ID, item2.InstanceID)
}
