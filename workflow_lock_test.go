package floxy

import (
	"context"
	"encoding/json"
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
	require.NotNil(t, item.LockToken)
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
	require.NotNil(t, firstItem.LockToken)

	store.mu.Lock()
	expired := time.Now().Add(-time.Second)
	store.queue[firstItem.ID].LockedUntil = &expired
	store.mu.Unlock()

	secondItem, err := store.DequeueStep(ctx, "worker-2")
	require.NoError(t, err)
	require.NotNil(t, secondItem)
	require.NotNil(t, secondItem.LockToken)
	require.Equal(t, firstItem.ID, secondItem.ID)
	require.NotEqual(t, *firstItem.LockToken, *secondItem.LockToken)
}

func TestMemoryStoreExtendQueueItemLock_RequiresToken(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	workflow, err := NewBuilder("extend-lock", 1, WithWorkflowLockTimeout(30*time.Second)).
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
	require.NotNil(t, item.LockToken)
	require.NotNil(t, item.LockedUntil)

	extended, err := store.ExtendQueueItemLock(ctx, item.ID, "wrong-token", time.Minute)
	require.NoError(t, err)
	require.False(t, extended)

	extended, err = store.ExtendQueueItemLock(ctx, item.ID, *item.LockToken, time.Minute)
	require.NoError(t, err)
	require.True(t, extended)

	owned, err := store.QueueItemLockStillOwned(ctx, item.ID, *item.LockToken)
	require.NoError(t, err)
	require.True(t, owned)

	owned, err = store.QueueItemLockStillOwned(ctx, item.ID, "wrong-token")
	require.NoError(t, err)
	require.False(t, owned)
}

func TestEngineExecuteNext_ExtendsQueueLockWhileHandlerRuns(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	engine := NewEngine(nil,
		WithEngineStore(store),
		WithEngineTxManager(NewMemoryTxManager()),
	)
	defer engine.Shutdown()

	handler := &blockingLockHandler{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	engine.RegisterHandler(handler)

	workflow, err := NewBuilder("heartbeat-lock", 1, WithWorkflowLockTimeout(45*time.Millisecond)).
		Step("start", handler.Name()).
		Build()
	require.NoError(t, err)
	require.NoError(t, engine.RegisterWorkflow(ctx, workflow))

	_, err = engine.Start(ctx, workflow.ID, json.RawMessage(`{"ok":true}`))
	require.NoError(t, err)

	errCh := make(chan error, 1)
	go func() {
		_, err := engine.ExecuteNext(ctx, "worker-1")
		errCh <- err
	}()

	select {
	case <-handler.started:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}

	time.Sleep(120 * time.Millisecond)

	item, err := store.DequeueStep(ctx, "worker-2")
	require.NoError(t, err)
	require.Nil(t, item)

	close(handler.release)

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("ExecuteNext did not finish")
	}
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
	require.NotNil(t, item1.LockToken)

	item2, err := store.DequeueStep(ctx, "worker-2")
	require.NoError(t, err)
	require.NotNil(t, item2)
	require.NotNil(t, item2.LockToken)
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
	require.NotNil(t, item1.LockToken)

	item2, err := store.DequeueStep(ctx, "worker-2")
	require.NoError(t, err)
	require.NotNil(t, item2)
	require.NotNil(t, item2.LockedUntil)
	require.NotNil(t, item2.LockToken)
	require.NotEqual(t, item1.ID, item2.ID)
	require.Equal(t, instance.ID, item1.InstanceID)
	require.Equal(t, instance.ID, item2.InstanceID)
}

type blockingLockHandler struct {
	started chan struct{}
	release chan struct{}
}

func (h *blockingLockHandler) Name() string {
	return "blocking-lock"
}

func (h *blockingLockHandler) Execute(ctx context.Context, stepCtx StepContext, input json.RawMessage) (json.RawMessage, error) {
	close(h.started)

	select {
	case <-h.release:
		return input, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
