package floxy

import (
	"context"
	"encoding/json"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Handlers are reused from engine_with_in_memory_store_test.go:
// - ValidateHandler
// - ProcessPaymentHandler
// - SendNotificationHandler
// - UpdateInventoryHandler
// - SkipInventoryHandler
// - FinalizeOrderHandler
// - ReserveResourceHandler
// - ProcessResourceHandler
// - ReleaseResourceHandler
// - CleanupResourceHandler

func newSQLiteStoreForTest(t *testing.T) *SQLiteStore {
	store, err := NewSQLiteInMemoryStore()
	require.NoError(t, err)
	return store
}

func TestSQLiteStoreWorkflowSimple(t *testing.T) {
	ctx := context.Background()
	store := newSQLiteStoreForTest(t)
	// Use a simple no-op tx manager
	txManager := NewMemoryTxManager()

	engine := NewEngine(nil,
		WithEngineStore(store),
		WithEngineTxManager(txManager),
	)
	defer engine.Shutdown()

	engine.RegisterHandler(&ValidateHandler{})
	engine.RegisterHandler(&ProcessPaymentHandler{})
	engine.RegisterHandler(&FinalizeOrderHandler{})

	workflowDef, err := NewBuilder("order-simple", 1).
		Step("validate-order", "validate").
		Step("process-payment", "process-payment").
		Then("finalize-order", "finalize-order").
		Build()
	require.NoError(t, err)

	require.NoError(t, engine.RegisterWorkflow(ctx, workflowDef))

	workerPool := NewWorkerPool(engine, 2, 25*time.Millisecond)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	workerPool.Start(ctx)

	input := json.RawMessage(`{"order_id":"1","amount":10.5}`)
	instanceID, err := engine.Start(ctx, workflowDef.ID, input)
	require.NoError(t, err)

	time.Sleep(time.Second)
	workerPool.Stop()
	cancel()

	inst, err := store.GetInstance(context.Background(), instanceID)
	require.NoError(t, err)
	assert.Equal(t, StatusCompleted, inst.Status)

	steps, err := store.GetStepsByInstance(context.Background(), instanceID)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(steps), 3)
}

func TestSQLiteStoreQueueOperations(t *testing.T) {
	ctx := context.Background()
	store := newSQLiteStoreForTest(t)

	stepID := int64(10)
	wf, err := NewBuilder("sqlite-queue-test", 1).Step("start", "handler").Build()
	require.NoError(t, err)
	require.NoError(t, store.SaveWorkflowDefinition(ctx, wf))
	instance, err := store.CreateInstance(ctx, wf.ID, nil)
	require.NoError(t, err)
	// enqueue
	require.NoError(t, store.EnqueueStep(ctx, instance.ID, &stepID, PriorityNormal, 0))
	// dequeue
	item, err := store.DequeueStep(ctx, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, item)
	assert.Equal(t, instance.ID, item.InstanceID)
	require.NotNil(t, item.StepID)
	assert.Equal(t, stepID, *item.StepID)
	// remove
	require.NoError(t, store.RemoveFromQueue(ctx, item.ID))
	// next dequeue returns nil
	item, err = store.DequeueStep(ctx, "worker-1")
	require.NoError(t, err)
	assert.Nil(t, item)
}

func TestSQLiteStoreEvents(t *testing.T) {
	ctx := context.Background()
	store := newSQLiteStoreForTest(t)

	instanceID := int64(1)
	stepID := int64(10)
	require.NoError(t, store.LogEvent(ctx, instanceID, &stepID, "test-event", map[string]any{"k": "v"}))
	events, err := store.GetWorkflowEvents(ctx, instanceID)
	require.NoError(t, err)
	found := false
	for _, ev := range events {
		if ev.EventType == "test-event" {
			found = true
			break
		}
	}
	assert.True(t, found, "should find test-event among events")
}

func TestSQLiteStoreRollbackWithCompensation(t *testing.T) {
	ctx := context.Background()
	store := newSQLiteStoreForTest(t)
	txManager := NewMemoryTxManager()

	engine := NewEngine(nil,
		WithEngineStore(store),
		WithEngineTxManager(txManager),
	)
	defer engine.Shutdown()

	engine.RegisterHandler(&ReserveResourceHandler{})
	engine.RegisterHandler(&ProcessResourceHandler{}) // this one fails to trigger compensation
	engine.RegisterHandler(&ReleaseResourceHandler{})
	engine.RegisterHandler(&CleanupResourceHandler{})

	workflowDef, err := NewBuilder("resource-workflow", 1).
		Step("reserve-resource", "reserve-resource").
		OnFailure("release-resource", "release-resource", WithStepMaxRetries(1)).
		Step("process-resource", "process-resource", WithStepMaxRetries(0)).
		Build()
	require.NoError(t, err)

	require.NoError(t, engine.RegisterWorkflow(ctx, workflowDef))

	workerPool := NewWorkerPool(engine, 2, 25*time.Millisecond)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	workerPool.Start(ctx)

	input := json.RawMessage(`{"resource_id": "RES-1"}`)
	instanceID, err := engine.Start(ctx, workflowDef.ID, input)
	require.NoError(t, err)

	time.Sleep(3 * time.Second)
	workerPool.Stop()
	cancel()

	inst, err := store.GetInstance(context.Background(), instanceID)
	require.NoError(t, err)
	assert.Equal(t, StatusFailed, inst.Status)

	steps, err := store.GetStepsByInstance(context.Background(), instanceID)
	require.NoError(t, err)
	// reserve should be rolled back by compensation; process should be rolled back on failure
	var reserve, process *WorkflowStep
	for i := range steps {
		if steps[i].StepName == "reserve-resource" {
			reserve = &steps[i]
		}
		if steps[i].StepName == "process-resource" {
			process = &steps[i]
		}
	}
	require.NotNil(t, reserve)
	assert.Equal(t, StepStatusRolledBack, reserve.Status)
	require.NotNil(t, process)
	assert.Equal(t, StepStatusRolledBack, process.Status)
}

func TestSQLiteStorePersistence(t *testing.T) {
	ctx := context.Background()

	// Create a temporary file for the database
	tmpFile := t.TempDir() + "/test.db"

	// Create a persistent SQLite store
	store, err := NewSQLiteStore(tmpFile)
	require.NoError(t, err)
	defer store.Close()

	// Create a workflow definition
	workflowDef := &WorkflowDefinition{
		ID:      "test-workflow-1",
		Name:    "test-workflow",
		Version: 1,
		Definition: GraphDefinition{
			Steps: map[string]*StepDefinition{
				"step1": {Type: "test", Name: "step1"},
			},
		},
	}

	// Save workflow definition
	err = store.SaveWorkflowDefinition(ctx, workflowDef)
	require.NoError(t, err)

	// Create an instance
	input := json.RawMessage(`{"test": "data"}`)
	instance, err := store.CreateInstance(ctx, workflowDef.ID, input)
	require.NoError(t, err)
	require.NotNil(t, instance)

	instanceID := instance.ID

	// Close the store
	err = store.Close()
	require.NoError(t, err)

	// Reopen the store with the same file
	store2, err := NewSQLiteStore(tmpFile)
	require.NoError(t, err)
	defer store2.Close()

	// Verify that the workflow definition still exists
	def, err := store2.GetWorkflowDefinition(ctx, workflowDef.ID)
	require.NoError(t, err)
	require.NotNil(t, def)
	assert.Equal(t, workflowDef.ID, def.ID)
	assert.Equal(t, workflowDef.Name, def.Name)

	// Verify that the instance still exists
	inst, err := store2.GetInstance(ctx, instanceID)
	require.NoError(t, err)
	require.NotNil(t, inst)
	assert.Equal(t, instanceID, inst.ID)
	assert.Equal(t, workflowDef.ID, inst.WorkflowID)
}

func TestSQLiteStoreAgingRateValidation(t *testing.T) {
	ctx := context.Background()
	store := newSQLiteStoreForTest(t)

	// Test NaN - should be clamped to 0.0
	store.SetAgingRate(math.NaN())
	store.SetAgingEnabled(true)
	// Should not cause SQL error when dequeuing
	item, err := store.DequeueStep(ctx, "worker-1")
	require.NoError(t, err)
	assert.Nil(t, item) // No items in queue, but no SQL error

	// Test positive infinity - should be clamped to 0.0
	store.SetAgingRate(math.Inf(1))
	item, err = store.DequeueStep(ctx, "worker-1")
	require.NoError(t, err)
	assert.Nil(t, item)

	// Test negative infinity - should be clamped to 0.0
	store.SetAgingRate(math.Inf(-1))
	item, err = store.DequeueStep(ctx, "worker-1")
	require.NoError(t, err)
	assert.Nil(t, item)

	// Test negative value - should be clamped to 0.0
	store.SetAgingRate(-10.0)
	item, err = store.DequeueStep(ctx, "worker-1")
	require.NoError(t, err)
	assert.Nil(t, item)

	// Test value above maximum - should be clamped to MaxAgingRate (100.0)
	store.SetAgingRate(1000.0)
	item, err = store.DequeueStep(ctx, "worker-1")
	require.NoError(t, err)
	assert.Nil(t, item)

	// Test valid values - should work without clamping
	store.SetAgingRate(0.5)
	item, err = store.DequeueStep(ctx, "worker-1")
	require.NoError(t, err)
	assert.Nil(t, item)

	store.SetAgingRate(50.0)
	item, err = store.DequeueStep(ctx, "worker-1")
	require.NoError(t, err)
	assert.Nil(t, item)

	store.SetAgingRate(100.0)
	item, err = store.DequeueStep(ctx, "worker-1")
	require.NoError(t, err)
	assert.Nil(t, item)

	// Test that aging actually works with valid values
	store.SetAgingRate(0.5)
	stepID := int64(10)
	wf, err := NewBuilder("sqlite-aging-test", 1).Step("start", "handler").Build()
	require.NoError(t, err)
	require.NoError(t, store.SaveWorkflowDefinition(ctx, wf))
	instance, err := store.CreateInstance(ctx, wf.ID, nil)
	require.NoError(t, err)
	require.NoError(t, store.EnqueueStep(ctx, instance.ID, &stepID, PriorityNormal, 0))

	// Dequeue should work with aging enabled
	item, err = store.DequeueStep(ctx, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, item)
	assert.Equal(t, instance.ID, item.InstanceID)
}
