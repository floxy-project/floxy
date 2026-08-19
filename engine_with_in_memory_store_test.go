package floxy

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type ValidateHandler struct{}

func (h *ValidateHandler) Name() string {
	return "validate"
}

func (h *ValidateHandler) Execute(ctx context.Context, stepCtx StepContext, input json.RawMessage) (json.RawMessage, error) {
	var data map[string]any
	if err := json.Unmarshal(input, &data); err != nil {
		return nil, err
	}

	log.Printf("[%s] Validating order: %v", stepCtx.StepName(), data)

	data["validated"] = true
	data["validation_time"] = time.Now().Unix()

	return json.Marshal(data)
}

type ProcessPaymentHandler struct{}

func (h *ProcessPaymentHandler) Name() string {
	return "process-payment"
}

func (h *ProcessPaymentHandler) Execute(ctx context.Context, stepCtx StepContext, input json.RawMessage) (json.RawMessage, error) {
	var data map[string]any
	if err := json.Unmarshal(input, &data); err != nil {
		return nil, err
	}

	log.Printf("[%s] Processing payment for order: %.2f", stepCtx.StepName(), data["amount"].(float64))

	data["payment_processed"] = true
	data["payment_id"] = fmt.Sprintf("PAY-%d", time.Now().Unix())

	return json.Marshal(data)
}

type SendNotificationHandler struct{}

func (h *SendNotificationHandler) Name() string {
	return "send-notification"
}

func (h *SendNotificationHandler) Execute(ctx context.Context, stepCtx StepContext, input json.RawMessage) (json.RawMessage, error) {
	var data map[string]any
	if err := json.Unmarshal(input, &data); err != nil {
		return nil, err
	}

	log.Printf("[%s] Sending notification to: %s", stepCtx.StepName(), data["email"])

	data["notification_sent"] = true

	return json.Marshal(data)
}

type UpdateInventoryHandler struct{}

func (h *UpdateInventoryHandler) Name() string {
	return "update-inventory"
}

func (h *UpdateInventoryHandler) Execute(ctx context.Context, stepCtx StepContext, input json.RawMessage) (json.RawMessage, error) {
	var data map[string]any
	if err := json.Unmarshal(input, &data); err != nil {
		return nil, err
	}

	log.Printf("[%s] Updating inventory for product: %s", stepCtx.StepName(), data["product_id"])

	data["inventory_updated"] = true

	return json.Marshal(data)
}

type SkipInventoryHandler struct{}

func (h *SkipInventoryHandler) Name() string {
	return "skip-inventory"
}

func (h *SkipInventoryHandler) Execute(ctx context.Context, stepCtx StepContext, input json.RawMessage) (json.RawMessage, error) {
	var data map[string]any
	if err := json.Unmarshal(input, &data); err != nil {
		return nil, err
	}

	log.Printf("[%s] Skipping inventory update (digital product)", stepCtx.StepName())

	data["inventory_skipped"] = true

	return json.Marshal(data)
}

type FinalizeOrderHandler struct{}

func (h *FinalizeOrderHandler) Name() string {
	return "finalize-order"
}

func (h *FinalizeOrderHandler) Execute(ctx context.Context, stepCtx StepContext, input json.RawMessage) (json.RawMessage, error) {
	var data map[string]any
	if err := json.Unmarshal(input, &data); err != nil {
		return nil, err
	}

	log.Printf("[%s] Finalizing order", stepCtx.StepName())

	data["order_finalized"] = true
	data["order_id"] = fmt.Sprintf("ORD-%d", time.Now().Unix())

	return json.Marshal(data)
}

type ReserveResourceHandler struct{}

func (h *ReserveResourceHandler) Name() string {
	return "reserve-resource"
}

func (h *ReserveResourceHandler) Execute(ctx context.Context, stepCtx StepContext, input json.RawMessage) (json.RawMessage, error) {
	var data map[string]any
	if err := json.Unmarshal(input, &data); err != nil {
		return nil, err
	}

	resourceID := data["resource_id"].(string)
	result := map[string]any{
		"resource_id": resourceID,
		"reserved":    true,
		"reserved_at": time.Now().Unix(),
	}

	return json.Marshal(result)
}

type ProcessResourceHandler struct{}

func (h *ProcessResourceHandler) Name() string {
	return "process-resource"
}

func (h *ProcessResourceHandler) Execute(ctx context.Context, stepCtx StepContext, input json.RawMessage) (json.RawMessage, error) {
	return nil, assert.AnError
}

type ReleaseResourceHandler struct{}

func (h *ReleaseResourceHandler) Name() string {
	return "release-resource"
}

func (h *ReleaseResourceHandler) Execute(ctx context.Context, stepCtx StepContext, input json.RawMessage) (json.RawMessage, error) {
	var data map[string]any
	if err := json.Unmarshal(input, &data); err != nil {
		return nil, err
	}

	resourceID := data["resource_id"].(string)
	result := map[string]any{
		"resource_id": resourceID,
		"released":    true,
		"released_at": time.Now().Unix(),
	}

	return json.Marshal(result)
}

type CleanupResourceHandler struct{}

func (h *CleanupResourceHandler) Name() string {
	return "cleanup-resource"
}

func (h *CleanupResourceHandler) Execute(ctx context.Context, stepCtx StepContext, input json.RawMessage) (json.RawMessage, error) {
	var data map[string]any
	if err := json.Unmarshal(input, &data); err != nil {
		return nil, err
	}

	resourceID := data["resource_id"].(string)
	result := map[string]any{
		"resource_id": resourceID,
		"cleaned_up":  true,
		"cleaned_at":  time.Now().Unix(),
	}

	return json.Marshal(result)
}

func TestInMemoryStoreWorkflowWithConditionAndForkJoin(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	txManager := NewMemoryTxManager()

	engine := NewEngine(nil,
		WithEngineStore(store),
		WithEngineTxManager(txManager),
	)
	defer engine.Shutdown()

	engine.RegisterHandler(&ValidateHandler{})
	engine.RegisterHandler(&ProcessPaymentHandler{})
	engine.RegisterHandler(&SendNotificationHandler{})
	engine.RegisterHandler(&UpdateInventoryHandler{})
	engine.RegisterHandler(&SkipInventoryHandler{})
	engine.RegisterHandler(&FinalizeOrderHandler{})

	workflowDef, err := NewBuilder("order-processing", 1).
		Step("validate-order", "validate").
		Step("process-payment", "process-payment").
		Fork("parallel-processing", func(branch1 *Builder) {
			branch1.Step("send-email", "send-notification").
				Condition("check-product-type", "{{ eq .product_type \"physical\" }}", func(elseBranch *Builder) {
					elseBranch.Step("skip-inventory", "skip-inventory")
				}).
				Then("update-inventory", "update-inventory")
		}, func(branch2 *Builder) {
			branch2.Step("send-sms", "send-notification")
		}).
		Join("sync-branches", JoinStrategyAll).
		Then("finalize-order", "finalize-order").
		Build()
	require.NoError(t, err)

	err = engine.RegisterWorkflow(ctx, workflowDef)
	require.NoError(t, err)

	workerPool := NewWorkerPool(engine, 3, 50*time.Millisecond)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	workerPool.Start(ctx)

	input := json.RawMessage(`{"order_id": "12345", "amount": 99.99, "email": "customer@example.com", "product_id": "PROD-001", "product_type": "physical"}`)
	instanceID, err := engine.Start(ctx, workflowDef.ID, input)
	require.NoError(t, err)

	time.Sleep(1 * time.Second)

	workerPool.Stop()
	cancel()

	instance, err := store.GetInstance(ctx, instanceID)
	require.NoError(t, err)
	assert.Equal(t, StatusCompleted, instance.Status)

	var outputData map[string]any
	require.NoError(t, json.Unmarshal(instance.Output, &outputData))
	assert.NotNil(t, outputData["order_finalized"])
	assert.True(t, outputData["order_finalized"].(bool))

	steps, err := store.GetStepsByInstance(ctx, instanceID)
	require.NoError(t, err)

	stepNames := make(map[string]bool)
	for _, step := range steps {
		stepNames[step.StepName] = true
	}

	assert.True(t, stepNames["validate-order"])
	assert.True(t, stepNames["process-payment"])
	assert.True(t, stepNames["send-email"])
	assert.True(t, stepNames["check-product-type"])
	assert.True(t, stepNames["update-inventory"])
	assert.True(t, stepNames["send-sms"])
	assert.True(t, stepNames["sync-branches"])
	assert.True(t, stepNames["finalize-order"])

	forkStep := findStepByName(steps, "parallel-processing")
	require.NotNil(t, forkStep)
	assert.Equal(t, StepTypeFork, forkStep.StepType)

	joinStep := findStepByName(steps, "sync-branches")
	require.NotNil(t, joinStep)
	assert.Equal(t, StepTypeJoin, joinStep.StepType)

	conditionStep := findStepByName(steps, "check-product-type")
	require.NotNil(t, conditionStep)
	assert.Equal(t, StepTypeCondition, conditionStep.StepType)

	updateInventoryStep := findStepByName(steps, "update-inventory")
	require.NotNil(t, updateInventoryStep)
	assert.Equal(t, StepStatusCompleted, updateInventoryStep.Status)

	skipInventoryStep := findStepByName(steps, "skip-inventory")
	assert.Nil(t, skipInventoryStep, "skip-inventory should not execute for physical product")

	stats, err := store.GetSummaryStats(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint(1), stats.TotalWorkflows)
	assert.Equal(t, uint(1), stats.CompletedWorkflows)

	events, err := store.GetWorkflowEvents(ctx, instanceID)
	require.NoError(t, err)
	assert.Greater(t, len(events), 0)
}

func TestInMemoryStoreWorkflowConditionDigitalProduct(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	txManager := NewMemoryTxManager()

	engine := NewEngine(nil,
		WithEngineStore(store),
		WithEngineTxManager(txManager),
	)
	defer engine.Shutdown()

	engine.RegisterHandler(&ValidateHandler{})
	engine.RegisterHandler(&ProcessPaymentHandler{})
	engine.RegisterHandler(&SendNotificationHandler{})
	engine.RegisterHandler(&UpdateInventoryHandler{})
	engine.RegisterHandler(&SkipInventoryHandler{})
	engine.RegisterHandler(&FinalizeOrderHandler{})

	workflowDef, err := NewBuilder("order-processing", 1).
		Step("validate-order", "validate").
		Step("process-payment", "process-payment").
		Fork("parallel-processing", func(branch1 *Builder) {
			branch1.Step("send-email", "send-notification").
				Condition("check-product-type", "{{ eq .product_type \"physical\" }}", func(elseBranch *Builder) {
					elseBranch.Step("skip-inventory", "skip-inventory")
				}).
				Then("update-inventory", "update-inventory")
		}, func(branch2 *Builder) {
			branch2.Step("send-sms", "send-notification")
		}).
		Join("sync-branches", JoinStrategyAll).
		Then("finalize-order", "finalize-order").
		Build()
	require.NoError(t, err)

	err = engine.RegisterWorkflow(ctx, workflowDef)
	require.NoError(t, err)

	workerPool := NewWorkerPool(engine, 3, 50*time.Millisecond)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	workerPool.Start(ctx)

	input := json.RawMessage(`{"order_id": "12346", "amount": 29.99, "email": "customer2@example.com", "product_id": "PROD-002", "product_type": "digital"}`)
	instanceID, err := engine.Start(ctx, workflowDef.ID, input)
	require.NoError(t, err)

	time.Sleep(1 * time.Second)

	workerPool.Stop()
	cancel()

	instance, err := store.GetInstance(ctx, instanceID)
	require.NoError(t, err)
	assert.Equal(t, StatusCompleted, instance.Status)

	steps, err := store.GetStepsByInstance(ctx, instanceID)
	require.NoError(t, err)

	updateInventoryStep := findStepByName(steps, "update-inventory")
	assert.Nil(t, updateInventoryStep, "update-inventory should not execute for digital product")

	skipInventoryStep := findStepByName(steps, "skip-inventory")
	require.NotNil(t, skipInventoryStep)
	assert.Equal(t, StepStatusCompleted, skipInventoryStep.Status)
}

func findStepByName(steps []WorkflowStep, name string) *WorkflowStep {
	for i := range steps {
		if steps[i].StepName == name {
			return &steps[i]
		}
	}
	return nil
}

func TestInMemoryStoreMultipleInstances(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	txManager := NewMemoryTxManager()

	engine := NewEngine(nil,
		WithEngineStore(store),
		WithEngineTxManager(txManager),
	)
	defer engine.Shutdown()

	engine.RegisterHandler(&ValidateHandler{})
	engine.RegisterHandler(&ProcessPaymentHandler{})
	engine.RegisterHandler(&SendNotificationHandler{})
	engine.RegisterHandler(&UpdateInventoryHandler{})
	engine.RegisterHandler(&SkipInventoryHandler{})
	engine.RegisterHandler(&FinalizeOrderHandler{})

	workflowDef, err := NewBuilder("order-processing", 1).
		Step("validate-order", "validate").
		Step("process-payment", "process-payment").
		Fork("parallel-processing", func(branch1 *Builder) {
			branch1.Step("send-email", "send-notification").
				Condition("check-product-type", "{{ eq .product_type \"physical\" }}", func(elseBranch *Builder) {
					elseBranch.Step("skip-inventory", "skip-inventory")
				}).
				Then("update-inventory", "update-inventory")
		}, func(branch2 *Builder) {
			branch2.Step("send-sms", "send-notification")
		}).
		Join("sync-branches", JoinStrategyAll).
		Then("finalize-order", "finalize-order").
		Build()
	require.NoError(t, err)

	err = engine.RegisterWorkflow(ctx, workflowDef)
	require.NoError(t, err)

	workerPool := NewWorkerPool(engine, 3, 50*time.Millisecond)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	workerPool.Start(ctx)

	input1 := json.RawMessage(`{"order_id": "12345", "amount": 99.99, "email": "customer@example.com", "product_id": "PROD-001", "product_type": "physical"}`)
	instanceID1, err := engine.Start(ctx, workflowDef.ID, input1)
	require.NoError(t, err)

	input2 := json.RawMessage(`{"order_id": "12346", "amount": 29.99, "email": "customer2@example.com", "product_id": "PROD-002", "product_type": "digital"}`)
	instanceID2, err := engine.Start(ctx, workflowDef.ID, input2)
	require.NoError(t, err)

	time.Sleep(1 * time.Second)

	workerPool.Stop()
	cancel()

	instance1, err := store.GetInstance(ctx, instanceID1)
	require.NoError(t, err)
	assert.Equal(t, StatusCompleted, instance1.Status)

	instance2, err := store.GetInstance(ctx, instanceID2)
	require.NoError(t, err)
	assert.Equal(t, StatusCompleted, instance2.Status)

	allInstances, err := store.GetAllWorkflowInstances(ctx)
	require.NoError(t, err)
	assert.Len(t, allInstances, 2)

	workflowInstances, err := store.GetWorkflowInstances(ctx, workflowDef.ID)
	require.NoError(t, err)
	assert.Len(t, workflowInstances, 2)

	stats, err := store.GetSummaryStats(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint(2), stats.TotalWorkflows)
	assert.Equal(t, uint(2), stats.CompletedWorkflows)
}

func TestInMemoryStoreWorkflowDefinition(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	def := &WorkflowDefinition{
		ID:      "test-workflow",
		Name:    "test-workflow",
		Version: 1,
		Definition: GraphDefinition{
			Steps: map[string]*StepDefinition{
				"step1": {
					Name: "step1",
					Type: StepTypeTask,
				},
			},
			Start: "step1",
		},
	}

	err := store.SaveWorkflowDefinition(ctx, def)
	require.NoError(t, err)

	retrieved, err := store.GetWorkflowDefinition(ctx, def.ID)
	require.NoError(t, err)
	assert.Equal(t, def.ID, retrieved.ID)
	assert.Equal(t, def.Name, retrieved.Name)
	assert.Equal(t, def.Version, retrieved.Version)

	definitions, err := store.GetWorkflowDefinitions(ctx)
	require.NoError(t, err)
	assert.Len(t, definitions, 1)
}

func TestInMemoryStoreQueueOperations(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	stepID := int64(10)
	wf, err := NewBuilder("queue-test", 1).Step("start", "handler").Build()
	require.NoError(t, err)
	require.NoError(t, store.SaveWorkflowDefinition(ctx, wf))
	instance, err := store.CreateInstance(ctx, wf.ID, nil)
	require.NoError(t, err)

	err = store.EnqueueStep(ctx, instance.ID, &stepID, PriorityNormal, 0)
	require.NoError(t, err)

	item, err := store.DequeueStep(ctx, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, item)
	assert.Equal(t, instance.ID, item.InstanceID)
	assert.Equal(t, stepID, *item.StepID)

	err = store.RemoveFromQueue(ctx, item.ID)
	require.NoError(t, err)

	item, err = store.DequeueStep(ctx, "worker-1")
	require.NoError(t, err)
	assert.Nil(t, item)
}

func TestInMemoryStoreJoinState(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	instanceID := int64(1)
	joinStepName := "join-step"

	err := store.CreateJoinState(ctx, instanceID, joinStepName, []string{"step1", "step2"}, JoinStrategyAll)
	require.NoError(t, err)

	state, err := store.GetJoinState(ctx, instanceID, joinStepName)
	require.NoError(t, err)
	assert.Equal(t, instanceID, state.InstanceID)
	assert.Equal(t, joinStepName, state.JoinStepName)
	assert.Len(t, state.WaitingFor, 2)
	assert.False(t, state.IsReady)

	isReady, err := store.UpdateJoinState(ctx, instanceID, joinStepName, "step1", true)
	require.NoError(t, err)
	assert.False(t, isReady)

	isReady, err = store.UpdateJoinState(ctx, instanceID, joinStepName, "step2", true)
	require.NoError(t, err)
	assert.True(t, isReady)

	state, err = store.GetJoinState(ctx, instanceID, joinStepName)
	require.NoError(t, err)
	assert.True(t, state.IsReady)
	assert.Len(t, state.Completed, 2)
}

func TestInMemoryStoreEvents(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	instanceID := int64(1)
	stepID := int64(10)

	err := store.LogEvent(ctx, instanceID, &stepID, "test-event", map[string]string{"key": "value"})
	require.NoError(t, err)

	events, err := store.GetWorkflowEvents(ctx, instanceID)
	require.NoError(t, err)
	assert.Len(t, events, 1)
	assert.Equal(t, "test-event", events[0].EventType)
}

func TestInMemoryStoreRollbackWithCompensation(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	txManager := NewMemoryTxManager()

	engine := NewEngine(nil,
		WithEngineStore(store),
		WithEngineTxManager(txManager),
	)
	defer engine.Shutdown()

	engine.RegisterHandler(&ReserveResourceHandler{})
	engine.RegisterHandler(&ProcessResourceHandler{})
	engine.RegisterHandler(&ReleaseResourceHandler{})
	engine.RegisterHandler(&CleanupResourceHandler{})

	workflowDef, err := NewBuilder("resource-workflow", 1).
		Step("reserve-resource", "reserve-resource").
		OnFailure("release-resource", "release-resource", WithStepMaxRetries(1)).
		Step("process-resource", "process-resource").
		Build()
	require.NoError(t, err)

	err = engine.RegisterWorkflow(ctx, workflowDef)
	require.NoError(t, err)

	workerPool := NewWorkerPool(engine, 2, 50*time.Millisecond)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	workerPool.Start(ctx)

	input := json.RawMessage(`{"resource_id": "RES-001"}`)
	instanceID, err := engine.Start(ctx, workflowDef.ID, input)
	require.NoError(t, err)

	time.Sleep(3 * time.Second)

	workerPool.Stop()
	cancel()

	instance, err := store.GetInstance(ctx, instanceID)
	require.NoError(t, err)
	assert.Equal(t, StatusFailed, instance.Status)

	steps, err := store.GetStepsByInstance(ctx, instanceID)
	require.NoError(t, err)

	t.Logf("All steps:")
	for _, s := range steps {
		t.Logf("  Step: %s, Status: %s, Type: %s", s.StepName, s.Status, s.StepType)
	}

	reserveStep := findStepByName(steps, "reserve-resource")
	require.NotNil(t, reserveStep)
	assert.Equal(t, StepStatusRolledBack, reserveStep.Status, "reserve-resource should be rolled back after compensation")

	processStep := findStepByName(steps, "process-resource")
	require.NotNil(t, processStep)
	assert.Equal(t, StepStatusRolledBack, processStep.Status, "process-resource should be rolled back after workflow failure")

	releaseStep := findStepByName(steps, "release-resource")
	if releaseStep != nil {
		assert.Equal(t, StepStatusCompleted, releaseStep.Status, "release-resource compensation should complete successfully")
	}

	events, err := store.GetWorkflowEvents(ctx, instanceID)
	require.NoError(t, err)

	hasCompensationEvent := false
	for _, event := range events {
		if event.EventType == "step_completed" {
			var payload map[string]any
			if err := json.Unmarshal(event.Payload, &payload); err == nil {
				if reason, ok := payload["reason"].(string); ok && reason == "compensation_success" {
					hasCompensationEvent = true
					break
				}
			}
		}
	}
	assert.True(t, hasCompensationEvent, "Should have compensation success events")
}
