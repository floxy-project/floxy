package floxy

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestWithLongRunningSteps(t *testing.T) {
	mockTxManager := NewMockTxManager(t)
	mockStore := NewMockStore(t)
	mockStore.EXPECT().GetCancelRequest(mock.Anything, mock.Anything).Return(nil, ErrEntityNotFound).Maybe()

	engine := NewEngine(nil,
		WithEngineTxManager(mockTxManager),
		WithEngineStore(mockStore),
		WithLongRunningSteps(),
	)
	defer engine.Shutdown()

	assert.True(t, engine.longRunningSteps)
}

func TestEngine_ExecuteNext_LongRunning_EmptyQueue(t *testing.T) {
	mockTxManager := NewMockTxManager(t)
	mockStore := NewMockStore(t)
	mockStore.EXPECT().GetCancelRequest(mock.Anything, mock.Anything).Return(nil, ErrEntityNotFound).Maybe()

	engine := NewEngine(nil,
		WithEngineTxManager(mockTxManager),
		WithEngineStore(mockStore),
		WithLongRunningSteps(),
	)
	defer engine.Shutdown()

	mockTxManager.EXPECT().ReadCommitted(mock.Anything, mock.Anything).Run(func(ctx context.Context, fn func(ctx context.Context) error) {
		fn(ctx)
	}).Return(nil)

	mockStore.EXPECT().DequeueStep(mock.Anything, "worker1").Return(nil, nil)

	empty, err := engine.ExecuteNext(context.Background(), "worker1")

	assert.NoError(t, err)
	assert.True(t, empty)
}

func TestEngine_ExecuteNext_LongRunning_TaskStep_Success(t *testing.T) {
	mockTxManager := NewMockTxManager(t)
	mockStore := NewMockStore(t)
	mockStore.EXPECT().GetCancelRequest(mock.Anything, mock.Anything).Return(nil, ErrEntityNotFound).Maybe()

	engine := NewEngine(nil,
		WithEngineTxManager(mockTxManager),
		WithEngineStore(mockStore),
		WithLongRunningSteps(),
	)
	defer engine.Shutdown()

	// Register a handler
	handlerCalled := atomic.Bool{}
	mockHandler := NewMockStepHandler(t)
	mockHandler.EXPECT().Name().Return("test-handler")
	mockHandler.EXPECT().Execute(mock.Anything, mock.Anything, mock.Anything).Run(func(ctx context.Context, stepCtx StepContext, input json.RawMessage) {
		handlerCalled.Store(true)
	}).Return(json.RawMessage(`{"result": "success"}`), nil)
	engine.RegisterHandler(mockHandler)

	workflowID := "test-workflow"
	stepID := int64(1)
	instanceID := int64(123)

	definition := &WorkflowDefinition{
		ID:   workflowID,
		Name: "Test Workflow",
		Definition: GraphDefinition{
			Start: "step1",
			Steps: map[string]*StepDefinition{
				"step1": {
					Name:       "step1",
					Type:       StepTypeTask,
					Handler:    "test-handler",
					MaxRetries: 3,
				},
			},
		},
	}

	instance := &WorkflowInstance{
		ID:         instanceID,
		WorkflowID: workflowID,
		Status:     StatusRunning,
	}

	step := &WorkflowStep{
		ID:         stepID,
		InstanceID: instanceID,
		StepName:   "step1",
		StepType:   StepTypeTask,
		Status:     StepStatusPending,
		Input:      json.RawMessage(`{"test": "input"}`),
	}

	queueItem := &QueueItem{
		ID:         1,
		InstanceID: instanceID,
		StepID:     &stepID,
	}

	// Transaction 1: Dequeue and prepare
	txCallCount := 0
	mockTxManager.EXPECT().ReadCommitted(mock.Anything, mock.Anything).Run(func(ctx context.Context, fn func(ctx context.Context) error) {
		txCallCount++
		fn(ctx)
	}).Return(nil).Times(2) // Called twice: once for prepare, once for finalize

	mockStore.EXPECT().DequeueStep(mock.Anything, "worker1").Return(queueItem, nil).Once()
	mockStore.EXPECT().GetInstance(mock.Anything, instanceID).Return(instance, nil).Maybe()
	mockStore.EXPECT().GetStepsByInstance(mock.Anything, instanceID).Return([]WorkflowStep{*step}, nil).Maybe()
	mockStore.EXPECT().GetWorkflowDefinition(mock.Anything, workflowID).Return(definition, nil).Maybe()
	mockStore.EXPECT().UpdateStep(mock.Anything, stepID, StepStatusRunning, mock.Anything, mock.Anything).Return(nil).Maybe()
	mockStore.EXPECT().LogEvent(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	mockStore.EXPECT().RemoveFromQueue(mock.Anything, mock.Anything).Return(nil).Maybe()

	// Transaction 2: Handle success
	mockStore.EXPECT().UpdateStep(mock.Anything, stepID, StepStatusCompleted, mock.Anything, mock.Anything).Return(nil).Maybe()
	mockStore.EXPECT().UpdateInstanceStatus(mock.Anything, instanceID, StatusCompleted, mock.Anything, mock.Anything).Return(nil).Maybe()

	empty, err := engine.ExecuteNext(context.Background(), "worker1")

	assert.NoError(t, err)
	assert.False(t, empty)
	assert.True(t, handlerCalled.Load(), "Handler should have been called")
	assert.Equal(t, 2, txCallCount, "Should have two transactions")
}

func TestEngine_ExecuteNext_LongRunning_TaskStep_HandlerError(t *testing.T) {
	mockTxManager := NewMockTxManager(t)
	mockStore := NewMockStore(t)
	mockStore.EXPECT().GetCancelRequest(mock.Anything, mock.Anything).Return(nil, ErrEntityNotFound).Maybe()

	engine := NewEngine(nil,
		WithEngineTxManager(mockTxManager),
		WithEngineStore(mockStore),
		WithLongRunningSteps(),
	)
	defer engine.Shutdown()

	// Register a handler that fails
	mockHandler := NewMockStepHandler(t)
	mockHandler.EXPECT().Name().Return("test-handler")
	mockHandler.EXPECT().Execute(mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("handler failed"))
	engine.RegisterHandler(mockHandler)

	workflowID := "test-workflow"
	stepID := int64(1)
	instanceID := int64(123)

	definition := &WorkflowDefinition{
		ID:   workflowID,
		Name: "Test Workflow",
		Definition: GraphDefinition{
			Start: "step1",
			Steps: map[string]*StepDefinition{
				"step1": {
					Name:       "step1",
					Type:       StepTypeTask,
					Handler:    "test-handler",
					MaxRetries: 0, // No retries at all
				},
			},
		},
	}

	instance := &WorkflowInstance{
		ID:         instanceID,
		WorkflowID: workflowID,
		Status:     StatusRunning,
	}

	step := &WorkflowStep{
		ID:         stepID,
		InstanceID: instanceID,
		StepName:   "step1",
		StepType:   StepTypeTask,
		Status:     StepStatusPending,
		MaxRetries: 0,
		Input:      json.RawMessage(`{"test": "input"}`),
	}

	queueItem := &QueueItem{
		ID:         1,
		InstanceID: instanceID,
		StepID:     &stepID,
	}

	mockTxManager.EXPECT().ReadCommitted(mock.Anything, mock.Anything).Run(func(ctx context.Context, fn func(ctx context.Context) error) {
		fn(ctx)
	}).Return(nil).Times(2)

	// All store calls - use Maybe() for flexibility
	mockStore.EXPECT().DequeueStep(mock.Anything, "worker1").Return(queueItem, nil).Once()
	mockStore.EXPECT().GetInstance(mock.Anything, instanceID).Return(instance, nil).Maybe()
	mockStore.EXPECT().GetStepsByInstance(mock.Anything, instanceID).Return([]WorkflowStep{*step}, nil).Maybe()
	mockStore.EXPECT().GetWorkflowDefinition(mock.Anything, workflowID).Return(definition, nil).Maybe()
	mockStore.EXPECT().UpdateStep(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	mockStore.EXPECT().LogEvent(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	mockStore.EXPECT().RemoveFromQueue(mock.Anything, mock.Anything).Return(nil).Maybe()
	mockStore.EXPECT().UpdateInstanceStatus(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	mockStore.EXPECT().EnqueueStep(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	empty, err := engine.ExecuteNext(context.Background(), "worker1")

	assert.NoError(t, err)
	assert.False(t, empty)
}

func TestEngine_ExecuteNext_LongRunning_NonTaskStep_UsesNormalExecution(t *testing.T) {
	mockTxManager := NewMockTxManager(t)
	mockStore := NewMockStore(t)
	mockStore.EXPECT().GetCancelRequest(mock.Anything, mock.Anything).Return(nil, ErrEntityNotFound).Maybe()

	engine := NewEngine(nil,
		WithEngineTxManager(mockTxManager),
		WithEngineStore(mockStore),
		WithLongRunningSteps(),
	)
	defer engine.Shutdown()

	workflowID := "test-workflow"
	stepID := int64(1)
	instanceID := int64(123)

	definition := &WorkflowDefinition{
		ID:   workflowID,
		Name: "Test Workflow",
		Definition: GraphDefinition{
			Start: "savepoint1",
			Steps: map[string]*StepDefinition{
				"savepoint1": {
					Name: "savepoint1",
					Type: StepTypeSavePoint,
				},
			},
		},
	}

	instance := &WorkflowInstance{
		ID:         instanceID,
		WorkflowID: workflowID,
		Status:     StatusRunning,
	}

	step := &WorkflowStep{
		ID:         stepID,
		InstanceID: instanceID,
		StepName:   "savepoint1",
		StepType:   StepTypeSavePoint,
		Status:     StepStatusPending,
		Input:      json.RawMessage(`{}`),
	}

	queueItem := &QueueItem{
		ID:         1,
		InstanceID: instanceID,
		StepID:     &stepID,
	}

	// For non-Task steps, only one transaction should be used (normal execution)
	txCallCount := 0
	mockTxManager.EXPECT().ReadCommitted(mock.Anything, mock.Anything).Run(func(ctx context.Context, fn func(ctx context.Context) error) {
		txCallCount++
		fn(ctx)
	}).Return(nil).Once()

	mockStore.EXPECT().DequeueStep(mock.Anything, "worker1").Return(queueItem, nil).Once()
	mockStore.EXPECT().GetInstance(mock.Anything, instanceID).Return(instance, nil).Maybe()
	mockStore.EXPECT().GetStepsByInstance(mock.Anything, instanceID).Return([]WorkflowStep{*step}, nil).Maybe()
	mockStore.EXPECT().GetWorkflowDefinition(mock.Anything, workflowID).Return(definition, nil).Maybe()
	mockStore.EXPECT().RemoveFromQueue(mock.Anything, mock.Anything).Return(nil).Maybe()
	mockStore.EXPECT().UpdateStep(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	mockStore.EXPECT().LogEvent(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	mockStore.EXPECT().UpdateInstanceStatus(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	empty, err := engine.ExecuteNext(context.Background(), "worker1")

	assert.NoError(t, err)
	assert.False(t, empty)
	assert.Equal(t, 1, txCallCount, "Non-task steps should use single transaction (normal execution)")
}

func TestEngine_ExecuteNext_LongRunning_MissingHandler_Rescheduled(t *testing.T) {
	mockTxManager := NewMockTxManager(t)
	mockStore := NewMockStore(t)
	mockStore.EXPECT().GetCancelRequest(mock.Anything, mock.Anything).Return(nil, ErrEntityNotFound).Maybe()

	engine := NewEngine(nil,
		WithEngineTxManager(mockTxManager),
		WithEngineStore(mockStore),
		WithLongRunningSteps(),
		WithMissingHandlerCooldown(time.Second),
	)
	defer engine.Shutdown()

	// No handler registered

	workflowID := "test-workflow"
	stepID := int64(1)
	instanceID := int64(123)

	definition := &WorkflowDefinition{
		ID:   workflowID,
		Name: "Test Workflow",
		Definition: GraphDefinition{
			Start: "step1",
			Steps: map[string]*StepDefinition{
				"step1": {
					Name:    "step1",
					Type:    StepTypeTask,
					Handler: "missing-handler",
				},
			},
		},
	}

	instance := &WorkflowInstance{
		ID:         instanceID,
		WorkflowID: workflowID,
		Status:     StatusRunning,
	}

	step := &WorkflowStep{
		ID:         stepID,
		InstanceID: instanceID,
		StepName:   "step1",
		StepType:   StepTypeTask,
		Status:     StepStatusPending,
	}

	queueItem := &QueueItem{
		ID:         1,
		InstanceID: instanceID,
		StepID:     &stepID,
	}

	mockTxManager.EXPECT().ReadCommitted(mock.Anything, mock.Anything).Run(func(ctx context.Context, fn func(ctx context.Context) error) {
		fn(ctx)
	}).Return(nil).Once()

	mockStore.EXPECT().DequeueStep(mock.Anything, "worker1").Return(queueItem, nil).Once()
	mockStore.EXPECT().GetInstance(mock.Anything, instanceID).Return(instance, nil).Once()
	mockStore.EXPECT().GetStepsByInstance(mock.Anything, instanceID).Return([]WorkflowStep{*step}, nil).Once()
	mockStore.EXPECT().GetWorkflowDefinition(mock.Anything, workflowID).Return(definition, nil).Once()
	mockStore.EXPECT().RescheduleAndReleaseQueueItem(mock.Anything, queueItem.ID, mock.Anything).Return(nil).Once()
	mockStore.EXPECT().LogEvent(mock.Anything, instanceID, mock.Anything, EventStepSkippedMissingHandler, mock.Anything).Return(nil).Maybe()

	empty, err := engine.ExecuteNext(context.Background(), "worker1")

	assert.NoError(t, err)
	assert.False(t, empty)
}

func TestEngine_ExecuteNext_LongRunning_DLQInstance_Skipped(t *testing.T) {
	mockTxManager := NewMockTxManager(t)
	mockStore := NewMockStore(t)
	mockStore.EXPECT().GetCancelRequest(mock.Anything, mock.Anything).Return(nil, ErrEntityNotFound).Maybe()

	engine := NewEngine(nil,
		WithEngineTxManager(mockTxManager),
		WithEngineStore(mockStore),
		WithLongRunningSteps(),
	)
	defer engine.Shutdown()

	stepID := int64(1)
	instanceID := int64(123)

	instance := &WorkflowInstance{
		ID:         instanceID,
		WorkflowID: "test-workflow",
		Status:     StatusDLQ, // Instance in DLQ state
	}

	queueItem := &QueueItem{
		ID:         1,
		InstanceID: instanceID,
		StepID:     &stepID,
	}

	mockTxManager.EXPECT().ReadCommitted(mock.Anything, mock.Anything).Run(func(ctx context.Context, fn func(ctx context.Context) error) {
		fn(ctx)
	}).Return(nil).Once()

	mockStore.EXPECT().DequeueStep(mock.Anything, "worker1").Return(queueItem, nil).Once()
	mockStore.EXPECT().GetInstance(mock.Anything, instanceID).Return(instance, nil).Once()
	mockStore.EXPECT().LogEvent(mock.Anything, instanceID, mock.Anything, EventStepFailed, mock.Anything).Return(nil).Once()
	mockStore.EXPECT().RemoveFromQueue(mock.Anything, queueItem.ID).Return(nil).Once()

	empty, err := engine.ExecuteNext(context.Background(), "worker1")

	assert.NoError(t, err)
	assert.False(t, empty)
}
