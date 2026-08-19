package floxy

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestNewEngine(t *testing.T) {
	mockTxManager := NewMockTxManager(t)
	mockStore := NewMockStore(t)
	mockStore.EXPECT().GetCancelRequest(mock.Anything, mock.Anything).Return(nil, ErrEntityNotFound).Maybe()

	engine := NewEngine(nil, WithEngineTxManager(mockTxManager), WithEngineStore(mockStore))
	defer engine.Shutdown()

	assert.NotNil(t, engine)
	assert.Equal(t, mockTxManager, engine.txManager)
	assert.Equal(t, mockStore, engine.store)
	assert.NotNil(t, engine.handlers)
	assert.Empty(t, engine.handlers)
}

func TestEngine_RegisterHandler(t *testing.T) {
	mockTxManager := NewMockTxManager(t)
	mockStore := NewMockStore(t)
	mockStore.EXPECT().GetCancelRequest(mock.Anything, mock.Anything).Return(nil, ErrEntityNotFound).Maybe()

	engine := NewEngine(nil, WithEngineTxManager(mockTxManager), WithEngineStore(mockStore))
	defer engine.Shutdown()

	mockHandler := NewMockStepHandler(t)
	mockHandler.EXPECT().Name().Return("test-handler")

	engine.RegisterHandler(mockHandler)

	assert.Len(t, engine.handlers, 1)
	//assert.Equal(t, mockHandler, engine.handlers["test-handler"])
}

func TestEngine_RegisterWorkflow_ValidDefinition(t *testing.T) {
	mockTxManager := NewMockTxManager(t)
	mockStore := NewMockStore(t)
	mockStore.EXPECT().GetCancelRequest(mock.Anything, mock.Anything).Return(nil, ErrEntityNotFound).Maybe()

	engine := NewEngine(nil, WithEngineTxManager(mockTxManager), WithEngineStore(mockStore))
	defer engine.Shutdown()

	definition := &WorkflowDefinition{
		ID:   "test-workflow",
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

	mockStore.EXPECT().SaveWorkflowDefinition(mock.Anything, definition).Return(nil)

	err := engine.RegisterWorkflow(context.Background(), definition)

	assert.NoError(t, err)
}

func TestEngine_RegisterWorkflow_InvalidDefinition(t *testing.T) {
	mockTxManager := NewMockTxManager(t)
	mockStore := NewMockStore(t)
	mockStore.EXPECT().GetCancelRequest(mock.Anything, mock.Anything).Return(nil, ErrEntityNotFound).Maybe()

	engine := NewEngine(nil, WithEngineTxManager(mockTxManager), WithEngineStore(mockStore))
	defer engine.Shutdown()

	tests := []struct {
		name       string
		definition *WorkflowDefinition
		expectErr  string
	}{
		{
			name: "empty name",
			definition: &WorkflowDefinition{
				ID:   "test-workflow",
				Name: "",
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
			},
			expectErr: "workflow name is required",
		},
		{
			name: "empty start step",
			definition: &WorkflowDefinition{
				ID:   "test-workflow",
				Name: "Test Workflow",
				Definition: GraphDefinition{
					Start: "",
					Steps: map[string]*StepDefinition{
						"step1": {
							Name:       "step1",
							Type:       StepTypeTask,
							Handler:    "test-handler",
							MaxRetries: 3,
						},
					},
				},
			},
			expectErr: "start step is required",
		},
		{
			name: "no steps",
			definition: &WorkflowDefinition{
				ID:   "test-workflow",
				Name: "Test Workflow",
				Definition: GraphDefinition{
					Start: "step1",
					Steps: map[string]*StepDefinition{},
				},
			},
			expectErr: "at least one step is required",
		},
		{
			name: "start step not found",
			definition: &WorkflowDefinition{
				ID:   "test-workflow",
				Name: "Test Workflow",
				Definition: GraphDefinition{
					Start: "step1",
					Steps: map[string]*StepDefinition{
						"step2": {
							Name:       "step2",
							Type:       StepTypeTask,
							Handler:    "test-handler",
							MaxRetries: 3,
						},
					},
				},
			},
			expectErr: "start step not found: step1",
		},
		{
			name: "unknown next step",
			definition: &WorkflowDefinition{
				ID:   "test-workflow",
				Name: "Test Workflow",
				Definition: GraphDefinition{
					Start: "step1",
					Steps: map[string]*StepDefinition{
						"step1": {
							Name:       "step1",
							Type:       StepTypeTask,
							Handler:    "test-handler",
							MaxRetries: 3,
							Next:       []string{"unknown-step"},
						},
					},
				},
			},
			expectErr: "step \"step1\" references unknown step: \"unknown-step\"",
		},
		{
			name: "unknown on_failure step",
			definition: &WorkflowDefinition{
				ID:   "test-workflow",
				Name: "Test Workflow",
				Definition: GraphDefinition{
					Start: "step1",
					Steps: map[string]*StepDefinition{
						"step1": {
							Name:       "step1",
							Type:       StepTypeTask,
							Handler:    "test-handler",
							MaxRetries: 3,
							OnFailure:  "unknown-step",
						},
					},
				},
			},
			expectErr: "step \"step1\" references unknown compensation step: \"unknown-step\"",
		},
		{
			name: "unknown parallel step",
			definition: &WorkflowDefinition{
				ID:   "test-workflow",
				Name: "Test Workflow",
				Definition: GraphDefinition{
					Start: "step1",
					Steps: map[string]*StepDefinition{
						"step1": {
							Name:       "step1",
							Type:       StepTypeFork,
							Handler:    "test-handler",
							MaxRetries: 3,
							Parallel:   []string{"unknown-step"},
						},
					},
				},
			},
			expectErr: "step \"step1\" references unknown parallel step: \"unknown-step\"",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := engine.RegisterWorkflow(context.Background(), tt.definition)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), tt.expectErr)
		})
	}
}

func TestEngine_Start_Success(t *testing.T) {
	mockTxManager := NewMockTxManager(t)
	mockStore := NewMockStore(t)
	mockStore.EXPECT().GetCancelRequest(mock.Anything, mock.Anything).Return(nil, ErrEntityNotFound).Maybe()
	engine := NewEngine(nil, WithEngineTxManager(mockTxManager), WithEngineStore(mockStore))
	defer engine.Shutdown()

	workflowID := "test-workflow"
	input := json.RawMessage(`{"key": "value"}`)

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
		ID:         123,
		WorkflowID: workflowID,
		Status:     StatusPending,
		Input:      input,
	}

	mockTxManager.EXPECT().ReadCommitted(mock.Anything, mock.Anything).Run(func(ctx context.Context, fn func(ctx context.Context) error) {
		fn(ctx)
	}).Return(nil)

	mockStore.EXPECT().GetWorkflowDefinition(mock.Anything, workflowID).Return(definition, nil)
	mockStore.EXPECT().CreateInstance(mock.Anything, workflowID, input).Return(instance, nil)
	mockStore.EXPECT().LogEvent(mock.Anything, instance.ID, mock.Anything, EventWorkflowStarted, mock.Anything).Return(nil)
	mockStore.EXPECT().UpdateInstanceStatus(mock.Anything, instance.ID, StatusRunning, mock.Anything, mock.Anything).Return(nil)
	mockStore.EXPECT().GetInstance(mock.Anything, instance.ID).Return(instance, nil)
	mockStore.EXPECT().GetWorkflowDefinition(mock.Anything, instance.WorkflowID).Return(definition, nil)
	mockStore.EXPECT().CreateStep(mock.Anything, mock.MatchedBy(func(step *WorkflowStep) bool {
		return step.InstanceID == instance.ID && step.StepName == "step1"
	})).Return(nil)
	mockStore.EXPECT().EnqueueStep(mock.Anything, instance.ID, mock.Anything, PriorityNormal, mock.Anything).Return(nil)

	instanceID, err := engine.Start(context.Background(), workflowID, input)

	assert.NoError(t, err)
	assert.Equal(t, int64(123), instanceID)
}

func TestEngine_Start_GetDefinitionError(t *testing.T) {
	mockTxManager := NewMockTxManager(t)
	mockStore := NewMockStore(t)
	mockStore.EXPECT().GetCancelRequest(mock.Anything, mock.Anything).Return(nil, ErrEntityNotFound).Maybe()
	engine := NewEngine(nil, WithEngineTxManager(mockTxManager), WithEngineStore(mockStore))
	defer engine.Shutdown()

	workflowID := "test-workflow"
	input := json.RawMessage(`{"key": "value"}`)

	mockTxManager.EXPECT().ReadCommitted(mock.Anything, mock.Anything).Run(func(ctx context.Context, fn func(ctx context.Context) error) {
		fn(ctx)
	}).Return(errors.New("get workflow definition: definition not found"))

	mockStore.EXPECT().GetWorkflowDefinition(mock.Anything, workflowID).Return(nil, errors.New("definition not found"))

	instanceID, err := engine.Start(context.Background(), workflowID, input)

	assert.Error(t, err)
	assert.Equal(t, int64(0), instanceID)
	assert.Contains(t, err.Error(), "get workflow definition")
}

func TestEngine_Start_CreateInstanceError(t *testing.T) {
	mockTxManager := NewMockTxManager(t)
	mockStore := NewMockStore(t)
	mockStore.EXPECT().GetCancelRequest(mock.Anything, mock.Anything).Return(nil, ErrEntityNotFound).Maybe()
	engine := NewEngine(nil, WithEngineTxManager(mockTxManager), WithEngineStore(mockStore))
	defer engine.Shutdown()

	workflowID := "test-workflow"
	input := json.RawMessage(`{"key": "value"}`)

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

	mockTxManager.EXPECT().ReadCommitted(mock.Anything, mock.Anything).Run(func(ctx context.Context, fn func(ctx context.Context) error) {
		fn(ctx)
	}).Return(errors.New("create instance: create instance failed"))

	mockStore.EXPECT().GetWorkflowDefinition(mock.Anything, workflowID).Return(definition, nil)
	mockStore.EXPECT().CreateInstance(mock.Anything, workflowID, input).Return(nil, errors.New("create instance failed"))

	instanceID, err := engine.Start(context.Background(), workflowID, input)

	assert.Error(t, err)
	assert.Equal(t, int64(0), instanceID)
	assert.Contains(t, err.Error(), "create instance")
}

func TestEngine_Start_NoStartStep(t *testing.T) {
	mockTxManager := NewMockTxManager(t)
	mockStore := NewMockStore(t)
	mockStore.EXPECT().GetCancelRequest(mock.Anything, mock.Anything).Return(nil, ErrEntityNotFound).Maybe()
	engine := NewEngine(nil, WithEngineTxManager(mockTxManager), WithEngineStore(mockStore))
	defer engine.Shutdown()

	workflowID := "test-workflow"
	input := json.RawMessage(`{"key": "value"}`)

	definition := &WorkflowDefinition{
		ID:   workflowID,
		Name: "Test Workflow",
		Definition: GraphDefinition{
			Start: "",
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

	mockTxManager.EXPECT().ReadCommitted(mock.Anything, mock.Anything).Run(func(ctx context.Context, fn func(ctx context.Context) error) {
		fn(ctx)
	}).Return(errors.New("no start step defined"))

	mockStore.EXPECT().GetWorkflowDefinition(mock.Anything, workflowID).Return(definition, nil)

	instanceID, err := engine.Start(context.Background(), workflowID, input)

	assert.Error(t, err)
	assert.Equal(t, int64(0), instanceID)
	assert.Contains(t, err.Error(), "no start step defined")
}

func TestEngine_ExecuteNext_NoQueueItem(t *testing.T) {
	mockTxManager := NewMockTxManager(t)
	mockStore := NewMockStore(t)
	mockStore.EXPECT().GetCancelRequest(mock.Anything, mock.Anything).Return(nil, ErrEntityNotFound).Maybe()
	engine := NewEngine(nil, WithEngineTxManager(mockTxManager), WithEngineStore(mockStore))
	defer engine.Shutdown()

	workerID := "worker-1"

	mockStore.EXPECT().DequeueStep(mock.Anything, workerID).Return(nil, nil)

	empty, err := engine.ExecuteNext(context.Background(), workerID)

	assert.NoError(t, err)
	assert.True(t, empty)
}

func TestEngine_ExecuteNext_DequeueError(t *testing.T) {
	mockTxManager := NewMockTxManager(t)
	mockStore := NewMockStore(t)
	mockStore.EXPECT().GetCancelRequest(mock.Anything, mock.Anything).Return(nil, ErrEntityNotFound).Maybe()
	engine := NewEngine(nil, WithEngineTxManager(mockTxManager), WithEngineStore(mockStore))
	defer engine.Shutdown()

	workerID := "worker-1"

	mockStore.EXPECT().DequeueStep(mock.Anything, workerID).Return(nil, errors.New("dequeue failed"))

	empty, err := engine.ExecuteNext(context.Background(), workerID)

	assert.Error(t, err)
	assert.False(t, empty)
	assert.Contains(t, err.Error(), "dequeue step")
}

func TestEngine_ExecuteNext_StepNotFound(t *testing.T) {
	mockTxManager := NewMockTxManager(t)
	mockStore := NewMockStore(t)
	mockStore.EXPECT().GetCancelRequest(mock.Anything, mock.Anything).Return(nil, ErrEntityNotFound).Maybe()
	engine := NewEngine(nil, WithEngineTxManager(mockTxManager), WithEngineStore(mockStore))
	defer engine.Shutdown()

	workerID := "worker-1"
	instanceID := int64(123)
	stepID := int64(456)

	queueItem := &QueueItem{
		ID:         789,
		InstanceID: instanceID,
		StepID:     &stepID,
	}

	instance := &WorkflowInstance{
		ID:         instanceID,
		WorkflowID: "test-workflow",
		Status:     StatusRunning,
	}

	steps := []WorkflowStep{}

	mockStore.EXPECT().DequeueStep(mock.Anything, workerID).Return(queueItem, nil)
	mockTxManager.EXPECT().ReadCommitted(mock.Anything, mock.Anything).Run(func(ctx context.Context, fn func(ctx context.Context) error) {
		mockStore.EXPECT().RemoveFromQueue(mock.Anything, queueItem.ID).Return(nil)
		mockStore.EXPECT().GetInstance(mock.Anything, instanceID).Return(instance, nil)
		mockStore.EXPECT().GetStepsByInstance(mock.Anything, instanceID).Return(steps, nil)
		fn(ctx)
	}).Return(errors.New("step not found: 456"))
	mockStore.EXPECT().ReleaseQueueItem(mock.Anything, queueItem.ID).Return(nil)

	empty, err := engine.ExecuteNext(context.Background(), workerID)

	assert.Error(t, err)
	assert.False(t, empty)
	assert.Contains(t, err.Error(), "step not found")
}

func TestEngine_ExecuteTask_Success(t *testing.T) {
	mockTxManager := NewMockTxManager(t)
	mockStore := NewMockStore(t)
	mockStore.EXPECT().GetCancelRequest(mock.Anything, mock.Anything).Return(nil, ErrEntityNotFound).Maybe()
	engine := NewEngine(nil, WithEngineTxManager(mockTxManager), WithEngineStore(mockStore))
	defer engine.Shutdown()

	instanceID := int64(123)
	stepID := int64(456)

	instance := &WorkflowInstance{
		ID:         instanceID,
		WorkflowID: "test-workflow",
		Status:     StatusRunning,
	}

	step := &WorkflowStep{
		ID:         stepID,
		InstanceID: instanceID,
		StepName:   "step1",
		StepType:   StepTypeTask,
		Status:     StepStatusPending,
		Input:      json.RawMessage(`{"key": "value"}`),
		MaxRetries: 3,
		RetryCount: 0,
	}

	stepDef := &StepDefinition{
		Name:       "step1",
		Type:       StepTypeTask,
		Handler:    "test-handler",
		MaxRetries: 3,
	}

	mockHandler := NewMockStepHandler(t)
	mockHandler.EXPECT().Name().Return("test-handler")
	engine.RegisterHandler(mockHandler)

	mockHandler.EXPECT().Execute(mock.Anything, mock.MatchedBy(func(ctx StepContext) bool {
		return ctx.InstanceID() == instanceID && ctx.StepName() == "step1" && ctx.RetryCount() == 0
	}), step.Input).Return(json.RawMessage(`{"result": "success"}`), nil)

	output, err := engine.executeTask(context.Background(), instance, step, stepDef)

	assert.NoError(t, err)
	assert.Equal(t, json.RawMessage(`{"result": "success"}`), output)
}

func TestEngine_ExecuteTask_HandlerNotFound(t *testing.T) {
	mockTxManager := NewMockTxManager(t)
	mockStore := NewMockStore(t)
	mockStore.EXPECT().GetCancelRequest(mock.Anything, mock.Anything).Return(nil, ErrEntityNotFound).Maybe()
	engine := NewEngine(nil, WithEngineTxManager(mockTxManager), WithEngineStore(mockStore))
	defer engine.Shutdown()

	instanceID := int64(123)
	stepID := int64(456)

	instance := &WorkflowInstance{
		ID:         instanceID,
		WorkflowID: "test-workflow",
		Status:     StatusRunning,
	}

	step := &WorkflowStep{
		ID:         stepID,
		InstanceID: instanceID,
		StepName:   "step1",
		StepType:   StepTypeTask,
		Status:     StepStatusPending,
		Input:      json.RawMessage(`{"key": "value"}`),
		MaxRetries: 3,
		RetryCount: 0,
	}

	stepDef := &StepDefinition{
		Name:       "step1",
		Type:       StepTypeTask,
		Handler:    "unknown-handler",
		MaxRetries: 3,
	}

	output, err := engine.executeTask(context.Background(), instance, step, stepDef)

	assert.Error(t, err)
	assert.Nil(t, output)
	assert.Contains(t, err.Error(), "handler not found")
}

func TestEngine_ExecuteSavePoint_Success(t *testing.T) {
	mockTxManager := NewMockTxManager(t)
	mockStore := NewMockStore(t)
	mockStore.EXPECT().GetCancelRequest(mock.Anything, mock.Anything).Return(nil, ErrEntityNotFound).Maybe()
	engine := NewEngine(nil, WithEngineTxManager(mockTxManager), WithEngineStore(mockStore))
	defer engine.Shutdown()

	instanceID := int64(123)
	stepID := int64(456)

	instance := &WorkflowInstance{
		ID:         instanceID,
		WorkflowID: "test-workflow",
		Status:     StatusRunning,
	}

	step := WorkflowStep{
		ID:         stepID,
		InstanceID: instanceID,
		StepName:   "savepoint1",
		StepType:   StepTypeSavePoint,
		Status:     StepStatusPending,
		Input:      json.RawMessage(`{"key": "value"}`),
		MaxRetries: 3,
		RetryCount: 0,
	}

	stepDef := &StepDefinition{
		Name:       "savepoint1",
		Type:       StepTypeSavePoint,
		MaxRetries: 3,
	}

	definition := &WorkflowDefinition{
		ID:   "test-workflow",
		Name: "Test Workflow",
		Definition: GraphDefinition{
			Start: "savepoint1",
			Steps: map[string]*StepDefinition{
				"savepoint1": stepDef,
			},
		},
	}

	mockStore.EXPECT().GetWorkflowDefinition(mock.Anything, instance.WorkflowID).Return(definition, nil)
	mockStore.EXPECT().UpdateStep(mock.Anything, stepID, StepStatusRunning, mock.Anything, mock.Anything).Return(nil)
	mockStore.EXPECT().LogEvent(mock.Anything, instanceID, &stepID, EventStepStarted, mock.Anything).Return(nil)
	mockStore.EXPECT().UpdateStep(mock.Anything, stepID, StepStatusCompleted, mock.Anything, mock.Anything).Return(nil)
	mockStore.EXPECT().LogEvent(mock.Anything, instanceID, &stepID, EventStepCompleted, mock.Anything).Return(nil)
	mockStore.EXPECT().GetInstance(mock.Anything, instanceID).Return(instance, nil)
	mockStore.EXPECT().GetWorkflowDefinition(mock.Anything, instance.WorkflowID).Return(definition, nil)
	mockStore.EXPECT().GetStepsByInstance(mock.Anything, instanceID).Return([]WorkflowStep{step}, nil)

	err := engine.executeStep(context.Background(), instance, &step)

	assert.NoError(t, err)
}

func TestEngine_ExecuteFork_Success(t *testing.T) {
	mockTxManager := NewMockTxManager(t)
	mockStore := NewMockStore(t)
	mockStore.EXPECT().GetCancelRequest(mock.Anything, mock.Anything).Return(nil, ErrEntityNotFound).Maybe()
	engine := NewEngine(nil, WithEngineTxManager(mockTxManager), WithEngineStore(mockStore))
	defer engine.Shutdown()

	instanceID := int64(123)
	stepID := int64(456)

	instance := &WorkflowInstance{
		ID:         instanceID,
		WorkflowID: "test-workflow",
		Status:     StatusRunning,
	}

	step := &WorkflowStep{
		ID:         stepID,
		InstanceID: instanceID,
		StepName:   "fork-step",
		StepType:   StepTypeFork,
		Status:     StepStatusPending,
		Input:      json.RawMessage(`{"key": "value"}`),
		MaxRetries: 3,
	}

	stepDef := &StepDefinition{
		Name:       "fork-step",
		Type:       StepTypeFork,
		Handler:    "fork-handler",
		MaxRetries: 3,
		Parallel:   []string{"parallel1", "parallel2"},
		Next:       []string{"join-step"},
	}

	definition := &WorkflowDefinition{
		ID:   "test-workflow",
		Name: "Test Workflow",
		Definition: GraphDefinition{
			Start: "fork-step",
			Steps: map[string]*StepDefinition{
				"fork-step": stepDef,
				"parallel1": {
					Name:       "parallel1",
					Type:       StepTypeTask,
					Handler:    "test-handler",
					MaxRetries: 3,
				},
				"parallel2": {
					Name:       "parallel2",
					Type:       StepTypeTask,
					Handler:    "test-handler",
					MaxRetries: 3,
				},
				"join-step": {
					Name:         "join-step",
					Type:         StepTypeJoin,
					WaitFor:      []string{"parallel1", "parallel2"},
					JoinStrategy: JoinStrategyAll,
				},
			},
		},
	}

	mockStore.EXPECT().LogEvent(mock.Anything, instanceID, &stepID, EventForkStarted, mock.Anything).Return(nil)
	mockStore.EXPECT().GetWorkflowDefinition(mock.Anything, instance.WorkflowID).Return(definition, nil)
	mockStore.EXPECT().CreateStep(mock.Anything, mock.MatchedBy(func(s *WorkflowStep) bool {
		return s.InstanceID == instanceID && s.StepName == "parallel1"
	})).Return(nil)
	mockStore.EXPECT().EnqueueStep(mock.Anything, instanceID, mock.Anything, PriorityNormal, mock.Anything).Return(nil)
	mockStore.EXPECT().CreateStep(mock.Anything, mock.MatchedBy(func(s *WorkflowStep) bool {
		return s.InstanceID == instanceID && s.StepName == "parallel2"
	})).Return(nil)
	mockStore.EXPECT().EnqueueStep(mock.Anything, instanceID, mock.Anything, PriorityNormal, mock.Anything).Return(nil)
	mockStore.EXPECT().CreateJoinState(mock.Anything, instanceID, "join-step", []string{"parallel1", "parallel2"}, JoinStrategyAll).Return(nil)
	mockStore.EXPECT().LogEvent(mock.Anything, instanceID, &stepID, EventJoinStateCreated, mock.Anything).Return(nil)

	output, err := engine.executeFork(context.Background(), instance, step, stepDef)

	assert.NoError(t, err)
	assert.Contains(t, string(output), "forked")
	assert.Contains(t, string(output), "parallel1")
	assert.Contains(t, string(output), "parallel2")
}

func TestEngine_ExecuteJoin_NotReady(t *testing.T) {
	mockTxManager := NewMockTxManager(t)
	mockStore := NewMockStore(t)
	mockStore.EXPECT().GetCancelRequest(mock.Anything, mock.Anything).Return(nil, ErrEntityNotFound).Maybe()
	engine := NewEngine(nil, WithEngineTxManager(mockTxManager), WithEngineStore(mockStore))
	defer engine.Shutdown()

	instanceID := int64(123)
	stepID := int64(456)

	instance := &WorkflowInstance{
		ID:         instanceID,
		WorkflowID: "test-workflow",
		Status:     StatusRunning,
	}

	step := &WorkflowStep{
		ID:         stepID,
		InstanceID: instanceID,
		StepName:   "join-step",
		StepType:   StepTypeJoin,
		Status:     StepStatusPending,
		Input:      json.RawMessage(`{"key": "value"}`),
		MaxRetries: 0,
	}

	joinState := &JoinState{
		InstanceID:   instanceID,
		JoinStepName: "join-step",
		WaitingFor:   []string{"step1", "step2"},
		Completed:    []string{"step1"},
		Failed:       []string{},
		JoinStrategy: JoinStrategyAll,
		IsReady:      false,
	}

	mockStore.EXPECT().GetJoinState(mock.Anything, instanceID, "join-step").Return(joinState, nil)
	mockStore.EXPECT().LogEvent(mock.Anything, instanceID, &stepID, EventJoinCheck, mock.Anything).Return(nil)

	output, err := engine.executeJoin(context.Background(), instance, step, nil)

	assert.Error(t, err)
	assert.Nil(t, output)
	assert.Contains(t, err.Error(), "join not ready")
}

func TestEngine_ExecuteJoin_Success(t *testing.T) {
	mockTxManager := NewMockTxManager(t)
	mockStore := NewMockStore(t)
	mockStore.EXPECT().GetCancelRequest(mock.Anything, mock.Anything).Return(nil, ErrEntityNotFound).Maybe()
	engine := NewEngine(nil, WithEngineTxManager(mockTxManager), WithEngineStore(mockStore))
	defer engine.Shutdown()

	instanceID := int64(123)
	stepID := int64(456)

	instance := &WorkflowInstance{
		ID:         instanceID,
		WorkflowID: "test-workflow",
		Status:     StatusRunning,
	}

	step := &WorkflowStep{
		ID:         stepID,
		InstanceID: instanceID,
		StepName:   "join-step",
		StepType:   StepTypeJoin,
		Status:     StepStatusPending,
		Input:      json.RawMessage(`{"key": "value"}`),
		MaxRetries: 0,
	}

	joinState := &JoinState{
		InstanceID:   instanceID,
		JoinStepName: "join-step",
		WaitingFor:   []string{"step1", "step2"},
		Completed:    []string{"step1", "step2"},
		Failed:       []string{},
		JoinStrategy: JoinStrategyAll,
		IsReady:      true,
	}

	steps := []WorkflowStep{
		{
			ID:         789,
			InstanceID: instanceID,
			StepName:   "step1",
			Status:     StepStatusCompleted,
			Output:     json.RawMessage(`{"result1": "success"}`),
		},
		{
			ID:         790,
			InstanceID: instanceID,
			StepName:   "step2",
			Status:     StepStatusCompleted,
			Output:     json.RawMessage(`{"result2": "success"}`),
		},
	}

	mockStore.EXPECT().GetJoinState(mock.Anything, instanceID, "join-step").Return(joinState, nil)
	mockStore.EXPECT().LogEvent(mock.Anything, instanceID, &stepID, EventJoinCheck, mock.Anything).Return(nil)
	mockStore.EXPECT().GetStepsByInstance(mock.Anything, instanceID).Return(steps, nil)
	mockStore.EXPECT().LogEvent(mock.Anything, instanceID, &stepID, EventJoinCompleted, mock.Anything).Return(nil)

	output, err := engine.executeJoin(context.Background(), instance, step, nil)

	assert.NoError(t, err)
	assert.NotNil(t, output)

	var result map[string]any
	err = json.Unmarshal(output, &result)
	assert.NoError(t, err)
	assert.Equal(t, "success", result["status"])
	assert.Contains(t, result, "outputs")
	assert.Contains(t, result, "completed")
	assert.Contains(t, result, "failed")
}

func TestEngine_ExecuteJoin_WithFailures(t *testing.T) {
	mockTxManager := NewMockTxManager(t)
	mockStore := NewMockStore(t)
	mockStore.EXPECT().GetCancelRequest(mock.Anything, mock.Anything).Return(nil, ErrEntityNotFound).Maybe()
	engine := NewEngine(nil, WithEngineTxManager(mockTxManager), WithEngineStore(mockStore))
	defer engine.Shutdown()

	instanceID := int64(123)
	stepID := int64(456)

	instance := &WorkflowInstance{
		ID:         instanceID,
		WorkflowID: "test-workflow",
		Status:     StatusRunning,
	}

	step := &WorkflowStep{
		ID:         stepID,
		InstanceID: instanceID,
		StepName:   "join-step",
		StepType:   StepTypeJoin,
		Status:     StepStatusPending,
		Input:      json.RawMessage(`{"key": "value"}`),
		MaxRetries: 0,
	}

	joinState := &JoinState{
		InstanceID:   instanceID,
		JoinStepName: "join-step",
		WaitingFor:   []string{"step1", "step2"},
		Completed:    []string{"step1"},
		Failed:       []string{"step2"},
		JoinStrategy: JoinStrategyAll,
		IsReady:      true,
	}

	steps := []WorkflowStep{
		{
			ID:         789,
			InstanceID: instanceID,
			StepName:   "step1",
			Status:     StepStatusCompleted,
			Output:     json.RawMessage(`{"result1": "success"}`),
		},
		{
			ID:         790,
			InstanceID: instanceID,
			StepName:   "step2",
			Status:     StepStatusFailed,
			Output:     json.RawMessage(`{}`),
		},
	}

	mockStore.EXPECT().GetJoinState(mock.Anything, instanceID, "join-step").Return(joinState, nil)
	mockStore.EXPECT().LogEvent(mock.Anything, instanceID, &stepID, EventJoinCheck, mock.Anything).Return(nil)
	mockStore.EXPECT().GetStepsByInstance(mock.Anything, instanceID).Return(steps, nil)

	output, err := engine.executeJoin(context.Background(), instance, step, nil)

	assert.Error(t, err)
	assert.NotNil(t, output)
	assert.Contains(t, err.Error(), "join failed")

	var result map[string]any
	err = json.Unmarshal(output, &result)
	assert.NoError(t, err)
	assert.Equal(t, "failed", result["status"])
}

func TestEngine_HandleStepSuccess_WithNextSteps(t *testing.T) {
	mockTxManager := NewMockTxManager(t)
	mockStore := NewMockStore(t)
	mockStore.EXPECT().GetCancelRequest(mock.Anything, mock.Anything).Return(nil, ErrEntityNotFound).Maybe()
	engine := NewEngine(nil, WithEngineTxManager(mockTxManager), WithEngineStore(mockStore))
	defer engine.Shutdown()

	instanceID := int64(123)
	stepID := int64(456)

	instance := &WorkflowInstance{
		ID:         instanceID,
		WorkflowID: "test-workflow",
		Status:     StatusRunning,
	}

	step := WorkflowStep{
		ID:         stepID,
		InstanceID: instanceID,
		StepName:   "step1",
		StepType:   StepTypeTask,
		Status:     StepStatusRunning,
		Input:      json.RawMessage(`{"key": "value"}`),
		MaxRetries: 3,
	}

	stepDef := &StepDefinition{
		Name:       "step1",
		Type:       StepTypeTask,
		Handler:    "test-handler",
		MaxRetries: 3,
		Next:       []string{"step2"},
	}

	output := json.RawMessage(`{"result": "success"}`)

	definition := &WorkflowDefinition{
		ID:   "test-workflow",
		Name: "Test Workflow",
		Definition: GraphDefinition{
			Start: "step1",
			Steps: map[string]*StepDefinition{
				"step1": stepDef,
				"step2": {
					Name:       "step2",
					Type:       StepTypeTask,
					Handler:    "test-handler",
					MaxRetries: 3,
				},
			},
		},
	}

	mockStore.EXPECT().UpdateStep(mock.Anything, stepID, StepStatusCompleted, output, mock.Anything).Return(nil)
	mockStore.EXPECT().LogEvent(mock.Anything, instanceID, &stepID, EventStepCompleted, mock.Anything).Return(nil)
	mockStore.EXPECT().GetInstance(mock.Anything, instanceID).Return(instance, nil)
	mockStore.EXPECT().GetWorkflowDefinition(mock.Anything, instance.WorkflowID).Return(definition, nil)
	mockStore.EXPECT().CreateStep(mock.Anything, mock.MatchedBy(func(s *WorkflowStep) bool {
		return s.InstanceID == instanceID && s.StepName == "step2"
	})).Return(nil)
	mockStore.EXPECT().EnqueueStep(mock.Anything, instanceID, mock.Anything, PriorityNormal, mock.Anything).Return(nil)

	err := engine.handleStepSuccess(context.Background(), instance, &step, stepDef, output, true)

	assert.NoError(t, err)
}

func TestEngine_HandleStepFailure_WithRetries(t *testing.T) {
	mockTxManager := NewMockTxManager(t)
	mockStore := NewMockStore(t)
	mockStore.EXPECT().GetCancelRequest(mock.Anything, mock.Anything).Return(nil, ErrEntityNotFound).Maybe()
	engine := NewEngine(nil, WithEngineTxManager(mockTxManager), WithEngineStore(mockStore))
	defer engine.Shutdown()

	instanceID := int64(123)
	stepID := int64(456)

	instance := &WorkflowInstance{
		ID:         instanceID,
		WorkflowID: "test-workflow",
		Status:     StatusRunning,
	}

	step := &WorkflowStep{
		ID:         stepID,
		InstanceID: instanceID,
		StepName:   "step1",
		StepType:   StepTypeTask,
		Status:     StepStatusRunning,
		Input:      json.RawMessage(`{"key": "value"}`),
		MaxRetries: 3,
		RetryCount: 1,
	}

	stepDef := &StepDefinition{
		Name:       "step1",
		Type:       StepTypeTask,
		Handler:    "test-handler",
		MaxRetries: 3,
	}

	stepErr := errors.New("step execution failed")

	mockStore.EXPECT().UpdateStep(mock.Anything, stepID, StepStatusFailed, mock.Anything, mock.Anything).Return(nil)
	mockStore.EXPECT().LogEvent(mock.Anything, instanceID, &stepID, EventStepRetry, mock.MatchedBy(func(data map[string]any) bool {
		return data[KeyRetryCount] == 2 // RetryCount was incremented from 1 to 2
	})).Return(nil)
	mockStore.EXPECT().EnqueueStep(mock.Anything, instanceID, &stepID, PriorityHigh, mock.Anything).Return(nil)

	err := engine.handleStepFailure(context.Background(), instance, step, stepDef, stepErr)

	assert.NoError(t, err)
}

func TestEngine_HandleStepFailure_NoRetriesLeft(t *testing.T) {
	mockTxManager := NewMockTxManager(t)
	mockStore := NewMockStore(t)
	mockStore.EXPECT().GetCancelRequest(mock.Anything, mock.Anything).Return(nil, ErrEntityNotFound).Maybe()
	engine := NewEngine(nil, WithEngineTxManager(mockTxManager), WithEngineStore(mockStore))
	defer engine.Shutdown()

	instanceID := int64(123)
	stepID := int64(456)

	instance := &WorkflowInstance{
		ID:         instanceID,
		WorkflowID: "test-workflow",
		Status:     StatusRunning,
	}

	step := WorkflowStep{
		ID:         stepID,
		InstanceID: instanceID,
		StepName:   "step1",
		StepType:   StepTypeTask,
		Status:     StepStatusRunning,
		Input:      json.RawMessage(`{"key": "value"}`),
		MaxRetries: 3,
		RetryCount: 3,
	}

	stepDef := &StepDefinition{
		Name:       "step1",
		Type:       StepTypeTask,
		Handler:    "test-handler",
		MaxRetries: 3,
	}

	definition := &WorkflowDefinition{
		ID:   "test-workflow",
		Name: "Test Workflow",
		Definition: GraphDefinition{
			Start: "step1",
			Steps: map[string]*StepDefinition{
				"step1": stepDef,
			},
		},
	}

	stepErr := errors.New("step execution failed")

	mockStore.EXPECT().UpdateStep(mock.Anything, stepID, StepStatusFailed, mock.Anything, mock.Anything).Return(nil)
	mockStore.EXPECT().LogEvent(mock.Anything, instanceID, &stepID, EventStepFailed, mock.Anything).Return(nil)
	mockStore.EXPECT().GetInstance(mock.Anything, instanceID).Return(instance, nil)
	mockStore.EXPECT().GetWorkflowDefinition(mock.Anything, instance.WorkflowID).Return(definition, nil)
	mockStore.EXPECT().GetStepsByInstance(mock.Anything, instanceID).Return([]WorkflowStep{step}, nil)
	mockStore.EXPECT().UpdateInstanceStatus(mock.Anything, instanceID, StatusFailed, mock.Anything, mock.Anything).Return(nil)

	err := engine.handleStepFailure(context.Background(), instance, &step, stepDef, stepErr)

	assert.NoError(t, err)
}

func TestEngine_HandleStepFailure_WithOnFailure(t *testing.T) {
	mockTxManager := NewMockTxManager(t)
	mockStore := NewMockStore(t)
	mockStore.EXPECT().GetCancelRequest(mock.Anything, mock.Anything).Return(nil, ErrEntityNotFound).Maybe()
	engine := NewEngine(nil, WithEngineTxManager(mockTxManager), WithEngineStore(mockStore))
	defer engine.Shutdown()

	instanceID := int64(123)
	stepID := int64(456)

	instance := &WorkflowInstance{
		ID:         instanceID,
		WorkflowID: "test-workflow",
		Status:     StatusRunning,
	}

	step := WorkflowStep{
		ID:         stepID,
		InstanceID: instanceID,
		StepName:   "step1",
		StepType:   StepTypeTask,
		Status:     StepStatusRunning,
		Input:      json.RawMessage(`{"key": "value"}`),
		MaxRetries: 3,
		RetryCount: 3,
	}

	stepDef := &StepDefinition{
		Name:       "step1",
		Type:       StepTypeTask,
		Handler:    "test-handler",
		MaxRetries: 3,
		OnFailure:  "compensation-step",
	}

	stepErr := errors.New("step execution failed")

	definition := &WorkflowDefinition{
		ID:   "test-workflow",
		Name: "Test Workflow",
		Definition: GraphDefinition{
			Start: "step1",
			Steps: map[string]*StepDefinition{
				"step1": stepDef,
				"compensation-step": {
					Name:       "compensation-step",
					Type:       StepTypeTask,
					Handler:    "compensation-handler",
					MaxRetries: 3,
				},
			},
		},
	}

	mockStore.EXPECT().UpdateStep(mock.Anything, stepID, StepStatusFailed, mock.Anything, mock.Anything).Return(nil)
	mockStore.EXPECT().LogEvent(mock.Anything, instanceID, &stepID, EventStepFailed, mock.Anything).Return(nil)
	mockStore.EXPECT().GetInstance(mock.Anything, instanceID).Return(instance, nil)
	mockStore.EXPECT().GetWorkflowDefinition(mock.Anything, instance.WorkflowID).Return(definition, nil)
	mockStore.EXPECT().GetStepsByInstance(mock.Anything, instanceID).Return([]WorkflowStep{step}, nil)
	mockStore.EXPECT().UpdateInstanceStatus(mock.Anything, instanceID, StatusFailed, mock.Anything, mock.Anything).Return(nil)

	err := engine.handleStepFailure(context.Background(), instance, &step, stepDef, stepErr)

	assert.NoError(t, err)
}

func TestEngine_GetStatus(t *testing.T) {
	mockTxManager := NewMockTxManager(t)
	mockStore := NewMockStore(t)
	mockStore.EXPECT().GetCancelRequest(mock.Anything, mock.Anything).Return(nil, ErrEntityNotFound).Maybe()
	engine := NewEngine(nil, WithEngineTxManager(mockTxManager), WithEngineStore(mockStore))
	defer engine.Shutdown()

	instanceID := int64(123)
	expectedInstance := &WorkflowInstance{
		ID:         instanceID,
		WorkflowID: "test-workflow",
		Status:     StatusRunning,
	}

	mockStore.EXPECT().GetInstance(mock.Anything, instanceID).Return(expectedInstance, nil)

	status, err := engine.GetStatus(context.Background(), instanceID)

	assert.NoError(t, err)
	assert.Equal(t, StatusRunning, status)
}

func TestEngine_GetSteps(t *testing.T) {
	mockTxManager := NewMockTxManager(t)
	mockStore := NewMockStore(t)
	mockStore.EXPECT().GetCancelRequest(mock.Anything, mock.Anything).Return(nil, ErrEntityNotFound).Maybe()
	engine := NewEngine(nil, WithEngineTxManager(mockTxManager), WithEngineStore(mockStore))
	defer engine.Shutdown()

	instanceID := int64(123)
	expectedSteps := []WorkflowStep{
		{
			ID:         456,
			InstanceID: instanceID,
			StepName:   "step1",
			Status:     StepStatusCompleted,
		},
		{
			ID:         789,
			InstanceID: instanceID,
			StepName:   "step2",
			Status:     StepStatusRunning,
		},
	}

	mockStore.EXPECT().GetStepsByInstance(mock.Anything, instanceID).Return(expectedSteps, nil)

	steps, err := engine.GetSteps(context.Background(), instanceID)

	assert.NoError(t, err)
	assert.Equal(t, expectedSteps, steps)
}

func TestEngine_ValidateDefinition_Valid(t *testing.T) {
	mockTxManager := NewMockTxManager(t)
	mockStore := NewMockStore(t)
	mockStore.EXPECT().GetCancelRequest(mock.Anything, mock.Anything).Return(nil, ErrEntityNotFound).Maybe()
	engine := NewEngine(nil, WithEngineTxManager(mockTxManager), WithEngineStore(mockStore))
	defer engine.Shutdown()

	definition := &WorkflowDefinition{
		ID:   "test-workflow",
		Name: "Test Workflow",
		Definition: GraphDefinition{
			Start: "step1",
			Steps: map[string]*StepDefinition{
				"step1": {
					Name:       "step1",
					Type:       StepTypeTask,
					Handler:    "test-handler",
					MaxRetries: 3,
					Next:       []string{"step2"},
				},
				"step2": {
					Name:       "step2",
					Type:       StepTypeTask,
					Handler:    "test-handler",
					MaxRetries: 3,
					OnFailure:  "compensation-step",
				},
				"compensation-step": {
					Name:       "compensation-step",
					Type:       StepTypeTask,
					Handler:    "compensation-handler",
					MaxRetries: 3,
				},
			},
		},
	}

	err := engine.validateDefinition(definition)

	assert.NoError(t, err)
}

func TestEngine_ValidateDefinition_ComplexWorkflow(t *testing.T) {
	mockTxManager := NewMockTxManager(t)
	mockStore := NewMockStore(t)
	mockStore.EXPECT().GetCancelRequest(mock.Anything, mock.Anything).Return(nil, ErrEntityNotFound).Maybe()
	engine := NewEngine(nil, WithEngineTxManager(mockTxManager), WithEngineStore(mockStore))
	defer engine.Shutdown()

	definition := &WorkflowDefinition{
		ID:   "complex-workflow",
		Name: "Complex Workflow",
		Definition: GraphDefinition{
			Start: "fork-step",
			Steps: map[string]*StepDefinition{
				"fork-step": {
					Name:       "fork-step",
					Type:       StepTypeFork,
					Handler:    "fork-handler",
					MaxRetries: 3,
					Parallel:   []string{"parallel1", "parallel2"},
					Next:       []string{"join-step"},
				},
				"parallel1": {
					Name:       "parallel1",
					Type:       StepTypeTask,
					Handler:    "test-handler",
					MaxRetries: 3,
				},
				"parallel2": {
					Name:       "parallel2",
					Type:       StepTypeTask,
					Handler:    "test-handler",
					MaxRetries: 3,
				},
				"join-step": {
					Name:         "join-step",
					Type:         StepTypeJoin,
					WaitFor:      []string{"parallel1", "parallel2"},
					JoinStrategy: JoinStrategyAll,
				},
			},
		},
	}

	err := engine.validateDefinition(definition)

	assert.NoError(t, err)
}

// -- jitteredCooldown -------------------------------------------------------

func Test_jitteredCooldown_BaseZero(t *testing.T) {
	engine, _ := newTestEngineWithStore(t)
	engine.missingHandlerCooldown = 0
	assert.Equal(t, time.Duration(0), engine.jitteredCooldown())
}

func Test_jitteredCooldown_NoJitter(t *testing.T) {
	engine, _ := newTestEngineWithStore(t)
	engine.missingHandlerCooldown = 100 * time.Millisecond
	engine.missingHandlerJitterPct = 0
	for i := 0; i < 5; i++ {
		assert.Equal(t, 100*time.Millisecond, engine.jitteredCooldown())
	}
}

func Test_jitteredCooldown_WithJitterWithinBounds(t *testing.T) {
	engine, _ := newTestEngineWithStore(t)
	base := 200 * time.Millisecond
	pct := 0.25 // 25%
	engine.missingHandlerCooldown = base
	engine.missingHandlerJitterPct = pct

	min := float64(base) * (1 - pct)
	max := float64(base) * (1 + pct)
	for i := 0; i < 20; i++ {
		d := engine.jitteredCooldown()
		dd := float64(d)
		if dd < min || dd > max {
			t.Fatalf("cooldown %v out of bounds [%v, %v]", d, time.Duration(min), time.Duration(max))
		}
	}
}

// -- shouldLogSkip ----------------------------------------------------------

func Test_shouldLogSkip_Throttle(t *testing.T) {
	engine, _ := newTestEngineWithStore(t)
	engine.missingHandlerLogThrottle = 50 * time.Millisecond

	key := "abc"
	assert.True(t, engine.shouldLogSkip(key))  // first allowed
	assert.False(t, engine.shouldLogSkip(key)) // throttled
	time.Sleep(60 * time.Millisecond)
	assert.True(t, engine.shouldLogSkip(key)) // allowed after throttle
}

func Test_shouldLogSkip_DisabledThrottle(t *testing.T) {
	engine, _ := newTestEngineWithStore(t)
	engine.missingHandlerLogThrottle = 0
	for i := 0; i < 3; i++ {
		assert.True(t, engine.shouldLogSkip("k"))
	}
}

// -- validateDefinition -----------------------------------------------------

func Test_validateDefinition_Success(t *testing.T) {
	engine, _ := newTestEngineWithStore(t)
	def := &WorkflowDefinition{
		ID:   "wf",
		Name: "wf",
		Definition: GraphDefinition{
			Start: "A",
			Steps: map[string]*StepDefinition{
				"A": {Name: "A", Type: StepTypeTask, Next: []string{"B"}, Prev: rootStepName, Handler: "handlerA"},
				"B": {Name: "B", Type: StepTypeTask, Prev: "A", Handler: "handlerB"},
			},
		},
	}
	assert.NoError(t, engine.validateDefinition(def))
}

func Test_validateDefinition_Errors(t *testing.T) {
	engine, _ := newTestEngineWithStore(t)

	// no name
	def1 := &WorkflowDefinition{Definition: GraphDefinition{Start: "A", Steps: map[string]*StepDefinition{"A": {Name: "A"}}}}
	assert.Error(t, engine.validateDefinition(def1))

	// no start
	def2 := &WorkflowDefinition{Name: "wf", Definition: GraphDefinition{Steps: map[string]*StepDefinition{"A": {Name: "A"}}}}
	assert.Error(t, engine.validateDefinition(def2))

	// no steps
	def3 := &WorkflowDefinition{Name: "wf", Definition: GraphDefinition{Start: "A", Steps: map[string]*StepDefinition{}}}
	assert.Error(t, engine.validateDefinition(def3))

	// start not found
	def4 := &WorkflowDefinition{Name: "wf", Definition: GraphDefinition{Start: "X", Steps: map[string]*StepDefinition{"A": {Name: "A"}}}}
	assert.Error(t, engine.validateDefinition(def4))

	// next ref unknown
	def5 := &WorkflowDefinition{
		Name: "wf",
		Definition: GraphDefinition{
			Start: "A",
			Steps: map[string]*StepDefinition{
				"A": {Name: "A", Next: []string{"Z"}},
			},
		},
	}
	assert.Error(t, engine.validateDefinition(def5))

	// onFailure unknown
	def6 := &WorkflowDefinition{
		Name: "wf",
		Definition: GraphDefinition{
			Start: "A",
			Steps: map[string]*StepDefinition{
				"A": {Name: "A", OnFailure: "C"},
			},
		},
	}
	assert.Error(t, engine.validateDefinition(def6))

	// parallel unknown
	def7 := &WorkflowDefinition{
		Name: "wf",
		Definition: GraphDefinition{
			Start: "A",
			Steps: map[string]*StepDefinition{
				"A": {Name: "A", Type: StepTypeFork, Parallel: []string{"P1"}},
			},
		},
	}
	assert.Error(t, engine.validateDefinition(def7))
}

// -- determineExecutedBranch ------------------------------------------------

func Test_determineExecutedBranch_Addl(t *testing.T) {
	engine, _ := newTestEngineWithStore(t)
	step := &StepDefinition{Name: "COND", Next: []string{"X"}, Else: "Y"}
	m := map[string]*WorkflowStep{
		"X": {StepName: "X", Status: StepStatusCompleted},
	}
	assert.Equal(t, "next", engine.determineExecutedBranch(step, m))

	m = map[string]*WorkflowStep{
		"Y": {StepName: "Y", Status: StepStatusCompensation},
	}
	assert.Equal(t, "else", engine.determineExecutedBranch(step, m))

	m = map[string]*WorkflowStep{}
	assert.Equal(t, "", engine.determineExecutedBranch(step, m))
}

// -- isStepInParallelBranch / isStepDescendantOf ----------------------------

func Test_isStepInParallelBranch_and_isStepDescendantOf(t *testing.T) {
	engine, _ := newTestEngineWithStore(t)
	def := buildForkJoinDef()
	fork := def.Definition.Steps["F"]

	assert.True(t, engine.isStepInParallelBranch("A", fork, def))
	assert.True(t, engine.isStepInParallelBranch("A1", fork, def))
	assert.True(t, engine.isStepDescendantOf("A1", "A", def))
	assert.False(t, engine.isStepDescendantOf("A", "A1", def))
	assert.False(t, engine.isStepInParallelBranch("J", fork, def))
}

// -- hasPendingStepsInParallelBranches -------------------------------------

func Test_hasPendingStepsInParallelBranches_TrueWhenOtherPending(t *testing.T) {
	ctx := context.Background()
	def := buildForkJoinDirectWaitForDef()
	engine, store := newTestEngineWithStore(t)

	instanceID := int64(200)
	store.EXPECT().GetInstance(mock.Anything, instanceID).
		Return(&WorkflowInstance{ID: instanceID, WorkflowID: def.ID}, nil).Maybe()
	store.EXPECT().GetWorkflowDefinition(mock.Anything, def.ID).Return(def, nil).Maybe()

	// Join.WaitFor = [A, B]. We have A completed, and a running descendant A1 not in WaitFor -> should be true
	steps := []WorkflowStep{
		{InstanceID: instanceID, StepName: "A", Status: StepStatusCompleted},
		{InstanceID: instanceID, StepName: "A1", Status: StepStatusRunning}, // not in WaitFor for J
	}
	join := def.Definition.Steps["J"]

	assert.True(t, engine.hasPendingStepsInParallelBranches(ctx, instanceID, join, steps))
}

func Test_hasPendingStepsInParallelBranches_FalseWhenNoOtherPending(t *testing.T) {
	ctx := context.Background()
	def := buildForkJoinDef()
	engine, store := newTestEngineWithStore(t)

	instanceID := int64(201)
	store.EXPECT().GetInstance(mock.Anything, instanceID).
		Return(&WorkflowInstance{ID: instanceID, WorkflowID: def.ID}, nil).Maybe()
	store.EXPECT().GetWorkflowDefinition(mock.Anything, def.ID).Return(def, nil).Maybe()

	steps := []WorkflowStep{
		{InstanceID: instanceID, StepName: "A1", Status: StepStatusCompleted},
		{InstanceID: instanceID, StepName: "B1", Status: StepStatusCompleted},
	}
	join := def.Definition.Steps["J"]

	assert.False(t, engine.hasPendingStepsInParallelBranches(ctx, instanceID, join, steps))
}

// -- GetStatus/GetSteps pass-throughs --------------------------------------

func Test_GetStatus_Passthrough(t *testing.T) {
	ctx := context.Background()
	engine, store := newTestEngineWithStore(t)
	instanceID := int64(300)

	status := StatusRunning
	store.EXPECT().GetInstance(mock.Anything, instanceID).
		Return(&WorkflowInstance{ID: instanceID, Status: status}, nil)

	got, err := engine.GetStatus(ctx, instanceID)
	assert.NoError(t, err)
	assert.Equal(t, status, got)
}

func Test_GetSteps_Passthrough(t *testing.T) {
	ctx := context.Background()
	engine, store := newTestEngineWithStore(t)
	instanceID := int64(301)

	expected := []WorkflowStep{{InstanceID: instanceID, StepName: "A"}}
	store.EXPECT().GetStepsByInstance(mock.Anything, instanceID).Return(expected, nil)

	got, err := engine.GetSteps(ctx, instanceID)
	assert.NoError(t, err)
	assert.Equal(t, expected, got)
}

// -- RegisterHandler/RegisterPlugin basic registration ----------------------

type dummyHandler struct{ name string }

func (d dummyHandler) Name() string { return d.name }
func (d dummyHandler) Execute(ctx context.Context, stepCtx StepContext, input json.RawMessage) (json.RawMessage, error) {
	return nil, nil
}

func Test_RegisterHandler_StoresWrappedHandler(t *testing.T) {
	engine, _ := newTestEngineWithStore(t)
	h := dummyHandler{name: "H"}
	engine.RegisterHandler(h)
	engine.mu.RLock()
	_, ok := engine.handlers["H"]
	engine.mu.RUnlock()
	assert.True(t, ok)
}

func Test_RegisterPlugin_CreatesManagerAndRegisters(t *testing.T) {
	engine, _ := newTestEngineWithStore(t)
	p := NewMockPlugin(t)
	p.EXPECT().Name().Return("p1").Maybe()
	engine.RegisterPlugin(p)
	assert.NotNil(t, engine.pluginManager)
}

func TestEngine_ExecuteNext_CompensationMissingHandler_RescheduleAndRelease(t *testing.T) {
	mockTxManager := NewMockTxManager(t)
	mockStore := NewMockStore(t)
	// Background cancel worker queries (ignored)
	mockStore.EXPECT().GetCancelRequest(mock.Anything, mock.Anything).Return(nil, ErrEntityNotFound).Maybe()

	// Configure engine so cooldown is deterministic and > 0
	engine := NewEngine(
		nil,
		WithEngineTxManager(mockTxManager),
		WithEngineStore(mockStore),
		WithMissingHandlerCooldown(time.Second),
		WithMissingHandlerJitterPct(0),
	)
	defer engine.Shutdown()

	workerID := "worker-1"
	instanceID := int64(10101)
	stepID := int64(555)
	queueItem := &QueueItem{ID: 777, InstanceID: instanceID, StepID: &stepID}

	instance := &WorkflowInstance{ID: instanceID, WorkflowID: "wf-comp", Status: StatusRunning}
	// The step to execute is in Compensation phase
	step := WorkflowStep{ID: stepID, InstanceID: instanceID, StepName: "S", StepType: StepTypeTask, Status: StepStatusCompensation}

	// Workflow definition with OnFailure handler that is NOT registered locally
	def := &WorkflowDefinition{
		ID:   instance.WorkflowID,
		Name: "wf-comp",
		Definition: GraphDefinition{
			Start: "S",
			Steps: map[string]*StepDefinition{
				"S":    {Name: "S", Type: StepTypeTask, OnFailure: "Comp"},
				"Comp": {Name: "Comp", Type: StepTypeTask, Handler: "comp-handler"},
			},
		},
	}

	mockStore.EXPECT().DequeueStep(mock.Anything, workerID).Return(queueItem, nil)
	// Expectations inside transaction
	mockTxManager.EXPECT().ReadCommitted(mock.Anything, mock.Anything).Run(func(ctx context.Context, fn func(ctx context.Context) error) {
		// Lookup instance and steps
		mockStore.EXPECT().GetInstance(mock.Anything, instanceID).Return(instance, nil)
		mockStore.EXPECT().GetStepsByInstance(mock.Anything, instanceID).Return([]WorkflowStep{step}, nil)
		// Definition is required to resolve OnFailure
		mockStore.EXPECT().GetWorkflowDefinition(mock.Anything, instance.WorkflowID).Return(def, nil)
		// Since no local handler registered, engine must reschedule with cooldown (>0)
		mockStore.EXPECT().RescheduleAndReleaseQueueItem(mock.Anything, queueItem.ID, time.Second).Return(nil)
		// Throttled log should be emitted once
		mockStore.EXPECT().LogEvent(mock.Anything, instance.ID, mock.Anything, EventStepSkippedMissingHandler, mock.Anything).Return(nil)

		_ = fn(ctx)
	}).Return(nil)

	empty, err := engine.ExecuteNext(context.Background(), workerID)
	assert.NoError(t, err)
	assert.False(t, empty)
	// Ensure engine did NOT remove the queue item explicitly (released instead)
	mockStore.AssertNotCalled(t, "RemoveFromQueue", mock.Anything, queueItem.ID)
}

func TestEngine_ExecuteNext_CompensationMissingHandler_ReleaseOnly(t *testing.T) {
	mockTxManager := NewMockTxManager(t)
	mockStore := NewMockStore(t)
	mockStore.EXPECT().GetCancelRequest(mock.Anything, mock.Anything).Return(nil, ErrEntityNotFound).Maybe()

	// Cooldown = 0 => immediate release (no reschedule)
	engine := NewEngine(
		nil,
		WithEngineTxManager(mockTxManager),
		WithEngineStore(mockStore),
		WithMissingHandlerCooldown(0),
		WithMissingHandlerJitterPct(0),
	)
	defer engine.Shutdown()

	workerID := "worker-2"
	instanceID := int64(20202)
	stepID := int64(556)
	queueItem := &QueueItem{ID: 778, InstanceID: instanceID, StepID: &stepID}

	instance := &WorkflowInstance{ID: instanceID, WorkflowID: "wf-comp-0", Status: StatusRunning}
	step := WorkflowStep{ID: stepID, InstanceID: instanceID, StepName: "S", StepType: StepTypeTask, Status: StepStatusCompensation}

	def := &WorkflowDefinition{
		ID:   instance.WorkflowID,
		Name: "wf-comp-0",
		Definition: GraphDefinition{
			Start: "S",
			Steps: map[string]*StepDefinition{
				"S":    {Name: "S", Type: StepTypeTask, OnFailure: "Comp"},
				"Comp": {Name: "Comp", Type: StepTypeTask, Handler: "comp-handler"},
			},
		},
	}

	mockStore.EXPECT().DequeueStep(mock.Anything, workerID).Return(queueItem, nil)
	mockTxManager.EXPECT().ReadCommitted(mock.Anything, mock.Anything).Run(func(ctx context.Context, fn func(ctx context.Context) error) {
		mockStore.EXPECT().GetInstance(mock.Anything, instanceID).Return(instance, nil)
		mockStore.EXPECT().GetStepsByInstance(mock.Anything, instanceID).Return([]WorkflowStep{step}, nil)
		mockStore.EXPECT().GetWorkflowDefinition(mock.Anything, instance.WorkflowID).Return(def, nil)
		// With zero cooldown we expect a simple release
		mockStore.EXPECT().ReleaseQueueItem(mock.Anything, queueItem.ID).Return(nil)
		// Log once
		mockStore.EXPECT().LogEvent(mock.Anything, instance.ID, mock.Anything, EventStepSkippedMissingHandler, mock.Anything).Return(nil)

		_ = fn(ctx)
	}).Return(nil)

	empty, err := engine.ExecuteNext(context.Background(), workerID)
	assert.NoError(t, err)
	assert.False(t, empty)
	mockStore.AssertNotCalled(t, "RemoveFromQueue", mock.Anything, queueItem.ID)
}

func TestEngine_ExecuteNext_TaskMissingHandler_RescheduleAndRelease(t *testing.T) {
	mockTxManager := NewMockTxManager(t)
	mockStore := NewMockStore(t)
	// Background cancel worker queries (ignored)
	mockStore.EXPECT().GetCancelRequest(mock.Anything, mock.Anything).Return(nil, ErrEntityNotFound).Maybe()

	// Configure engine so cooldown is deterministic and > 0
	engine := NewEngine(
		nil,
		WithEngineTxManager(mockTxManager),
		WithEngineStore(mockStore),
		WithMissingHandlerCooldown(time.Second),
		WithMissingHandlerJitterPct(0),
	)
	defer engine.Shutdown()

	workerID := "worker-task-1"
	instanceID := int64(30303)
	stepID := int64(901)
	queueItem := &QueueItem{ID: 9901, InstanceID: instanceID, StepID: &stepID}

	instance := &WorkflowInstance{ID: instanceID, WorkflowID: "wf-task", Status: StatusRunning}
	// Normal task step (not compensation)
	step := WorkflowStep{ID: stepID, InstanceID: instanceID, StepName: "TaskStep", StepType: StepTypeTask, Status: StepStatusPending}

	def := &WorkflowDefinition{
		ID:   instance.WorkflowID,
		Name: "wf-task",
		Definition: GraphDefinition{
			Start: "TaskStep",
			Steps: map[string]*StepDefinition{
				"TaskStep": {Name: "TaskStep", Type: StepTypeTask, Handler: "missing-handler"},
			},
		},
	}

	mockStore.EXPECT().DequeueStep(mock.Anything, workerID).Return(queueItem, nil)
	mockTxManager.EXPECT().ReadCommitted(mock.Anything, mock.Anything).Run(func(ctx context.Context, fn func(ctx context.Context) error) {
		mockStore.EXPECT().GetInstance(mock.Anything, instanceID).Return(instance, nil)
		mockStore.EXPECT().GetStepsByInstance(mock.Anything, instanceID).Return([]WorkflowStep{step}, nil)
		mockStore.EXPECT().GetWorkflowDefinition(mock.Anything, instance.WorkflowID).Return(def, nil)
		// Expect reschedule+release due to cooldown > 0
		mockStore.EXPECT().RescheduleAndReleaseQueueItem(mock.Anything, queueItem.ID, time.Second).Return(nil)
		// Throttled log emitted once
		mockStore.EXPECT().LogEvent(mock.Anything, instance.ID, mock.Anything, EventStepSkippedMissingHandler, mock.Anything).Return(nil)

		_ = fn(ctx)
	}).Return(nil)

	empty, err := engine.ExecuteNext(context.Background(), workerID)
	assert.NoError(t, err)
	assert.False(t, empty)
	// Ensure explicit removal was not called
	mockStore.AssertNotCalled(t, "RemoveFromQueue", mock.Anything, queueItem.ID)
}

func TestEngine_ExecuteNext_TaskMissingHandler_ReleaseOnly(t *testing.T) {
	mockTxManager := NewMockTxManager(t)
	mockStore := NewMockStore(t)
	mockStore.EXPECT().GetCancelRequest(mock.Anything, mock.Anything).Return(nil, ErrEntityNotFound).Maybe()

	// Cooldown = 0 => immediate release
	engine := NewEngine(
		nil,
		WithEngineTxManager(mockTxManager),
		WithEngineStore(mockStore),
		WithMissingHandlerCooldown(0),
		WithMissingHandlerJitterPct(0),
	)
	defer engine.Shutdown()

	workerID := "worker-task-2"
	instanceID := int64(40404)
	stepID := int64(902)
	queueItem := &QueueItem{ID: 9902, InstanceID: instanceID, StepID: &stepID}

	instance := &WorkflowInstance{ID: instanceID, WorkflowID: "wf-task-0", Status: StatusRunning}
	step := WorkflowStep{ID: stepID, InstanceID: instanceID, StepName: "TaskStep", StepType: StepTypeTask, Status: StepStatusPending}

	def := &WorkflowDefinition{
		ID:   instance.WorkflowID,
		Name: "wf-task-0",
		Definition: GraphDefinition{
			Start: "TaskStep",
			Steps: map[string]*StepDefinition{
				"TaskStep": {Name: "TaskStep", Type: StepTypeTask, Handler: "missing-handler"},
			},
		},
	}

	mockStore.EXPECT().DequeueStep(mock.Anything, workerID).Return(queueItem, nil)
	mockTxManager.EXPECT().ReadCommitted(mock.Anything, mock.Anything).Run(func(ctx context.Context, fn func(ctx context.Context) error) {
		mockStore.EXPECT().GetInstance(mock.Anything, instanceID).Return(instance, nil)
		mockStore.EXPECT().GetStepsByInstance(mock.Anything, instanceID).Return([]WorkflowStep{step}, nil)
		mockStore.EXPECT().GetWorkflowDefinition(mock.Anything, instance.WorkflowID).Return(def, nil)
		mockStore.EXPECT().ReleaseQueueItem(mock.Anything, queueItem.ID).Return(nil)
		mockStore.EXPECT().LogEvent(mock.Anything, instance.ID, mock.Anything, EventStepSkippedMissingHandler, mock.Anything).Return(nil)

		_ = fn(ctx)
	}).Return(nil)

	empty, err := engine.ExecuteNext(context.Background(), workerID)
	assert.NoError(t, err)
	assert.False(t, empty)
	mockStore.AssertNotCalled(t, "RemoveFromQueue", mock.Anything, queueItem.ID)
}

func TestEngine_ExecuteNext_TaskMissingHandler_LogThrottling(t *testing.T) {
	mockTxManager := NewMockTxManager(t)
	mockStore := NewMockStore(t)
	mockStore.EXPECT().GetCancelRequest(mock.Anything, mock.Anything).Return(nil, ErrEntityNotFound).Maybe()

	// Cooldown = 0 for speed; keep default log throttle (>0)
	engine := NewEngine(
		nil,
		WithEngineTxManager(mockTxManager),
		WithEngineStore(mockStore),
		WithMissingHandlerCooldown(0),
		WithMissingHandlerJitterPct(0),
	)
	defer engine.Shutdown()

	workerID := "worker-task-3"
	instanceID := int64(50505)
	stepID1 := int64(903)
	stepID2 := int64(904)
	queueItem1 := &QueueItem{ID: 9903, InstanceID: instanceID, StepID: &stepID1}
	queueItem2 := &QueueItem{ID: 9904, InstanceID: instanceID, StepID: &stepID2}

	instance := &WorkflowInstance{ID: instanceID, WorkflowID: "wf-task-throttle", Status: StatusRunning}
	step1 := WorkflowStep{ID: stepID1, InstanceID: instanceID, StepName: "TaskStep", StepType: StepTypeTask, Status: StepStatusPending}
	step2 := WorkflowStep{ID: stepID2, InstanceID: instanceID, StepName: "TaskStep", StepType: StepTypeTask, Status: StepStatusPending}

	def := &WorkflowDefinition{
		ID:   instance.WorkflowID,
		Name: "wf-task-throttle",
		Definition: GraphDefinition{
			Start: "TaskStep",
			Steps: map[string]*StepDefinition{
				"TaskStep": {Name: "TaskStep", Type: StepTypeTask, Handler: "missing-handler"},
			},
		},
	}

	// First execution: should log
	mockStore.EXPECT().DequeueStep(mock.Anything, workerID).Return(queueItem1, nil)
	mockTxManager.EXPECT().ReadCommitted(mock.Anything, mock.Anything).Run(func(ctx context.Context, fn func(ctx context.Context) error) {
		mockStore.EXPECT().GetInstance(mock.Anything, instanceID).Return(instance, nil)
		mockStore.EXPECT().GetStepsByInstance(mock.Anything, instanceID).Return([]WorkflowStep{step1}, nil)
		mockStore.EXPECT().GetWorkflowDefinition(mock.Anything, instance.WorkflowID).Return(def, nil)
		mockStore.EXPECT().ReleaseQueueItem(mock.Anything, queueItem1.ID).Return(nil)
		mockStore.EXPECT().LogEvent(mock.Anything, instance.ID, mock.Anything, EventStepSkippedMissingHandler, mock.Anything).Return(nil)

		_ = fn(ctx)
	}).Return(nil)

	// Second execution immediately: should NOT log again due to throttling
	mockStore.EXPECT().DequeueStep(mock.Anything, workerID).Return(queueItem2, nil)
	mockTxManager.EXPECT().ReadCommitted(mock.Anything, mock.Anything).Run(func(ctx context.Context, fn func(ctx context.Context) error) {
		mockStore.EXPECT().GetInstance(mock.Anything, instanceID).Return(instance, nil)
		mockStore.EXPECT().GetStepsByInstance(mock.Anything, instanceID).Return([]WorkflowStep{step2}, nil)
		mockStore.EXPECT().GetWorkflowDefinition(mock.Anything, instance.WorkflowID).Return(def, nil)
		mockStore.EXPECT().ReleaseQueueItem(mock.Anything, queueItem2.ID).Return(nil)
		// No LogEvent expectation here — throttled

		_ = fn(ctx)
	}).Return(nil)

	empty, err := engine.ExecuteNext(context.Background(), workerID)
	assert.NoError(t, err)
	assert.False(t, empty)

	empty, err = engine.ExecuteNext(context.Background(), workerID)
	assert.NoError(t, err)
	assert.False(t, empty)

	// Ensure RemoveFromQueue wasn't called for either item
	mockStore.AssertNotCalled(t, "RemoveFromQueue", mock.Anything, mock.Anything)
}

// ==== MakeHumanDecision tests ====

func TestMakeHumanDecision_GetStepError(t *testing.T) {
	mockTxManager := NewMockTxManager(t)
	mockStore := NewMockStore(t)
	mockStore.EXPECT().GetCancelRequest(mock.Anything, mock.Anything).Return(nil, ErrEntityNotFound).Maybe()
	engine := NewEngine(nil, WithEngineTxManager(mockTxManager), WithEngineStore(mockStore))
	defer engine.Shutdown()

	stepID := int64(1)

	mockTxManager.EXPECT().ReadCommitted(mock.Anything, mock.Anything).Run(func(ctx context.Context, fn func(ctx context.Context) error) {
		mockStore.EXPECT().GetStepByID(mock.Anything, stepID).Return(nil, errors.New("db err"))
		_ = fn(ctx)
	}).Return(errors.New("get step: db err"))

	err := engine.MakeHumanDecision(context.Background(), stepID, "user", HumanDecisionConfirmed, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "get step")
}

func TestMakeHumanDecision_NotHumanStep(t *testing.T) {
	mockTxManager := NewMockTxManager(t)
	mockStore := NewMockStore(t)
	mockStore.EXPECT().GetCancelRequest(mock.Anything, mock.Anything).Return(nil, ErrEntityNotFound).Maybe()
	engine := NewEngine(nil, WithEngineTxManager(mockTxManager), WithEngineStore(mockStore))
	defer engine.Shutdown()

	stepID := int64(2)
	step := &WorkflowStep{ID: stepID, StepType: StepTypeTask}

	mockTxManager.EXPECT().ReadCommitted(mock.Anything, mock.Anything).Run(func(ctx context.Context, fn func(ctx context.Context) error) {
		mockStore.EXPECT().GetStepByID(mock.Anything, stepID).Return(step, nil)
		_ = fn(ctx)
	}).Return(errors.New("step 2 is not a human step"))

	err := engine.MakeHumanDecision(context.Background(), stepID, "user", HumanDecisionConfirmed, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not a human step")
}

func TestMakeHumanDecision_NotWaitingStatus(t *testing.T) {
	mockTxManager := NewMockTxManager(t)
	mockStore := NewMockStore(t)
	mockStore.EXPECT().GetCancelRequest(mock.Anything, mock.Anything).Return(nil, ErrEntityNotFound).Maybe()
	engine := NewEngine(nil, WithEngineTxManager(mockTxManager), WithEngineStore(mockStore))
	defer engine.Shutdown()

	stepID := int64(3)
	step := &WorkflowStep{ID: stepID, StepType: StepTypeHuman, Status: StepStatusPending}

	mockTxManager.EXPECT().ReadCommitted(mock.Anything, mock.Anything).Run(func(ctx context.Context, fn func(ctx context.Context) error) {
		mockStore.EXPECT().GetStepByID(mock.Anything, stepID).Return(step, nil)
		_ = fn(ctx)
	}).Return(errors.New("step 3 is not waiting for decision (current status: pending)"))

	err := engine.MakeHumanDecision(context.Background(), stepID, "user", HumanDecisionConfirmed, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not waiting for decision")
}

func TestMakeHumanDecision_CreateDecisionError(t *testing.T) {
	mockTxManager := NewMockTxManager(t)
	mockStore := NewMockStore(t)
	mockStore.EXPECT().GetCancelRequest(mock.Anything, mock.Anything).Return(nil, ErrEntityNotFound).Maybe()
	engine := NewEngine(nil, WithEngineTxManager(mockTxManager), WithEngineStore(mockStore))
	defer engine.Shutdown()

	stepID := int64(4)
	step := &WorkflowStep{ID: stepID, InstanceID: 10, StepName: "H", StepType: StepTypeHuman, Status: StepStatusWaitingDecision}

	mockTxManager.EXPECT().ReadCommitted(mock.Anything, mock.Anything).Run(func(ctx context.Context, fn func(ctx context.Context) error) {
		mockStore.EXPECT().GetStepByID(mock.Anything, stepID).Return(step, nil)
		mockStore.EXPECT().CreateHumanDecision(mock.Anything, mock.Anything).Return(errors.New("create fail"))
		_ = fn(ctx)
	}).Return(errors.New("create human decision: create fail"))

	err := engine.MakeHumanDecision(context.Background(), stepID, "user", HumanDecisionConfirmed, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "create human decision")
}

func TestMakeHumanDecision_UpdateStepStatusError(t *testing.T) {
	mockTxManager := NewMockTxManager(t)
	mockStore := NewMockStore(t)
	mockStore.EXPECT().GetCancelRequest(mock.Anything, mock.Anything).Return(nil, ErrEntityNotFound).Maybe()
	engine := NewEngine(nil, WithEngineTxManager(mockTxManager), WithEngineStore(mockStore))
	defer engine.Shutdown()

	stepID := int64(5)
	step := &WorkflowStep{ID: stepID, InstanceID: 10, StepName: "H", StepType: StepTypeHuman, Status: StepStatusWaitingDecision}

	mockTxManager.EXPECT().ReadCommitted(mock.Anything, mock.Anything).Run(func(ctx context.Context, fn func(ctx context.Context) error) {
		mockStore.EXPECT().GetStepByID(mock.Anything, stepID).Return(step, nil)
		mockStore.EXPECT().CreateHumanDecision(mock.Anything, mock.Anything).Return(nil)
		mockStore.EXPECT().UpdateStepStatus(mock.Anything, stepID, StepStatusConfirmed).Return(errors.New("upd err"))
		// LogEvent may be called, but we won't assert since error occurs before function returns
		_ = fn(ctx)
	}).Return(errors.New("update step status: upd err"))

	err := engine.MakeHumanDecision(context.Background(), stepID, "user", HumanDecisionConfirmed, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "update step status")
}

func TestMakeHumanDecision_Confirmed_PendingInstance_Continues(t *testing.T) {
	mockTxManager := NewMockTxManager(t)
	mockStore := NewMockStore(t)
	mockStore.EXPECT().GetCancelRequest(mock.Anything, mock.Anything).Return(nil, ErrEntityNotFound).Maybe()
	engine := NewEngine(nil, WithEngineTxManager(mockTxManager), WithEngineStore(mockStore))
	defer engine.Shutdown()

	stepID := int64(6)
	instanceID := int64(20)
	step := &WorkflowStep{ID: stepID, InstanceID: instanceID, StepName: "H", StepType: StepTypeHuman, Status: StepStatusWaitingDecision}
	instance := &WorkflowInstance{ID: instanceID, WorkflowID: "wf", Status: StatusPending}
	def := &WorkflowDefinition{ID: instance.WorkflowID, Definition: GraphDefinition{Steps: map[string]*StepDefinition{"H": {Name: "H", Type: StepTypeHuman}}}}

	mockTxManager.EXPECT().ReadCommitted(mock.Anything, mock.Anything).Run(func(ctx context.Context, fn func(ctx context.Context) error) {
		mockStore.EXPECT().GetStepByID(mock.Anything, stepID).Return(step, nil)
		mockStore.EXPECT().CreateHumanDecision(mock.Anything, mock.Anything).Return(nil)
		mockStore.EXPECT().UpdateStepStatus(mock.Anything, stepID, StepStatusConfirmed).Return(nil)
		mockStore.EXPECT().LogEvent(mock.Anything, instanceID, &stepID, EventStepCompleted, mock.Anything).Return(nil).Maybe()
		mockStore.EXPECT().GetInstance(mock.Anything, instanceID).Return(instance, nil)
		mockStore.EXPECT().UpdateInstanceStatus(mock.Anything, instanceID, StatusRunning, mock.Anything, mock.Anything).Return(nil)
		// continueWorkflowAfterHumanDecision -> GetWorkflowDefinition then early-return in handleStepSuccess
		mockStore.EXPECT().GetWorkflowDefinition(mock.Anything, instance.WorkflowID).Return(def, nil)

		_ = fn(ctx)
	}).Return(nil)

	err := engine.MakeHumanDecision(context.Background(), stepID, "user", HumanDecisionConfirmed, nil)
	assert.NoError(t, err)
}

func TestMakeHumanDecision_Confirmed_RunningInstance_NoStatusUpdate(t *testing.T) {
	mockTxManager := NewMockTxManager(t)
	mockStore := NewMockStore(t)
	mockStore.EXPECT().GetCancelRequest(mock.Anything, mock.Anything).Return(nil, ErrEntityNotFound).Maybe()
	engine := NewEngine(nil, WithEngineTxManager(mockTxManager), WithEngineStore(mockStore))
	defer engine.Shutdown()

	stepID := int64(7)
	instanceID := int64(21)
	step := &WorkflowStep{ID: stepID, InstanceID: instanceID, StepName: "H", StepType: StepTypeHuman, Status: StepStatusWaitingDecision}
	instance := &WorkflowInstance{ID: instanceID, WorkflowID: "wf", Status: StatusRunning}
	def := &WorkflowDefinition{ID: instance.WorkflowID, Definition: GraphDefinition{Steps: map[string]*StepDefinition{"H": {Name: "H", Type: StepTypeHuman}}}}

	mockTxManager.EXPECT().ReadCommitted(mock.Anything, mock.Anything).Run(func(ctx context.Context, fn func(ctx context.Context) error) {
		mockStore.EXPECT().GetStepByID(mock.Anything, stepID).Return(step, nil)
		mockStore.EXPECT().CreateHumanDecision(mock.Anything, mock.Anything).Return(nil)
		mockStore.EXPECT().UpdateStepStatus(mock.Anything, stepID, StepStatusConfirmed).Return(nil)
		mockStore.EXPECT().LogEvent(mock.Anything, instanceID, &stepID, EventStepCompleted, mock.Anything).Return(nil).Maybe()
		mockStore.EXPECT().GetInstance(mock.Anything, instanceID).Return(instance, nil)
		// No UpdateInstanceStatus expected
		mockStore.EXPECT().GetWorkflowDefinition(mock.Anything, instance.WorkflowID).Return(def, nil)
		_ = fn(ctx)
	}).Return(nil)

	err := engine.MakeHumanDecision(context.Background(), stepID, "user", HumanDecisionConfirmed, nil)
	assert.NoError(t, err)
}

func TestMakeHumanDecision_Confirmed_GetInstanceError(t *testing.T) {
	mockTxManager := NewMockTxManager(t)
	mockStore := NewMockStore(t)
	mockStore.EXPECT().GetCancelRequest(mock.Anything, mock.Anything).Return(nil, ErrEntityNotFound).Maybe()
	engine := NewEngine(nil, WithEngineTxManager(mockTxManager), WithEngineStore(mockStore))
	defer engine.Shutdown()

	stepID := int64(8)
	instanceID := int64(22)
	step := &WorkflowStep{ID: stepID, InstanceID: instanceID, StepName: "H", StepType: StepTypeHuman, Status: StepStatusWaitingDecision}

	mockTxManager.EXPECT().ReadCommitted(mock.Anything, mock.Anything).Run(func(ctx context.Context, fn func(ctx context.Context) error) {
		mockStore.EXPECT().GetStepByID(mock.Anything, stepID).Return(step, nil)
		mockStore.EXPECT().CreateHumanDecision(mock.Anything, mock.Anything).Return(nil)
		mockStore.EXPECT().UpdateStepStatus(mock.Anything, stepID, StepStatusConfirmed).Return(nil)
		mockStore.EXPECT().LogEvent(mock.Anything, instanceID, &stepID, EventStepCompleted, mock.Anything).Return(nil).Maybe()
		mockStore.EXPECT().GetInstance(mock.Anything, instanceID).Return(nil, errors.New("get inst err"))
		_ = fn(ctx)
	}).Return(errors.New("get instance: get inst err"))

	err := engine.MakeHumanDecision(context.Background(), stepID, "user", HumanDecisionConfirmed, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "get instance")
}

func TestMakeHumanDecision_Confirmed_UpdateInstanceStatusError(t *testing.T) {
	mockTxManager := NewMockTxManager(t)
	mockStore := NewMockStore(t)
	mockStore.EXPECT().GetCancelRequest(mock.Anything, mock.Anything).Return(nil, ErrEntityNotFound).Maybe()
	engine := NewEngine(nil, WithEngineTxManager(mockTxManager), WithEngineStore(mockStore))
	defer engine.Shutdown()

	stepID := int64(9)
	instanceID := int64(23)
	step := &WorkflowStep{ID: stepID, InstanceID: instanceID, StepName: "H", StepType: StepTypeHuman, Status: StepStatusWaitingDecision}
	instance := &WorkflowInstance{ID: instanceID, WorkflowID: "wf", Status: StatusPending}

	mockTxManager.EXPECT().ReadCommitted(mock.Anything, mock.Anything).Run(func(ctx context.Context, fn func(ctx context.Context) error) {
		mockStore.EXPECT().GetStepByID(mock.Anything, stepID).Return(step, nil)
		mockStore.EXPECT().CreateHumanDecision(mock.Anything, mock.Anything).Return(nil)
		mockStore.EXPECT().UpdateStepStatus(mock.Anything, stepID, StepStatusConfirmed).Return(nil)
		mockStore.EXPECT().LogEvent(mock.Anything, instanceID, &stepID, EventStepCompleted, mock.Anything).Return(nil).Maybe()
		mockStore.EXPECT().GetInstance(mock.Anything, instanceID).Return(instance, nil)
		mockStore.EXPECT().UpdateInstanceStatus(mock.Anything, instanceID, StatusRunning, mock.Anything, mock.Anything).Return(errors.New("upd inst"))
		_ = fn(ctx)
	}).Return(errors.New("update instance status: upd inst"))

	err := engine.MakeHumanDecision(context.Background(), stepID, "user", HumanDecisionConfirmed, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "update instance status")
}

func TestMakeHumanDecision_Confirmed_ContinueError_GetDef(t *testing.T) {
	mockTxManager := NewMockTxManager(t)
	mockStore := NewMockStore(t)
	mockStore.EXPECT().GetCancelRequest(mock.Anything, mock.Anything).Return(nil, ErrEntityNotFound).Maybe()
	engine := NewEngine(nil, WithEngineTxManager(mockTxManager), WithEngineStore(mockStore))
	defer engine.Shutdown()

	stepID := int64(10)
	instanceID := int64(24)
	step := &WorkflowStep{ID: stepID, InstanceID: instanceID, StepName: "H", StepType: StepTypeHuman, Status: StepStatusWaitingDecision}
	instance := &WorkflowInstance{ID: instanceID, WorkflowID: "wf", Status: StatusRunning}

	mockTxManager.EXPECT().ReadCommitted(mock.Anything, mock.Anything).Run(func(ctx context.Context, fn func(ctx context.Context) error) {
		mockStore.EXPECT().GetStepByID(mock.Anything, stepID).Return(step, nil)
		mockStore.EXPECT().CreateHumanDecision(mock.Anything, mock.Anything).Return(nil)
		mockStore.EXPECT().UpdateStepStatus(mock.Anything, stepID, StepStatusConfirmed).Return(nil)
		mockStore.EXPECT().LogEvent(mock.Anything, instanceID, &stepID, EventStepCompleted, mock.Anything).Return(nil).Maybe()
		mockStore.EXPECT().GetInstance(mock.Anything, instanceID).Return(instance, nil)
		mockStore.EXPECT().GetWorkflowDefinition(mock.Anything, instance.WorkflowID).Return(nil, errors.New("no def"))
		_ = fn(ctx)
	}).Return(errors.New("get workflow definition: no def"))

	err := engine.MakeHumanDecision(context.Background(), stepID, "user", HumanDecisionConfirmed, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "get workflow definition")
}

func TestMakeHumanDecision_Rejected_AbortsWorkflow(t *testing.T) {
	mockTxManager := NewMockTxManager(t)
	mockStore := NewMockStore(t)
	mockStore.EXPECT().GetCancelRequest(mock.Anything, mock.Anything).Return(nil, ErrEntityNotFound).Maybe()
	engine := NewEngine(nil, WithEngineTxManager(mockTxManager), WithEngineStore(mockStore))
	defer engine.Shutdown()

	stepID := int64(11)
	instanceID := int64(25)
	step := &WorkflowStep{ID: stepID, InstanceID: instanceID, StepName: "H", StepType: StepTypeHuman, Status: StepStatusWaitingDecision}

	mockTxManager.EXPECT().ReadCommitted(mock.Anything, mock.Anything).Run(func(ctx context.Context, fn func(ctx context.Context) error) {
		mockStore.EXPECT().GetStepByID(mock.Anything, stepID).Return(step, nil)
		mockStore.EXPECT().CreateHumanDecision(mock.Anything, mock.Anything).Return(nil)
		mockStore.EXPECT().UpdateStepStatus(mock.Anything, stepID, StepStatusRejected).Return(nil)
		mockStore.EXPECT().LogEvent(mock.Anything, instanceID, &stepID, EventStepCompleted, mock.Anything).Return(nil).Maybe()
		mockStore.EXPECT().UpdateInstanceStatus(mock.Anything, instanceID, StatusAborted, mock.Anything, mock.Anything).Return(nil)
		_ = fn(ctx)
	}).Return(nil)

	err := engine.MakeHumanDecision(context.Background(), stepID, "user", HumanDecisionRejected, nil)
	assert.NoError(t, err)
}

func TestMakeHumanDecision_Rejected_UpdateInstanceStatusError(t *testing.T) {
	mockTxManager := NewMockTxManager(t)
	mockStore := NewMockStore(t)
	mockStore.EXPECT().GetCancelRequest(mock.Anything, mock.Anything).Return(nil, ErrEntityNotFound).Maybe()
	engine := NewEngine(nil, WithEngineTxManager(mockTxManager), WithEngineStore(mockStore))
	defer engine.Shutdown()

	stepID := int64(12)
	instanceID := int64(26)
	step := &WorkflowStep{ID: stepID, InstanceID: instanceID, StepName: "H", StepType: StepTypeHuman, Status: StepStatusWaitingDecision}

	mockTxManager.EXPECT().ReadCommitted(mock.Anything, mock.Anything).Run(func(ctx context.Context, fn func(ctx context.Context) error) {
		mockStore.EXPECT().GetStepByID(mock.Anything, stepID).Return(step, nil)
		mockStore.EXPECT().CreateHumanDecision(mock.Anything, mock.Anything).Return(nil)
		mockStore.EXPECT().UpdateStepStatus(mock.Anything, stepID, StepStatusRejected).Return(nil)
		mockStore.EXPECT().LogEvent(mock.Anything, instanceID, &stepID, EventStepCompleted, mock.Anything).Return(nil).Maybe()
		mockStore.EXPECT().UpdateInstanceStatus(mock.Anything, instanceID, StatusAborted, mock.Anything, mock.Anything).Return(errors.New("fail"))
		_ = fn(ctx)
	}).Return(errors.New("fail"))

	err := engine.MakeHumanDecision(context.Background(), stepID, "user", HumanDecisionRejected, nil)
	assert.Error(t, err)
}

// ==== CancelWorkflow & AbortWorkflow unit tests ====

func TestCancelWorkflow_GetInstanceError(t *testing.T) {
	mockTxManager := NewMockTxManager(t)
	mockStore := NewMockStore(t)
	mockStore.EXPECT().GetCancelRequest(mock.Anything, mock.Anything).Return(nil, ErrEntityNotFound).Maybe()
	engine := NewEngine(nil, WithEngineTxManager(mockTxManager), WithEngineStore(mockStore))
	defer engine.Shutdown()

	ctx := context.Background()
	instanceID := int64(1001)

	mockTxManager.EXPECT().ReadCommitted(mock.Anything, mock.Anything).Run(func(ctx context.Context, fn func(ctx context.Context) error) {
		mockStore.EXPECT().GetInstance(mock.Anything, instanceID).Return(nil, errors.New("db err"))
		_ = fn(ctx)
	}).Return(errors.New("get instance: db err"))

	err := engine.CancelWorkflow(ctx, instanceID, "user", "because")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "get instance")
}

func TestCancelWorkflow_TerminalStateError(t *testing.T) {
	mockTxManager := NewMockTxManager(t)
	mockStore := NewMockStore(t)
	mockStore.EXPECT().GetCancelRequest(mock.Anything, mock.Anything).Return(nil, ErrEntityNotFound).Maybe()
	engine := NewEngine(nil, WithEngineTxManager(mockTxManager), WithEngineStore(mockStore))
	defer engine.Shutdown()

	ctx := context.Background()
	instanceID := int64(1002)
	inst := &WorkflowInstance{ID: instanceID, Status: StatusCompleted}

	mockTxManager.EXPECT().ReadCommitted(mock.Anything, mock.Anything).Run(func(ctx context.Context, fn func(ctx context.Context) error) {
		mockStore.EXPECT().GetInstance(mock.Anything, instanceID).Return(inst, nil)
		_ = fn(ctx)
	}).Return(errors.New("workflow 1002 is already in terminal state: completed"))

	err := engine.CancelWorkflow(ctx, instanceID, "user", "because")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already in terminal state")
}

func TestCancelWorkflow_CreateCancelRequestError(t *testing.T) {
	mockTxManager := NewMockTxManager(t)
	mockStore := NewMockStore(t)
	mockStore.EXPECT().GetCancelRequest(mock.Anything, mock.Anything).Return(nil, ErrEntityNotFound).Maybe()
	engine := NewEngine(nil, WithEngineTxManager(mockTxManager), WithEngineStore(mockStore))
	defer engine.Shutdown()

	ctx := context.Background()
	instanceID := int64(1003)
	inst := &WorkflowInstance{ID: instanceID, Status: StatusRunning}

	mockTxManager.EXPECT().ReadCommitted(mock.Anything, mock.Anything).Run(func(ctx context.Context, fn func(ctx context.Context) error) {
		mockStore.EXPECT().GetInstance(mock.Anything, instanceID).Return(inst, nil)
		mockStore.EXPECT().CreateCancelRequest(mock.Anything, mock.Anything).Return(errors.New("insert err"))
		_ = fn(ctx)
	}).Return(errors.New("create cancel request: insert err"))

	err := engine.CancelWorkflow(ctx, instanceID, "user", "because")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "create cancel request")
}

func TestCancelWorkflow_Success(t *testing.T) {
	mockTxManager := NewMockTxManager(t)
	mockStore := NewMockStore(t)
	mockStore.EXPECT().GetCancelRequest(mock.Anything, mock.Anything).Return(nil, ErrEntityNotFound).Maybe()
	engine := NewEngine(nil, WithEngineTxManager(mockTxManager), WithEngineStore(mockStore))
	defer engine.Shutdown()

	ctx := context.Background()
	instanceID := int64(1004)
	inst := &WorkflowInstance{ID: instanceID, Status: StatusRunning}
	requestedBy := "user"
	reason := "cancel reason"

	mockTxManager.EXPECT().ReadCommitted(mock.Anything, mock.Anything).Run(func(ctx context.Context, fn func(ctx context.Context) error) {
		mockStore.EXPECT().GetInstance(mock.Anything, instanceID).Return(inst, nil)
		mockStore.EXPECT().CreateCancelRequest(mock.Anything, mock.MatchedBy(func(req *WorkflowCancelRequest) bool {
			return req.InstanceID == instanceID && req.RequestedBy == requestedBy && req.CancelType == CancelTypeCancel && req.Reason != nil && *req.Reason == reason
		})).Return(nil)
		mockStore.EXPECT().LogEvent(mock.Anything, instanceID, mock.Anything, EventCancellationStarted, mock.MatchedBy(func(data map[string]any) bool { return data[KeyCancelType] == CancelTypeCancel })).Return(nil)
		_ = fn(ctx)
	}).Return(nil)

	err := engine.CancelWorkflow(ctx, instanceID, requestedBy, reason)
	assert.NoError(t, err)
}

func TestAbortWorkflow_GetInstanceError(t *testing.T) {
	mockTxManager := NewMockTxManager(t)
	mockStore := NewMockStore(t)
	mockStore.EXPECT().GetCancelRequest(mock.Anything, mock.Anything).Return(nil, ErrEntityNotFound).Maybe()
	engine := NewEngine(nil, WithEngineTxManager(mockTxManager), WithEngineStore(mockStore))
	defer engine.Shutdown()

	ctx := context.Background()
	instanceID := int64(2001)

	mockTxManager.EXPECT().ReadCommitted(mock.Anything, mock.Anything).Run(func(ctx context.Context, fn func(ctx context.Context) error) {
		mockStore.EXPECT().GetInstance(mock.Anything, instanceID).Return(nil, errors.New("db err"))
		_ = fn(ctx)
	}).Return(errors.New("get instance: db err"))

	err := engine.AbortWorkflow(ctx, instanceID, "user", "fatal")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "get instance")
}

func TestAbortWorkflow_TerminalStateError(t *testing.T) {
	mockTxManager := NewMockTxManager(t)
	mockStore := NewMockStore(t)
	mockStore.EXPECT().GetCancelRequest(mock.Anything, mock.Anything).Return(nil, ErrEntityNotFound).Maybe()
	engine := NewEngine(nil, WithEngineTxManager(mockTxManager), WithEngineStore(mockStore))
	defer engine.Shutdown()

	ctx := context.Background()
	instanceID := int64(2002)
	inst := &WorkflowInstance{ID: instanceID, Status: StatusCancelled}

	mockTxManager.EXPECT().ReadCommitted(mock.Anything, mock.Anything).Run(func(ctx context.Context, fn func(ctx context.Context) error) {
		mockStore.EXPECT().GetInstance(mock.Anything, instanceID).Return(inst, nil)
		_ = fn(ctx)
	}).Return(errors.New("workflow 2002 is already in terminal state: cancelled"))

	err := engine.AbortWorkflow(ctx, instanceID, "user", "fatal")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already in terminal state")
}

func TestAbortWorkflow_CreateCancelRequestError(t *testing.T) {
	mockTxManager := NewMockTxManager(t)
	mockStore := NewMockStore(t)
	mockStore.EXPECT().GetCancelRequest(mock.Anything, mock.Anything).Return(nil, ErrEntityNotFound).Maybe()
	engine := NewEngine(nil, WithEngineTxManager(mockTxManager), WithEngineStore(mockStore))
	defer engine.Shutdown()

	ctx := context.Background()
	instanceID := int64(2003)
	inst := &WorkflowInstance{ID: instanceID, Status: StatusRunning}

	mockTxManager.EXPECT().ReadCommitted(mock.Anything, mock.Anything).Run(func(ctx context.Context, fn func(ctx context.Context) error) {
		mockStore.EXPECT().GetInstance(mock.Anything, instanceID).Return(inst, nil)
		mockStore.EXPECT().CreateCancelRequest(mock.Anything, mock.Anything).Return(errors.New("insert err"))
		_ = fn(ctx)
	}).Return(errors.New("create cancel request: insert err"))

	err := engine.AbortWorkflow(ctx, instanceID, "user", "fatal")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "create cancel request")
}

func TestAbortWorkflow_Success(t *testing.T) {
	mockTxManager := NewMockTxManager(t)
	mockStore := NewMockStore(t)
	mockStore.EXPECT().GetCancelRequest(mock.Anything, mock.Anything).Return(nil, ErrEntityNotFound).Maybe()
	engine := NewEngine(nil, WithEngineTxManager(mockTxManager), WithEngineStore(mockStore))
	defer engine.Shutdown()

	ctx := context.Background()
	instanceID := int64(2004)
	inst := &WorkflowInstance{ID: instanceID, Status: StatusRunning}
	requestedBy := "user"
	reason := "fatal reason"

	mockTxManager.EXPECT().ReadCommitted(mock.Anything, mock.Anything).Run(func(ctx context.Context, fn func(ctx context.Context) error) {
		mockStore.EXPECT().GetInstance(mock.Anything, instanceID).Return(inst, nil)
		mockStore.EXPECT().CreateCancelRequest(mock.Anything, mock.MatchedBy(func(req *WorkflowCancelRequest) bool {
			return req.InstanceID == instanceID && req.RequestedBy == requestedBy && req.CancelType == CancelTypeAbort && req.Reason != nil && *req.Reason == reason
		})).Return(nil)
		mockStore.EXPECT().LogEvent(mock.Anything, instanceID, mock.Anything, EventAbortStarted, mock.MatchedBy(func(data map[string]any) bool { return data[KeyCancelType] == CancelTypeAbort })).Return(nil)
		_ = fn(ctx)
	}).Return(nil)

	err := engine.AbortWorkflow(ctx, instanceID, requestedBy, reason)
	assert.NoError(t, err)
}
