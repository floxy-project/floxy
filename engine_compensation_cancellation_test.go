package floxy

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// ========================= executeCompensationStep =========================

func Test_executeCompensationStep_NoOnFailure_MarksRolledBack(t *testing.T) {
	ctx := context.Background()
	engine, store := newTestEngineWithStore(t)

	def := &WorkflowDefinition{
		ID:   "wf_comp_1",
		Name: "wf_comp_1",
		Definition: GraphDefinition{
			Start: "A",
			Steps: map[string]*StepDefinition{
				"A": {Name: "A", Type: StepTypeTask, OnFailure: "", Prev: rootStepName},
			},
		},
	}

	step := &WorkflowStep{ID: 11, InstanceID: 111, StepName: "A", StepType: StepTypeTask, Input: json.RawMessage(`{"n":1}`)}

	store.EXPECT().GetWorkflowDefinition(mock.Anything, def.ID).Return(def, nil)
	store.EXPECT().UpdateStep(mock.Anything, step.ID, StepStatusRolledBack, step.Input, (*string)(nil)).Return(nil)

	instance := &WorkflowInstance{ID: step.InstanceID, WorkflowID: def.ID}
	err := engine.executeCompensationStep(ctx, instance, step, nil)
	assert.NoError(t, err)
}

func Test_executeCompensationStep_OnFailureHandlerMissing_MarksRolledBack(t *testing.T) {
	ctx := context.Background()
	engine, store := newTestEngineWithStore(t)

	def := &WorkflowDefinition{
		ID:   "wf_comp_2",
		Name: "wf_comp_2",
		Definition: GraphDefinition{
			Start: "B",
			Steps: map[string]*StepDefinition{
				"B":      {Name: "B", Type: StepTypeTask, OnFailure: "B_comp", Prev: rootStepName},
				"B_comp": {Name: "B_comp", Type: StepTypeTask, Handler: "B_comp", MaxRetries: 0},
			},
		},
	}

	step := &WorkflowStep{ID: 12, InstanceID: 222, StepName: "B", StepType: StepTypeTask}

	store.EXPECT().GetWorkflowDefinition(mock.Anything, def.ID).Return(def, nil)
	// No handler registered in engine.handlers -> should mark rolled back
	store.EXPECT().UpdateStep(mock.Anything, step.ID, StepStatusRolledBack, step.Input, (*string)(nil)).Return(nil)

	instance := &WorkflowInstance{ID: step.InstanceID, WorkflowID: def.ID}
	err := engine.executeCompensationStep(ctx, instance, step, nil)
	assert.NoError(t, err)
}

func Test_executeCompensationStep_HandlerSuccess_RolledBackAndEvent(t *testing.T) {
	ctx := context.Background()
	engine, store := newTestEngineWithStore(t)

	// Register a mock handler that succeeds
	mockHandler := NewMockStepHandler(t)
	mockHandler.EXPECT().Name().Return("B_comp").Maybe()
	mockHandler.EXPECT().Execute(mock.Anything, mock.Anything, mock.Anything).Return(json.RawMessage(`{"ok":true}`), nil)
	engine.RegisterHandler(mockHandler)

	def := &WorkflowDefinition{
		ID:   "wf_comp_3",
		Name: "wf_comp_3",
		Definition: GraphDefinition{
			Start: "B",
			Steps: map[string]*StepDefinition{
				"B":      {Name: "B", Type: StepTypeTask, OnFailure: "B_comp", Prev: rootStepName},
				"B_comp": {Name: "B_comp", Type: StepTypeTask, Handler: "B_comp", MaxRetries: 3},
			},
		},
	}
	step := &WorkflowStep{ID: 13, InstanceID: 333, StepName: "B", StepType: StepTypeTask}

	store.EXPECT().GetWorkflowDefinition(mock.Anything, def.ID).Return(def, nil)
	store.EXPECT().UpdateStep(mock.Anything, step.ID, StepStatusRolledBack, step.Input, (*string)(nil)).Return(nil)
	store.EXPECT().LogEvent(mock.Anything, step.InstanceID, &step.ID, EventStepCompleted, mock.Anything).Return(nil).Maybe()
	store.EXPECT().GetStepsByInstance(mock.Anything, step.InstanceID).Return(
		[]WorkflowStep{{InstanceID: step.InstanceID, StepName: "B", Status: StepStatusCompleted}}, nil,
	).Times(3)

	instance := &WorkflowInstance{ID: step.InstanceID, WorkflowID: def.ID}
	err := engine.executeCompensationStep(ctx, instance, step, nil)
	assert.NoError(t, err)
}

func Test_executeCompensationStep_HandlerFails_WithRetry(t *testing.T) {
	ctx := context.Background()
	engine, store := newTestEngineWithStore(t)

	// Handler that fails
	mockHandler := NewMockStepHandler(t)
	mockHandler.EXPECT().Name().Return("C_comp").Maybe()
	mockHandler.EXPECT().Execute(mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("boom"))
	engine.RegisterHandler(mockHandler)

	def := &WorkflowDefinition{
		ID:   "wf_comp_4",
		Name: "wf_comp_4",
		Definition: GraphDefinition{
			Start: "C",
			Steps: map[string]*StepDefinition{
				"C":      {Name: "C", Type: StepTypeTask, OnFailure: "C_comp", Delay: 0, Prev: rootStepName},
				"C_comp": {Name: "C_comp", Type: StepTypeTask, Handler: "C_comp", MaxRetries: 2},
			},
		},
	}
	step := &WorkflowStep{ID: 14, InstanceID: 444, StepName: "C", StepType: StepTypeTask, CompensationRetryCount: 0}

	store.EXPECT().GetWorkflowDefinition(mock.Anything, def.ID).Return(def, nil)
	store.EXPECT().UpdateStepCompensationRetry(mock.Anything, step.ID, 1, StepStatusCompensation).Return(nil)
	store.EXPECT().EnqueueStep(mock.Anything, step.InstanceID, &step.ID, PriorityHigh, mock.Anything).Return(nil)
	store.EXPECT().LogEvent(mock.Anything, step.InstanceID, &step.ID, EventStepFailed, mock.Anything).Return(nil).Maybe()

	instance := &WorkflowInstance{ID: step.InstanceID, WorkflowID: def.ID}
	err := engine.executeCompensationStep(ctx, instance, step, nil)
	assert.NoError(t, err)
}

func Test_executeCompensationStep_HandlerFails_MaxRetriesExceeded(t *testing.T) {
	ctx := context.Background()
	engine, store := newTestEngineWithStore(t)

	mockHandler := NewMockStepHandler(t)
	mockHandler.EXPECT().Name().Return("D_comp").Maybe()
	mockHandler.EXPECT().Execute(mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("fail"))
	engine.RegisterHandler(mockHandler)

	def := &WorkflowDefinition{
		ID:   "wf_comp_5",
		Name: "wf_comp_5",
		Definition: GraphDefinition{
			Start: "D",
			Steps: map[string]*StepDefinition{
				"D":      {Name: "D", Type: StepTypeTask, OnFailure: "D_comp", Prev: rootStepName},
				"D_comp": {Name: "D_comp", Type: StepTypeTask, Handler: "D_comp", MaxRetries: 0},
			},
		},
	}
	step := &WorkflowStep{ID: 15, InstanceID: 555, StepName: "D", StepType: StepTypeTask, CompensationRetryCount: 0}

	store.EXPECT().GetWorkflowDefinition(mock.Anything, def.ID).Return(def, nil)
	store.EXPECT().UpdateStep(mock.Anything, step.ID, StepStatusFailed, step.Input, mock.AnythingOfType("*string")).Return(nil)
	store.EXPECT().LogEvent(mock.Anything, step.InstanceID, &step.ID, EventStepFailed, mock.Anything).Return(nil).Maybe()

	instance := &WorkflowInstance{ID: step.InstanceID, WorkflowID: def.ID}
	err := engine.executeCompensationStep(ctx, instance, step, nil)
	assert.NoError(t, err)
}

// ============================== handleCancellation ==============================

func Test_handleCancellation_Cancel_NoCompletedSteps_SetsCancelled(t *testing.T) {
	ctx := context.Background()
	engine, store := newTestEngineWithStore(t)

	def := &WorkflowDefinition{ID: "wf_cancel_1", Definition: GraphDefinition{Steps: map[string]*StepDefinition{}}}
	instance := &WorkflowInstance{ID: 9001, WorkflowID: def.ID, Status: StatusRunning}
	step := &WorkflowStep{ID: 901, InstanceID: instance.ID, StepName: "cur", StepType: StepTypeTask}
	reason := "user requested"
	req := &WorkflowCancelRequest{InstanceID: instance.ID, RequestedBy: "alice", CancelType: CancelTypeCancel, Reason: &reason}

	// stopActiveSteps: no active steps
	store.EXPECT().GetActiveStepsForUpdate(mock.Anything, instance.ID).Return([]WorkflowStep{}, nil)

	// Update to cancelling
	store.EXPECT().UpdateInstanceStatus(mock.Anything, instance.ID, StatusCancelling, json.RawMessage(nil), (*string)(nil)).Return(nil)
	store.EXPECT().GetWorkflowDefinition(mock.Anything, def.ID).Return(def, nil)
	// No steps completed
	store.EXPECT().GetStepsByInstance(mock.Anything, instance.ID).Return([]WorkflowStep{
		{InstanceID: instance.ID, StepName: "A", Status: StepStatusFailed},
		{InstanceID: instance.ID, StepName: "B", Status: StepStatusPending},
	}, nil)

	// Final cancelled status and event
	store.EXPECT().UpdateInstanceStatus(mock.Anything, instance.ID, StatusCancelled, json.RawMessage(nil), mock.AnythingOfType("*string")).Return(nil)
	store.EXPECT().LogEvent(mock.Anything, instance.ID, &step.ID, EventWorkflowCancelled, mock.Anything).Return(nil).Maybe()
	store.EXPECT().DeleteCancelRequest(mock.Anything, instance.ID).Return(nil)

	err := engine.handleCancellation(ctx, instance, step, req)
	assert.NoError(t, err)
}

func Test_handleCancellation_Cancel_RollbackFails_SetsFailed(t *testing.T) {
	ctx := context.Background()
	engine, store := newTestEngineWithStore(t)

	// Definition WITHOUT the completed step to force rollback error
	def := &WorkflowDefinition{ID: "wf_cancel_2", Definition: GraphDefinition{Steps: map[string]*StepDefinition{
		"S1": {Name: "S1", Type: StepTypeTask, Prev: rootStepName},
	}}}
	instance := &WorkflowInstance{ID: 9002, WorkflowID: def.ID, Status: StatusRunning}
	step := &WorkflowStep{ID: 902, InstanceID: instance.ID, StepName: "cur", StepType: StepTypeTask}
	reason := "cleanup"
	req := &WorkflowCancelRequest{InstanceID: instance.ID, RequestedBy: "bob", CancelType: CancelTypeCancel, Reason: &reason}

	store.EXPECT().GetActiveStepsForUpdate(mock.Anything, instance.ID).Return([]WorkflowStep{}, nil)
	store.EXPECT().UpdateInstanceStatus(mock.Anything, instance.ID, StatusCancelling, json.RawMessage(nil), (*string)(nil)).Return(nil)
	store.EXPECT().GetWorkflowDefinition(mock.Anything, def.ID).Return(def, nil)
	// Steps include a completed step X that is missing in def.Steps -> rollback will fail with step definition not found
	store.EXPECT().GetStepsByInstance(mock.Anything, instance.ID).Return([]WorkflowStep{
		{InstanceID: instance.ID, StepName: "S1", Status: StepStatusFailed},
		{InstanceID: instance.ID, StepName: "X", Status: StepStatusCompleted}, // last completed
	}, nil)

	// On rollback failure, engine logs cancel event with error, sets instance to failed, deletes cancel request
	store.EXPECT().LogEvent(mock.Anything, instance.ID, &step.ID, EventWorkflowCancelled, mock.Anything).Return(nil).Maybe()
	store.EXPECT().UpdateInstanceStatus(mock.Anything, instance.ID, StatusFailed, json.RawMessage(nil), mock.AnythingOfType("*string")).Return(nil)
	store.EXPECT().DeleteCancelRequest(mock.Anything, instance.ID).Return(nil)

	err := engine.handleCancellation(ctx, instance, step, req)
	assert.Error(t, err)
}

func Test_handleCancellation_Abort_SetsAborted(t *testing.T) {
	ctx := context.Background()
	engine, store := newTestEngineWithStore(t)

	def := &WorkflowDefinition{ID: "wf_cancel_3", Definition: GraphDefinition{Steps: map[string]*StepDefinition{}}}
	instance := &WorkflowInstance{ID: 9003, WorkflowID: def.ID, Status: StatusRunning}
	step := &WorkflowStep{ID: 903, InstanceID: instance.ID, StepName: "cur", StepType: StepTypeTask}
	reason := "not needed"
	req := &WorkflowCancelRequest{InstanceID: instance.ID, RequestedBy: "zoe", CancelType: CancelTypeAbort, Reason: &reason}

	store.EXPECT().GetActiveStepsForUpdate(mock.Anything, instance.ID).Return([]WorkflowStep{}, nil)
	store.EXPECT().UpdateInstanceStatus(mock.Anything, instance.ID, StatusAborted, json.RawMessage(nil), mock.AnythingOfType("*string")).Return(nil)
	store.EXPECT().LogEvent(mock.Anything, instance.ID, &step.ID, EventWorkflowAborted, mock.Anything).Return(nil).Maybe()
	store.EXPECT().DeleteCancelRequest(mock.Anything, instance.ID).Return(nil)

	err := engine.handleCancellation(ctx, instance, step, req)
	assert.NoError(t, err)
}
