package floxy

import (
	"context"
	"encoding/json"
	"time"
)

type Store interface {
	SetAgingEnabled(enabled bool)
	SetAgingRate(rate float64)
	SaveWorkflowDefinition(ctx context.Context, def *WorkflowDefinition) error
	GetWorkflowDefinition(ctx context.Context, id string) (*WorkflowDefinition, error)
	CreateInstance(
		ctx context.Context,
		workflowID string,
		input json.RawMessage,
	) (*WorkflowInstance, error)
	UpdateInstanceStatus(
		ctx context.Context,
		instanceID int64,
		status WorkflowStatus,
		output json.RawMessage,
		errMsg *string,
	) error
	GetInstance(ctx context.Context, instanceID int64) (*WorkflowInstance, error)
	CreateStep(ctx context.Context, step *WorkflowStep) error
	UpdateStep(
		ctx context.Context,
		stepID int64,
		status StepStatus,
		output json.RawMessage,
		errMsg *string,
	) error
	GetStepsByInstance(ctx context.Context, instanceID int64) ([]WorkflowStep, error)
	EnqueueStep(
		ctx context.Context,
		instanceID int64,
		stepID *int64,
		priority Priority,
		delay time.Duration,
	) error
	UpdateStepCompensationRetry(
		ctx context.Context,
		stepID int64,
		retryCount int,
		status StepStatus,
	) error
	DequeueStep(ctx context.Context, workerID string) (*QueueItem, error)
	ExtendQueueItemLock(ctx context.Context, queueID int64, lockToken string, ttl time.Duration) (bool, error)
	QueueItemLockStillOwned(ctx context.Context, queueID int64, lockToken string) (bool, error)
	RemoveFromQueue(ctx context.Context, queueID int64) error
	ReleaseQueueItem(ctx context.Context, queueID int64) error
	RescheduleAndReleaseQueueItem(ctx context.Context, queueID int64, delay time.Duration) error
	LogEvent(
		ctx context.Context,
		instanceID int64,
		stepID *int64,
		eventType string,
		payload any,
	) error
	CreateJoinState(
		ctx context.Context,
		instanceID int64,
		joinStepName string,
		waitingFor []string,
		strategy JoinStrategy,
	) error
	UpdateJoinState(
		ctx context.Context,
		instanceID int64,
		joinStepName, completedStep string,
		success bool,
	) (bool, error)
	GetJoinState(ctx context.Context, instanceID int64, joinStepName string) (*JoinState, error)
	AddToJoinWaitFor(ctx context.Context, instanceID int64, joinStepName, stepToAdd string) error
	ReplaceInJoinWaitFor(ctx context.Context, instanceID int64, joinStepName, virtualStep, realStep string) error
	GetSummaryStats(ctx context.Context) (*SummaryStats, error)
	GetActiveInstances(ctx context.Context) ([]ActiveWorkflowInstance, error)
	GetWorkflowDefinitions(ctx context.Context) ([]WorkflowDefinition, error)
	GetWorkflowInstances(ctx context.Context, workflowID string) ([]WorkflowInstance, error)
	GetAllWorkflowInstances(ctx context.Context) ([]WorkflowInstance, error)
	GetWorkflowInstancesPaginated(ctx context.Context, workflowID string, offset int, limit int) ([]WorkflowInstance, int64, error)
	GetAllWorkflowInstancesPaginated(ctx context.Context, offset int, limit int) ([]WorkflowInstance, int64, error)
	GetWorkflowSteps(ctx context.Context, instanceID int64) ([]WorkflowStep, error)
	GetWorkflowEvents(ctx context.Context, instanceID int64) ([]WorkflowEvent, error)
	GetWorkflowStats(ctx context.Context) ([]WorkflowStats, error)
	GetActiveStepsForUpdate(ctx context.Context, instanceID int64) ([]WorkflowStep, error)
	CreateCancelRequest(ctx context.Context, req *WorkflowCancelRequest) error
	GetCancelRequest(ctx context.Context, instanceID int64) (*WorkflowCancelRequest, error)
	DeleteCancelRequest(ctx context.Context, instanceID int64) error

	// Human decision methods
	CreateHumanDecision(ctx context.Context, decision *HumanDecisionRecord) error
	GetHumanDecision(ctx context.Context, stepID int64) (*HumanDecisionRecord, error)
	UpdateStepStatus(ctx context.Context, stepID int64, status StepStatus) error
	GetStepByID(ctx context.Context, stepID int64) (*WorkflowStep, error)
	GetHumanDecisionStepByInstanceID(ctx context.Context, instanceID int64) (*WorkflowStep, error)

	// DLQ methods
	CreateDeadLetterRecord(ctx context.Context, rec *DeadLetterRecord) error
	RequeueDeadLetter(
		ctx context.Context,
		dlqID int64,
		newInput *json.RawMessage,
	) error
	ListDeadLetters(ctx context.Context, offset int, limit int) ([]DeadLetterRecord, int64, error)
	GetDeadLetterByID(ctx context.Context, id int64) (*DeadLetterRecord, error)
	PauseActiveStepsAndClearQueue(ctx context.Context, instanceID int64) error

	// Cleanup methods
	CleanupOldWorkflows(ctx context.Context, daysToKeep int) (int64, error)
}
