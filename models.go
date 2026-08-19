package floxy

import (
	"encoding/json"
	"time"
)

type Priority int

const (
	PriorityLow    Priority = 0
	PriorityLower  Priority = 25
	PriorityNormal Priority = 50
	PriorityHigher Priority = 75
	PriorityHigh   Priority = 100
)

type WorkflowStatus string

const (
	StatusPending     WorkflowStatus = "pending"
	StatusRunning     WorkflowStatus = "running"
	StatusCompleted   WorkflowStatus = "completed"
	StatusFailed      WorkflowStatus = "failed"
	StatusRollingBack WorkflowStatus = "rolling_back"
	StatusCancelling  WorkflowStatus = "cancelling"
	StatusCancelled   WorkflowStatus = "cancelled"
	StatusAborted     WorkflowStatus = "aborted"
	StatusDLQ         WorkflowStatus = "dlq"
)

type StepStatus string

const (
	StepStatusPending         StepStatus = "pending"
	StepStatusRunning         StepStatus = "running"
	StepStatusCompleted       StepStatus = "completed"
	StepStatusFailed          StepStatus = "failed"
	StepStatusSkipped         StepStatus = "skipped"
	StepStatusCompensation    StepStatus = "compensation"
	StepStatusRolledBack      StepStatus = "rolled_back"
	StepStatusWaitingDecision StepStatus = "waiting_decision"
	StepStatusConfirmed       StepStatus = "confirmed"
	StepStatusRejected        StepStatus = "rejected"
	StepStatusPaused          StepStatus = "paused"
)

type StepType string

const (
	StepTypeTask      StepType = "task"
	StepTypeParallel  StepType = "parallel"
	StepTypeCondition StepType = "condition"
	StepTypeFork      StepType = "fork"
	StepTypeJoin      StepType = "join"
	StepTypeSavePoint StepType = "save_point"
	StepTypeHuman     StepType = "human"
)

type JoinStrategy string

const (
	JoinStrategyAll JoinStrategy = "all"
	JoinStrategyAny JoinStrategy = "any"
)

type HumanDecision string

const (
	HumanDecisionConfirmed HumanDecision = "confirmed"
	HumanDecisionRejected  HumanDecision = "rejected"
)

type CancelType string

const (
	CancelTypeCancel CancelType = "cancel"
	CancelTypeAbort  CancelType = "abort"
)

type RetryStrategy uint8

const (
	RetryStrategyFixed       RetryStrategy = iota // Fixed delay between retries
	RetryStrategyExponential                      // Exponential backoff: delay = base * 2^attempt
	RetryStrategyLinear                           // Linear backoff: delay = base * attempt
)

type WorkflowDefinition struct {
	ID         string          `json:"id"`
	Name       string          `json:"name"`
	Version    int             `json:"version"`
	Definition GraphDefinition `json:"definition"`
	CreatedAt  time.Time       `json:"created_at"`
}

type GraphDefinition struct {
	Steps               map[string]*StepDefinition `json:"steps"`
	Start               string                     `json:"start"`
	DLQEnabled          bool                       `json:"dlq_enabled"`
	WorkflowLockTimeout time.Duration              `json:"workflow_lock_timeout,omitempty"`
}

type StepDefinition struct {
	Name          string         `json:"name"`
	Type          StepType       `json:"type"`
	Handler       string         `json:"handler"`
	MaxRetries    int            `json:"max_retries"`
	Next          []string       `json:"next,omitempty"`
	Prev          string         `json:"prev,omitempty"`          // previous step in the chain
	Else          string         `json:"else,omitempty"`          // alternative step for condition steps for false branch
	OnFailure     string         `json:"on_failure,omitempty"`    // compensation step
	Condition     string         `json:"condition,omitempty"`     // for conditional transitions
	Parallel      []string       `json:"parallel,omitempty"`      // for parallel steps (fork)
	WaitFor       []string       `json:"wait_for,omitempty"`      // for join, we are waiting for these steps to be completed
	JoinStrategy  JoinStrategy   `json:"join_strategy,omitempty"` // "all" (default) or "any"
	Metadata      map[string]any `json:"metadata,omitempty"`
	NoIdempotent  bool           `json:"no_idempotent"`
	Delay         time.Duration  `json:"delay,omitempty"`
	RetryDelay    time.Duration  `json:"retry_delay,omitempty"`
	RetryStrategy RetryStrategy  `json:"retry_strategy,omitempty"` // Strategy for retry delays: fixed, exponential, linear
	Timeout       time.Duration  `json:"timeout,omitempty"`
}

type WorkflowInstance struct {
	ID          int64           `json:"id"`
	WorkflowID  string          `json:"workflow_id"`
	Status      WorkflowStatus  `json:"status"`
	Input       json.RawMessage `json:"input"`
	Output      json.RawMessage `json:"output"`
	Error       *string         `json:"error"`
	LockedUntil *time.Time      `json:"locked_until,omitempty"`
	StartedAt   *time.Time      `json:"started_at"`
	CompletedAt *time.Time      `json:"completed_at"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
}

type WorkflowStep struct {
	ID                     int64           `json:"id"`
	InstanceID             int64           `json:"instance_id"`
	StepName               string          `json:"step_name"`
	StepType               StepType        `json:"step_type"`
	Status                 StepStatus      `json:"status"`
	Input                  json.RawMessage `json:"input"`
	Output                 json.RawMessage `json:"output"`
	Error                  *string         `json:"error"`
	RetryCount             int             `json:"retry_count"`
	MaxRetries             int             `json:"max_retries"`
	CompensationRetryCount int             `json:"compensation_retry_count"`
	IdempotencyKey         string          `json:"idempotency_key"`
	StartedAt              *time.Time      `json:"started_at"`
	CompletedAt            *time.Time      `json:"completed_at"`
	CreatedAt              time.Time       `json:"created_at"`
}

type QueueItem struct {
	ID          int64      `json:"id"`
	InstanceID  int64      `json:"instance_id"`
	StepID      *int64     `json:"step_id"`
	ScheduledAt time.Time  `json:"scheduled_at"`
	AttemptedAt *time.Time `json:"attempted_at"`
	AttemptedBy *string    `json:"attempted_by"`
	Priority    int        `json:"priority"`
}

type WorkflowEvent struct {
	ID         int64           `json:"id"`
	InstanceID int64           `json:"instance_id"`
	StepID     *int64          `json:"step_id"`
	EventType  string          `json:"event_type"`
	Payload    json.RawMessage `json:"payload"`
	CreatedAt  time.Time       `json:"created_at"`
}

type WorkflowCancelRequest struct {
	ID          int64      `json:"id"`
	InstanceID  int64      `json:"instance_id"`
	RequestedBy string     `json:"requested_by"`
	CancelType  CancelType `json:"cancel_type"`
	Reason      *string    `json:"reason"`
	CreatedAt   time.Time  `json:"created_at"`
}

type JoinState struct {
	InstanceID   int64        `json:"instance_id"`
	JoinStepName string       `json:"join_step_name"`
	WaitingFor   []string     `json:"waiting_for"`
	Completed    []string     `json:"completed"`
	Failed       []string     `json:"failed"`
	JoinStrategy JoinStrategy `json:"join_strategy"`
	IsReady      bool         `json:"is_ready"`
	CreatedAt    time.Time    `json:"created_at"`
	UpdatedAt    time.Time    `json:"updated_at"`
}

type SummaryStats struct {
	TotalWorkflows     uint `json:"total_workflows"`
	CompletedWorkflows uint `json:"completed_workflows"`
	FailedWorkflows    uint `json:"failed_workflows"`
	RunningWorkflows   uint `json:"running_workflows"`
	PendingWorkflows   uint `json:"pending_workflows"`
	ActiveWorkflows    uint `json:"active_workflows"`
}

type ActiveWorkflowInstance struct {
	ID              int64     `json:"id"`
	WorkflowID      string    `json:"workflow_id"`
	WorkflowName    string    `json:"workflow_name"`
	Status          string    `json:"status"`
	StartedAt       time.Time `json:"started_at"`
	UpdatedAt       time.Time `json:"updated_at"`
	CurrentStep     string    `json:"current_step"`
	TotalSteps      int       `json:"total_steps"`
	CompletedSteps  int       `json:"completed_steps"`
	RolledBackSteps int       `json:"rolled_back_steps"`
}

type HumanDecisionRecord struct {
	ID         int64         `json:"id"`
	InstanceID int64         `json:"instance_id"`
	StepID     int64         `json:"step_id"`
	DecidedBy  string        `json:"decided_by"`
	Decision   HumanDecision `json:"decision"`
	Comment    *string       `json:"comment"`
	DecidedAt  time.Time     `json:"decided_at"`
	CreatedAt  time.Time     `json:"created_at"`
}

type HumanDecisionWaitingEvent struct {
	InstanceID int64           `json:"instance_id"`
	OutputData json.RawMessage `json:"output_data"`
}

// DeadLetterRecord represents a record stored in the Dead Letter Queue
// used to resume failed steps later.
type DeadLetterRecord struct {
	ID         int64           `json:"id"`
	InstanceID int64           `json:"instance_id"`
	WorkflowID string          `json:"workflow_id"`
	StepID     int64           `json:"step_id"`
	StepName   string          `json:"step_name"`
	StepType   string          `json:"step_type"`
	Input      json.RawMessage `json:"input"`
	Error      *string         `json:"error"`
	Reason     string          `json:"reason"`
	CreatedAt  time.Time       `json:"created_at"`
}
