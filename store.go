package floxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/lib/pq"
)

var _ Store = (*StoreImpl)(nil)

// workflowDefCacheEntry represents a cached workflow definition with expiration time
type workflowDefCacheEntry struct {
	def       *WorkflowDefinition
	expiresAt time.Time
}

type StoreImpl struct {
	db           Tx
	agingEnabled bool
	agingRate    float64

	// Workflow definition cache
	defCache    sync.Map // map[string]*workflowDefCacheEntry
	defCacheTTL time.Duration
}

func NewStore(pool *pgxpool.Pool) *StoreImpl {
	return &StoreImpl{
		db:           pool,
		agingEnabled: true,
		agingRate:    0.5,
		defCacheTTL:  time.Hour, // Default: 1 hour
	}
}

func (store *StoreImpl) SetAgingEnabled(enabled bool) {
	store.agingEnabled = enabled
}

func (store *StoreImpl) SetAgingRate(rate float64) {
	store.agingRate = rate
}

// SetDefinitionCacheTTL sets the TTL for workflow definition cache
// Set to 0 to disable caching
func (store *StoreImpl) SetDefinitionCacheTTL(ttl time.Duration) {
	store.defCacheTTL = ttl
	if ttl == 0 {
		// If caching is disabled, clear the cache
		store.ClearDefinitionCache()
	}
}

// ClearDefinitionCache clears all cached workflow definitions
func (store *StoreImpl) ClearDefinitionCache() {
	store.defCache.Range(func(key, value interface{}) bool {
		store.defCache.Delete(key)
		return true
	})
}

// InvalidateDefinitionCache removes a specific workflow definition from cache
func (store *StoreImpl) InvalidateDefinitionCache(id string) {
	store.defCache.Delete(id)
}

func (store *StoreImpl) SaveWorkflowDefinition(ctx context.Context, def *WorkflowDefinition) error {
	if def == nil {
		return errors.New("workflow definition is nil")
	}

	executor := store.getExecutor(ctx)

	const query = `
INSERT INTO workflows.workflow_definitions (id, name, version, definition, created_at)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (name, version) DO UPDATE
SET definition = EXCLUDED.definition
RETURNING id, created_at`

	definitionJSON, err := json.Marshal(def.Definition)
	if err != nil {
		return fmt.Errorf("marshal definition: %w", err)
	}

	err = executor.QueryRow(ctx, query,
		def.ID, def.Name, def.Version, definitionJSON, time.Now(),
	).Scan(&def.ID, &def.CreatedAt)

	if err == nil {
		// Invalidate cache for this workflow definition
		store.InvalidateDefinitionCache(def.ID)
	}

	return err
}

func (store *StoreImpl) GetWorkflowDefinition(ctx context.Context, id string) (*WorkflowDefinition, error) {
	// Check cache first (if caching is enabled)
	if store.defCacheTTL > 0 {
		if cached, ok := store.defCache.Load(id); ok {
			entry := cached.(*workflowDefCacheEntry)
			// Check if cache entry is still valid
			if time.Now().Before(entry.expiresAt) {
				// Return a copy to prevent external modifications
				return entry.def, nil
			}
			// Cache expired, remove it
			store.defCache.Delete(id)
		}
	}

	// Cache miss or disabled - fetch from database
	executor := store.getExecutor(ctx)

	const query = `
SELECT id, name, version, definition, created_at
FROM workflows.workflow_definitions
WHERE id = $1`

	var def WorkflowDefinition
	var definitionJSON []byte

	err := executor.QueryRow(ctx, query, id).Scan(
		&def.ID, &def.Name, &def.Version, &definitionJSON, &def.CreatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrEntityNotFound
		}

		return nil, err
	}

	if err := json.Unmarshal(definitionJSON, &def.Definition); err != nil {
		return nil, fmt.Errorf("unmarshal definition: %w", err)
	}

	// Store in cache (if caching is enabled)
	if store.defCacheTTL > 0 {
		entry := &workflowDefCacheEntry{
			def:       &def,
			expiresAt: time.Now().Add(store.defCacheTTL),
		}
		store.defCache.Store(id, entry)
	}

	return &def, nil
}

func (store *StoreImpl) CreateInstance(
	ctx context.Context,
	workflowID string,
	input json.RawMessage,
) (*WorkflowInstance, error) {
	executor := store.getExecutor(ctx)

	const query = `
INSERT INTO workflows.workflow_instances (workflow_id, status, input, created_at, updated_at)
VALUES ($1, $2, $3, $4, $4)
RETURNING id, workflow_id, status, input, locked_until, created_at, updated_at`

	now := time.Now()
	instance := &WorkflowInstance{}

	err := executor.QueryRow(ctx, query,
		workflowID, StatusPending, input, now,
	).Scan(
		&instance.ID, &instance.WorkflowID, &instance.Status,
		&instance.Input, &instance.LockedUntil, &instance.CreatedAt, &instance.UpdatedAt,
	)

	return instance, err
}

func (store *StoreImpl) UpdateInstanceStatus(
	ctx context.Context,
	instanceID int64,
	status WorkflowStatus,
	output json.RawMessage,
	errMsg *string,
) error {
	executor := store.getExecutor(ctx)

	const query = `
UPDATE workflows.workflow_instances
SET status = $2, output = $3, error = $4, updated_at = $5,
	locked_until = CASE WHEN $2 IN ('completed', 'failed', 'cancelled', 'aborted', 'dlq') THEN NULL ELSE locked_until END,
	completed_at = CASE WHEN $2 IN ('completed', 'failed', 'cancelled') THEN $5 ELSE completed_at END,
	started_at = CASE WHEN started_at IS NULL AND $2 = 'running' THEN $5 ELSE started_at END
WHERE id = $1`

	_, err := executor.Exec(ctx, query, instanceID, status, output, errMsg, time.Now())

	return err
}

func (store *StoreImpl) GetInstance(ctx context.Context, instanceID int64) (*WorkflowInstance, error) {
	executor := store.getExecutor(ctx)

	const query = `
SELECT id, workflow_id, status, input, output, error,
	   locked_until, started_at, completed_at, created_at, updated_at
FROM workflows.workflow_instances
WHERE id = $1`

	instance := &WorkflowInstance{}
	err := executor.QueryRow(ctx, query, instanceID).Scan(
		&instance.ID, &instance.WorkflowID, &instance.Status,
		&instance.Input, &instance.Output, &instance.Error,
		&instance.LockedUntil,
		&instance.StartedAt, &instance.CompletedAt,
		&instance.CreatedAt, &instance.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrEntityNotFound
		}

		return nil, err
	}

	return instance, nil
}

func (store *StoreImpl) CreateStep(ctx context.Context, step *WorkflowStep) error {
	if step == nil {
		return errors.New("workflow step is nil")
	}

	if step.IdempotencyKey == "" {
		step.IdempotencyKey = uuid.NewString()
	}

	executor := store.getExecutor(ctx)

	const query = `
INSERT INTO workflows.workflow_steps 
(instance_id, step_name, step_type, status, input, max_retries, created_at, idempotency_key)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
RETURNING id, created_at`

	return executor.QueryRow(ctx, query,
		step.InstanceID, step.StepName, step.StepType,
		step.Status, step.Input, step.MaxRetries, time.Now(), step.IdempotencyKey,
	).Scan(&step.ID, &step.CreatedAt)
}

func (store *StoreImpl) UpdateStep(
	ctx context.Context,
	stepID int64,
	status StepStatus,
	output json.RawMessage,
	errMsg *string,
) error {
	executor := store.getExecutor(ctx)

	const query = `
UPDATE workflows.workflow_steps
SET status = $2, output = $3, error = $4,
	completed_at = CASE WHEN $2 IN ('completed', 'failed', 'skipped') THEN $5 ELSE completed_at END,
	started_at = CASE WHEN started_at IS NULL AND $2 = 'running' THEN $5 ELSE started_at END,
	retry_count = CASE WHEN $2 = 'failed' THEN retry_count + 1 ELSE retry_count END
WHERE id = $1`

	_, err := executor.Exec(ctx, query, stepID, status, output, errMsg, time.Now())

	return err
}

func (store *StoreImpl) GetStepsByInstance(ctx context.Context, instanceID int64) ([]WorkflowStep, error) {
	executor := store.getExecutor(ctx)

	const query = `
SELECT id, instance_id, step_name, step_type, status, input, output, error,
	retry_count, max_retries, idempotency_key, started_at, completed_at, created_at
FROM workflows.workflow_steps
WHERE instance_id = $1
ORDER BY created_at`

	rows, err := executor.Query(ctx, query, instanceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	steps := make([]WorkflowStep, 0)
	for rows.Next() {
		step := WorkflowStep{}
		err := rows.Scan(
			&step.ID, &step.InstanceID, &step.StepName, &step.StepType,
			&step.Status, &step.Input, &step.Output, &step.Error,
			&step.RetryCount, &step.MaxRetries, &step.IdempotencyKey,
			&step.StartedAt, &step.CompletedAt, &step.CreatedAt,
		)
		if err != nil {
			return nil, err
		}

		steps = append(steps, step)
	}

	return steps, rows.Err()
}

func (store *StoreImpl) EnqueueStep(
	ctx context.Context,
	instanceID int64,
	stepID *int64,
	priority Priority,
	delay time.Duration,
) error {
	executor := store.getExecutor(ctx)

	const query = `
INSERT INTO workflows.workflow_queue (instance_id, step_id, scheduled_at, priority)
VALUES ($1, $2, $3, $4)`

	scheduledAt := time.Now().Add(delay)
	_, err := executor.Exec(ctx, query, instanceID, stepID, scheduledAt, priority)

	return err
}

func (store *StoreImpl) DequeueStep(ctx context.Context, workerID string) (*QueueItem, error) {
	executor := store.getExecutor(ctx)

	now := time.Now()
	defaultLockNanos := defaultWorkflowLockTimeout.Nanoseconds()

	var query string
	var args []any
	if store.agingEnabled && store.agingRate > 0 {
		// Priority aging: increase effective priority as items wait
		query = `
WITH next_item AS (
	SELECT
		q.id,
		wi.id AS instance_id,
		$1::timestamptz + (
			COALESCE(NULLIF(NULLIF(wd.definition->>'workflow_lock_timeout', '')::bigint, 0), $2)::double precision / 1000000000.0
		) * INTERVAL '1 second' AS locked_until
	FROM workflows.workflow_queue q
	JOIN workflows.workflow_instances wi ON wi.id = q.instance_id
	JOIN workflows.workflow_definitions wd ON wd.id = wi.workflow_id
	WHERE q.scheduled_at <= $1
		AND (wi.locked_until IS NULL OR wi.locked_until < $1)
	ORDER BY
		LEAST(100,
			q.priority + FLOOR(EXTRACT(EPOCH FROM ($1 - q.scheduled_at)) * $3)
		) DESC,
		q.scheduled_at ASC
	LIMIT 1
	FOR UPDATE OF q, wi SKIP LOCKED
),
locked_instance AS (
	UPDATE workflows.workflow_instances wi
	SET status = CASE WHEN wi.status IN ('pending', 'running') THEN 'running' ELSE wi.status END,
		locked_until = next_item.locked_until,
		updated_at = $1
	FROM next_item
	WHERE wi.id = next_item.instance_id
	RETURNING wi.id
)
UPDATE workflows.workflow_queue
SET attempted_at = $1, attempted_by = $4
FROM next_item, locked_instance
WHERE workflows.workflow_queue.id = next_item.id
RETURNING workflows.workflow_queue.id,
	workflows.workflow_queue.instance_id,
	workflows.workflow_queue.step_id,
	workflows.workflow_queue.scheduled_at,
	workflows.workflow_queue.attempted_at,
	workflows.workflow_queue.attempted_by,
	workflows.workflow_queue.priority`
		args = []any{now, defaultLockNanos, store.agingRate, workerID}
	} else {
		query = `
WITH next_item AS (
	SELECT
		q.id,
		wi.id AS instance_id,
		$1::timestamptz + (
			COALESCE(NULLIF(NULLIF(wd.definition->>'workflow_lock_timeout', '')::bigint, 0), $2)::double precision / 1000000000.0
		) * INTERVAL '1 second' AS locked_until
	FROM workflows.workflow_queue q
	JOIN workflows.workflow_instances wi ON wi.id = q.instance_id
	JOIN workflows.workflow_definitions wd ON wd.id = wi.workflow_id
	WHERE q.scheduled_at <= $1
		AND (wi.locked_until IS NULL OR wi.locked_until < $1)
	ORDER BY q.priority DESC, q.scheduled_at ASC
	LIMIT 1
	FOR UPDATE OF q, wi SKIP LOCKED
),
locked_instance AS (
	UPDATE workflows.workflow_instances wi
	SET status = CASE WHEN wi.status IN ('pending', 'running') THEN 'running' ELSE wi.status END,
		locked_until = next_item.locked_until,
		updated_at = $1
	FROM next_item
	WHERE wi.id = next_item.instance_id
	RETURNING wi.id
)
UPDATE workflows.workflow_queue
SET attempted_at = $1, attempted_by = $3
FROM next_item, locked_instance
WHERE workflows.workflow_queue.id = next_item.id
RETURNING workflows.workflow_queue.id,
	workflows.workflow_queue.instance_id,
	workflows.workflow_queue.step_id,
	workflows.workflow_queue.scheduled_at,
	workflows.workflow_queue.attempted_at,
	workflows.workflow_queue.attempted_by,
	workflows.workflow_queue.priority`
		args = []any{now, defaultLockNanos, workerID}
	}

	item := &QueueItem{}
	err := executor.QueryRow(ctx, query, args...).Scan(
		&item.ID, &item.InstanceID, &item.StepID,
		&item.ScheduledAt, &item.AttemptedAt, &item.AttemptedBy, &item.Priority,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}

	return item, err
}

func (store *StoreImpl) RemoveFromQueue(ctx context.Context, queueID int64) error {
	executor := store.getExecutor(ctx)

	const query = `
WITH deleted_item AS (
	DELETE FROM workflows.workflow_queue
	WHERE id = $1
	RETURNING instance_id
)
UPDATE workflows.workflow_instances wi
SET locked_until = NULL
FROM deleted_item
WHERE wi.id = deleted_item.instance_id`
	_, err := executor.Exec(ctx, query, queueID)

	return err
}

func (store *StoreImpl) ReleaseQueueItem(ctx context.Context, queueID int64) error {
	executor := store.getExecutor(ctx)

	const query = `
WITH released_item AS (
	UPDATE workflows.workflow_queue
	SET attempted_at = NULL, attempted_by = NULL
	WHERE id = $1
	RETURNING instance_id
)
UPDATE workflows.workflow_instances wi
SET locked_until = NULL
FROM released_item
WHERE wi.id = released_item.instance_id`
	_, err := executor.Exec(ctx, query, queueID)

	return err
}

func (store *StoreImpl) RescheduleAndReleaseQueueItem(ctx context.Context, queueID int64, delay time.Duration) error {
	executor := store.getExecutor(ctx)

	const query = `
WITH released_item AS (
UPDATE workflows.workflow_queue
SET scheduled_at = GREATEST(scheduled_at, $2),
    attempted_at = NULL,
    attempted_by = NULL
WHERE id = $1
RETURNING instance_id
)
UPDATE workflows.workflow_instances wi
SET locked_until = NULL
FROM released_item
WHERE wi.id = released_item.instance_id`

	scheduledAt := time.Now().Add(delay)
	_, err := executor.Exec(ctx, query, queueID, scheduledAt)
	return err
}

func (store *StoreImpl) LogEvent(
	ctx context.Context,
	instanceID int64,
	stepID *int64,
	eventType string,
	payload any,
) error {
	executor := store.getExecutor(ctx)

	const query = `
INSERT INTO workflows.workflow_events (instance_id, step_id, event_type, payload, created_at)
VALUES ($1, $2, $3, $4, $5)`

	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	_, err = executor.Exec(ctx, query, instanceID, stepID, eventType, payloadJSON, time.Now())

	return err
}

func (store *StoreImpl) CreateJoinState(
	ctx context.Context,
	instanceID int64,
	joinStepName string,
	waitingFor []string,
	strategy JoinStrategy,
) error {
	executor := store.getExecutor(ctx)

	const query = `
INSERT INTO workflows.workflow_join_state
    (instance_id, join_step_name, waiting_for, join_strategy, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $5)
ON CONFLICT (instance_id, join_step_name) DO NOTHING`

	waitingForJSON, err := json.Marshal(waitingFor)
	if err != nil {
		return err
	}

	if strategy == "" {
		strategy = "all"
	}

	_, err = executor.Exec(ctx, query, instanceID, joinStepName, waitingForJSON, strategy, time.Now())

	return err
}

func (store *StoreImpl) UpdateJoinState(
	ctx context.Context,
	instanceID int64,
	joinStepName, completedStep string,
	success bool,
) (bool, error) {
	executor := store.getExecutor(ctx)

	const query = `
SELECT waiting_for, completed, failed, join_strategy
FROM workflows.workflow_join_state
WHERE instance_id = $1 AND join_step_name = $2
FOR UPDATE`

	var waitingForJSON, completedJSON, failedJSON []byte
	var strategy JoinStrategy

	err := executor.QueryRow(ctx, query, instanceID, joinStepName).Scan(
		&waitingForJSON, &completedJSON, &failedJSON, &strategy,
	)
	if err != nil {
		return false, err
	}

	var waitingFor, completed, failed []string
	_ = json.Unmarshal(waitingForJSON, &waitingFor)
	_ = json.Unmarshal(completedJSON, &completed)
	_ = json.Unmarshal(failedJSON, &failed)

	// Check if step is already in completed or failed lists to avoid duplicates
	if success {
		alreadyInCompleted := false
		for _, c := range completed {
			if c == completedStep {
				alreadyInCompleted = true
				break
			}
		}
		if !alreadyInCompleted {
			completed = append(completed, completedStep)
		}
		// Remove from failed if it was there (shouldn't happen, but just in case)
		for i, f := range failed {
			if f == completedStep {
				failed = append(failed[:i], failed[i+1:]...)
				break
			}
		}
	} else {
		alreadyInFailed := false
		for _, f := range failed {
			if f == completedStep {
				alreadyInFailed = true
				break
			}
		}
		if !alreadyInFailed {
			failed = append(failed, completedStep)
		}
		// Remove from completed if it was there (shouldn't happen, but just in case)
		for i, c := range completed {
			if c == completedStep {
				completed = append(completed[:i], completed[i+1:]...)
				break
			}
		}
	}

	isReady := checkJoinReady(waitingFor, completed, failed, strategy)

	const updateQuery = `
UPDATE workflows.workflow_join_state
SET completed = $1, failed = $2, is_ready = $3, updated_at = $4
WHERE instance_id = $5 AND join_step_name = $6`

	completedJSON, _ = json.Marshal(completed)
	failedJSON, _ = json.Marshal(failed)

	_, err = executor.Exec(ctx, updateQuery,
		completedJSON, failedJSON, isReady, time.Now(), instanceID, joinStepName,
	)
	if err != nil {
		return false, err
	}

	return isReady, nil
}

func (store *StoreImpl) GetJoinState(ctx context.Context, instanceID int64, joinStepName string) (*JoinState, error) {
	executor := store.getExecutor(ctx)

	const query = `
SELECT instance_id, join_step_name, waiting_for, completed, failed, join_strategy, is_ready, created_at, updated_at
FROM workflows.workflow_join_state
WHERE instance_id = $1 AND join_step_name = $2`

	var state JoinState
	var waitingForJSON, completedJSON, failedJSON []byte

	err := executor.QueryRow(ctx, query, instanceID, joinStepName).Scan(
		&state.InstanceID,
		&state.JoinStepName,
		&waitingForJSON,
		&completedJSON,
		&failedJSON,
		&state.JoinStrategy,
		&state.IsReady,
		&state.CreatedAt,
		&state.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrEntityNotFound
		}

		return nil, err
	}

	_ = json.Unmarshal(waitingForJSON, &state.WaitingFor)
	_ = json.Unmarshal(completedJSON, &state.Completed)
	_ = json.Unmarshal(failedJSON, &state.Failed)

	return &state, nil
}

func (store *StoreImpl) AddToJoinWaitFor(
	ctx context.Context,
	instanceID int64,
	joinStepName, stepToAdd string,
) error {
	executor := store.getExecutor(ctx)

	const query = `
SELECT waiting_for, completed, failed, join_strategy
FROM workflows.workflow_join_state
WHERE instance_id = $1 AND join_step_name = $2
FOR UPDATE`

	var waitingForJSON, completedJSON, failedJSON []byte
	var strategy JoinStrategy

	err := executor.QueryRow(ctx, query, instanceID, joinStepName).Scan(
		&waitingForJSON, &completedJSON, &failedJSON, &strategy,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// JoinState doesn't exist yet, create it with the step to add
			return store.CreateJoinState(ctx, instanceID, joinStepName, []string{stepToAdd}, JoinStrategyAll)
		}
		return err
	}

	var waitingFor, completed, failed []string
	_ = json.Unmarshal(waitingForJSON, &waitingFor)
	_ = json.Unmarshal(completedJSON, &completed)
	_ = json.Unmarshal(failedJSON, &failed)

	// Check if step is already in waitingFor list
	for _, w := range waitingFor {
		if w == stepToAdd {
			return nil // Already in the list, nothing to do
		}
	}

	// Add step to waitingFor list
	waitingFor = append(waitingFor, stepToAdd)

	// Recalculate isReady based on updated waitingFor
	isReady := checkJoinReady(waitingFor, completed, failed, strategy)

	const updateQuery = `
UPDATE workflows.workflow_join_state
SET waiting_for = $1, is_ready = $2, updated_at = $3
WHERE instance_id = $4 AND join_step_name = $5`

	waitingForJSON, _ = json.Marshal(waitingFor)

	_, err = executor.Exec(ctx, updateQuery,
		waitingForJSON, isReady, time.Now(), instanceID, joinStepName,
	)

	return err
}

func (store *StoreImpl) ReplaceInJoinWaitFor(
	ctx context.Context,
	instanceID int64,
	joinStepName, virtualStep, realStep string,
) error {
	executor := store.getExecutor(ctx)

	const query = `
SELECT waiting_for, completed, failed, join_strategy
FROM workflows.workflow_join_state
WHERE instance_id = $1 AND join_step_name = $2
FOR UPDATE`

	var waitingForJSON, completedJSON, failedJSON []byte
	var strategy JoinStrategy

	err := executor.QueryRow(ctx, query, instanceID, joinStepName).Scan(
		&waitingForJSON, &completedJSON, &failedJSON, &strategy,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// JoinState doesn't exist yet, create it with the real step
			return store.CreateJoinState(ctx, instanceID, joinStepName, []string{realStep}, JoinStrategyAll)
		}
		return err
	}

	var waitingFor, completed, failed []string
	_ = json.Unmarshal(waitingForJSON, &waitingFor)
	_ = json.Unmarshal(completedJSON, &completed)
	_ = json.Unmarshal(failedJSON, &failed)

	// Replace virtual step with real step in waitingFor list
	found := false
	for i, w := range waitingFor {
		if w == virtualStep {
			waitingFor[i] = realStep
			found = true
			break
		}
	}

	if !found {
		// Virtual step not found, just add the real step
		waitingFor = append(waitingFor, realStep)
	}

	// Check if real step already exists and what its status is
	// If it's already completed or failed, add it to the appropriate list
	const checkStepQuery = `
SELECT status
FROM workflows.workflow_steps
WHERE instance_id = $1 AND step_name = $2
ORDER BY created_at DESC
LIMIT 1`

	var stepStatus StepStatus
	err = executor.QueryRow(ctx, checkStepQuery, instanceID, realStep).Scan(&stepStatus)
	if err == nil {
		// Step exists, check if it's already in completed or failed lists
		isInCompleted := false
		isInFailed := false
		for _, c := range completed {
			if c == realStep {
				isInCompleted = true
				break
			}
		}
		for _, f := range failed {
			if f == realStep {
				isInFailed = true
				break
			}
		}

		// If step is completed or failed/rolled_back, add it to appropriate list
		if !isInCompleted && !isInFailed {
			if stepStatus == StepStatusCompleted {
				completed = append(completed, realStep)
			} else if stepStatus == StepStatusFailed || stepStatus == StepStatusRolledBack {
				// RolledBack also means the step failed, so add to failed list
				failed = append(failed, realStep)
			}
		}
	}

	// Recalculate isReady based on updated waitingFor, completed, and failed
	isReady := checkJoinReady(waitingFor, completed, failed, strategy)

	const updateQuery = `
UPDATE workflows.workflow_join_state
SET waiting_for = $1, completed = $2, failed = $3, is_ready = $4, updated_at = $5
WHERE instance_id = $6 AND join_step_name = $7`

	waitingForJSON, _ = json.Marshal(waitingFor)
	completedJSON, _ = json.Marshal(completed)
	failedJSON, _ = json.Marshal(failed)

	_, err = executor.Exec(ctx, updateQuery,
		waitingForJSON, completedJSON, failedJSON, isReady, time.Now(), instanceID, joinStepName,
	)

	return err
}

func (store *StoreImpl) UpdateStepCompensationRetry(
	ctx context.Context,
	stepID int64,
	retryCount int,
	status StepStatus,
) error {
	executor := store.getExecutor(ctx)

	const query = `
UPDATE workflows.workflow_steps 
SET compensation_retry_count = $1, status = $2
WHERE id = $3`

	_, err := executor.Exec(ctx, query, retryCount, string(status), stepID)
	return err
}

func (store *StoreImpl) GetSummaryStats(ctx context.Context) (*SummaryStats, error) {
	executor := store.getExecutor(ctx)

	const query = `
SELECT 
	COUNT(*) as total_workflows,
	COUNT(*) FILTER (WHERE status = 'completed') as completed_workflows,
	COUNT(*) FILTER (WHERE status = 'failed') as failed_workflows,
	COUNT(*) FILTER (WHERE status = 'running') as running_workflows,
	COUNT(*) FILTER (WHERE status = 'pending') as pending_workflows
FROM workflows.workflow_instances`

	var stats SummaryStats
	err := executor.QueryRow(ctx, query).Scan(
		&stats.TotalWorkflows,
		&stats.CompletedWorkflows,
		&stats.FailedWorkflows,
		&stats.RunningWorkflows,
		&stats.PendingWorkflows,
	)
	if err != nil {
		return nil, err
	}

	const activeWorkflowsQuery = `SELECT COUNT(*) as active_workflows FROM workflows.active_workflows`
	err = executor.QueryRow(ctx, activeWorkflowsQuery).Scan(&stats.ActiveWorkflows)
	if err != nil {
		return nil, err
	}

	return &stats, nil
}

func (store *StoreImpl) GetActiveInstances(ctx context.Context) ([]ActiveWorkflowInstance, error) {
	executor := store.getExecutor(ctx)

	const query = `
SELECT 
    wi.id,
    wi.workflow_id,
    w.name as workflow_name,
    wi.status,
    wi.created_at as started_at,
    wi.updated_at,
    (SELECT step_name FROM workflows.workflow_steps WHERE instance_id = wi.id AND status = 'running' LIMIT 1) as current_step,
    (SELECT COUNT(*) FROM workflows.workflow_steps WHERE instance_id = wi.id) as total_steps,
    (SELECT COUNT(*) FROM workflows.workflow_steps WHERE instance_id = wi.id AND status = 'completed') as completed_steps,
    (SELECT COUNT(*) FROM workflows.workflow_steps WHERE instance_id = wi.id AND status = 'rolled_back') as rolled_back_steps
FROM workflows.workflow_instances wi
JOIN workflows.workflow_definitions w ON wi.workflow_id = w.id
WHERE wi.status IN ('running', 'pending', 'dlq')
ORDER BY wi.created_at DESC`

	rows, err := executor.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	instances := make([]ActiveWorkflowInstance, 0)
	for rows.Next() {
		var instance ActiveWorkflowInstance
		var currentStep *string
		err := rows.Scan(
			&instance.ID,
			&instance.WorkflowID,
			&instance.WorkflowName,
			&instance.Status,
			&instance.StartedAt,
			&instance.UpdatedAt,
			&currentStep,
			&instance.TotalSteps,
			&instance.CompletedSteps,
			&instance.RolledBackSteps,
		)
		if err != nil {
			return nil, err
		}
		if currentStep != nil {
			instance.CurrentStep = *currentStep
		}
		instances = append(instances, instance)
	}

	return instances, rows.Err()
}

func (store *StoreImpl) GetWorkflowDefinitions(ctx context.Context) ([]WorkflowDefinition, error) {
	executor := store.getExecutor(ctx)

	const query = `
SELECT id, name, version, definition, created_at
FROM workflows.workflow_definitions
ORDER BY name, version DESC`

	rows, err := executor.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	definitions := make([]WorkflowDefinition, 0)
	for rows.Next() {
		var def WorkflowDefinition
		var definitionBytes []byte
		err := rows.Scan(
			&def.ID,
			&def.Name,
			&def.Version,
			&definitionBytes,
			&def.CreatedAt,
		)
		if err != nil {
			return nil, err
		}

		if err := json.Unmarshal(definitionBytes, &def.Definition); err != nil {
			return nil, err
		}
		definitions = append(definitions, def)
	}

	return definitions, rows.Err()
}

func (store *StoreImpl) GetWorkflowInstances(ctx context.Context, workflowID string) ([]WorkflowInstance, error) {
	executor := store.getExecutor(ctx)

	const query = `
SELECT id, workflow_id, status, input, output, error, 
		locked_until, started_at, completed_at, created_at, updated_at
FROM workflows.workflow_instances
WHERE workflow_id = $1
ORDER BY created_at DESC`

	rows, err := executor.Query(ctx, query, workflowID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	instances := make([]WorkflowInstance, 0)
	for rows.Next() {
		var instance WorkflowInstance
		err := rows.Scan(
			&instance.ID,
			&instance.WorkflowID,
			&instance.Status,
			&instance.Input,
			&instance.Output,
			&instance.Error,
			&instance.LockedUntil,
			&instance.StartedAt,
			&instance.CompletedAt,
			&instance.CreatedAt,
			&instance.UpdatedAt,
		)
		if err != nil {
			return nil, err
		}
		instances = append(instances, instance)
	}

	return instances, rows.Err()
}

func (store *StoreImpl) GetAllWorkflowInstances(ctx context.Context) ([]WorkflowInstance, error) {
	executor := store.getExecutor(ctx)

	const query = `
SELECT id, workflow_id, status, input, output, error, 
		locked_until, started_at, completed_at, created_at, updated_at
FROM workflows.workflow_instances
ORDER BY created_at DESC`

	rows, err := executor.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	instances := make([]WorkflowInstance, 0)
	for rows.Next() {
		var instance WorkflowInstance
		err := rows.Scan(
			&instance.ID,
			&instance.WorkflowID,
			&instance.Status,
			&instance.Input,
			&instance.Output,
			&instance.Error,
			&instance.LockedUntil,
			&instance.StartedAt,
			&instance.CompletedAt,
			&instance.CreatedAt,
			&instance.UpdatedAt,
		)
		if err != nil {
			return nil, err
		}
		instances = append(instances, instance)
	}

	return instances, rows.Err()
}

func (store *StoreImpl) GetWorkflowInstancesPaginated(ctx context.Context, workflowID string, offset int, limit int) ([]WorkflowInstance, int64, error) {
	executor := store.getExecutor(ctx)

	const countQuery = `
SELECT COUNT(*)
FROM workflows.workflow_instances
WHERE workflow_id = $1`

	var total int64
	if err := executor.QueryRow(ctx, countQuery, workflowID).Scan(&total); err != nil {
		return nil, 0, err
	}

	const query = `
SELECT id, workflow_id, status, input, output, error, 
		locked_until, started_at, completed_at, created_at, updated_at
FROM workflows.workflow_instances
WHERE workflow_id = $1
ORDER BY created_at DESC
LIMIT $2 OFFSET $3`

	rows, err := executor.Query(ctx, query, workflowID, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	instances := make([]WorkflowInstance, 0)
	for rows.Next() {
		var instance WorkflowInstance
		err := rows.Scan(
			&instance.ID,
			&instance.WorkflowID,
			&instance.Status,
			&instance.Input,
			&instance.Output,
			&instance.Error,
			&instance.LockedUntil,
			&instance.StartedAt,
			&instance.CompletedAt,
			&instance.CreatedAt,
			&instance.UpdatedAt,
		)
		if err != nil {
			return nil, 0, err
		}
		instances = append(instances, instance)
	}

	return instances, total, rows.Err()
}

func (store *StoreImpl) GetAllWorkflowInstancesPaginated(ctx context.Context, offset int, limit int) ([]WorkflowInstance, int64, error) {
	executor := store.getExecutor(ctx)

	const countQuery = `SELECT COUNT(*) FROM workflows.workflow_instances`

	var total int64
	if err := executor.QueryRow(ctx, countQuery).Scan(&total); err != nil {
		return nil, 0, err
	}

	const query = `
SELECT id, workflow_id, status, input, output, error, 
		locked_until, started_at, completed_at, created_at, updated_at
FROM workflows.workflow_instances
ORDER BY created_at DESC
LIMIT $1 OFFSET $2`

	rows, err := executor.Query(ctx, query, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	instances := make([]WorkflowInstance, 0)
	for rows.Next() {
		var instance WorkflowInstance
		err := rows.Scan(
			&instance.ID,
			&instance.WorkflowID,
			&instance.Status,
			&instance.Input,
			&instance.Output,
			&instance.Error,
			&instance.LockedUntil,
			&instance.StartedAt,
			&instance.CompletedAt,
			&instance.CreatedAt,
			&instance.UpdatedAt,
		)
		if err != nil {
			return nil, 0, err
		}
		instances = append(instances, instance)
	}

	return instances, total, rows.Err()
}

func (store *StoreImpl) GetWorkflowSteps(ctx context.Context, instanceID int64) ([]WorkflowStep, error) {
	executor := store.getExecutor(ctx)

	const query = `
SELECT id, instance_id, step_name, step_type, status, input, output, error,
		retry_count, max_retries, compensation_retry_count,
		started_at, completed_at, created_at
FROM workflows.workflow_steps
WHERE instance_id = $1
ORDER BY created_at`

	rows, err := executor.Query(ctx, query, instanceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	steps := make([]WorkflowStep, 0)
	for rows.Next() {
		var step WorkflowStep
		err := rows.Scan(
			&step.ID,
			&step.InstanceID,
			&step.StepName,
			&step.StepType,
			&step.Status,
			&step.Input,
			&step.Output,
			&step.Error,
			&step.RetryCount,
			&step.MaxRetries,
			&step.CompensationRetryCount,
			&step.StartedAt,
			&step.CompletedAt,
			&step.CreatedAt,
		)
		if err != nil {
			return nil, err
		}
		steps = append(steps, step)
	}

	return steps, rows.Err()
}

func (store *StoreImpl) GetActiveStepsForUpdate(ctx context.Context, instanceID int64) ([]WorkflowStep, error) {
	executor := store.getExecutor(ctx)

	const query = `
SELECT id, instance_id, step_name, step_type, status, input, output, error,
    retry_count, max_retries, compensation_retry_count, idempotency_key,
    started_at, completed_at, created_at
FROM workflows.workflow_steps
WHERE instance_id = $1 
    AND status IN ('pending', 'running', 'waiting_decision')
ORDER BY created_at DESC
FOR UPDATE SKIP LOCKED`

	rows, err := executor.Query(ctx, query, instanceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	steps := make([]WorkflowStep, 0)
	for rows.Next() {
		var step WorkflowStep
		err := rows.Scan(
			&step.ID, &step.InstanceID, &step.StepName, &step.StepType,
			&step.Status, &step.Input, &step.Output, &step.Error,
			&step.RetryCount, &step.MaxRetries, &step.CompensationRetryCount,
			&step.IdempotencyKey, &step.StartedAt, &step.CompletedAt, &step.CreatedAt,
		)
		if err != nil {
			return nil, err
		}
		steps = append(steps, step)
	}

	return steps, rows.Err()
}

func (store *StoreImpl) CreateCancelRequest(ctx context.Context, req *WorkflowCancelRequest) error {
	if req == nil {
		return errors.New("cancel request is nil")
	}

	executor := store.getExecutor(ctx)

	const query = `
INSERT INTO workflows.workflow_cancel_requests (instance_id, requested_by, cancel_type, reason, created_at)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (instance_id) DO NOTHING
RETURNING id, created_at`

	return executor.QueryRow(ctx, query,
		req.InstanceID, req.RequestedBy, req.CancelType, req.Reason, time.Now(),
	).Scan(&req.ID, &req.CreatedAt)
}

func (store *StoreImpl) GetCancelRequest(ctx context.Context, instanceID int64) (*WorkflowCancelRequest, error) {
	executor := store.getExecutor(ctx)

	const query = `
SELECT id, instance_id, requested_by, cancel_type, reason, created_at
FROM workflows.workflow_cancel_requests
WHERE instance_id = $1`

	var req WorkflowCancelRequest
	err := executor.QueryRow(ctx, query, instanceID).Scan(
		&req.ID, &req.InstanceID, &req.RequestedBy, &req.CancelType, &req.Reason, &req.CreatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrEntityNotFound
		}

		return nil, err
	}

	return &req, nil
}

func (store *StoreImpl) DeleteCancelRequest(ctx context.Context, instanceID int64) error {
	executor := store.getExecutor(ctx)

	const query = `DELETE FROM workflows.workflow_cancel_requests WHERE instance_id = $1`
	_, err := executor.Exec(ctx, query, instanceID)

	return err
}

func (store *StoreImpl) GetWorkflowEvents(ctx context.Context, instanceID int64) ([]WorkflowEvent, error) {
	executor := store.getExecutor(ctx)

	const query = `
SELECT id, instance_id, step_id, event_type, payload, created_at
FROM workflows.workflow_events
WHERE instance_id = $1
ORDER BY created_at`

	rows, err := executor.Query(ctx, query, instanceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	events := make([]WorkflowEvent, 0)
	for rows.Next() {
		var event WorkflowEvent
		err := rows.Scan(
			&event.ID,
			&event.InstanceID,
			&event.StepID,
			&event.EventType,
			&event.Payload,
			&event.CreatedAt,
		)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}

	return events, rows.Err()
}

func (store *StoreImpl) GetWorkflowStats(ctx context.Context) ([]WorkflowStats, error) {
	executor := store.getExecutor(ctx)

	const query = `
SELECT 
	name,
	version,
	total_instances,
	completed,
	failed,
	running,
	avg_duration_seconds
FROM workflows.workflow_stats`

	rows, err := executor.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	stats := make([]WorkflowStats, 0)
	for rows.Next() {
		var s WorkflowStats
		var avgSeconds *float64

		err := rows.Scan(
			&s.WorkflowName,
			&s.Version,
			&s.TotalInstances,
			&s.CompletedInstances,
			&s.FailedInstances,
			&s.RunningInstances,
			&avgSeconds,
		)
		if err != nil {
			return nil, err
		}

		if avgSeconds != nil {
			s.AverageDuration = time.Duration(*avgSeconds * float64(time.Second))
		}

		stats = append(stats, s)
	}

	return stats, rows.Err()
}

func (store *StoreImpl) CreateHumanDecision(ctx context.Context, decision *HumanDecisionRecord) error {
	if decision == nil {
		return errors.New("human decision is nil")
	}

	executor := store.getExecutor(ctx)

	const query = `
INSERT INTO workflows.workflow_human_decisions (instance_id, step_id, decided_by, decision, comment, decided_at, created_at)
VALUES ($1, $2, $3, $4, $5, $6, $7)
RETURNING id, created_at`

	return executor.QueryRow(ctx, query,
		decision.InstanceID, decision.StepID, decision.DecidedBy,
		decision.Decision, decision.Comment, decision.DecidedAt, time.Now(),
	).Scan(&decision.ID, &decision.CreatedAt)
}

func (store *StoreImpl) GetHumanDecision(ctx context.Context, stepID int64) (*HumanDecisionRecord, error) {
	executor := store.getExecutor(ctx)

	const query = `
SELECT id, instance_id, step_id, decided_by, decision, comment, decided_at, created_at
FROM workflows.workflow_human_decisions
WHERE step_id = $1`

	var decision HumanDecisionRecord
	err := executor.QueryRow(ctx, query, stepID).Scan(
		&decision.ID, &decision.InstanceID, &decision.StepID,
		&decision.DecidedBy, &decision.Decision, &decision.Comment,
		&decision.DecidedAt, &decision.CreatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrEntityNotFound
		}

		return nil, err
	}

	return &decision, nil
}

func (store *StoreImpl) UpdateStepStatus(ctx context.Context, stepID int64, status StepStatus) error {
	executor := store.getExecutor(ctx)

	const query = `UPDATE workflows.workflow_steps SET status = $2 WHERE id = $1`

	_, err := executor.Exec(ctx, query, stepID, status)

	return err
}

func (store *StoreImpl) GetStepByID(ctx context.Context, stepID int64) (*WorkflowStep, error) {
	executor := store.getExecutor(ctx)

	const query = `
SELECT id, instance_id, step_name, step_type, status, input, output, error,
	retry_count, max_retries, compensation_retry_count, idempotency_key,
	started_at, completed_at, created_at
FROM workflows.workflow_steps
WHERE id = $1`

	var step WorkflowStep
	err := executor.QueryRow(ctx, query, stepID).Scan(
		&step.ID, &step.InstanceID, &step.StepName, &step.StepType,
		&step.Status, &step.Input, &step.Output, &step.Error,
		&step.RetryCount, &step.MaxRetries, &step.CompensationRetryCount,
		&step.IdempotencyKey, &step.StartedAt, &step.CompletedAt, &step.CreatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrEntityNotFound
		}
		return nil, err
	}

	return &step, nil
}

func (store *StoreImpl) GetHumanDecisionStepByInstanceID(ctx context.Context, instanceID int64) (*WorkflowStep, error) {
	executor := store.getExecutor(ctx)

	const query = `
SELECT id, instance_id, step_name, step_type, status, input, output, error,
	retry_count, max_retries, compensation_retry_count, idempotency_key,
	started_at, completed_at, created_at
FROM workflows.workflow_steps
WHERE instance_id = $1 AND step_type = $2
ORDER BY created_at DESC
LIMIT 1`

	var step WorkflowStep
	err := executor.QueryRow(ctx, query, instanceID, StepTypeHuman).Scan(
		&step.ID, &step.InstanceID, &step.StepName, &step.StepType,
		&step.Status, &step.Input, &step.Output, &step.Error,
		&step.RetryCount, &step.MaxRetries, &step.CompensationRetryCount,
		&step.IdempotencyKey, &step.StartedAt, &step.CompletedAt, &step.CreatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrEntityNotFound
		}

		return nil, err
	}

	return &step, nil
}

func (store *StoreImpl) CleanupOldWorkflows(ctx context.Context, daysToKeep int) (int64, error) {
	executor := store.getExecutor(ctx)

	const query = `SELECT workflows.cleanup_old_workflows($1)`

	var deletedCount int64
	err := executor.QueryRow(ctx, query, daysToKeep).Scan(&deletedCount)
	if err != nil {
		return 0, fmt.Errorf("failed to cleanup old workflows: %w", err)
	}

	return deletedCount, nil
}

func (store *StoreImpl) CreateDeadLetterRecord(
	ctx context.Context,
	rec *DeadLetterRecord,
) error {
	if rec == nil {
		return errors.New("dead letter record is nil")
	}

	executor := store.getExecutor(ctx)

	const query = `
INSERT INTO workflows.workflow_dlq (
	instance_id, workflow_id, step_id, step_name, step_type, input, error, reason
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`

	_, err := executor.Exec(ctx, query,
		rec.InstanceID,
		rec.WorkflowID,
		rec.StepID,
		rec.StepName,
		rec.StepType,
		rec.Input,
		rec.Error,
		rec.Reason,
	)
	return err
}

func (store *StoreImpl) RequeueDeadLetter(
	ctx context.Context,
	dlqID int64,
	newInput *json.RawMessage,
) error {
	executor := store.getExecutor(ctx)

	const query = `
WITH dlq AS (
    SELECT id, instance_id, step_id, input
    FROM workflows.workflow_dlq
    WHERE id = $1
    FOR UPDATE
), upd_step AS (
    UPDATE workflows.workflow_steps ws
    SET status = 'pending',
        input = COALESCE($2, (SELECT input FROM dlq), ws.input),
        error = NULL,
        retry_count = 0,
        compensation_retry_count = 0,
        started_at = NULL,
        completed_at = NULL
    FROM dlq
    WHERE ws.id = dlq.step_id
    RETURNING ws.id AS step_id, ws.instance_id AS instance_id
), enq AS (
    INSERT INTO workflows.workflow_queue (instance_id, step_id)
    SELECT instance_id, step_id FROM upd_step
    RETURNING 1
), upd_inst AS (
    UPDATE workflows.workflow_instances wi
    SET status = 'running', error = NULL
    FROM upd_step
    WHERE wi.id = upd_step.instance_id AND wi.status IN ('failed','dlq')
    RETURNING wi.id
), upd_join AS (
    UPDATE workflows.workflow_steps ws
    SET status = 'pending'
    FROM upd_step
    WHERE ws.instance_id = upd_step.instance_id AND ws.step_type = 'join' AND ws.status = 'paused'
    RETURNING ws.id
)
DELETE FROM workflows.workflow_dlq d USING dlq WHERE d.id = dlq.id;`

	var input any
	if newInput != nil {
		input = *newInput
	} else {
		input = nil
	}

	_, err := executor.Exec(ctx, query, dlqID, input)

	return err
}

func (store *StoreImpl) ListDeadLetters(ctx context.Context, offset int, limit int) ([]DeadLetterRecord, int64, error) {
	executor := store.getExecutor(ctx)

	const countQuery = `SELECT COUNT(*) FROM workflows.workflow_dlq`
	var total int64
	if err := executor.QueryRow(ctx, countQuery).Scan(&total); err != nil {
		return nil, 0, err
	}

	const selectQuery = `
SELECT id, instance_id, workflow_id, step_id, step_name, step_type, input, error, reason, created_at
FROM workflows.workflow_dlq
ORDER BY created_at DESC
OFFSET $1 LIMIT $2`

	rows, err := executor.Query(ctx, selectQuery, offset, limit)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	records := make([]DeadLetterRecord, 0)
	for rows.Next() {
		rec := DeadLetterRecord{}
		if err := rows.Scan(
			&rec.ID,
			&rec.InstanceID,
			&rec.WorkflowID,
			&rec.StepID,
			&rec.StepName,
			&rec.StepType,
			&rec.Input,
			&rec.Error,
			&rec.Reason,
			&rec.CreatedAt,
		); err != nil {
			return nil, 0, err
		}
		records = append(records, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}

	return records, total, nil
}

func (store *StoreImpl) GetDeadLetterByID(ctx context.Context, id int64) (*DeadLetterRecord, error) {
	executor := store.getExecutor(ctx)

	const query = `
SELECT id, instance_id, workflow_id, step_id, step_name, step_type, input, error, reason, created_at
FROM workflows.workflow_dlq
WHERE id = $1`

	rec := DeadLetterRecord{}
	if err := executor.QueryRow(ctx, query, id).Scan(
		&rec.ID,
		&rec.InstanceID,
		&rec.WorkflowID,
		&rec.StepID,
		&rec.StepName,
		&rec.StepType,
		&rec.Input,
		&rec.Error,
		&rec.Reason,
		&rec.CreatedAt,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrEntityNotFound
		}
		return nil, err
	}

	return &rec, nil
}

func (store *StoreImpl) PauseActiveStepsAndClearQueue(ctx context.Context, instanceID int64) error {
	executor := store.getExecutor(ctx)

	// Pause all running steps for this instance
	const pauseRunning = `UPDATE workflows.workflow_steps SET status = 'paused' WHERE instance_id = $1 AND status = 'running'`
	if _, err := executor.Exec(ctx, pauseRunning, instanceID); err != nil {
		return err
	}

	// Remove all queued items for the instance to prevent further automatic progress
	const clearQueue = `DELETE FROM workflows.workflow_queue WHERE instance_id = $1`
	if _, err := executor.Exec(ctx, clearQueue, instanceID); err != nil {
		return err
	}

	return nil
}

func (store *StoreImpl) getExecutor(ctx context.Context) Tx {
	if tx := TxFromContext(ctx); tx != nil {
		return tx
	}

	return store.db
}

func checkJoinReady(waitingFor, completed, failed []string, strategy JoinStrategy) bool {
	if strategy == JoinStrategyAny {
		return len(completed) > 0 || len(failed) > 0
	}

	totalProcessed := len(completed) + len(failed)

	return totalProcessed >= len(waitingFor)
}
