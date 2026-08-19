package floxy

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
)

var _ Store = (*MemoryStore)(nil)

type MemoryStore struct {
	mu                  sync.RWMutex
	definitions         map[string]*WorkflowDefinition
	instances           map[int64]*WorkflowInstance
	steps               map[int64]*WorkflowStep
	stepsByInstance     map[int64][]int64
	queue               map[int64]*QueueItem
	events              []*WorkflowEvent
	eventsByInstance    map[int64][]int64
	joinStates          map[string]*JoinState
	cancelRequests      map[int64]*WorkflowCancelRequest
	humanDecisions      map[int64]*HumanDecisionRecord
	deadLetters         map[int64]*DeadLetterRecord
	nextInstanceID      int64
	nextStepID          int64
	nextQueueID         int64
	nextEventID         int64
	nextCancelRequestID int64
	nextHumanDecisionID int64
	nextDeadLetterID    int64
	agingEnabled        bool
	agingRate           float64
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		definitions:         make(map[string]*WorkflowDefinition),
		instances:           make(map[int64]*WorkflowInstance),
		steps:               make(map[int64]*WorkflowStep),
		stepsByInstance:     make(map[int64][]int64),
		queue:               make(map[int64]*QueueItem),
		events:              make([]*WorkflowEvent, 0),
		eventsByInstance:    make(map[int64][]int64),
		joinStates:          make(map[string]*JoinState),
		cancelRequests:      make(map[int64]*WorkflowCancelRequest),
		humanDecisions:      make(map[int64]*HumanDecisionRecord),
		deadLetters:         make(map[int64]*DeadLetterRecord),
		nextInstanceID:      1,
		nextStepID:          1,
		nextQueueID:         1,
		nextEventID:         1,
		nextCancelRequestID: 1,
		nextHumanDecisionID: 1,
		nextDeadLetterID:    1,
		agingEnabled:        true,
		agingRate:           0.5,
	}
}

func (s *MemoryStore) SetAgingEnabled(enabled bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.agingEnabled = enabled
}

func (s *MemoryStore) SetAgingRate(rate float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.agingRate = rate
}

func (s *MemoryStore) SaveWorkflowDefinition(ctx context.Context, def *WorkflowDefinition) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, exists := s.definitions[def.ID]; exists && existing != nil {
		def.ID = existing.ID
		def.CreatedAt = existing.CreatedAt
	} else {
		if def.ID == "" {
			def.ID = uuid.NewString()
		}
		def.CreatedAt = time.Now()
	}

	s.definitions[def.ID] = def

	return nil
}

func (s *MemoryStore) GetWorkflowDefinition(ctx context.Context, id string) (*WorkflowDefinition, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	def, exists := s.definitions[id]
	if !exists {
		return nil, ErrEntityNotFound
	}

	return def, nil
}

func (s *MemoryStore) CreateInstance(
	ctx context.Context,
	workflowID string,
	input json.RawMessage,
) (*WorkflowInstance, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	instance := &WorkflowInstance{
		ID:         s.nextInstanceID,
		WorkflowID: workflowID,
		Status:     StatusPending,
		Input:      input,
		CreatedAt:  now,
		UpdatedAt:  now,
	}

	s.instances[instance.ID] = instance
	s.nextInstanceID++

	return instance, nil
}

func (s *MemoryStore) UpdateInstanceStatus(
	ctx context.Context,
	instanceID int64,
	status WorkflowStatus,
	output json.RawMessage,
	errMsg *string,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	instance, exists := s.instances[instanceID]
	if !exists {
		return ErrEntityNotFound
	}

	instance.Status = status
	instance.Output = output
	instance.Error = errMsg
	instance.UpdatedAt = time.Now()

	if status == StatusCompleted ||
		status == StatusFailed ||
		status == StatusCancelled ||
		status == StatusAborted ||
		status == StatusDLQ {
		instance.LockedUntil = nil
	}

	if status == StatusCompleted || status == StatusFailed || status == StatusCancelled {
		now := time.Now()
		instance.CompletedAt = &now
	}

	if instance.StartedAt == nil && status == StatusRunning {
		now := time.Now()
		instance.StartedAt = &now
	}

	return nil
}

func (s *MemoryStore) GetInstance(ctx context.Context, instanceID int64) (*WorkflowInstance, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	instance, exists := s.instances[instanceID]
	if !exists {
		return nil, ErrEntityNotFound
	}

	return instance, nil
}

func (s *MemoryStore) CreateStep(ctx context.Context, step *WorkflowStep) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if step.IdempotencyKey == "" {
		step.IdempotencyKey = uuid.NewString()
	}

	step.ID = s.nextStepID
	step.CreatedAt = time.Now()
	s.nextStepID++

	s.steps[step.ID] = step
	s.stepsByInstance[step.InstanceID] = append(s.stepsByInstance[step.InstanceID], step.ID)

	return nil
}

func (s *MemoryStore) UpdateStep(
	ctx context.Context,
	stepID int64,
	status StepStatus,
	output json.RawMessage,
	errMsg *string,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	step, exists := s.steps[stepID]
	if !exists {
		return ErrEntityNotFound
	}

	now := time.Now()
	step.Status = status
	step.Output = output
	step.Error = errMsg

	if status == StepStatusCompleted || status == StepStatusFailed || status == StepStatusSkipped {
		step.CompletedAt = &now
	}

	if step.StartedAt == nil && status == StepStatusRunning {
		step.StartedAt = &now
	}

	if status == StepStatusFailed {
		step.RetryCount++
	}

	return nil
}

func (s *MemoryStore) GetStepsByInstance(ctx context.Context, instanceID int64) ([]WorkflowStep, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stepIDs, exists := s.stepsByInstance[instanceID]
	if !exists {
		return []WorkflowStep{}, nil
	}

	steps := make([]WorkflowStep, 0, len(stepIDs))
	for _, stepID := range stepIDs {
		if step, ok := s.steps[stepID]; ok {
			steps = append(steps, *step)
		}
	}

	sort.Slice(steps, func(i, j int) bool {
		return steps[i].CreatedAt.Before(steps[j].CreatedAt)
	})

	return steps, nil
}

func (s *MemoryStore) EnqueueStep(
	ctx context.Context,
	instanceID int64,
	stepID *int64,
	priority Priority,
	delay time.Duration,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	item := &QueueItem{
		ID:          s.nextQueueID,
		InstanceID:  instanceID,
		StepID:      stepID,
		ScheduledAt: time.Now().Add(delay),
		Priority:    int(priority),
	}

	s.queue[item.ID] = item
	s.nextQueueID++

	return nil
}

func (s *MemoryStore) DequeueStep(ctx context.Context, workerID string) (*QueueItem, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	var selectedItem *QueueItem
	maxPriority := -1

	for _, item := range s.queue {
		if item.AttemptedAt != nil {
			continue
		}
		if item.ScheduledAt.After(now) {
			continue
		}
		instance := s.instances[item.InstanceID]
		if instance == nil {
			continue
		}
		if instance.LockedUntil != nil && !instance.LockedUntil.Before(now) {
			continue
		}

		priority := item.Priority
		if s.agingEnabled && s.agingRate > 0 {
			waitTime := now.Sub(item.ScheduledAt).Seconds()
			priority = int(float64(priority) + waitTime*s.agingRate)
			if priority > 100 {
				priority = 100
			}
		}

		if priority > maxPriority ||
			(priority == maxPriority && selectedItem != nil && item.ScheduledAt.Before(selectedItem.ScheduledAt)) {
			maxPriority = priority
			selectedItem = item
		}
	}

	if selectedItem == nil {
		return nil, nil
	}

	selectedItem.AttemptedAt = &now
	selectedItem.AttemptedBy = &workerID
	if instance := s.instances[selectedItem.InstanceID]; instance != nil {
		lockTimeout := defaultWorkflowLockTimeout
		if def := s.definitions[instance.WorkflowID]; def != nil && def.Definition.WorkflowLockTimeout > 0 {
			lockTimeout = def.Definition.WorkflowLockTimeout
		}
		lockedUntil := now.Add(lockTimeout)
		if instance.Status == StatusPending || instance.Status == StatusRunning {
			instance.Status = StatusRunning
		}
		instance.LockedUntil = &lockedUntil
		instance.UpdatedAt = now
	}

	result := *selectedItem

	return &result, nil
}

func (s *MemoryStore) RemoveFromQueue(ctx context.Context, queueID int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if item := s.queue[queueID]; item != nil {
		if instance := s.instances[item.InstanceID]; instance != nil {
			instance.LockedUntil = nil
		}
	}
	delete(s.queue, queueID)

	return nil
}

func (s *MemoryStore) ReleaseQueueItem(ctx context.Context, queueID int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	item, exists := s.queue[queueID]
	if !exists {
		return ErrEntityNotFound
	}

	item.AttemptedAt = nil
	item.AttemptedBy = nil
	if instance := s.instances[item.InstanceID]; instance != nil {
		instance.LockedUntil = nil
	}

	return nil
}

func (s *MemoryStore) RescheduleAndReleaseQueueItem(ctx context.Context, queueID int64, delay time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	item, exists := s.queue[queueID]
	if !exists {
		return ErrEntityNotFound
	}

	now := time.Now()
	newScheduledAt := now.Add(delay)
	if newScheduledAt.Before(item.ScheduledAt) {
		newScheduledAt = item.ScheduledAt
	}

	item.ScheduledAt = newScheduledAt
	item.AttemptedAt = nil
	item.AttemptedBy = nil
	if instance := s.instances[item.InstanceID]; instance != nil {
		instance.LockedUntil = nil
	}

	return nil
}

func (s *MemoryStore) LogEvent(
	ctx context.Context,
	instanceID int64,
	stepID *int64,
	eventType string,
	payload any,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	event := &WorkflowEvent{
		ID:         s.nextEventID,
		InstanceID: instanceID,
		StepID:     stepID,
		EventType:  eventType,
		Payload:    payloadJSON,
		CreatedAt:  time.Now(),
	}

	s.events = append(s.events, event)
	s.eventsByInstance[instanceID] = append(s.eventsByInstance[instanceID], event.ID)
	s.nextEventID++

	return nil
}

func (s *MemoryStore) CreateJoinState(
	ctx context.Context,
	instanceID int64,
	joinStepName string,
	waitingFor []string,
	strategy JoinStrategy,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := s.joinStateKey(instanceID, joinStepName)
	if _, exists := s.joinStates[key]; exists {
		return nil
	}

	if strategy == "" {
		strategy = JoinStrategyAll
	}

	now := time.Now()
	state := &JoinState{
		InstanceID:   instanceID,
		JoinStepName: joinStepName,
		WaitingFor:   waitingFor,
		Completed:    []string{},
		Failed:       []string{},
		JoinStrategy: strategy,
		IsReady:      false,
		CreatedAt:    now,
		UpdatedAt:    now,
	}

	s.joinStates[key] = state

	return nil
}

func (s *MemoryStore) UpdateJoinState(
	ctx context.Context,
	instanceID int64,
	joinStepName, completedStep string,
	success bool,
) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := s.joinStateKey(instanceID, joinStepName)
	state, exists := s.joinStates[key]
	if !exists {
		return false, ErrEntityNotFound
	}

	if success {
		found := false
		for _, c := range state.Completed {
			if c == completedStep {
				found = true
				break
			}
		}
		if !found {
			state.Completed = append(state.Completed, completedStep)
		}
		for i, f := range state.Failed {
			if f == completedStep {
				state.Failed = append(state.Failed[:i], state.Failed[i+1:]...)
				break
			}
		}
	} else {
		found := false
		for _, f := range state.Failed {
			if f == completedStep {
				found = true
				break
			}
		}
		if !found {
			state.Failed = append(state.Failed, completedStep)
		}
		for i, c := range state.Completed {
			if c == completedStep {
				state.Completed = append(state.Completed[:i], state.Completed[i+1:]...)
				break
			}
		}
	}

	state.IsReady = s.checkJoinReady(state.WaitingFor, state.Completed, state.Failed, state.JoinStrategy)
	state.UpdatedAt = time.Now()

	return state.IsReady, nil
}

func (s *MemoryStore) GetJoinState(ctx context.Context, instanceID int64, joinStepName string) (*JoinState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	key := s.joinStateKey(instanceID, joinStepName)
	state, exists := s.joinStates[key]
	if !exists {
		return nil, ErrEntityNotFound
	}

	return state, nil
}

func (s *MemoryStore) AddToJoinWaitFor(ctx context.Context, instanceID int64, joinStepName, stepToAdd string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := s.joinStateKey(instanceID, joinStepName)
	state, exists := s.joinStates[key]

	if !exists {
		return s.CreateJoinState(ctx, instanceID, joinStepName, []string{stepToAdd}, JoinStrategyAll)
	}

	for _, w := range state.WaitingFor {
		if w == stepToAdd {
			return nil
		}
	}

	state.WaitingFor = append(state.WaitingFor, stepToAdd)
	state.IsReady = s.checkJoinReady(state.WaitingFor, state.Completed, state.Failed, state.JoinStrategy)
	state.UpdatedAt = time.Now()

	return nil
}

func (s *MemoryStore) ReplaceInJoinWaitFor(ctx context.Context, instanceID int64, joinStepName, virtualStep, realStep string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := s.joinStateKey(instanceID, joinStepName)
	state, exists := s.joinStates[key]

	if !exists {
		return s.CreateJoinState(ctx, instanceID, joinStepName, []string{realStep}, JoinStrategyAll)
	}

	found := false
	for i, w := range state.WaitingFor {
		if w == virtualStep {
			state.WaitingFor[i] = realStep
			found = true
			break
		}
	}

	if !found {
		state.WaitingFor = append(state.WaitingFor, realStep)
	}

	steps := s.stepsByInstance[instanceID]
	for _, stepID := range steps {
		step := s.steps[stepID]
		if step != nil && step.StepName == realStep {
			inCompleted := false
			inFailed := false
			for _, c := range state.Completed {
				if c == realStep {
					inCompleted = true
					break
				}
			}
			for _, f := range state.Failed {
				if f == realStep {
					inFailed = true
					break
				}
			}

			if !inCompleted && !inFailed {
				if step.Status == StepStatusCompleted {
					state.Completed = append(state.Completed, realStep)
				} else if step.Status == StepStatusFailed || step.Status == StepStatusRolledBack {
					state.Failed = append(state.Failed, realStep)
				}
			}
			break
		}
	}

	state.IsReady = s.checkJoinReady(state.WaitingFor, state.Completed, state.Failed, state.JoinStrategy)
	state.UpdatedAt = time.Now()

	return nil
}

func (s *MemoryStore) checkJoinReady(waitingFor, completed, failed []string, strategy JoinStrategy) bool {
	if strategy == JoinStrategyAny {
		return len(completed) > 0 || len(failed) > 0
	}

	totalProcessed := len(completed) + len(failed)
	return totalProcessed >= len(waitingFor)
}

func (s *MemoryStore) UpdateStepCompensationRetry(
	ctx context.Context,
	stepID int64,
	retryCount int,
	status StepStatus,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	step, exists := s.steps[stepID]
	if !exists {
		return ErrEntityNotFound
	}

	step.CompensationRetryCount = retryCount
	step.Status = status

	return nil
}

func (s *MemoryStore) GetSummaryStats(ctx context.Context) (*SummaryStats, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stats := &SummaryStats{}
	for _, instance := range s.instances {
		stats.TotalWorkflows++
		switch instance.Status {
		case StatusCompleted:
			stats.CompletedWorkflows++
		case StatusFailed:
			stats.FailedWorkflows++
		case StatusRunning:
			stats.RunningWorkflows++
		case StatusPending:
			stats.PendingWorkflows++
		}
		if instance.Status == StatusRunning || instance.Status == StatusPending {
			stats.ActiveWorkflows++
		}
	}

	return stats, nil
}

func (s *MemoryStore) GetActiveInstances(ctx context.Context) ([]ActiveWorkflowInstance, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	activeInstances := make([]ActiveWorkflowInstance, 0)
	for _, instance := range s.instances {
		if instance.Status != StatusRunning && instance.Status != StatusPending && instance.Status != StatusDLQ {
			continue
		}

		active := ActiveWorkflowInstance{
			ID:         instance.ID,
			WorkflowID: instance.WorkflowID,
			Status:     string(instance.Status),
			StartedAt:  instance.CreatedAt,
			UpdatedAt:  instance.UpdatedAt,
		}

		def, _ := s.definitions[instance.WorkflowID]
		if def != nil {
			active.WorkflowName = def.Name
		}

		stepIDs := s.stepsByInstance[instance.ID]
		active.TotalSteps = len(stepIDs)
		for _, stepID := range stepIDs {
			step := s.steps[stepID]
			if step != nil {
				if step.Status == StepStatusRunning {
					active.CurrentStep = step.StepName
				}
				if step.Status == StepStatusCompleted {
					active.CompletedSteps++
				}
				if step.Status == StepStatusRolledBack {
					active.RolledBackSteps++
				}
			}
		}

		activeInstances = append(activeInstances, active)
	}

	sort.Slice(activeInstances, func(i, j int) bool {
		return activeInstances[i].StartedAt.After(activeInstances[j].StartedAt)
	})

	return activeInstances, nil
}

func (s *MemoryStore) GetWorkflowDefinitions(ctx context.Context) ([]WorkflowDefinition, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	definitions := make([]WorkflowDefinition, 0, len(s.definitions))
	for _, def := range s.definitions {
		definitions = append(definitions, *def)
	}

	sort.Slice(definitions, func(i, j int) bool {
		if definitions[i].Name != definitions[j].Name {
			return definitions[i].Name < definitions[j].Name
		}
		return definitions[i].Version > definitions[j].Version
	})

	return definitions, nil
}

func (s *MemoryStore) GetWorkflowInstances(ctx context.Context, workflowID string) ([]WorkflowInstance, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	instances := make([]WorkflowInstance, 0)
	for _, instance := range s.instances {
		if instance.WorkflowID == workflowID {
			instances = append(instances, *instance)
		}
	}

	sort.Slice(instances, func(i, j int) bool {
		return instances[i].CreatedAt.After(instances[j].CreatedAt)
	})

	return instances, nil
}

func (s *MemoryStore) GetAllWorkflowInstances(ctx context.Context) ([]WorkflowInstance, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	instances := make([]WorkflowInstance, 0, len(s.instances))
	for _, instance := range s.instances {
		instances = append(instances, *instance)
	}

	sort.Slice(instances, func(i, j int) bool {
		return instances[i].CreatedAt.After(instances[j].CreatedAt)
	})

	return instances, nil
}

func (s *MemoryStore) GetWorkflowInstancesPaginated(ctx context.Context, workflowID string, offset int, limit int) ([]WorkflowInstance, int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	instances := make([]WorkflowInstance, 0)
	for _, instance := range s.instances {
		if instance.WorkflowID == workflowID {
			instances = append(instances, *instance)
		}
	}

	sort.Slice(instances, func(i, j int) bool {
		return instances[i].CreatedAt.After(instances[j].CreatedAt)
	})

	total := int64(len(instances))

	if offset >= len(instances) {
		return []WorkflowInstance{}, total, nil
	}

	end := offset + limit
	if end > len(instances) {
		end = len(instances)
	}

	return instances[offset:end], total, nil
}

func (s *MemoryStore) GetAllWorkflowInstancesPaginated(ctx context.Context, offset int, limit int) ([]WorkflowInstance, int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	instances := make([]WorkflowInstance, 0, len(s.instances))
	for _, instance := range s.instances {
		instances = append(instances, *instance)
	}

	sort.Slice(instances, func(i, j int) bool {
		return instances[i].CreatedAt.After(instances[j].CreatedAt)
	})

	total := int64(len(instances))

	if offset >= len(instances) {
		return []WorkflowInstance{}, total, nil
	}

	end := offset + limit
	if end > len(instances) {
		end = len(instances)
	}

	return instances[offset:end], total, nil
}

func (s *MemoryStore) GetWorkflowSteps(ctx context.Context, instanceID int64) ([]WorkflowStep, error) {
	return s.GetStepsByInstance(ctx, instanceID)
}

func (s *MemoryStore) GetWorkflowEvents(ctx context.Context, instanceID int64) ([]WorkflowEvent, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	eventIDs, exists := s.eventsByInstance[instanceID]
	if !exists {
		return []WorkflowEvent{}, nil
	}

	events := make([]WorkflowEvent, 0, len(eventIDs))
	for _, eventID := range eventIDs {
		for _, event := range s.events {
			if event.ID == eventID {
				events = append(events, *event)
				break
			}
		}
	}

	sort.Slice(events, func(i, j int) bool {
		return events[i].CreatedAt.Before(events[j].CreatedAt)
	})

	return events, nil
}

func (s *MemoryStore) GetWorkflowStats(ctx context.Context) ([]WorkflowStats, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	statsMap := make(map[string]*WorkflowStats)

	for _, instance := range s.instances {
		def, exists := s.definitions[instance.WorkflowID]
		if !exists {
			continue
		}

		key := fmt.Sprintf("%s:%d", def.Name, def.Version)
		stats, exists := statsMap[key]
		if !exists {
			stats = &WorkflowStats{
				WorkflowName:       def.Name,
				Version:            def.Version,
				TotalInstances:     0,
				CompletedInstances: 0,
				FailedInstances:    0,
				RunningInstances:   0,
				AverageDuration:    0,
			}
			statsMap[key] = stats
		}

		stats.TotalInstances++
		switch instance.Status {
		case StatusCompleted:
			stats.CompletedInstances++
		case StatusFailed:
			stats.FailedInstances++
		case StatusRunning:
			stats.RunningInstances++
		}
	}

	statsList := make([]WorkflowStats, 0, len(statsMap))
	for _, stats := range statsMap {
		statsList = append(statsList, *stats)
	}

	return statsList, nil
}

func (s *MemoryStore) GetActiveStepsForUpdate(ctx context.Context, instanceID int64) ([]WorkflowStep, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stepIDs, exists := s.stepsByInstance[instanceID]
	if !exists {
		return []WorkflowStep{}, nil
	}

	steps := make([]WorkflowStep, 0)
	for _, stepID := range stepIDs {
		step := s.steps[stepID]
		if step != nil && (step.Status == StepStatusPending ||
			step.Status == StepStatusRunning ||
			step.Status == StepStatusWaitingDecision) {
			steps = append(steps, *step)
		}
	}

	sort.Slice(steps, func(i, j int) bool {
		return steps[i].CreatedAt.After(steps[j].CreatedAt)
	})

	return steps, nil
}

func (s *MemoryStore) CreateCancelRequest(ctx context.Context, req *WorkflowCancelRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.cancelRequests[req.InstanceID]; exists {
		return nil
	}

	req.ID = s.nextCancelRequestID
	req.CreatedAt = time.Now()
	s.nextCancelRequestID++

	s.cancelRequests[req.InstanceID] = req

	return nil
}

func (s *MemoryStore) GetCancelRequest(ctx context.Context, instanceID int64) (*WorkflowCancelRequest, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	req, exists := s.cancelRequests[instanceID]
	if !exists {
		return nil, ErrEntityNotFound
	}

	return req, nil
}

func (s *MemoryStore) DeleteCancelRequest(ctx context.Context, instanceID int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.cancelRequests, instanceID)

	return nil
}

func (s *MemoryStore) CreateHumanDecision(ctx context.Context, decision *HumanDecisionRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	decision.ID = s.nextHumanDecisionID
	decision.CreatedAt = time.Now()
	s.nextHumanDecisionID++

	s.humanDecisions[decision.StepID] = decision

	return nil
}

func (s *MemoryStore) GetHumanDecision(ctx context.Context, stepID int64) (*HumanDecisionRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	decision, exists := s.humanDecisions[stepID]
	if !exists {
		return nil, ErrEntityNotFound
	}

	return decision, nil
}

func (s *MemoryStore) UpdateStepStatus(ctx context.Context, stepID int64, status StepStatus) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	step, exists := s.steps[stepID]
	if !exists {
		return ErrEntityNotFound
	}

	step.Status = status

	return nil
}

func (s *MemoryStore) GetStepByID(ctx context.Context, stepID int64) (*WorkflowStep, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	step, exists := s.steps[stepID]
	if !exists {
		return nil, ErrEntityNotFound
	}

	return step, nil
}

func (s *MemoryStore) GetHumanDecisionStepByInstanceID(ctx context.Context, instanceID int64) (*WorkflowStep, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stepIDs, exists := s.stepsByInstance[instanceID]
	if !exists {
		return nil, ErrEntityNotFound
	}

	for i := len(stepIDs) - 1; i >= 0; i-- {
		step := s.steps[stepIDs[i]]
		if step != nil && step.StepType == StepTypeHuman {
			return step, nil
		}
	}

	return nil, ErrEntityNotFound
}

func (s *MemoryStore) CreateDeadLetterRecord(ctx context.Context, rec *DeadLetterRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	rec.ID = s.nextDeadLetterID
	rec.CreatedAt = time.Now()
	s.nextDeadLetterID++

	s.deadLetters[rec.ID] = rec

	return nil
}

func (s *MemoryStore) RequeueDeadLetter(
	ctx context.Context,
	dlqID int64,
	newInput *json.RawMessage,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	rec, exists := s.deadLetters[dlqID]
	if !exists {
		return ErrEntityNotFound
	}

	step, exists := s.steps[rec.StepID]
	if !exists {
		return ErrEntityNotFound
	}

	step.Status = StepStatusPending
	if newInput != nil {
		step.Input = *newInput
	}
	step.Error = nil
	step.RetryCount = 0
	step.CompensationRetryCount = 0
	step.StartedAt = nil
	step.CompletedAt = nil

	instance, exists := s.instances[rec.InstanceID]
	if exists && (instance.Status == StatusFailed || instance.Status == StatusDLQ) {
		instance.Status = StatusRunning
		instance.Error = nil
	}

	stepIDs := s.stepsByInstance[rec.InstanceID]
	for _, stepID := range stepIDs {
		step := s.steps[stepID]
		if step != nil && step.StepType == StepTypeJoin && step.Status == StepStatusPaused {
			step.Status = StepStatusPending
		}
	}

	delete(s.deadLetters, dlqID)

	return nil
}

func (s *MemoryStore) ListDeadLetters(ctx context.Context, offset int, limit int) ([]DeadLetterRecord, int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	total := int64(len(s.deadLetters))
	records := make([]DeadLetterRecord, 0, len(s.deadLetters))
	for _, rec := range s.deadLetters {
		records = append(records, *rec)
	}

	sort.Slice(records, func(i, j int) bool {
		return records[i].CreatedAt.After(records[j].CreatedAt)
	})

	start := offset
	end := offset + limit
	if start > len(records) {
		start = len(records)
	}
	if end > len(records) {
		end = len(records)
	}

	if start < end {
		records = records[start:end]
	} else {
		records = []DeadLetterRecord{}
	}

	return records, total, nil
}

func (s *MemoryStore) GetDeadLetterByID(ctx context.Context, id int64) (*DeadLetterRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rec, exists := s.deadLetters[id]
	if !exists {
		return nil, ErrEntityNotFound
	}

	return rec, nil
}

func (s *MemoryStore) PauseActiveStepsAndClearQueue(ctx context.Context, instanceID int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	stepIDs, exists := s.stepsByInstance[instanceID]
	if exists {
		for _, stepID := range stepIDs {
			step := s.steps[stepID]
			if step != nil && step.Status == StepStatusRunning {
				step.Status = StepStatusPaused
			}
		}
	}

	for id, item := range s.queue {
		if item.InstanceID == instanceID {
			delete(s.queue, id)
		}
	}

	return nil
}

func (s *MemoryStore) CleanupOldWorkflows(ctx context.Context, daysToKeep int) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cutoffTime := time.Now().AddDate(0, 0, -daysToKeep)
	deleted := int64(0)

	for id, instance := range s.instances {
		if instance.CompletedAt != nil && instance.CompletedAt.Before(cutoffTime) {
			delete(s.instances, id)
			delete(s.stepsByInstance, id)

			for stepID, step := range s.steps {
				if step.InstanceID == id {
					delete(s.steps, stepID)
				}
			}

			delete(s.eventsByInstance, id)
			delete(s.cancelRequests, id)

			for joinKey := range s.joinStates {
				if s.joinStates[joinKey].InstanceID == id {
					delete(s.joinStates, joinKey)
				}
			}

			deleted++
		}
	}

	return deleted, nil
}

func (s *MemoryStore) joinStateKey(instanceID int64, joinStepName string) string {
	return fmt.Sprintf("%d:%s", instanceID, joinStepName)
}
