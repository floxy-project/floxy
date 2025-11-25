package floxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	defaultCancelWorkerInterval = 100 * time.Millisecond
	defaultShutdownTimeout      = 5 * time.Second
)

type Engine struct {
	txManager            TxManager
	store                Store
	handlers             map[string]StepHandler
	mu                   sync.RWMutex
	cancelContexts       map[int64]map[int64]context.CancelFunc // instanceID -> stepID -> cancel function
	cancelMu             sync.RWMutex
	cancelWorkerInterval time.Duration

	// Shutdown logic controls
	shutdownCh     chan struct{}
	shutdownOnce   sync.Once
	shutdownCtx    context.Context
	shutdownCancel context.CancelFunc
	activeSteps    sync.WaitGroup
	isShuttingDown bool
	shutdownMu     sync.RWMutex

	humanDecisionWaitingOnce   sync.Once
	humanDecisionWaitingEvents chan HumanDecisionWaitingEvent

	pluginManager *PluginManager

	// Missing-handler behavior controls
	missingHandlerCooldown    time.Duration
	missingHandlerLogThrottle time.Duration
	missingHandlerJitterPct   float64
	skipLogMu                 sync.Mutex
	skipLogNextAllowed        map[string]time.Time
}

func NewEngine(pool *pgxpool.Pool, opts ...EngineOption) *Engine {
	shutdownCtx, shutdownCancel := context.WithCancel(context.Background())

	engine := &Engine{
		txManager:            NewTxManager(pool),
		store:                NewStore(pool),
		handlers:             make(map[string]StepHandler),
		cancelContexts:       make(map[int64]map[int64]context.CancelFunc),
		shutdownCh:           make(chan struct{}),
		cancelWorkerInterval: defaultCancelWorkerInterval,
		shutdownCtx:          shutdownCtx,
		shutdownCancel:       shutdownCancel,
		// defaults for missing-handler behavior
		missingHandlerCooldown:    time.Second,
		missingHandlerLogThrottle: 5 * time.Second,
		missingHandlerJitterPct:   0.2,
		skipLogNextAllowed:        make(map[string]time.Time),
	}

	for _, opt := range opts {
		opt(engine)
	}

	go engine.cancelRequestsWorker()

	return engine
}

// jitteredCooldown returns cooldown with +/-jitter percentage applied.
func (engine *Engine) jitteredCooldown() time.Duration {
	base := engine.missingHandlerCooldown
	if base <= 0 {
		return 0
	}
	pct := engine.missingHandlerJitterPct
	if pct <= 0 {
		return base
	}
	// random in [-pct, +pct]
	delta := (rand.Float64()*2 - 1) * pct
	adj := float64(base) * (1 + delta)
	if adj < float64(time.Millisecond) {
		adj = float64(time.Millisecond)
	}

	return time.Duration(adj)
}

// shouldLogSkip returns true if we should emit a skip event for the given key now.
func (engine *Engine) shouldLogSkip(key string) bool {
	if engine.missingHandlerLogThrottle <= 0 {
		return true
	}

	now := time.Now()

	engine.skipLogMu.Lock()
	defer engine.skipLogMu.Unlock()

	if next, ok := engine.skipLogNextAllowed[key]; ok && now.Before(next) {
		return false
	}

	engine.skipLogNextAllowed[key] = now.Add(engine.missingHandlerLogThrottle)

	return true
}

func (engine *Engine) RegisterHandler(handler StepHandler) {
	engine.mu.Lock()
	defer engine.mu.Unlock()
	engine.handlers[handler.Name()] = wrapProcessPanicHandler(handler)
}

func (engine *Engine) RegisterPlugin(plugin Plugin) {
	engine.mu.Lock()
	defer engine.mu.Unlock()

	if engine.pluginManager == nil {
		engine.pluginManager = NewPluginManager()
	}

	engine.pluginManager.Register(plugin)
}

func (engine *Engine) RegisterWorkflow(ctx context.Context, def *WorkflowDefinition) error {
	if err := engine.validateDefinition(def); err != nil {
		return fmt.Errorf("invalid workflow definition: %w", err)
	}

	return engine.store.SaveWorkflowDefinition(ctx, def)
}

// RequeueFromDLQ extracts a record from the DLQ and re-enqueues its step.
// If newInput is non-nil, it will be used as the step input before enqueueing.
func (engine *Engine) RequeueFromDLQ(ctx context.Context, dlqID int64, newInput *json.RawMessage) error {
	return engine.txManager.ReadCommitted(ctx, func(ctx context.Context) error {
		if err := engine.store.RequeueDeadLetter(ctx, dlqID, newInput); err != nil {
			return fmt.Errorf("requeue dead letter: %w", err)
		}

		return nil
	})
}

func (engine *Engine) Start(ctx context.Context, workflowID string, input json.RawMessage) (int64, error) {
	var instanceID int64

	err := engine.txManager.ReadCommitted(ctx, func(ctx context.Context) error {
		def, err := engine.store.GetWorkflowDefinition(ctx, workflowID)
		if err != nil {
			return fmt.Errorf("get workflow definition: %w", err)
		}

		if err := engine.validateDefinition(def); err != nil {
			return fmt.Errorf("invalid workflow definition: %w", err)
		}

		instance, err := engine.store.CreateInstance(ctx, workflowID, input)
		if err != nil {
			return fmt.Errorf("create instance: %w", err)
		}

		// PLUGIN HOOK: OnWorkflowStart
		if engine.pluginManager != nil {
			if err := engine.pluginManager.ExecuteWorkflowStart(ctx, instance); err != nil {
				return fmt.Errorf("plugin hook failed: %w", err)
			}
		}

		_ = engine.store.LogEvent(ctx, instance.ID, nil, EventWorkflowStarted, map[string]any{
			KeyWorkflowID: workflowID,
		})

		if err := engine.store.UpdateInstanceStatus(ctx, instance.ID, StatusRunning, nil, nil); err != nil {
			return fmt.Errorf("update status: %w", err)
		}

		startStep := def.Definition.Start
		if startStep == "" {
			return errors.New("no start step defined")
		}

		if err := engine.enqueueNextSteps(ctx, instance.ID, []string{startStep}, input); err != nil {
			return fmt.Errorf("enqueue start step: %w", err)
		}

		instanceID = instance.ID

		return nil
	})
	if err != nil {
		return 0, err
	}

	return instanceID, nil
}

func (engine *Engine) Shutdown(timeoutOpt ...time.Duration) error {
	var shutdownErr error

	engine.shutdownOnce.Do(func() {
		slog.Info("[floxy] starting graceful shutdown")

		engine.shutdownMu.Lock()
		engine.isShuttingDown = true
		engine.shutdownMu.Unlock()

		close(engine.shutdownCh)
		engine.shutdownCancel()

		var timeout time.Duration
		if len(timeoutOpt) > 0 {
			timeout = timeoutOpt[0]
		}

		if timeout == 0 {
			timeout = defaultShutdownTimeout
		}

		done := make(chan struct{})
		go func() {
			engine.activeSteps.Wait()
			close(done)
		}()

		select {
		case <-done:
			slog.Info("[floxy] graceful shutdown completed")
		case <-time.After(timeout):
			shutdownErr = fmt.Errorf("shutdown timeout after %v", timeout)
			slog.Warn("[floxy] shutdown timeout", "timeout", timeout)
		}
	})

	return shutdownErr
}

func (engine *Engine) ExecuteNext(ctx context.Context, workerID string) (empty bool, err error) {
	if engine.isShutdown() {
		return true, nil
	}

	err = engine.txManager.ReadCommitted(ctx, func(ctx context.Context) error {
		if engine.isShutdown() {
			empty = true

			return nil
		}

		item, err := engine.store.DequeueStep(ctx, workerID)
		if err != nil {
			return fmt.Errorf("dequeue step: %w", err)
		}

		if item == nil {
			empty = true

			return nil
		}

		if engine.isShutdown() {
			_ = engine.store.ReleaseQueueItem(ctx, item.ID)
			empty = true

			return nil
		}

		engine.activeSteps.Add(1)
		defer engine.activeSteps.Done()

		removeFromQueue := true
		defer func() {
			if removeFromQueue {
				_ = engine.store.RemoveFromQueue(ctx, item.ID)
			}
		}()

		instance, err := engine.store.GetInstance(ctx, item.InstanceID)
		if err != nil {
			return fmt.Errorf("get instance: %w", err)
		}

		// If workflow is in DLQ state, skip execution to prevent progress until operator intervention
		if instance.Status == StatusDLQ {
			_ = engine.store.LogEvent(ctx, instance.ID, nil, EventStepFailed, map[string]any{
				KeyMessage: "instance in DLQ state, skipping queued item",
			})
			return nil
		}

		var step *WorkflowStep
		if item.StepID == nil {
			step, err = engine.createFirstStep(ctx, instance)
			if err != nil {
				return fmt.Errorf("create first step: %w", err)
			}
		} else {
			steps, err := engine.store.GetStepsByInstance(ctx, instance.ID)
			if err != nil {
				return fmt.Errorf("get steps: %w", err)
			}

			for _, currStep := range steps {
				currStep := currStep
				if currStep.ID == *item.StepID {
					step = &currStep

					break
				}
			}

			if step == nil {
				return fmt.Errorf("step not found: %d", *item.StepID)
			}
		}

		// Do not execute skipped or paused steps
		if step.Status == StepStatusSkipped ||
			step.Status == StepStatusPaused ||
			step.Status == StepStatusRolledBack {
			if err := engine.store.RemoveFromQueue(ctx, step.ID); err != nil {
				return fmt.Errorf("remove step from queue: %w", err)
			}

			return nil
		}

		// Check if this is a compensation
		if step.Status == StepStatusCompensation {
			// For distributed setup: if local engine doesn't have the compensation handler,
			// release the queue item so another service can execute the compensation.
			def, err := engine.store.GetWorkflowDefinition(ctx, instance.WorkflowID)
			if err != nil {
				return fmt.Errorf("get workflow definition: %w", err)
			}
			stepDef, ok := def.Definition.Steps[step.StepName]
			if !ok {
				return fmt.Errorf("step definition not found: %s", step.StepName)
			}
			if stepDef.OnFailure != "" {
				if onFailDef, ok := def.Definition.Steps[stepDef.OnFailure]; ok {
					engine.mu.RLock()
					_, has := engine.handlers[onFailDef.Handler]
					engine.mu.RUnlock()
					if !has {
						// Reschedule with cooldown and release so another service can pick it up later
						delay := engine.jitteredCooldown()
						if delay > 0 {
							if err := engine.store.RescheduleAndReleaseQueueItem(ctx, item.ID, delay); err != nil {
								return fmt.Errorf("reschedule queue item: %w", err)
							}
						} else {
							if err := engine.store.ReleaseQueueItem(ctx, item.ID); err != nil {
								return fmt.Errorf("release queue item: %w", err)
							}
						}

						removeFromQueue = false
						// Throttle skip logs to avoid flooding
						logKey := fmt.Sprintf("comp-skip:%d:%s", instance.ID, step.StepName)
						if engine.shouldLogSkip(logKey) {
							_ = engine.store.LogEvent(ctx, instance.ID, nil, EventStepSkippedMissingHandler, map[string]any{
								KeyStepName: step.StepName,
								KeyMessage:  "no local compensation handler registered; rescheduled",
							})
						}

						return nil
					}
				}
			}

			return engine.executeCompensationStep(ctx, instance, step)
		}

		// Distributed handlers: if this is a task step and no local handler is registered,
		// release the queue item so another service can pick it up, without failing the step.
		def, err := engine.store.GetWorkflowDefinition(ctx, instance.WorkflowID)
		if err != nil {
			return fmt.Errorf("get workflow definition: %w", err)
		}
		stepDef, ok := def.Definition.Steps[step.StepName]
		if !ok {
			return fmt.Errorf("step definition not found: %s", step.StepName)
		}
		if step.StepType == StepTypeTask {
			engine.mu.RLock()
			_, has := engine.handlers[stepDef.Handler]
			engine.mu.RUnlock()
			if !has {
				// Reschedule with cooldown and release so another service can pick it up later
				delay := engine.jitteredCooldown()
				if delay > 0 {
					if err := engine.store.RescheduleAndReleaseQueueItem(ctx, item.ID, delay); err != nil {
						return fmt.Errorf("reschedule queue item: %w", err)
					}
				} else {
					if err := engine.store.ReleaseQueueItem(ctx, item.ID); err != nil {
						return fmt.Errorf("release queue item: %w", err)
					}
				}
				removeFromQueue = false
				// Throttle skip logs to avoid flooding
				logKey := fmt.Sprintf("task-skip:%d:%s", instance.ID, step.StepName)
				if engine.shouldLogSkip(logKey) {
					_ = engine.store.LogEvent(ctx, instance.ID, nil, EventStepSkippedMissingHandler, map[string]any{
						KeyStepName: step.StepName,
						KeyMessage:  "no local handler registered; rescheduled",
					})
				}
				return nil
			}
		}

		return engine.executeStep(ctx, instance, step)
	})
	if err != nil {
		return empty, err
	}

	return empty, nil
}

func (engine *Engine) MakeHumanDecision(
	ctx context.Context,
	stepID int64,
	decidedBy string,
	decision HumanDecision,
	comment *string,
) error {
	return engine.txManager.ReadCommitted(ctx, func(ctx context.Context) error {
		step, err := engine.store.GetStepByID(ctx, stepID)
		if err != nil {
			return fmt.Errorf("get step: %w", err)
		}

		if step.StepType != StepTypeHuman {
			return fmt.Errorf("step %d is not a human step", stepID)
		}

		if step.Status != StepStatusWaitingDecision {
			return fmt.Errorf("step %d is not waiting for decision (current status: %s)", stepID, step.Status)
		}

		decisionRecord := &HumanDecisionRecord{
			InstanceID: step.InstanceID,
			StepID:     stepID,
			DecidedBy:  decidedBy,
			Decision:   decision,
			Comment:    comment,
			DecidedAt:  time.Now(),
		}

		if err := engine.store.CreateHumanDecision(ctx, decisionRecord); err != nil {
			return fmt.Errorf("create human decision: %w", err)
		}

		var newStatus StepStatus
		switch decision {
		case HumanDecisionConfirmed:
			newStatus = StepStatusConfirmed
		case HumanDecisionRejected:
			newStatus = StepStatusRejected
		}

		if err := engine.store.UpdateStepStatus(ctx, stepID, newStatus); err != nil {
			return fmt.Errorf("update step status: %w", err)
		}

		_ = engine.store.LogEvent(ctx, step.InstanceID, &stepID, EventStepCompleted, map[string]any{
			KeyStepName:  step.StepName,
			KeyDecision:  decision,
			KeyDecidedBy: decidedBy,
		})

		// If the decision is confirmed, continue workflow execution
		if decision == HumanDecisionConfirmed {
			// Set the workflow to running status if it was paused
			instance, err := engine.store.GetInstance(ctx, step.InstanceID)
			if err != nil {
				return fmt.Errorf("get instance: %w", err)
			}

			if instance.Status == StatusPending {
				if err := engine.store.UpdateInstanceStatus(ctx, step.InstanceID, StatusRunning, nil, nil); err != nil {
					return fmt.Errorf("update instance status: %w", err)
				}
			}

			// Continue execution of next steps
			return engine.continueWorkflowAfterHumanDecision(ctx, instance, step)
		} else {
			// If the decision is rejected, stop the workflow
			return engine.store.UpdateInstanceStatus(ctx, step.InstanceID, StatusAborted, nil, nil)
		}
	})
}

func (engine *Engine) CancelWorkflow(ctx context.Context, instanceID int64, requestedBy, reason string) error {
	return engine.txManager.ReadCommitted(ctx, func(ctx context.Context) error {
		instance, err := engine.store.GetInstance(ctx, instanceID)
		if err != nil {
			return fmt.Errorf("get instance: %w", err)
		}

		if instance.Status == StatusCompleted ||
			instance.Status == StatusFailed ||
			instance.Status == StatusCancelled ||
			instance.Status == StatusAborted {
			return fmt.Errorf("workflow %d is already in terminal state: %s", instanceID, instance.Status)
		}

		req := &WorkflowCancelRequest{
			InstanceID:  instanceID,
			RequestedBy: requestedBy,
			CancelType:  CancelTypeCancel,
			Reason:      &reason,
		}

		if err := engine.store.CreateCancelRequest(ctx, req); err != nil {
			return fmt.Errorf("create cancel request: %w", err)
		}

		_ = engine.store.LogEvent(ctx, instanceID, nil, EventCancellationStarted, map[string]any{
			KeyRequestedBy: requestedBy,
			KeyReason:      reason,
			KeyCancelType:  CancelTypeCancel,
		})

		return nil
	})
}

func (engine *Engine) AbortWorkflow(ctx context.Context, instanceID int64, requestedBy, reason string) error {
	return engine.txManager.ReadCommitted(ctx, func(ctx context.Context) error {
		instance, err := engine.store.GetInstance(ctx, instanceID)
		if err != nil {
			return fmt.Errorf("get instance: %w", err)
		}

		if instance.Status == StatusCompleted ||
			instance.Status == StatusFailed ||
			instance.Status == StatusCancelled ||
			instance.Status == StatusAborted {
			return fmt.Errorf("workflow %d is already in terminal state: %s", instanceID, instance.Status)
		}

		req := &WorkflowCancelRequest{
			InstanceID:  instanceID,
			RequestedBy: requestedBy,
			CancelType:  CancelTypeAbort,
			Reason:      &reason,
		}

		if err := engine.store.CreateCancelRequest(ctx, req); err != nil {
			return fmt.Errorf("create cancel request: %w", err)
		}

		_ = engine.store.LogEvent(ctx, instanceID, nil, EventAbortStarted, map[string]any{
			KeyRequestedBy: requestedBy,
			KeyReason:      reason,
			KeyCancelType:  CancelTypeAbort,
		})

		return nil
	})
}

func (engine *Engine) isShutdown() bool {
	engine.shutdownMu.RLock()
	defer engine.shutdownMu.RUnlock()

	return engine.isShuttingDown
}

func (engine *Engine) cancelRequestsWorker() {
	ticker := time.NewTicker(engine.cancelWorkerInterval)
	defer ticker.Stop()

	for {
		select {
		case <-engine.shutdownCh:
			return
		case <-ticker.C:
			engine.processCancelRequests()
		}
	}
}

func (engine *Engine) processCancelRequests() {
	engine.cancelMu.RLock()
	instanceIDs := make([]int64, 0, len(engine.cancelContexts))
	for instanceID := range engine.cancelContexts {
		instanceIDs = append(instanceIDs, instanceID)
	}
	engine.cancelMu.RUnlock()

	for _, instanceID := range instanceIDs {
		ctx := context.Background()
		_, err := engine.store.GetCancelRequest(ctx, instanceID)
		if err != nil {
			if !errors.Is(err, ErrEntityNotFound) {
				slog.Error("[floxy] get cancel request for instance failed",
					"instance_id", instanceID, "error", err)
			}

			continue
		}

		engine.cancelMu.Lock()
		if stepContexts, exists := engine.cancelContexts[instanceID]; exists {
			for stepID, cancelFunc := range stepContexts {
				cancelFunc()
				delete(stepContexts, stepID)
			}

			delete(engine.cancelContexts, instanceID)
		}
		engine.cancelMu.Unlock()
	}
}

func (engine *Engine) registerInstanceContext(instanceID int64, stepID int64, cancel context.CancelFunc) {
	engine.cancelMu.Lock()
	defer engine.cancelMu.Unlock()

	if engine.cancelContexts[instanceID] == nil {
		engine.cancelContexts[instanceID] = make(map[int64]context.CancelFunc)
	}
	engine.cancelContexts[instanceID][stepID] = cancel
}

func (engine *Engine) unregisterInstanceContext(instanceID int64, stepID int64) {
	engine.cancelMu.Lock()
	defer engine.cancelMu.Unlock()

	if stepContexts, exists := engine.cancelContexts[instanceID]; exists {
		delete(stepContexts, stepID)
		if len(stepContexts) == 0 {
			delete(engine.cancelContexts, instanceID)
		}
	}
}

func (engine *Engine) stopActiveSteps(ctx context.Context, instanceID int64) error {
	activeSteps, err := engine.store.GetActiveStepsForUpdate(ctx, instanceID)
	if err != nil {
		return fmt.Errorf("get active steps: %w", err)
	}

	for _, step := range activeSteps {
		skipMsg := "Stopped due to workflow cancellation/abort"
		if err := engine.store.UpdateStep(ctx, step.ID, StepStatusSkipped, nil, &skipMsg); err != nil {
			return fmt.Errorf("update step %d status to skipped: %w", step.ID, err)
		}

		_ = engine.store.LogEvent(ctx, instanceID, &step.ID, EventStepFailed, map[string]any{
			KeyStepName: step.StepName,
			KeyReason:   "workflow_stopped",
		})
	}

	return nil
}

func (engine *Engine) executeStep(ctx context.Context, instance *WorkflowInstance, step *WorkflowStep) error {
	// If workflow is in DLQ state, do not execute any steps until operator requeues
	if instance.Status == StatusDLQ {
		return nil
	}

	def, err := engine.store.GetWorkflowDefinition(ctx, instance.WorkflowID)
	if err != nil {
		return fmt.Errorf("get workflow definition: %w", err)
	}

	stepDef, ok := def.Definition.Steps[step.StepName]
	if !ok {
		return fmt.Errorf("step definition not found: %s", step.StepName)
	}

	// PLUGIN HOOK: OnStepStart
	if engine.pluginManager != nil {
		if err := engine.pluginManager.ExecuteStepStart(ctx, instance, step); err != nil {
			return fmt.Errorf("plugin hook OnStepStart failed: %w", err)
		}
	}

	handlerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	engine.registerInstanceContext(instance.ID, step.ID, cancel)
	defer engine.unregisterInstanceContext(instance.ID, step.ID)

	cancelReq, err := engine.store.GetCancelRequest(ctx, instance.ID)
	if err == nil && cancelReq != nil {
		return engine.handleCancellation(ctx, instance, step, cancelReq)
	}

	if stepDef.Timeout != 0 {
		var timeoutCancel context.CancelFunc
		handlerCtx, timeoutCancel = context.WithTimeout(handlerCtx, stepDef.Timeout)
		defer timeoutCancel()
	}

	if err := engine.store.UpdateStep(ctx, step.ID, StepStatusRunning, nil, nil); err != nil {
		return fmt.Errorf("update step status: %w", err)
	}

	_ = engine.store.LogEvent(ctx, instance.ID, &step.ID, EventStepStarted, map[string]any{
		KeyStepName: step.StepName,
		KeyStepType: stepDef.Type,
	})

	var output json.RawMessage
	var stepErr error
	next := true

	switch stepDef.Type {
	case StepTypeTask:
		output, stepErr = engine.executeTask(handlerCtx, instance, step, stepDef)
	case StepTypeFork:
		output, stepErr = engine.executeFork(handlerCtx, instance, step, stepDef)
	case StepTypeJoin:
		output, stepErr = engine.executeJoin(handlerCtx, instance, step, stepDef)
	case StepTypeParallel:
		output, stepErr = engine.executeFork(handlerCtx, instance, step, stepDef)
	case StepTypeSavePoint:
		output = step.Input
	case StepTypeCondition:
		output, next, stepErr = engine.executeCondition(handlerCtx, instance, step, stepDef)
	case StepTypeHuman:
		var aborted bool
		output, aborted, stepErr = engine.executeHuman(handlerCtx, instance, step, stepDef)
		if stepErr == nil && aborted {
			return nil
		}
	default:
		stepErr = fmt.Errorf("unsupported step type: %s", stepDef.Type)
	}

	if errors.Is(handlerCtx.Err(), context.Canceled) {
		cancelReq, err = engine.store.GetCancelRequest(ctx, instance.ID)
		if err == nil && cancelReq != nil {
			return engine.handleCancellation(ctx, instance, step, cancelReq)
		}
	}

	if stepErr != nil {
		// PLUGIN HOOK: OnStepFailed
		if engine.pluginManager != nil {
			if errPlugin := engine.pluginManager.ExecuteStepFailed(ctx, instance, step, stepErr); errPlugin != nil {
				slog.Warn("[floxy] plugin hook OnStepFailed failed", "error", err)
			}
		}

		return engine.handleStepFailure(ctx, instance, step, stepDef, stepErr)
	}

	// PLUGIN HOOK: OnStepComplete
	if engine.pluginManager != nil {
		if err := engine.pluginManager.ExecuteStepComplete(ctx, instance, step); err != nil {
			slog.Warn("[floxy] plugin hook OnStepComplete failed", "error", err)
		}
	}

	return engine.handleStepSuccess(ctx, instance, step, stepDef, output, next)
}

func (engine *Engine) handleCancellation(
	ctx context.Context,
	instance *WorkflowInstance,
	step *WorkflowStep,
	req *WorkflowCancelRequest,
) error {
	if err := engine.stopActiveSteps(ctx, instance.ID); err != nil {
		return fmt.Errorf("stop active steps: %w", err)
	}

	reason := ""
	if req.Reason != nil {
		reason = *req.Reason
	}

	if req.CancelType == CancelTypeCancel {
		err := engine.store.UpdateInstanceStatus(ctx, instance.ID, StatusCancelling, nil, nil)
		if err != nil {
			return fmt.Errorf("update instance status to cancelling: %w", err)
		}

		def, err := engine.store.GetWorkflowDefinition(ctx, instance.WorkflowID)
		if err != nil {
			return fmt.Errorf("get workflow definition: %w", err)
		}

		// Get fresh steps after stopActiveSteps to ensure we have current statuses
		steps, err := engine.store.GetStepsByInstance(ctx, instance.ID)
		if err != nil {
			return fmt.Errorf("get steps: %w", err)
		}

		// Create stepMap with pointers to actual step values, not copies
		stepMap := make(map[string]*WorkflowStep)
		for i := range steps {
			stepMap[steps[i].StepName] = &steps[i]
		}

		var lastCompletedStep *WorkflowStep
		for i := len(steps) - 1; i >= 0; i-- {
			if steps[i].Status == StepStatusCompleted {
				lastCompletedStep = &steps[i]

				break
			}
		}

		if lastCompletedStep != nil {
			err := engine.rollbackStepChain(ctx, instance.ID, lastCompletedStep.StepName, rootStepName, def, stepMap, false, 0)
			if err != nil {
				_ = engine.store.LogEvent(ctx, instance.ID, &step.ID, EventWorkflowCancelled, map[string]any{
					KeyRequestedBy: req.RequestedBy,
					KeyReason:      reason,
					KeyError:       fmt.Sprintf("rollback failed: %v", err),
				})

				errMsg := fmt.Sprintf("cancellation rollback failed: %v", err)
				_ = engine.store.UpdateInstanceStatus(ctx, instance.ID, StatusFailed, nil, &errMsg)
				_ = engine.store.DeleteCancelRequest(ctx, instance.ID)

				return fmt.Errorf("rollback failed: %w", err)
			}
		}

		cancelMsg := fmt.Sprintf("Cancelled by %s: %s", req.RequestedBy, reason)
		err = engine.store.UpdateInstanceStatus(ctx, instance.ID, StatusCancelled, nil, &cancelMsg)
		if err != nil {
			return fmt.Errorf("update instance status to cancelled: %w", err)
		}

		_ = engine.store.LogEvent(ctx, instance.ID, &step.ID, EventWorkflowCancelled, map[string]any{
			KeyRequestedBy: req.RequestedBy,
			KeyReason:      reason,
		})
	} else {
		abortMsg := fmt.Sprintf("Aborted by %s: %s", req.RequestedBy, reason)
		if err := engine.store.UpdateInstanceStatus(ctx, instance.ID, StatusAborted, nil, &abortMsg); err != nil {
			return fmt.Errorf("update instance status to aborted: %w", err)
		}

		_ = engine.store.LogEvent(ctx, instance.ID, &step.ID, EventWorkflowAborted, map[string]any{
			KeyRequestedBy: req.RequestedBy,
			KeyReason:      reason,
		})
	}

	_ = engine.store.DeleteCancelRequest(ctx, instance.ID)

	return nil
}

func (engine *Engine) executeCompensationStep(ctx context.Context, instance *WorkflowInstance, step *WorkflowStep) error {
	def, err := engine.store.GetWorkflowDefinition(ctx, instance.WorkflowID)
	if err != nil {
		return fmt.Errorf("get workflow definition: %w", err)
	}

	stepDef, ok := def.Definition.Steps[step.StepName]
	if !ok {
		return fmt.Errorf("step definition not found: %s", step.StepName)
	}

	if stepDef.Timeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, stepDef.Timeout)
		defer cancel()
	}

	onFailureStep, ok := def.Definition.Steps[stepDef.OnFailure]
	if !ok {
		// No compensation handler, mark as rolled back
		if err := engine.store.UpdateStep(ctx, step.ID, StepStatusRolledBack, step.Input, nil); err != nil {
			return fmt.Errorf("update step status: %w", err)
		}
		return nil
	}

	handler, exists := engine.handlers[onFailureStep.Handler]
	if !exists {
		// Handler not found, mark as rolled back
		if err := engine.store.UpdateStep(ctx, step.ID, StepStatusRolledBack, step.Input, nil); err != nil {
			return fmt.Errorf("update step status: %w", err)
		}
		return nil
	}

	variables := make(map[string]any, len(onFailureStep.Metadata)+1)
	for k, v := range onFailureStep.Metadata {
		variables[k] = v
	}
	variables["reason"] = "compensation"

	stepCtx := executionContext{
		instanceID:     step.InstanceID,
		stepName:       step.StepName,
		idempotencyKey: step.IdempotencyKey,
		retryCount:     step.CompensationRetryCount,
		variables:      variables,
	}

	// Execute the compensation handler
	_, compensationErr := handler.Execute(ctx, &stepCtx, step.Input)
	if compensationErr != nil {
		// Compensation failed, check if we can retry
		if step.CompensationRetryCount < onFailureStep.MaxRetries {
			// Retry compensation
			newRetryCount := step.CompensationRetryCount + 1
			if err := engine.store.UpdateStepCompensationRetry(ctx, step.ID, newRetryCount, StepStatusCompensation); err != nil {
				return fmt.Errorf("update compensation retry: %w", err)
			}

			// Re-enqueue for retry
			retryDelay := CalculateRetryDelay(onFailureStep.RetryStrategy, onFailureStep.RetryDelay, newRetryCount)
			if retryDelay == 0 {
				retryDelay = onFailureStep.Delay
			}
			if err := engine.store.EnqueueStep(ctx, step.InstanceID, &step.ID, PriorityHigh, retryDelay); err != nil {
				return fmt.Errorf("enqueue compensation retry: %w", err)
			}

			_ = engine.store.LogEvent(ctx, step.InstanceID, &step.ID, EventStepFailed, map[string]any{
				KeyStepName:   step.StepName,
				KeyStepType:   step.StepType,
				KeyError:      compensationErr.Error(),
				KeyRetryCount: newRetryCount,
				KeyReason:     "compensation_retry",
			})

			return nil
		} else {
			// Max retries exceeded, mark as failed
			errorMsg := compensationErr.Error()
			if err := engine.store.UpdateStep(ctx, step.ID, StepStatusFailed, step.Input, &errorMsg); err != nil {
				return fmt.Errorf("update step status: %w", err)
			}

			_ = engine.store.LogEvent(ctx, step.InstanceID, &step.ID, EventStepFailed, map[string]any{
				KeyStepName: step.StepName,
				KeyStepType: step.StepType,
				KeyError:    "compensation max retries exceeded",
			})

			return nil
		}
	}

	// Compensation successful, mark as rolled back
	if err := engine.store.UpdateStep(ctx, step.ID, StepStatusRolledBack, step.Input, nil); err != nil {
		return fmt.Errorf("update step status: %w", err)
	}

	_ = engine.store.LogEvent(ctx, step.InstanceID, &step.ID, EventStepCompleted, map[string]any{
		KeyStepName:   step.StepName,
		KeyStepType:   step.StepType,
		KeyRetryCount: step.CompensationRetryCount,
		KeyReason:     "compensation_success",
	})

	// Check if all compensation steps are finished
	// If no more unfinished steps and no compensation steps, finalize workflow status
	if !engine.hasUnfinishedSteps(ctx, step.InstanceID) && !engine.hasStepsInCompensation(ctx, step.InstanceID) {
		// All compensation done, now check final status
		if engine.hasFailedOrRolledBackSteps(ctx, step.InstanceID) {
			// Mark workflow as failed
			errMsg := "workflow has failed or rolled back steps"
			if err := engine.store.UpdateInstanceStatus(ctx, step.InstanceID, StatusFailed, nil, &errMsg); err != nil {
				return fmt.Errorf("update instance status to failed: %w", err)
			}

			_ = engine.store.LogEvent(ctx, step.InstanceID, nil, EventWorkflowFailed, map[string]any{
				KeyWorkflowID: instance.WorkflowID,
				KeyReason:     errMsg,
			})

			// PLUGIN HOOK: OnWorkflowFailed
			if engine.pluginManager != nil {
				finalInstance, _ := engine.store.GetInstance(ctx, step.InstanceID)
				if finalInstance != nil {
					if errPlugin := engine.pluginManager.ExecuteWorkflowFailed(ctx, finalInstance); errPlugin != nil {
						slog.Warn("[floxy] plugin hook OnWorkflowFailed failed", "error", errPlugin)
					}
				}
			}
		}
	}

	return nil
}

func (engine *Engine) executeTask(
	ctx context.Context,
	instance *WorkflowInstance,
	step *WorkflowStep,
	stepDef *StepDefinition,
) (json.RawMessage, error) {
	engine.mu.RLock()
	handler, ok := engine.handlers[stepDef.Handler]
	engine.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("handler not found: %s", stepDef.Handler)
	}

	execCtx := &executionContext{
		instanceID:     instance.ID,
		stepName:       step.StepName,
		idempotencyKey: step.IdempotencyKey,
		retryCount:     step.RetryCount,
		variables:      stepDef.Metadata,
	}

	return handler.Execute(ctx, execCtx, step.Input)
}

func (engine *Engine) executeFork(
	ctx context.Context,
	instance *WorkflowInstance,
	step *WorkflowStep,
	stepDef *StepDefinition,
) (json.RawMessage, error) {
	_ = engine.store.LogEvent(ctx, instance.ID, &step.ID, EventForkStarted, map[string]any{
		KeyParallelSteps: stepDef.Parallel,
	})

	def, err := engine.store.GetWorkflowDefinition(ctx, instance.WorkflowID)
	if err != nil {
		return nil, fmt.Errorf("get workflow definition: %w", err)
	}

	for _, parallelStepName := range stepDef.Parallel {
		parallelStepDef, ok := def.Definition.Steps[parallelStepName]
		if !ok {
			return nil, fmt.Errorf("parallel step definition not found: %s", parallelStepName)
		}

		parallelStep := &WorkflowStep{
			InstanceID: instance.ID,
			StepName:   parallelStepName,
			StepType:   parallelStepDef.Type,
			Status:     StepStatusPending,
			Input:      step.Input,
			MaxRetries: parallelStepDef.MaxRetries,
		}

		if err := engine.store.CreateStep(ctx, parallelStep); err != nil {
			return nil, fmt.Errorf("create fork step %s: %w", parallelStepName, err)
		}

		if err := engine.store.EnqueueStep(ctx, instance.ID, &parallelStep.ID, PriorityNormal, parallelStepDef.Delay); err != nil {
			return nil, fmt.Errorf("enqueue fork step %s: %w", parallelStepName, err)
		}
	}

	for _, nextStepName := range stepDef.Next {
		nextStepDef, ok := def.Definition.Steps[nextStepName]

		if ok && nextStepDef.Type == StepTypeJoin {
			strategy := nextStepDef.JoinStrategy
			if strategy == "" {
				strategy = JoinStrategyAll
			}

			waitFor := nextStepDef.WaitFor
			if len(waitFor) == 0 {
				waitFor = stepDef.Parallel
			}

			err := engine.store.CreateJoinState(ctx, instance.ID, nextStepName, waitFor, strategy)
			if err != nil {
				return nil, fmt.Errorf("create join state: %w", err)
			}

			_ = engine.store.LogEvent(ctx, instance.ID, &step.ID, EventJoinStateCreated, map[string]any{
				KeyJoinStep:   nextStepName,
				KeyWaitingFor: waitFor,
				KeyStrategy:   strategy,
			})
		}
	}

	return json.Marshal(map[string]any{
		KeyStatus:        "forked",
		KeyParallelSteps: stepDef.Parallel,
	})
}

func (engine *Engine) executeJoin(
	ctx context.Context,
	instance *WorkflowInstance,
	step *WorkflowStep,
	_ *StepDefinition,
) (json.RawMessage, error) {
	joinState, err := engine.store.GetJoinState(ctx, instance.ID, step.StepName)
	if err != nil {
		return nil, fmt.Errorf("get join state: %w", err)
	}

	_ = engine.store.LogEvent(ctx, instance.ID, &step.ID, EventJoinCheck, map[string]any{
		KeyWaitingFor: joinState.WaitingFor,
		KeyCompleted:  joinState.Completed,
		KeyFailed:     joinState.Failed,
		KeyIsReady:    joinState.IsReady,
	})

	if !joinState.IsReady {
		return nil, fmt.Errorf("join not ready: waiting for %v", joinState.WaitingFor)
	}

	results := make(map[string]any)
	results[KeyCompleted] = joinState.Completed
	results[KeyFailed] = joinState.Failed
	results[KeyStrategy] = joinState.JoinStrategy

	steps, err := engine.store.GetStepsByInstance(ctx, instance.ID)
	if err != nil {
		return nil, fmt.Errorf("get steps: %w", err)
	}

	outputs := make(map[string]json.RawMessage)
	for _, s := range steps {
		for _, waitFor := range joinState.WaitingFor {
			if s.StepName == waitFor && s.Status == StepStatusCompleted {
				outputs[s.StepName] = s.Output
			}
		}
	}
	results[KeyOutputs] = outputs

	if len(joinState.Failed) > 0 && joinState.JoinStrategy == JoinStrategyAll {
		results[KeyStatus] = "failed"
		failedData, _ := json.Marshal(results)

		return failedData, fmt.Errorf("join failed: %d steps failed", len(joinState.Failed))
	}

	results[KeyStatus] = "success"

	_ = engine.store.LogEvent(ctx, instance.ID, &step.ID, EventJoinCompleted, results)

	return json.Marshal(results)
}

func (engine *Engine) executeCondition(
	ctx context.Context,
	instance *WorkflowInstance,
	step *WorkflowStep,
	stepDef *StepDefinition,
) (json.RawMessage, bool, error) {
	var inputData map[string]any
	_ = json.Unmarshal(step.Input, &inputData)

	stepCtx := executionContext{
		instanceID:     step.InstanceID,
		stepName:       step.StepName,
		idempotencyKey: step.IdempotencyKey,
		variables:      inputData,
	}

	result, err := evaluateCondition(stepDef.Condition, &stepCtx)
	if err != nil {
		_ = engine.store.LogEvent(ctx, instance.ID, &step.ID, EventConditionCheck, map[string]any{
			KeyStepName: step.StepName,
			KeyError:    err.Error(),
		})

		return nil, false, fmt.Errorf("evaluate condition: %w", err)
	}

	_ = engine.store.LogEvent(ctx, instance.ID, &step.ID, EventConditionCheck, map[string]any{
		KeyStepName: step.StepName,
		KeyResult:   result,
	})

	return step.Input, result, nil
}

func (engine *Engine) executeHuman(
	ctx context.Context,
	instance *WorkflowInstance,
	step *WorkflowStep,
	stepDef *StepDefinition,
) (json.RawMessage, bool, error) {
	// Check if there's already a decision for this step
	decision, err := engine.store.GetHumanDecision(ctx, step.ID)
	if err != nil && !errors.Is(err, ErrEntityNotFound) {
		return nil, false, fmt.Errorf("get human decision: %w", err)
	}

	if decision != nil {
		// Decision already made, process it
		return engine.processHumanDecision(ctx, instance, step, decision)
	}

	// No decision yet, set step to waiting state
	step.Status = StepStatusWaitingDecision
	if err := engine.store.UpdateStepStatus(ctx, step.ID, StepStatusWaitingDecision); err != nil {
		return nil, false, fmt.Errorf("update step status to waiting_decision: %w", err)
	}

	if err := engine.store.EnqueueStep(ctx, instance.ID, &step.ID, PriorityHigher, stepDef.Delay); err != nil {
		return nil, false, fmt.Errorf("enqueue step: %w", err)
	}

	_ = engine.store.LogEvent(ctx, instance.ID, &step.ID, EventStepStarted, map[string]any{
		KeyStepName: step.StepName,
		KeyStepType: stepDef.Type,
		KeyReason:   "waiting_for_human_decision",
	})

	// Parse input data and add waiting status
	var inputData map[string]any
	if err := json.Unmarshal(step.Input, &inputData); err != nil {
		// If input is not valid JSON, create empty map
		inputData = make(map[string]any)
	}

	// Add waiting status to the data
	inputData["status"] = "waiting_decision"
	inputData["message"] = "Step is waiting for human decision"

	// Encode back to JSON
	output, err := json.Marshal(inputData)
	if err != nil {
		return nil, false, fmt.Errorf("marshal output: %w", err)
	}

	event := HumanDecisionWaitingEvent{
		InstanceID: instance.ID,
		OutputData: output,
	}

	if engine.humanDecisionWaitingEvents != nil {
		engine.humanDecisionWaitingEvents <- event
	}

	return output, false, nil
}

func (engine *Engine) processHumanDecision(
	ctx context.Context,
	instance *WorkflowInstance,
	step *WorkflowStep,
	decision *HumanDecisionRecord,
) (json.RawMessage, bool, error) {
	// Update step status based on decision
	var newStatus StepStatus

	switch decision.Decision {
	case HumanDecisionConfirmed:
		newStatus = StepStatusConfirmed

		_ = engine.store.LogEvent(ctx, instance.ID, &step.ID, EventStepCompleted, map[string]any{
			KeyStepName:  step.StepName,
			KeyDecision:  decision.Decision,
			KeyDecidedBy: decision.DecidedBy,
		})

	case HumanDecisionRejected:
		newStatus = StepStatusRejected

		errMsg := "Step was rejected by human"
		if err := engine.store.UpdateInstanceStatus(ctx, instance.ID, StatusAborted, nil, &errMsg); err != nil {
			return nil, false, fmt.Errorf("update instance status: %w", err)
		}

		_ = engine.store.LogEvent(ctx, instance.ID, &step.ID, EventStepFailed, map[string]any{
			KeyStepName:  step.StepName,
			KeyDecision:  decision.Decision,
			KeyDecidedBy: decision.DecidedBy,
		})
	}

	// Update step status
	if err := engine.store.UpdateStepStatus(ctx, step.ID, newStatus); err != nil {
		return nil, false, fmt.Errorf("update step status: %w", err)
	}

	// Parse input data and add decision information
	var inputData map[string]any
	if err := json.Unmarshal(step.Input, &inputData); err != nil {
		// If input is not valid JSON, create empty map
		inputData = make(map[string]any)
	}

	// Add decision information to the data
	inputData["status"] = string(decision.Decision)
	inputData["decided_by"] = decision.DecidedBy
	if decision.Comment != nil {
		inputData["comment"] = *decision.Comment
	}
	inputData["decided_at"] = decision.DecidedAt

	// Encode back to JSON
	output, err := json.Marshal(inputData)
	if err != nil {
		return nil, false, fmt.Errorf("marshal output: %w", err)
	}

	return output, newStatus == StepStatusRejected, nil
}

func (engine *Engine) continueWorkflowAfterHumanDecision(
	ctx context.Context,
	instance *WorkflowInstance,
	step *WorkflowStep,
) error {
	// Get workflow definition
	def, err := engine.store.GetWorkflowDefinition(ctx, instance.WorkflowID)
	if err != nil {
		return fmt.Errorf("get workflow definition: %w", err)
	}

	stepDef, ok := def.Definition.Steps[step.StepName]
	if !ok {
		return fmt.Errorf("step definition not found: %s", step.StepName)
	}

	// Continue execution of next steps
	output := json.RawMessage(`{"status": "confirmed"}`)
	return engine.handleStepSuccess(ctx, instance, step, stepDef, output, true)
}

func (engine *Engine) handleStepSuccess(
	ctx context.Context,
	instance *WorkflowInstance,
	step *WorkflowStep,
	stepDef *StepDefinition,
	output json.RawMessage,
	next bool,
) error {
	// For human steps waiting for decision, don't update status
	if stepDef.Type == StepTypeHuman && step.Status == StepStatusWaitingDecision {
		// Don't continue execution, wait for human decision
		return nil
	}

	if err := engine.store.UpdateStep(ctx, step.ID, StepStatusCompleted, output, nil); err != nil {
		return fmt.Errorf("update step: %w", err)
	}

	_ = engine.store.LogEvent(ctx, instance.ID, &step.ID, EventStepCompleted, map[string]any{
		KeyStepName: step.StepName,
	})

	// Check if this is a terminal step in a fork branch
	def, err := engine.store.GetWorkflowDefinition(ctx, instance.WorkflowID)
	if err == nil {
		// First, check if we're in a Condition branch and need to replace virtual step
		conditionStepName := engine.findConditionStepInBranch(stepDef, def)
		if conditionStepName != "" {
			// Find Join step for this fork branch
			joinStepName, err := engine.findJoinStepForForkBranch(ctx, instance.ID, step.StepName, def)
			if err == nil && joinStepName != "" {
				virtualStep := fmt.Sprintf("cond#%s", conditionStepName)
				// Replace virtual step with real terminal step
				if err := engine.store.ReplaceInJoinWaitFor(ctx, instance.ID, joinStepName, virtualStep, step.StepName); err != nil {
					_ = engine.store.LogEvent(ctx, instance.ID, &step.ID, EventStepCompleted, map[string]any{
						KeyStepName: step.StepName,
						KeyError:    fmt.Sprintf("Failed to replace virtual step in join waitFor: %v", err),
					})
				} else {
					_ = engine.store.LogEvent(ctx, instance.ID, &step.ID, EventStepCompleted, map[string]any{
						KeyStepName: step.StepName,
						KeyMessage:  fmt.Sprintf("Replaced virtual step %s with %s in join %s", virtualStep, step.StepName, joinStepName),
					})
					// After replacing virtual step with real step, notify Join about the completion
					// This ensures Join is aware that the real step has completed
					if err := engine.notifyJoinStepsForStep(ctx, instance.ID, joinStepName, step.StepName, true); err != nil {
						_ = engine.store.LogEvent(ctx, instance.ID, &step.ID, EventStepCompleted, map[string]any{
							KeyStepName: step.StepName,
							KeyError:    fmt.Sprintf("Failed to notify join after virtual step replacement: %v", err),
						})
					}
				}
			}
		} else if engine.isTerminalStepInForkBranch(ctx, instance.ID, step.StepName, def) {
			// Not in Condition branch, use dynamic detection
			joinStepName, err := engine.findJoinStepForForkBranch(ctx, instance.ID, step.StepName, def)
			if err == nil && joinStepName != "" {
				// Check if this step is not already in the WaitFor list
				joinState, err := engine.store.GetJoinState(ctx, instance.ID, joinStepName)
				if err == nil && joinState != nil {
					isAlreadyWaiting := false
					isVirtual := false
					for _, waitFor := range joinState.WaitingFor {
						if waitFor == step.StepName {
							isAlreadyWaiting = true
							break
						}
						// Check if this is a virtual step (shouldn't happen here, but just in case)
						if len(waitFor) > 5 && waitFor[:5] == "cond#" {
							isVirtual = true
						}
					}
					if !isAlreadyWaiting && !isVirtual {
						// Add this terminal step to the Join step's WaitFor list
						if err := engine.store.AddToJoinWaitFor(ctx, instance.ID, joinStepName, step.StepName); err != nil {
							// Log error but don't fail the step
							_ = engine.store.LogEvent(ctx, instance.ID, &step.ID, EventStepCompleted, map[string]any{
								KeyStepName: step.StepName,
								KeyError:    fmt.Sprintf("Failed to add step to join waitFor: %v", err),
							})
						} else {
							_ = engine.store.LogEvent(ctx, instance.ID, &step.ID, EventStepCompleted, map[string]any{
								KeyStepName: step.StepName,
								KeyMessage:  fmt.Sprintf("Added terminal step to join %s waitFor", joinStepName),
							})
						}
					}
				} else {
					// JoinState doesn't exist yet, create it with this terminal step
					joinStepDef, ok := def.Definition.Steps[joinStepName]
					if ok {
						strategy := joinStepDef.JoinStrategy
						if strategy == "" {
							strategy = JoinStrategyAll
						}
						if err := engine.store.CreateJoinState(ctx, instance.ID, joinStepName, []string{step.StepName}, strategy); err != nil {
							_ = engine.store.LogEvent(ctx, instance.ID, &step.ID, EventStepCompleted, map[string]any{
								KeyStepName: step.StepName,
								KeyError:    fmt.Sprintf("Failed to create join state: %v", err),
							})
						}
					}
				}
			}
		}
	}

	if err := engine.notifyJoinSteps(ctx, instance.ID, step.StepName, true); err != nil {
		return fmt.Errorf("notify join steps: %w", err)
	}

	if (next && len(stepDef.Next) == 0) || (!next && stepDef.Else == "") {
		if !engine.hasUnfinishedSteps(ctx, instance.ID) {
			return engine.completeWorkflow(ctx, instance, output)
		}

		return nil
	}

	// Get workflow definition if we haven't already
	if def == nil {
		var err error
		def, err = engine.store.GetWorkflowDefinition(ctx, instance.WorkflowID)
		if err != nil {
			return fmt.Errorf("get workflow definition: %w", err)
		}
	}

	if next {
		for _, nextStepName := range stepDef.Next {
			nextStepDef, ok := def.Definition.Steps[nextStepName]
			if !ok {
				return fmt.Errorf("next step definition not found: %s", nextStepName)
			}

			if nextStepDef.Type == StepTypeJoin {
				continue
			}

			if err := engine.enqueueNextSteps(ctx, instance.ID, []string{nextStepName}, output); err != nil {
				return err
			}
		}
	} else {
		if err := engine.enqueueNextSteps(ctx, instance.ID, []string{stepDef.Else}, output); err != nil {
			return err
		}
	}

	return nil
}

func (engine *Engine) handleStepFailure(
	ctx context.Context,
	instance *WorkflowInstance,
	step *WorkflowStep,
	stepDef *StepDefinition,
	stepErr error,
) error {
	errMsg := stepErr.Error()

	if (step.RetryCount == 0 && stepDef.MaxRetries > 0) ||
		(step.RetryCount > 0 && step.RetryCount < step.MaxRetries && !stepDef.NoIdempotent) {

		if err := engine.store.UpdateStep(ctx, step.ID, StepStatusFailed, nil, &errMsg); err != nil {
			return fmt.Errorf("update step: %w", err)
		}

		_ = engine.store.LogEvent(ctx, instance.ID, &step.ID, EventStepRetry, map[string]any{
			KeyStepName:   step.StepName,
			KeyRetryCount: step.RetryCount + 1,
			KeyError:      errMsg,
		})

		return engine.store.EnqueueStep(ctx, instance.ID, &step.ID, PriorityHigh, stepDef.Delay)
	}

	// If DLQ mode is enabled, pause instead of failing and skip rollback
	if def, defErr := engine.store.GetWorkflowDefinition(ctx, instance.WorkflowID); defErr == nil && def.Definition.DLQEnabled {
		// Mark step as paused with error
		if err := engine.store.UpdateStep(ctx, step.ID, StepStatusPaused, nil, &errMsg); err != nil {
			return fmt.Errorf("update step (paused): %w", err)
		}

		_ = engine.store.LogEvent(ctx, instance.ID, &step.ID, EventStepFailed, map[string]any{
			KeyStepName: step.StepName,
			KeyError:    errMsg,
			KeyReason:   "dlq",
		})

		// Notify join steps about failure in this branch
		if err := engine.notifyJoinSteps(ctx, instance.ID, step.StepName, false); err != nil {
			return fmt.Errorf("notify join steps: %w", err)
		}

		// Create DLQ record
		reason := "dlq enabled: rollback/compensation skipped"
		rec := &DeadLetterRecord{
			InstanceID: step.InstanceID,
			WorkflowID: def.ID,
			StepID:     step.ID,
			StepName:   step.StepName,
			StepType:   string(step.StepType),
			Input:      step.Input,
			Error:      &errMsg,
			Reason:     reason,
		}
		if err := engine.store.CreateDeadLetterRecord(ctx, rec); err != nil {
			return fmt.Errorf("create dead letter record: %w", err)
		}

		// Freeze execution: pause active running steps and clear the instance queue
		if err := engine.store.PauseActiveStepsAndClearQueue(ctx, instance.ID); err != nil {
			return fmt.Errorf("freeze instance for dlq: %w", err)
		}

		// Set workflow instance to DLQ state
		if err := engine.store.UpdateInstanceStatus(ctx, instance.ID, StatusDLQ, nil, &errMsg); err != nil {
			return fmt.Errorf("update instance status to dlq: %w", err)
		}

		return nil
	}

	if err := engine.store.UpdateStep(ctx, step.ID, StepStatusFailed, nil, &errMsg); err != nil {
		return fmt.Errorf("update step: %w", err)
	}

	_ = engine.store.LogEvent(ctx, instance.ID, &step.ID, EventStepFailed, map[string]any{
		KeyStepName: step.StepName,
		KeyError:    errMsg,
	})

	// Check if this is a terminal step in a fork branch with Condition
	// If so, replace virtual step with real step before notifying Join
	def, defErr := engine.store.GetWorkflowDefinition(ctx, instance.WorkflowID)
	if defErr == nil {
		// Check if we're in a Condition branch and need to replace virtual step
		conditionStepName := engine.findConditionStepInBranch(stepDef, def)
		if conditionStepName != "" {
			// Find Join step for this fork branch
			joinStepName, err := engine.findJoinStepForForkBranch(ctx, instance.ID, step.StepName, def)
			if err == nil && joinStepName != "" {
				virtualStep := fmt.Sprintf("cond#%s", conditionStepName)
				// Replace virtual step with real terminal step (even though it failed)
				if err := engine.store.ReplaceInJoinWaitFor(ctx, instance.ID, joinStepName, virtualStep, step.StepName); err != nil {
					_ = engine.store.LogEvent(ctx, instance.ID, &step.ID, EventStepFailed, map[string]any{
						KeyStepName: step.StepName,
						KeyError:    fmt.Sprintf("Failed to replace virtual step in join waitFor: %v", err),
					})
				} else {
					_ = engine.store.LogEvent(ctx, instance.ID, &step.ID, EventStepFailed, map[string]any{
						KeyStepName: step.StepName,
						KeyMessage:  fmt.Sprintf("Replaced virtual step %s with %s in join %s (failed)", virtualStep, step.StepName, joinStepName),
					})
					// After replacing virtual step with real step, notify Join about the failure
					// This ensures Join is aware that the real step has failed
					if err := engine.notifyJoinStepsForStep(ctx, instance.ID, joinStepName, step.StepName, false); err != nil {
						_ = engine.store.LogEvent(ctx, instance.ID, &step.ID, EventStepFailed, map[string]any{
							KeyStepName: step.StepName,
							KeyError:    fmt.Sprintf("Failed to notify join after virtual step replacement: %v", err),
						})
					}
				}
			}
		}
	}

	if err := engine.notifyJoinSteps(ctx, instance.ID, step.StepName, false); err != nil {
		return fmt.Errorf("notify join steps: %w", err)
	}

	// PREVENTIVE FIX: Stop parallel branches before rollback
	// This is the first line of defense - try to prevent parallel steps from completing
	// However, this doesn't guarantee success (step might already be executing in worker)
	if def == nil {
		var defErr error
		def, defErr = engine.store.GetWorkflowDefinition(ctx, instance.WorkflowID)
		if defErr != nil {
			def = nil
		}
	}

	if def != nil {
		// Check if this step is part of a fork branch
		if engine.isStepInForkBranch(ctx, instance.ID, step.StepName, def) {
			// Stop all active steps in the same fork branch (parallel siblings)
			if err := engine.stopParallelBranchesInFork(ctx, instance.ID, step.StepName, def); err != nil {
				slog.Warn("[floxy] failed to stop parallel branches", "error", err)
			}
		}
	}

	// Try to rollback to save point before handling failure
	if def != nil {
		if rollbackErr := engine.rollbackToSavePointOrRoot(ctx, instance.ID, step, def); rollbackErr != nil {
			// Log rollback error but continue with failure handling
			_ = engine.store.LogEvent(ctx, instance.ID, &step.ID, EventStepFailed, map[string]any{
				KeyStepName: step.StepName,
				KeyError:    fmt.Sprintf("rollback failed: %v", rollbackErr),
			})
		}
	}

	if err := engine.store.UpdateInstanceStatus(ctx, instance.ID, StatusFailed, nil, &errMsg); err != nil {
		return fmt.Errorf("update instance status: %w", err)
	}

	// PLUGIN HOOK: OnWorkflowFailed
	if engine.pluginManager != nil {
		finalInstance, _ := engine.store.GetInstance(ctx, instance.ID)
		if finalInstance != nil {
			if errPlugin := engine.pluginManager.ExecuteWorkflowFailed(ctx, finalInstance); errPlugin != nil {
				slog.Warn("[floxy] plugin hook OnWorkflowFailed failed", "error", errPlugin)
			}
		}
	}

	return nil
}

// notifyJoinStepsForStep notifies a specific Join step about a specific step completion.
// This is used after replacing virtual steps to ensure Join is aware of real step completion.
func (engine *Engine) notifyJoinStepsForStep(
	ctx context.Context,
	instanceID int64,
	joinStepName, completedStepName string,
	success bool,
) error {
	instance, err := engine.store.GetInstance(ctx, instanceID)
	if err != nil {
		return err
	}

	// Get Join step definition to check strategy
	def, err := engine.store.GetWorkflowDefinition(ctx, instance.WorkflowID)
	if err != nil {
		return err
	}

	stepDef, ok := def.Definition.Steps[joinStepName]
	if !ok || stepDef.Type != StepTypeJoin {
		return fmt.Errorf("join step %s not found or not a join step", joinStepName)
	}

	// Update join state for this specific step
	isReady, err := engine.store.UpdateJoinState(ctx, instanceID, joinStepName, completedStepName, success)
	if err != nil {
		return fmt.Errorf("update join state for %s: %w", joinStepName, err)
	}

	var readySteps []WorkflowStep
	// Additional check: don't consider join ready if there are still pending/running steps
	if isReady {
		readySteps, err = engine.store.GetStepsByInstance(ctx, instanceID)
		if err != nil {
			return err
		}

		hasPendingSteps := engine.hasPendingStepsInParallelBranches(ctx, instanceID, stepDef, readySteps)
		if hasPendingSteps {
			isReady = false
			_, _ = engine.store.UpdateJoinState(ctx, instanceID, joinStepName, completedStepName, success)
		}
	}

	_ = engine.store.LogEvent(ctx, instanceID, nil, EventJoinUpdated, map[string]any{
		KeyJoinStep:      joinStepName,
		KeyCompletedStep: completedStepName,
		KeySuccess:       success,
		KeyIsReady:       isReady,
	})

	if isReady {
		joinStepExists := false
		for _, readyStep := range readySteps {
			if readyStep.StepName == joinStepName {
				joinStepExists = true

				break
			}
		}

		if !joinStepExists {
			var joinInput json.RawMessage
			for _, readyStep := range readySteps {
				if readyStep.StepName == completedStepName {
					joinInput = readyStep.Input

					break
				}
			}

			joinStep := &WorkflowStep{
				InstanceID: instanceID,
				StepName:   joinStepName,
				StepType:   StepTypeJoin,
				Status:     StepStatusPending,
				Input:      joinInput,
				MaxRetries: 0,
			}

			if instance.Status == StatusDLQ {
				joinStep.Status = StepStatusPaused
				if err := engine.store.CreateStep(ctx, joinStep); err != nil {
					return fmt.Errorf("create join step: %w", err)
				}
				_ = engine.store.LogEvent(ctx, instanceID, &joinStep.ID, EventJoinReady, map[string]any{
					KeyJoinStep: joinStepName,
				})
			} else {
				if err := engine.store.CreateStep(ctx, joinStep); err != nil {
					return fmt.Errorf("create join step: %w", err)
				}
				if err := engine.store.EnqueueStep(ctx, instanceID, &joinStep.ID, PriorityNormal, 0); err != nil {
					return fmt.Errorf("enqueue join step: %w", err)
				}
				_ = engine.store.LogEvent(ctx, instanceID, &joinStep.ID, EventJoinReady, map[string]any{
					KeyJoinStep: joinStepName,
				})
			}
		}
	}

	return nil
}

func (engine *Engine) notifyJoinSteps(
	ctx context.Context,
	instanceID int64,
	completedStepName string,
	success bool,
) error {
	instance, err := engine.store.GetInstance(ctx, instanceID)
	if err != nil {
		return err
	}

	def, err := engine.store.GetWorkflowDefinition(ctx, instance.WorkflowID)
	if err != nil {
		return err
	}

	for stepName, stepDef := range def.Definition.Steps {
		if stepDef.Type != StepTypeJoin {
			continue
		}

		// Check if Join is waiting for this step by checking JoinState (which has actual waitFor after replacements)
		// Also check stepDef.WaitFor for backwards compatibility and for virtual steps that haven't been replaced yet
		waitingForThis := false

		// First check JoinState (actual state after virtual step replacements)
		joinState, err := engine.store.GetJoinState(ctx, instanceID, stepName)
		if err == nil && joinState != nil {
			for _, waitFor := range joinState.WaitingFor {
				if waitFor == completedStepName {
					waitingForThis = true

					break
				}
			}
		}

		// Also check stepDef.WaitFor for virtual steps and initial setup
		if !waitingForThis {
			for _, waitFor := range stepDef.WaitFor {
				if waitFor == completedStepName {
					waitingForThis = true

					break
				}
			}
		}

		if !waitingForThis {
			continue
		}

		isReady, err := engine.store.UpdateJoinState(ctx, instanceID, stepName, completedStepName, success)
		if err != nil {
			return fmt.Errorf("update join state for %s: %w", stepName, err)
		}

		// Additional check: don't consider join ready if there are still pending/running steps
		// in parallel branches that could affect the join result
		var readySteps []WorkflowStep
		if isReady {
			readySteps, err = engine.store.GetStepsByInstance(ctx, instanceID)
			if err != nil {
				return err
			}

			hasPendingSteps := engine.hasPendingStepsInParallelBranches(ctx, instanceID, stepDef, readySteps)
			if hasPendingSteps {
				isReady = false
				// Update the join state to reflect that it's not ready
				_, _ = engine.store.UpdateJoinState(ctx, instanceID, stepName, completedStepName, success)
			}
		}

		_ = engine.store.LogEvent(ctx, instanceID, nil, EventJoinUpdated, map[string]any{
			KeyJoinStep:      stepName,
			KeyCompletedStep: completedStepName,
			KeySuccess:       success,
			KeyIsReady:       isReady,
		})

		if isReady {
			joinStepExists := false
			for _, readyStep := range readySteps {
				if readyStep.StepName == stepName {
					joinStepExists = true

					break
				}
			}

			if !joinStepExists {
				var joinInput json.RawMessage
				for _, readyStep := range readySteps {
					if readyStep.StepName == completedStepName {
						joinInput = readyStep.Input

						break
					}
				}

				joinStep := &WorkflowStep{
					InstanceID: instanceID,
					StepName:   stepName,
					StepType:   StepTypeJoin,
					Status:     StepStatusPending,
					Input:      joinInput,
					MaxRetries: 0,
				}

				// If the instance is in DLQ, create the join step as paused and do not enqueue it
				if instance.Status == StatusDLQ {
					joinStep.Status = StepStatusPaused
					if err := engine.store.CreateStep(ctx, joinStep); err != nil {
						return fmt.Errorf("create join step: %w", err)
					}
					_ = engine.store.LogEvent(ctx, instanceID, &joinStep.ID, EventJoinReady, map[string]any{
						KeyJoinStep: stepName,
					})
				} else {
					if err := engine.store.CreateStep(ctx, joinStep); err != nil {
						return fmt.Errorf("create join step: %w", err)
					}
					if err := engine.store.EnqueueStep(ctx, instanceID, &joinStep.ID, PriorityNormal, 0); err != nil {
						return fmt.Errorf("enqueue join step: %w", err)
					}
					_ = engine.store.LogEvent(ctx, instanceID, &joinStep.ID, EventJoinReady, map[string]any{
						KeyJoinStep: stepName,
					})
				}
			}
		}
	}

	return nil
}

func (engine *Engine) hasUnfinishedSteps(ctx context.Context, instanceID int64) bool {
	steps, err := engine.store.GetStepsByInstance(ctx, instanceID)
	if err != nil {
		return false
	}

	for _, step := range steps {
		if step.Status == StepStatusPending || step.Status == StepStatusRunning || step.Status == StepStatusPaused {
			return true
		}
	}

	return false
}

func (engine *Engine) hasFailedOrRolledBackSteps(ctx context.Context, instanceID int64) bool {
	steps, err := engine.store.GetStepsByInstance(ctx, instanceID)
	if err != nil {
		return false
	}

	for _, step := range steps {
		if step.Status == StepStatusFailed || step.Status == StepStatusRolledBack {
			return true
		}
	}

	return false
}

func (engine *Engine) hasStepsInCompensation(ctx context.Context, instanceID int64) bool {
	steps, err := engine.store.GetStepsByInstance(ctx, instanceID)
	if err != nil {
		return false
	}

	for _, step := range steps {
		if step.Status == StepStatusCompensation {
			return true
		}
	}

	return false
}

// enqueueCompletedStepsForRollback finds completed steps after savepoint and enqueues them for compensation.
// This follows the saga pattern: rollback happens step-by-step through the queue, not in one transaction.
func (engine *Engine) enqueueCompletedStepsForRollback(ctx context.Context, instanceID int64) error {
	steps, err := engine.store.GetStepsByInstance(ctx, instanceID)
	if err != nil {
		return fmt.Errorf("get steps by instance: %w", err)
	}

	// Find the last savepoint by created_at
	var lastSavePoint *WorkflowStep
	for i := range steps {
		step := &steps[i]
		if step.StepType == StepTypeSavePoint {
			if lastSavePoint == nil || step.CreatedAt.After(lastSavePoint.CreatedAt) {
				lastSavePoint = step
			}
		}
	}

	// Get workflow definition
	instance, err := engine.store.GetInstance(ctx, instanceID)
	if err != nil {
		return fmt.Errorf("get instance: %w", err)
	}

	def, err := engine.store.GetWorkflowDefinition(ctx, instance.WorkflowID)
	if err != nil {
		return fmt.Errorf("get workflow definition: %w", err)
	}

	// Find all completed steps created after the last savepoint
	var stepsToRollback []*WorkflowStep
	for i := range steps {
		step := &steps[i]
		// Only process completed steps that need rollback
		if step.Status != StepStatusCompleted {
			continue
		}
		// If there's a savepoint, only rollback steps created after it
		if lastSavePoint != nil && step.CreatedAt.After(lastSavePoint.CreatedAt) {
			// Check if step has compensation handler defined
			stepDef, ok := def.Definition.Steps[step.StepName]
			if ok && stepDef.OnFailure != "" {
				stepsToRollback = append(stepsToRollback, step)
			} else {
				// No compensation handler, just mark as rolled_back
				if err := engine.store.UpdateStep(ctx, step.ID, StepStatusRolledBack, step.Input, nil); err != nil {
					slog.Warn("[floxy] failed to mark step as rolled_back", "step_id", step.ID, "error", err)
				}
				_ = engine.store.LogEvent(ctx, instanceID, &step.ID, EventStepCompleted, map[string]any{
					KeyStepName: step.StepName,
					KeyReason:   "rolled_back_no_compensation",
				})
			}
		} else if lastSavePoint == nil {
			// If no savepoint, rollback all completed steps
			stepDef, ok := def.Definition.Steps[step.StepName]
			if ok && stepDef.OnFailure != "" {
				stepsToRollback = append(stepsToRollback, step)
			} else {
				if err := engine.store.UpdateStep(ctx, step.ID, StepStatusRolledBack, step.Input, nil); err != nil {
					slog.Warn("[floxy] failed to mark step as rolled_back", "step_id", step.ID, "error", err)
				}
				_ = engine.store.LogEvent(ctx, instanceID, &step.ID, EventStepCompleted, map[string]any{
					KeyStepName: step.StepName,
					KeyReason:   "rolled_back_no_compensation",
				})
			}
		}
	}

	sort.Slice(stepsToRollback, func(i, j int) bool {
		return stepsToRollback[i].CreatedAt.After(stepsToRollback[j].CreatedAt)
	})

	// Mark each step as requiring compensation and enqueue for processing
	for idx, step := range stepsToRollback {
		// Update step status to compensation with retry count = 0
		if err := engine.store.UpdateStepCompensationRetry(ctx, step.ID, 0, StepStatusCompensation); err != nil {
			slog.Warn("[floxy] failed to mark step for compensation", "step_id", step.ID, "error", err)
			continue
		}

		// Enqueue step for compensation processing
		delay := time.Millisecond * time.Duration(idx*10)
		if err := engine.store.EnqueueStep(ctx, instanceID, &step.ID, PriorityHigh, delay); err != nil {
			slog.Warn("[floxy] failed to enqueue step for compensation", "step_id", step.ID, "error", err)
			continue
		}

		_ = engine.store.LogEvent(ctx, instanceID, &step.ID, EventStepStarted, map[string]any{
			KeyStepName: step.StepName,
			KeyReason:   "enqueued_for_rollback_after_failure",
		})
	}

	return nil
}

func (engine *Engine) enqueueNextSteps(
	ctx context.Context,
	instanceID int64,
	nextSteps []string,
	input json.RawMessage,
) error {
	instance, err := engine.store.GetInstance(ctx, instanceID)
	if err != nil {
		return fmt.Errorf("get instance: %w", err)
	}

	// Do not enqueue new steps while the instance is in DLQ state
	if instance.Status == StatusDLQ {
		return nil
	}

	def, err := engine.store.GetWorkflowDefinition(ctx, instance.WorkflowID)
	if err != nil {
		return fmt.Errorf("get workflow definition: %w", err)
	}

	for _, nextStepName := range nextSteps {
		stepDef, ok := def.Definition.Steps[nextStepName]
		if !ok {
			return fmt.Errorf("step definition not found: %s", nextStepName)
		}

		step := &WorkflowStep{
			InstanceID: instanceID,
			StepName:   nextStepName,
			StepType:   stepDef.Type,
			Status:     StepStatusPending,
			Input:      input,
			MaxRetries: stepDef.MaxRetries,
		}

		if err := engine.store.CreateStep(ctx, step); err != nil {
			return fmt.Errorf("create step: %w", err)
		}

		if err := engine.store.EnqueueStep(ctx, instanceID, &step.ID, PriorityNormal, stepDef.Delay); err != nil {
			return fmt.Errorf("enqueue step: %w", err)
		}
	}

	return nil
}

func (engine *Engine) createFirstStep(ctx context.Context, instance *WorkflowInstance) (*WorkflowStep, error) {
	def, err := engine.store.GetWorkflowDefinition(ctx, instance.WorkflowID)
	if err != nil {
		return nil, err
	}

	startStepDef, ok := def.Definition.Steps[def.Definition.Start]
	if !ok {
		return nil, fmt.Errorf("start step definition not found: %s", def.Definition.Start)
	}

	step := &WorkflowStep{
		InstanceID: instance.ID,
		StepName:   def.Definition.Start,
		StepType:   startStepDef.Type,
		Status:     StepStatusPending,
		Input:      instance.Input,
		MaxRetries: startStepDef.MaxRetries,
	}

	if err := engine.store.CreateStep(ctx, step); err != nil {
		return nil, err
	}

	return step, nil
}

func (engine *Engine) completeWorkflow(ctx context.Context, instance *WorkflowInstance, output json.RawMessage) error {
	// Check if there are any failed or rolled_back steps
	// If so, the workflow should be marked as failed, not completed
	if engine.hasFailedOrRolledBackSteps(ctx, instance.ID) {
		// REACTIVE FIX: Enqueue completed steps after savepoint for compensation
		// This is the second line of defense - handles steps that completed despite
		// preventive measures (stopParallelBranchesInFork).
		// Race condition scenario: worker already picked up step from queue before we marked it as skipped.
		// DEFENSE IN DEPTH: Preventive (stop pending/running) + Reactive (rollback completed)
		if err := engine.enqueueCompletedStepsForRollback(ctx, instance.ID); err != nil {
			slog.Warn("[floxy] failed to enqueue completed steps for rollback", "error", err)
			// Continue with marking workflow as failed even if enqueue fails
		}

		// Check if there are any steps enqueued for compensation
		// If yes, don't mark workflow as failed yet - let compensation finish first
		hasCompensationSteps := engine.hasStepsInCompensation(ctx, instance.ID)
		if hasCompensationSteps {
			// Compensation is in progress, don't mark as failed yet
			_ = engine.store.LogEvent(ctx, instance.ID, nil, EventWorkflowFailed, map[string]any{
				KeyWorkflowID: instance.WorkflowID,
				KeyReason:     "compensation_in_progress",
			})
			return nil
		}

		errMsg := "workflow has failed or rolled back steps"
		if err := engine.store.UpdateInstanceStatus(ctx, instance.ID, StatusFailed, nil, &errMsg); err != nil {
			return fmt.Errorf("update instance status to failed: %w", err)
		}

		_ = engine.store.LogEvent(ctx, instance.ID, nil, EventWorkflowFailed, map[string]any{
			KeyWorkflowID: instance.WorkflowID,
			KeyReason:     errMsg,
		})

		// PLUGIN HOOK: OnWorkflowFailed
		if engine.pluginManager != nil {
			finalInstance, _ := engine.store.GetInstance(ctx, instance.ID)
			if finalInstance != nil {
				if errPlugin := engine.pluginManager.ExecuteWorkflowFailed(ctx, finalInstance); errPlugin != nil {
					slog.Warn("[floxy] plugin hook OnWorkflowFailed failed", "error", errPlugin)
				}
			}
		}

		return nil
	}

	if err := engine.store.UpdateInstanceStatus(ctx, instance.ID, StatusCompleted, output, nil); err != nil {
		return fmt.Errorf("update instance status: %w", err)
	}

	_ = engine.store.LogEvent(ctx, instance.ID, nil, EventWorkflowCompleted, map[string]any{
		KeyWorkflowID: instance.WorkflowID,
	})

	// PLUGIN HOOK: OnWorkflowComplete
	if engine.pluginManager != nil {
		// Reload instance to get the final state
		finalInstance, _ := engine.store.GetInstance(ctx, instance.ID)
		if finalInstance != nil {
			if errPlugin := engine.pluginManager.ExecuteWorkflowComplete(ctx, finalInstance); errPlugin != nil {
				slog.Warn("[floxy] plugin hook OnWorkflowComplete failed", "error", errPlugin)
			}
		}
	}

	return nil
}

func (engine *Engine) GetStatus(ctx context.Context, instanceID int64) (WorkflowStatus, error) {
	instance, err := engine.store.GetInstance(ctx, instanceID)
	if err != nil {
		return "", fmt.Errorf("get instance: %w", err)
	}

	return instance.Status, nil
}

func (engine *Engine) GetSteps(ctx context.Context, instanceID int64) ([]WorkflowStep, error) {
	return engine.store.GetStepsByInstance(ctx, instanceID)
}

func (engine *Engine) HumanDecisionWaitingEvents() <-chan HumanDecisionWaitingEvent {
	engine.humanDecisionWaitingOnce.Do(func() {
		engine.humanDecisionWaitingEvents = make(chan HumanDecisionWaitingEvent)
	})

	return engine.humanDecisionWaitingEvents
}

func (engine *Engine) validateDefinition(def *WorkflowDefinition) error {
	if def.Name == "" {
		return fmt.Errorf("workflow name is required")
	}

	if def.Definition.Start == "" {
		return fmt.Errorf("start step is required")
	}

	if len(def.Definition.Steps) == 0 {
		return fmt.Errorf("at least one step is required")
	}

	if _, ok := def.Definition.Steps[def.Definition.Start]; !ok {
		return fmt.Errorf("start step not found: %s", def.Definition.Start)
	}

	return ValidateWorkflowDefinition(def)
}

func (engine *Engine) rollbackToSavePointOrRoot(
	ctx context.Context,
	instanceID int64,
	failedStep *WorkflowStep,
	def *WorkflowDefinition,
) error {
	savePointName := engine.findNearestSavePoint(failedStep.StepName, def) // nearest save point or empty string (root)

	return engine.rollbackStepsToSavePoint(ctx, instanceID, failedStep, savePointName, def)
}

func (engine *Engine) findNearestSavePoint(stepName string, def *WorkflowDefinition) string {
	visited := make(map[string]bool)

	for stepName != "" {
		if visited[stepName] {
			break // Prevent infinite loops
		}
		visited[stepName] = true

		stepDef, ok := def.Definition.Steps[stepName]
		if !ok {
			break
		}
		if stepDef.Prev == "" {
			for _, stepDefCurr := range def.Definition.Steps {
				if stepDefCurr.Prev != "" {
					stepDef = stepDefCurr

					break
				}
			}
		}

		if stepDef.Type == StepTypeSavePoint {
			return stepName
		}

		stepName = stepDef.Prev
	}

	return rootStepName
}

func (engine *Engine) rollbackStepsToSavePoint(
	ctx context.Context,
	instanceID int64,
	failedStep *WorkflowStep,
	savePointName string,
	def *WorkflowDefinition,
) error {
	steps, err := engine.store.GetStepsByInstance(ctx, instanceID)
	if err != nil {
		return fmt.Errorf("get steps by instance: %w", err)
	}

	stepMap := make(map[string]*WorkflowStep)
	for _, step := range steps {
		step := step
		stepMap[step.StepName] = &step
	}
	if _, exists := stepMap[failedStep.StepName]; !exists {
		stepMap[failedStep.StepName] = failedStep
	}

	// Use visited map to prevent double rollback of steps in nested fork/join structures
	visited := make(map[string]bool)

	return engine.rollbackStepChainWithVisited(ctx, instanceID, failedStep.StepName, savePointName, def, stepMap, visited, false, 0)
}

// rollbackStepChain is a wrapper for backward compatibility that creates a visited map
// and delegates to rollbackStepChainWithVisited.
func (engine *Engine) rollbackStepChain(
	ctx context.Context,
	instanceID int64,
	currentStep, savePointName string,
	def *WorkflowDefinition,
	stepMap map[string]*WorkflowStep,
	isParallel bool,
	depth int,
) error {
	visited := make(map[string]bool)

	return engine.rollbackStepChainWithVisited(ctx, instanceID, currentStep, savePointName, def, stepMap, visited, isParallel, depth)
}

// rollbackStepChainWithVisited performs rollback of a step chain with support for nested fork/join.
// The visited map prevents double rollback of steps that may be reached through multiple paths
// in nested fork/join structures.
func (engine *Engine) rollbackStepChainWithVisited(
	ctx context.Context,
	instanceID int64,
	currentStep, savePointName string,
	def *WorkflowDefinition,
	stepMap map[string]*WorkflowStep,
	visited map[string]bool,
	isParallel bool,
	depth int,
) error {
	// PLUGIN HOOK: OnRollbackStepChain
	if engine.pluginManager != nil {
		if err := engine.pluginManager.ExecuteRollbackStepChain(ctx, instanceID, currentStep, depth); err != nil {
			slog.Warn("[floxy] plugin hook OnRollbackStepChain failed", "error", err)
		}
	}

	// Prevent cycles and double rollback of already processed steps
	if visited[currentStep] {
		return nil
	}
	visited[currentStep] = true

	if currentStep == savePointName {
		return nil // Reached save point
	}

	stepDef, ok := def.Definition.Steps[currentStep]
	if !ok {
		return fmt.Errorf("step definition not found: %s", currentStep)
	}

	// Handle Fork/Parallel steps: traverse all parallel branches first (depth-first)
	if stepDef.Type == StepTypeFork || stepDef.Type == StepTypeParallel {
		for _, parallelStepName := range stepDef.Parallel {
			if err := engine.rollbackStepChainWithVisited(ctx, instanceID, parallelStepName, savePointName, def, stepMap, visited, true, depth+1); err != nil {
				return err
			}
		}
	}

	// For parallel branches, traverse all subsequent steps in the chain
	if isParallel {
		if stepDef.Type == StepTypeCondition {
			// For condition steps, determine which branch was executed
			if step, exists := stepMap[currentStep]; exists && step.Status == StepStatusCompleted {
				executedBranch := engine.determineExecutedBranch(stepDef, stepMap)

				if executedBranch == "next" {
					for _, nextStepName := range stepDef.Next {
						if err := engine.rollbackStepChainWithVisited(ctx, instanceID, nextStepName, savePointName, def, stepMap, visited, true, depth+1); err != nil {
							return err
						}
					}
				} else if executedBranch == "else" && stepDef.Else != "" {
					if err := engine.rollbackStepChainWithVisited(ctx, instanceID, stepDef.Else, savePointName, def, stepMap, visited, true, depth+1); err != nil {
						return err
					}
				}
			}
		} else {
			// For non-condition steps, traverse all next steps
			for _, nextStepName := range stepDef.Next {
				if err := engine.rollbackStepChainWithVisited(ctx, instanceID, nextStepName, savePointName, def, stepMap, visited, true, depth+1); err != nil {
					return err
				}
			}
		}
	}

	// Perform the actual rollback for completed or failed steps
	if step, exists := stepMap[currentStep]; exists &&
		(step.Status == StepStatusCompleted || step.Status == StepStatusFailed) {
		if err := engine.rollbackStep(ctx, step, def); err != nil {
			return fmt.Errorf("rollback step %s: %w", currentStep, err)
		}
	}

	// Continue traversing backwards through Prev links
	if stepDef.Prev != "" && stepDef.Prev != rootStepName && !isParallel {
		prevDef, prevOk := def.Definition.Steps[stepDef.Prev]
		if prevOk && (prevDef.Type == StepTypeFork || prevDef.Type == StepTypeParallel) {
			// Previous step is a Fork/Parallel - rollback ALL branches before continuing
			// This ensures that when we traverse backwards from a nested fork,
			// all sibling branches of the parent fork are also rolled back
			for _, parallelStepName := range prevDef.Parallel {
				if err := engine.rollbackStepChainWithVisited(ctx, instanceID, parallelStepName, savePointName, def, stepMap, visited, true, depth+1); err != nil {
					return err
				}
			}
		}
		// Continue backwards traversal
		if err := engine.rollbackStepChainWithVisited(ctx, instanceID, stepDef.Prev, savePointName, def, stepMap, visited, false, depth+1); err != nil {
			return err
		}
	}

	return nil
}

func (engine *Engine) rollbackStep(ctx context.Context, step *WorkflowStep, def *WorkflowDefinition) error {
	stepDef, ok := def.Definition.Steps[step.StepName]
	if !ok {
		return fmt.Errorf("step definition not found: %s", step.StepName)
	}

	onFailureStep, ok := def.Definition.Steps[stepDef.OnFailure]
	if !ok {
		// No compensation handler, mark as rolled back directly
		if err := engine.store.UpdateStep(ctx, step.ID, StepStatusRolledBack, step.Input, nil); err != nil {
			return fmt.Errorf("update step status: %w", err)
		}

		return nil
	}

	// Check if we need to retry compensation
	if step.CompensationRetryCount >= onFailureStep.MaxRetries {
		// Max compensation retries exceeded, mark as failed
		if err := engine.store.UpdateStep(ctx, step.ID, StepStatusFailed, step.Input, nil); err != nil {
			return fmt.Errorf("update step status: %w", err)
		}
		reason := "compensation max retries exceeded"
		_ = engine.store.LogEvent(ctx, step.InstanceID, &step.ID, EventStepFailed, map[string]any{
			KeyStepName: step.StepName,
			KeyStepType: step.StepType,
			KeyError:    reason,
		})

		// Send to Dead Letter Queue
		rec := &DeadLetterRecord{
			InstanceID: step.InstanceID,
			WorkflowID: def.ID,
			StepID:     step.ID,
			StepName:   step.StepName,
			StepType:   string(step.StepType),
			Input:      step.Input,
			Error:      step.Error,
			Reason:     reason,
		}
		if err := engine.store.CreateDeadLetterRecord(ctx, rec); err != nil {
			return fmt.Errorf("create dead letter record: %w", err)
		}

		return nil
	}

	// Increment compensation retry count and update status to compensation
	newRetryCount := step.CompensationRetryCount + 1
	if err := engine.store.UpdateStepCompensationRetry(ctx, step.ID, newRetryCount, StepStatusCompensation); err != nil {
		return fmt.Errorf("update step compensation retry: %w", err)
	}

	// Enqueue compensation step for execution
	retryDelay := CalculateRetryDelay(onFailureStep.RetryStrategy, onFailureStep.RetryDelay, newRetryCount)
	if retryDelay == 0 {
		retryDelay = onFailureStep.Delay
	}
	if err := engine.store.EnqueueStep(ctx, step.InstanceID, &step.ID, PriorityHigh, retryDelay); err != nil {
		return fmt.Errorf("enqueue compensation step: %w", err)
	}

	_ = engine.store.LogEvent(ctx, step.InstanceID, &step.ID, EventStepStarted, map[string]any{
		KeyStepName:   step.StepName,
		KeyStepType:   step.StepType,
		KeyRetryCount: newRetryCount,
		KeyReason:     "compensation",
	})

	return nil
}

func (engine *Engine) determineExecutedBranch(
	stepDef *StepDefinition,
	stepMap map[string]*WorkflowStep,
) string {
	// Check if any steps from the Next branch were executed
	for _, nextStepName := range stepDef.Next {
		if step, exists := stepMap[nextStepName]; exists &&
			(step.Status == StepStatusCompleted ||
				step.Status == StepStatusFailed ||
				step.Status == StepStatusCompensation) {
			return "next"
		}
	}

	// Check if any steps from the Else branch were executed
	if stepDef.Else != "" {
		if step, exists := stepMap[stepDef.Else]; exists &&
			(step.Status == StepStatusCompleted ||
				step.Status == StepStatusFailed ||
				step.Status == StepStatusCompensation) {
			return "else"
		}
	}

	// If no branch was executed, return an empty string
	return ""
}

// hasPendingStepsInParallelBranches checks if there are any pending/running steps
// in parallel branches that could affect the join result
func (engine *Engine) hasPendingStepsInParallelBranches(
	ctx context.Context,
	instanceID int64,
	joinStepDef *StepDefinition,
	allSteps []WorkflowStep,
) bool {
	// Get the fork step that created the parallel branches
	forkStepName := ""
	for _, waitFor := range joinStepDef.WaitFor {
		// Find the fork step that created this parallel branch
		// by looking for steps that have this step in their Parallel array
		for _, step := range allSteps {
			if step.StepName == waitFor {
				// This is a step from a parallel branch
				// We need to find the fork step that created this branch
				forkStepName = engine.findForkStepForParallelStep(ctx, instanceID, waitFor)

				break
			}
		}
		if forkStepName != "" {
			break
		}
	}

	if forkStepName == "" {
		return false
	}

	// Get workflow definition to find the fork step
	instance, err := engine.store.GetInstance(ctx, instanceID)
	if err != nil {
		return false
	}

	def, err := engine.store.GetWorkflowDefinition(ctx, instance.WorkflowID)
	if err != nil {
		return false
	}

	forkStepDef, ok := def.Definition.Steps[forkStepName]
	if !ok || forkStepDef.Type != StepTypeFork {
		return false
	}

	// Check if there are any pending/running steps in the parallel branches
	// that are not in the WaitFor list (i.e., dynamically created steps)
	for _, step := range allSteps {
		if step.Status == StepStatusPending || step.Status == StepStatusRunning {
			// Check if this step belongs to one of the parallel branches
			if engine.isStepInParallelBranch(step.StepName, forkStepDef, def) {
				// Check if this step is not in the WaitFor list
				isInWaitFor := false
				for _, waitFor := range joinStepDef.WaitFor {
					if step.StepName == waitFor {
						isInWaitFor = true

						break
					}
				}
				if !isInWaitFor {
					return true
				}
			}
		}
	}

	return false
}

// findForkStepForParallelStep finds the fork step that created the given parallel step
func (engine *Engine) findForkStepForParallelStep(ctx context.Context, instanceID int64, parallelStepName string) string {
	instance, err := engine.store.GetInstance(ctx, instanceID)
	if err != nil {
		return ""
	}

	def, err := engine.store.GetWorkflowDefinition(ctx, instance.WorkflowID)
	if err != nil {
		return ""
	}

	// Look for fork steps that have this step in their Parallel array
	for stepName, stepDef := range def.Definition.Steps {
		if stepDef.Type == StepTypeFork {
			for _, parallelStep := range stepDef.Parallel {
				if parallelStep == parallelStepName {
					return stepName
				}
			}
		}
	}

	return ""
}

// findForkStepForStepInBranch finds the nearest Fork step that contains the given step
// anywhere in its branches. This works for any step within a fork branch, not just
// the first step of the branch.
func (engine *Engine) findForkStepForStepInBranch(stepName string, def *WorkflowDefinition) string {
	if stepName == "" || stepName == rootStepName {
		return ""
	}

	// First check if this step is directly in a Fork's Parallel array
	for forkName, forkDef := range def.Definition.Steps {
		if forkDef.Type == StepTypeFork || forkDef.Type == StepTypeParallel {
			for _, parallelStep := range forkDef.Parallel {
				if parallelStep == stepName {
					return forkName
				}
			}
		}
	}

	// Traverse backwards through Prev links to find a Fork
	visited := make(map[string]bool)
	current := stepName

	for current != "" && current != rootStepName {
		if visited[current] {
			break
		}
		visited[current] = true

		currentDef, ok := def.Definition.Steps[current]
		if !ok {
			break
		}

		// Check if the previous step is a Fork
		if currentDef.Prev != "" && currentDef.Prev != rootStepName {
			prevDef, prevOk := def.Definition.Steps[currentDef.Prev]
			if prevOk && (prevDef.Type == StepTypeFork || prevDef.Type == StepTypeParallel) {
				// Verify that current is in the parallel branches
				for _, parallelStep := range prevDef.Parallel {
					if engine.isStepReachableFrom(current, parallelStep, def, make(map[string]bool)) {
						return currentDef.Prev
					}
				}
			}
		}

		current = currentDef.Prev
	}

	return ""
}

// isStepReachableFrom checks if targetStep is reachable from startStep by traversing Next links.
func (engine *Engine) isStepReachableFrom(targetStep, startStep string, def *WorkflowDefinition, visited map[string]bool) bool {
	if startStep == targetStep {
		return true
	}

	if visited[startStep] {
		return false
	}
	visited[startStep] = true

	stepDef, ok := def.Definition.Steps[startStep]
	if !ok {
		return false
	}

	// Check Next steps
	for _, nextStep := range stepDef.Next {
		if engine.isStepReachableFrom(targetStep, nextStep, def, visited) {
			return true
		}
	}

	// Check Else branch for conditions
	if stepDef.Else != "" {
		if engine.isStepReachableFrom(targetStep, stepDef.Else, def, visited) {
			return true
		}
	}

	// Check parallel branches for Fork/Parallel
	for _, parallelStep := range stepDef.Parallel {
		if engine.isStepReachableFrom(targetStep, parallelStep, def, visited) {
			return true
		}
	}

	return false
}

// isStepInForkBranch checks if a step is part of any fork/parallel branch.
func (engine *Engine) isStepInForkBranch(
	ctx context.Context,
	instanceID int64,
	stepName string,
	def *WorkflowDefinition,
) bool {
	return engine.findForkStepForParallelStep(ctx, instanceID, stepName) != ""
}

// stopParallelBranchesInFork stops all active steps in parallel branches of the same fork.
// PREVENTIVE MEASURE: Try to prevent parallel steps from completing during rollback.
// Note: This doesn't guarantee success - steps might already be executing in workers.
// The REACTIVE measure (enqueueCompletedStepsForRollback) handles steps that complete anyway.
func (engine *Engine) stopParallelBranchesInFork(
	ctx context.Context,
	instanceID int64,
	failedStepName string,
	def *WorkflowDefinition,
) error {
	// Find the fork step that contains this failed step
	forkStepName := engine.findForkStepForParallelStep(ctx, instanceID, failedStepName)
	if forkStepName == "" {
		// Not in a fork, nothing to stop
		return nil
	}

	forkStepDef, ok := def.Definition.Steps[forkStepName]
	if !ok || forkStepDef.Type != StepTypeFork {
		return nil
	}

	// Get all steps in this instance
	steps, err := engine.store.GetStepsByInstance(ctx, instanceID)
	if err != nil {
		return fmt.Errorf("get steps: %w", err)
	}

	// Find all active steps (pending/running) in parallel branches
	for _, step := range steps {
		// Skip the failed step itself
		if step.StepName == failedStepName {
			continue
		}

		// Check if this step is in a parallel branch
		if !engine.isStepInParallelBranch(step.StepName, forkStepDef, def) {
			continue
		}

		// Only stop active steps (pending/running)
		if step.Status != StepStatusPending && step.Status != StepStatusRunning {
			continue
		}

		// Mark step as skipped
		skipMsg := "Skipped due to parallel branch failure"
		if err := engine.store.UpdateStep(ctx, step.ID, StepStatusSkipped, nil, &skipMsg); err != nil {
			slog.Warn("[floxy] failed to skip step in parallel branch", "step_id", step.ID, "error", err)
			continue
		}

		_ = engine.store.LogEvent(ctx, instanceID, &step.ID, EventStepFailed, map[string]any{
			KeyStepName: step.StepName,
			KeyReason:   "parallel_branch_failure",
			KeyMessage:  skipMsg,
		})
	}

	return nil
}

// isStepInParallelBranch checks if a step belongs to one of the parallel branches
func (engine *Engine) isStepInParallelBranch(stepName string, forkStepDef *StepDefinition, def *WorkflowDefinition) bool {
	// Check if this step is a direct parallel step
	for _, parallelStep := range forkStepDef.Parallel {
		if stepName == parallelStep {
			return true
		}
	}

	// Check if this step is a descendant of any parallel step
	for _, parallelStep := range forkStepDef.Parallel {
		if engine.isStepDescendantOf(stepName, parallelStep, def) {
			return true
		}
	}

	return false
}

// isStepDescendantOf checks if a step is a descendant of another step
func (engine *Engine) isStepDescendantOf(stepName, ancestorStepName string, def *WorkflowDefinition) bool {
	visited := make(map[string]bool)

	for stepName != "" {
		if visited[stepName] {
			break // Prevent infinite loops
		}
		visited[stepName] = true

		stepDef, ok := def.Definition.Steps[stepName]
		if !ok {
			break
		}

		if stepName == ancestorStepName {
			return true
		}

		// Check if this step is a descendant through Next or Else
		if stepDef.Prev != "" {
			stepName = stepDef.Prev
		} else {
			break
		}
	}

	return false
}

// isTerminalStepInForkBranch checks if a step is a terminal step in a fork branch.
// A step is terminal if it has no Next steps, is not a Join step, and is in a fork branch.
func (engine *Engine) isTerminalStepInForkBranch(
	ctx context.Context,
	instanceID int64,
	stepName string,
	def *WorkflowDefinition,
) bool {
	stepDef, ok := def.Definition.Steps[stepName]
	if !ok {
		return false
	}

	// Join steps are not terminal (they are the end of fork branches)
	if stepDef.Type == StepTypeJoin {
		return false
	}

	// A step is terminal if it has no Next steps
	if len(stepDef.Next) > 0 {
		return false
	}

	// Check if this step is actually in a fork branch by finding the fork step
	forkStepName := engine.findForkStepForParallelStep(ctx, instanceID, stepName)
	if forkStepName != "" {
		return true
	}

	// Try to find fork by traversing backwards through Prev
	visited := make(map[string]bool)
	current := stepDef.Prev
	for current != "" && current != rootStepName {
		if visited[current] {
			break
		}
		visited[current] = true

		currentDef, ok := def.Definition.Steps[current]
		if !ok {
			break
		}

		if currentDef.Type == StepTypeFork {
			return true
		}

		// Check if this step is in the Parallel array of a fork
		for _, defStep := range def.Definition.Steps {
			if defStep.Type == StepTypeFork {
				for _, parallelStep := range defStep.Parallel {
					if parallelStep == current {
						return true
					}
				}
			}
		}

		current = currentDef.Prev
	}

	return false
}

// findJoinStepForForkBranch finds the Join step that should wait for steps in the given fork branch.
// It looks for a Join step that follows the fork step which created the branch containing stepName.
func (engine *Engine) findJoinStepForForkBranch(
	ctx context.Context,
	instanceID int64,
	stepName string,
	def *WorkflowDefinition,
) (string, error) {
	// Find the fork step that created the branch containing this step
	forkStepName := engine.findForkStepForParallelStep(ctx, instanceID, stepName)
	if forkStepName == "" {
		// Try to find fork by traversing backwards through Prev
		stepDef, ok := def.Definition.Steps[stepName]
		if !ok {
			return "", nil
		}

		// Traverse backwards to find a fork step
		visited := make(map[string]bool)
		current := stepDef.Prev
		for current != "" && current != rootStepName {
			if visited[current] {
				break
			}
			visited[current] = true

			currentDef, ok := def.Definition.Steps[current]
			if !ok {
				break
			}

			if currentDef.Type == StepTypeFork {
				forkStepName = current
				break
			}

			// Check if this step is in the Parallel array of a fork
			for name, defStep := range def.Definition.Steps {
				if defStep.Type == StepTypeFork {
					for _, parallelStep := range defStep.Parallel {
						if parallelStep == current {
							forkStepName = name
							break
						}
					}
					if forkStepName != "" {
						break
					}
				}
			}

			if forkStepName != "" {
				break
			}

			current = currentDef.Prev
		}
	}

	if forkStepName == "" {
		return "", nil // Not in a fork branch
	}

	// Find Join step that follows this fork step
	forkStepDef, ok := def.Definition.Steps[forkStepName]
	if !ok {
		return "", nil
	}

	// Look for Join step in Next steps of the fork
	for _, nextStepName := range forkStepDef.Next {
		nextStepDef, ok := def.Definition.Steps[nextStepName]
		if ok && nextStepDef.Type == StepTypeJoin {
			return nextStepName, nil
		}
	}

	return "", nil
}

// findConditionStepInBranch finds the Condition step in the branch that contains stepName.
// It traverses backwards from stepName through Prev links to find the first Condition step.
// Returns empty string if no Condition step is found.
func (engine *Engine) findConditionStepInBranch(
	stepDef *StepDefinition,
	def *WorkflowDefinition,
) string {
	visited := make(map[string]bool)
	current := stepDef.Prev

	for current != "" && current != rootStepName {
		if visited[current] {
			break // Prevent cycles
		}
		visited[current] = true

		currentDef, ok := def.Definition.Steps[current]
		if !ok {
			break
		}

		// Check if this is a Condition step
		if currentDef.Type == StepTypeCondition {
			return current
		}

		// Continue traversing backwards
		current = currentDef.Prev
	}

	return ""
}
