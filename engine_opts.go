package floxy

import (
	"time"
)

type EngineOption func(engine *Engine)

func WithEngineCancelInterval(interval time.Duration) EngineOption {
	return func(engine *Engine) {
		engine.cancelWorkerInterval = interval
	}
}

// WithEngineAwaitPollInterval sets the polling interval for StartAwait method.
func WithEngineAwaitPollInterval(interval time.Duration) EngineOption {
	return func(engine *Engine) {
		engine.awaitPollInterval = interval
	}
}

func WithEngineTxManager(txManager TxManager) EngineOption {
	return func(engine *Engine) {
		engine.txManager = txManager
	}
}

func WithEngineStore(store Store) EngineOption {
	return func(engine *Engine) {
		engine.store = store
	}
}

func WithEnginePluginManager(pluginManager *PluginManager) EngineOption {
	return func(e *Engine) {
		e.pluginManager = pluginManager
	}
}

// WithMissingHandlerCooldown Distributed missing-handler behavior options
func WithMissingHandlerCooldown(d time.Duration) EngineOption {
	return func(e *Engine) {
		e.missingHandlerCooldown = d
	}
}

func WithMissingHandlerLogThrottle(d time.Duration) EngineOption {
	return func(e *Engine) {
		e.missingHandlerLogThrottle = d
	}
}

// WithMissingHandlerJitterPct Percent in [0,1], e.g. 0.2 = +/-20% jitter
func WithMissingHandlerJitterPct(pct float64) EngineOption {
	return func(e *Engine) {
		if pct < 0 {
			pct = 0
		}
		e.missingHandlerJitterPct = pct
	}
}

// Queue starvation control (priority aging)

// WithQueueAgingEnabled toggles SQL-side priority aging in dequeue ordering.
func WithQueueAgingEnabled(enabled bool) EngineOption {
	return func(e *Engine) {
		e.store.SetAgingEnabled(enabled)
	}
}

// WithQueueAgingRate sets the points-per-second rate for priority aging (e.g., 0.5).
// Effective priority is min(100, priority + floor(wait_seconds * rate)).
func WithQueueAgingRate(rate float64) EngineOption {
	return func(e *Engine) {
		if rate < 0 {
			rate = 0
		}
		e.store.SetAgingRate(rate)
	}
}

// WithLongRunningSteps enables a mode optimized for handlers that run for extended periods
// (minutes to hours). In this mode, the handler execution happens outside of the database
// transaction, which:
//
//   - Releases the database connection during handler execution
//   - Makes the "running" status immediately visible to other connections
//   - Prevents connection pool exhaustion with many concurrent workers
//
// Trade-offs:
//
//   - If a worker crashes during handler execution, the step will be stuck in "running" status.
//     You should implement a recovery mechanism to reset stuck steps (e.g., a cron job that
//     resets steps in "running" status for longer than expected execution time).
//
//   - Handlers should be idempotent or use the IdempotencyKey from StepContext to handle
//     potential re-execution after recovery.
//
// Example usage:
//
//	engine := NewEngine(pool, WithLongRunningSteps())
func WithLongRunningSteps() EngineOption {
	return func(e *Engine) {
		e.longRunningSteps = true
	}
}
