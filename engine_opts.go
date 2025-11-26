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
