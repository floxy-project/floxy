package floxy

import (
	"log"
	"time"
)

type StepOption func(step *StepDefinition)

func WithStepMaxRetries(maxRetries int) StepOption {
	return func(step *StepDefinition) {
		if step.NoIdempotent {
			log.Println("WithStepMaxRetries: unable to set MaxRetries to not idempotent step")

			return
		}

		step.MaxRetries = maxRetries
	}
}

func WithStepMetadata(metadata map[string]any) StepOption {
	return func(step *StepDefinition) {
		step.Metadata = metadata
	}
}

func WithStepNoIdempotent() StepOption {
	return func(step *StepDefinition) {
		step.NoIdempotent = true
		step.MaxRetries = 1
	}
}

func WithStepDelay(delay time.Duration) StepOption {
	return func(step *StepDefinition) {
		step.Delay = delay
	}
}

func WithStepRetryDelay(retryDelay time.Duration) StepOption {
	return func(step *StepDefinition) {
		step.RetryDelay = retryDelay
	}
}

func WithStepTimeout(timeout time.Duration) StepOption {
	return func(step *StepDefinition) {
		step.Timeout = timeout
	}
}

func WithStepRetryStrategy(strategy RetryStrategy) StepOption {
	return func(step *StepDefinition) {
		step.RetryStrategy = strategy
	}
}

type BuilderOption func(builder *Builder)

func WithBuilderMaxRetries(maxRetries int) BuilderOption {
	return func(builder *Builder) {
		builder.defaultMaxRetries = maxRetries
	}
}

// WithDLQEnabled enables or disables Dead Letter Queue mode for the workflow.
// When enabled, failed steps will be sent to DLQ and the engine will skip rollback/compensation.
func WithDLQEnabled(enabled bool) BuilderOption {
	return func(builder *Builder) {
		builder.dlqEnabled = enabled
	}
}

// WithWorkflowLockTimeout sets how long a queue item lease lasts after a worker claims it.
// If unset or non-positive, the store falls back to the default lock timeout.
func WithWorkflowLockTimeout(timeout time.Duration) BuilderOption {
	return func(builder *Builder) {
		if timeout <= 0 {
			timeout = 0
		}
		builder.workflowLockTimeout = timeout
	}
}
