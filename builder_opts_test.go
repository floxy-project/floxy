package floxy

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestWithBuilderMaxRetries(t *testing.T) {
	type args struct {
		maxRetries int
	}
	tests := []struct {
		name           string
		args           args
		wantMaxRetries int
	}{
		{
			name:           "positive max retries",
			args:           args{maxRetries: 5},
			wantMaxRetries: 5,
		},
		{
			name:           "zero max retries",
			args:           args{maxRetries: 0},
			wantMaxRetries: 0,
		},
		{
			name:           "negative max retries",
			args:           args{maxRetries: -1},
			wantMaxRetries: -1,
		},
		{
			name:           "large max retries",
			args:           args{maxRetries: 100},
			wantMaxRetries: 100,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := &Builder{
				name:              "test",
				version:           1,
				steps:             make(map[string]*StepDefinition),
				defaultMaxRetries: 3,
			}

			opt := WithBuilderMaxRetries(tt.args.maxRetries)
			opt(builder)

			assert.Equalf(t, tt.wantMaxRetries, builder.defaultMaxRetries, "WithBuilderMaxRetries(%v)", tt.args.maxRetries)
		})
	}
}

func TestWithWorkflowLockTimeout(t *testing.T) {
	builder := &Builder{}

	WithWorkflowLockTimeout(30 * time.Second)(builder)

	assert.Equal(t, 30*time.Second, builder.workflowLockTimeout)
}

func TestWithWorkflowLockTimeout_NonPositiveUsesDefault(t *testing.T) {
	builder := &Builder{}

	WithWorkflowLockTimeout(-time.Second)(builder)

	assert.Zero(t, builder.workflowLockTimeout)
}

func TestWithStepDelay(t *testing.T) {
	type args struct {
		delay time.Duration
	}
	tests := []struct {
		name      string
		args      args
		wantDelay time.Duration
	}{
		{
			name:      "zero delay",
			args:      args{delay: 0},
			wantDelay: 0,
		},
		{
			name:      "positive delay",
			args:      args{delay: time.Second},
			wantDelay: time.Second,
		},
		{
			name:      "large delay",
			args:      args{delay: 5 * time.Minute},
			wantDelay: 5 * time.Minute,
		},
		{
			name:      "hour delay",
			args:      args{delay: time.Hour},
			wantDelay: time.Hour,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			step := &StepDefinition{
				Name: "test_step",
				Type: StepTypeTask,
			}

			opt := WithStepDelay(tt.args.delay)
			opt(step)

			assert.Equalf(t, tt.wantDelay, step.Delay, "WithStepDelay(%v)", tt.args.delay)
		})
	}
}

func TestWithStepMaxRetries(t *testing.T) {
	type args struct {
		maxRetries int
	}
	tests := []struct {
		name           string
		args           args
		step           *StepDefinition
		wantMaxRetries int
	}{
		{
			name: "positive max retries",
			args: args{maxRetries: 5},
			step: &StepDefinition{
				Name:         "test_step",
				Type:         StepTypeTask,
				MaxRetries:   3,
				NoIdempotent: false,
			},
			wantMaxRetries: 5,
		},
		{
			name: "zero max retries",
			args: args{maxRetries: 0},
			step: &StepDefinition{
				Name:         "test_step",
				Type:         StepTypeTask,
				MaxRetries:   3,
				NoIdempotent: false,
			},
			wantMaxRetries: 0,
		},
		{
			name: "negative max retries",
			args: args{maxRetries: -1},
			step: &StepDefinition{
				Name:         "test_step",
				Type:         StepTypeTask,
				MaxRetries:   3,
				NoIdempotent: false,
			},
			wantMaxRetries: -1,
		},
		{
			name: "no idempotent step - should not change",
			args: args{maxRetries: 5},
			step: &StepDefinition{
				Name:         "test_step",
				Type:         StepTypeTask,
				MaxRetries:   1,
				NoIdempotent: true,
			},
			wantMaxRetries: 1, // Should remain unchanged
		},
		{
			name: "no idempotent step with different initial value",
			args: args{maxRetries: 10},
			step: &StepDefinition{
				Name:         "test_step",
				Type:         StepTypeTask,
				MaxRetries:   3,
				NoIdempotent: true,
			},
			wantMaxRetries: 3, // Should remain unchanged
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opt := WithStepMaxRetries(tt.args.maxRetries)
			opt(tt.step)

			assert.Equalf(t, tt.wantMaxRetries, tt.step.MaxRetries, "WithStepMaxRetries(%v)", tt.args.maxRetries)
		})
	}
}

func TestWithStepMetadata(t *testing.T) {
	type args struct {
		metadata map[string]any
	}
	tests := []struct {
		name         string
		args         args
		wantMetadata map[string]any
	}{
		{
			name:         "empty metadata",
			args:         args{metadata: map[string]any{}},
			wantMetadata: map[string]any{},
		},
		{
			name: "single key metadata",
			args: args{metadata: map[string]any{
				"key1": "value1",
			}},
			wantMetadata: map[string]any{
				"key1": "value1",
			},
		},
		{
			name: "multiple keys metadata",
			args: args{metadata: map[string]any{
				"key1": "value1",
				"key2": 42,
				"key3": true,
			}},
			wantMetadata: map[string]any{
				"key1": "value1",
				"key2": 42,
				"key3": true,
			},
		},
		{
			name: "nested metadata",
			args: args{metadata: map[string]any{
				"key1": map[string]any{
					"nested1": "value1",
					"nested2": 123,
				},
			}},
			wantMetadata: map[string]any{
				"key1": map[string]any{
					"nested1": "value1",
					"nested2": 123,
				},
			},
		},
		{
			name: "metadata with slice",
			args: args{metadata: map[string]any{
				"tags": []string{"tag1", "tag2", "tag3"},
			}},
			wantMetadata: map[string]any{
				"tags": []string{"tag1", "tag2", "tag3"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			step := &StepDefinition{
				Name:     "test_step",
				Type:     StepTypeTask,
				Metadata: make(map[string]any),
			}

			opt := WithStepMetadata(tt.args.metadata)
			opt(step)

			assert.Equal(t, tt.wantMetadata, step.Metadata, "WithStepMetadata(%v)", tt.args.metadata)
		})
	}
}

func TestWithStepNoIdempotent(t *testing.T) {
	tests := []struct {
		name              string
		initialMaxRetries int
		wantNoIdempotent  bool
		wantMaxRetries    int
	}{
		{
			name:              "default max retries",
			initialMaxRetries: 3,
			wantNoIdempotent:  true,
			wantMaxRetries:    1,
		},
		{
			name:              "zero max retries",
			initialMaxRetries: 0,
			wantNoIdempotent:  true,
			wantMaxRetries:    1,
		},
		{
			name:              "large max retries",
			initialMaxRetries: 10,
			wantNoIdempotent:  true,
			wantMaxRetries:    1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			step := &StepDefinition{
				Name:       "test_step",
				Type:       StepTypeTask,
				MaxRetries: tt.initialMaxRetries,
			}

			opt := WithStepNoIdempotent()
			opt(step)

			assert.Equalf(t, tt.wantNoIdempotent, step.NoIdempotent, "WithStepNoIdempotent() - NoIdempotent")
			assert.Equalf(t, tt.wantMaxRetries, step.MaxRetries, "WithStepNoIdempotent() - MaxRetries")
		})
	}
}

func TestWithStepRetryDelay(t *testing.T) {
	type args struct {
		retryDelay time.Duration
	}
	tests := []struct {
		name           string
		args           args
		wantRetryDelay time.Duration
	}{
		{
			name:           "zero retry delay",
			args:           args{retryDelay: 0},
			wantRetryDelay: 0,
		},
		{
			name:           "positive retry delay",
			args:           args{retryDelay: time.Second},
			wantRetryDelay: time.Second,
		},
		{
			name:           "large retry delay",
			args:           args{retryDelay: 10 * time.Minute},
			wantRetryDelay: 10 * time.Minute,
		},
		{
			name:           "millisecond retry delay",
			args:           args{retryDelay: 500 * time.Millisecond},
			wantRetryDelay: 500 * time.Millisecond,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			step := &StepDefinition{
				Name: "test_step",
				Type: StepTypeTask,
			}

			opt := WithStepRetryDelay(tt.args.retryDelay)
			opt(step)

			assert.Equalf(t, tt.wantRetryDelay, step.RetryDelay, "WithStepRetryDelay(%v)", tt.args.retryDelay)
		})
	}
}

func TestWithStepTimeout(t *testing.T) {
	type args struct {
		timeout time.Duration
	}
	tests := []struct {
		name        string
		args        args
		wantTimeout time.Duration
	}{
		{
			name:        "zero timeout",
			args:        args{timeout: 0},
			wantTimeout: 0,
		},
		{
			name:        "positive timeout",
			args:        args{timeout: 30 * time.Second},
			wantTimeout: 30 * time.Second,
		},
		{
			name:        "large timeout",
			args:        args{timeout: 1 * time.Hour},
			wantTimeout: 1 * time.Hour,
		},
		{
			name:        "minute timeout",
			args:        args{timeout: 5 * time.Minute},
			wantTimeout: 5 * time.Minute,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			step := &StepDefinition{
				Name: "test_step",
				Type: StepTypeTask,
			}

			opt := WithStepTimeout(tt.args.timeout)
			opt(step)

			assert.Equalf(t, tt.wantTimeout, step.Timeout, "WithStepTimeout(%v)", tt.args.timeout)
		})
	}
}

func TestWithStepRetryStrategy(t *testing.T) {
	type args struct {
		strategy RetryStrategy
	}
	tests := []struct {
		name         string
		args         args
		wantStrategy RetryStrategy
	}{
		{
			name:         "fixed strategy",
			args:         args{strategy: RetryStrategyFixed},
			wantStrategy: RetryStrategyFixed,
		},
		{
			name:         "exponential strategy",
			args:         args{strategy: RetryStrategyExponential},
			wantStrategy: RetryStrategyExponential,
		},
		{
			name:         "linear strategy",
			args:         args{strategy: RetryStrategyLinear},
			wantStrategy: RetryStrategyLinear,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			step := &StepDefinition{
				Name: "test_step",
				Type: StepTypeTask,
			}

			opt := WithStepRetryStrategy(tt.args.strategy)
			opt(step)

			assert.Equalf(t, tt.wantStrategy, step.RetryStrategy, "WithStepRetryStrategy(%v)", tt.args.strategy)
		})
	}
}
