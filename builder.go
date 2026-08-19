package floxy

import (
	"errors"
	"fmt"
	"time"
)

const (
	defaultMaxRetries          = 3
	defaultWorkflowLockTimeout = 1 * time.Minute

	rootStepName = "_root_"
)

type Builder struct {
	name         string
	version      int
	steps        map[string]*StepDefinition
	startStep    string
	currentStep  string
	nestingLevel int

	subBuilders         []*Builder
	defaultMaxRetries   int
	dlqEnabled          bool
	workflowLockTimeout time.Duration

	err error
}

func NewBuilder(name string, version int, opts ...BuilderOption) *Builder {
	builder := &Builder{
		name:              name,
		version:           version,
		steps:             make(map[string]*StepDefinition),
		defaultMaxRetries: defaultMaxRetries,
		nestingLevel:      1,
	}

	for _, opt := range opts {
		opt(builder)
	}

	return builder
}

func (builder *Builder) Step(name, handler string, opts ...StepOption) *Builder {
	if builder.err != nil {
		return builder
	}

	if _, ok := builder.steps[name]; ok {
		builder.err = fmt.Errorf("step %q already exists", name)

		return builder
	}

	step := &StepDefinition{
		Name:          name,
		Type:          StepTypeTask,
		Handler:       handler,
		MaxRetries:    builder.defaultMaxRetries,
		Next:          []string{},
		Prev:          builder.currentStep,
		Metadata:      make(map[string]any),
		RetryStrategy: RetryStrategyFixed, // Default strategy
	}

	for _, opt := range opts {
		opt(step)
	}

	builder.steps[name] = step

	if builder.startStep == "" {
		builder.startStep = name
		step.Prev = rootStepName
	}

	if builder.currentStep != "" && builder.currentStep != name {
		builder.steps[builder.currentStep].Next = append(builder.steps[builder.currentStep].Next, name)
	}

	builder.currentStep = name

	return builder
}

func (builder *Builder) Then(name, handler string, opts ...StepOption) *Builder {
	return builder.Step(name, handler, opts...)
}

func (builder *Builder) OnFailure(name, handler string, opts ...StepOption) *Builder {
	if builder.err != nil {
		return builder
	}

	if builder.currentStep == "" {
		builder.err = fmt.Errorf("OnFailure %q called with no step", name)

		return builder
	}

	compensation := &StepDefinition{
		Name:          name,
		Type:          StepTypeTask,
		Handler:       handler,
		MaxRetries:    builder.defaultMaxRetries,
		Prev:          "",                 // Compensation steps don't have prev in the main flow
		RetryStrategy: RetryStrategyFixed, // Default strategy
	}
	for _, opt := range opts {
		opt(compensation)
	}

	builder.steps[name] = compensation
	builder.steps[builder.currentStep].OnFailure = name

	return builder
}

func (builder *Builder) Parallel(name string, tasks ...*StepDefinition) *Builder {
	if builder.err != nil {
		return builder
	}

	parallelStep := &StepDefinition{
		Name:     name,
		Type:     StepTypeParallel,
		Prev:     builder.currentStep,
		Metadata: make(map[string]any),
		Parallel: []string{},
	}

	if _, ok := builder.steps[name]; ok {
		builder.err = fmt.Errorf("duplicate parallel step %q", name)

		return builder
	}
	builder.steps[name] = parallelStep

	if builder.currentStep != "" && builder.currentStep != name {
		builder.steps[builder.currentStep].Next = append(builder.steps[builder.currentStep].Next, name)
	}

	for i, task := range tasks {
		if task.Name == "" {
			builder.err = fmt.Errorf("tasks[%d] in parallel %q missing step name", i, name)

			return builder
		}
		if task.Handler == "" {
			builder.err = fmt.Errorf("tasks[%d] in parallel %q missing step handler", i, name)

			return builder
		}
		if _, ok := builder.steps[task.Name]; ok {
			builder.err = fmt.Errorf("duplicate step %q in parallel %q", task.Name, name)

			return builder
		}

		// Set the parallel step as prev for each task
		task.Prev = name
		builder.steps[task.Name] = task

		parallelStep.Parallel = append(parallelStep.Parallel, task.Name)
	}

	joinStepName := name + "_join"
	parallelStep.Next = append(parallelStep.Next, joinStepName)

	return builder.JoinStep(joinStepName, parallelStep.Parallel, JoinStrategyAll)
}

func (builder *Builder) Fork(name string, branches ...func(branch *Builder)) *Builder {
	if builder.err != nil {
		return builder
	}

	if len(branches) < 2 {
		builder.err = fmt.Errorf("ForkFlow %q called with count of branches < 2", name)

		return builder
	}

	forkStep := &StepDefinition{
		Name:     name,
		Type:     StepTypeFork,
		Prev:     builder.currentStep,
		Metadata: make(map[string]any),
	}

	builder.steps[name] = forkStep

	if builder.startStep == "" {
		builder.startStep = name
		forkStep.Prev = rootStepName
	}

	if builder.currentStep != "" && builder.currentStep != name {
		builder.steps[builder.currentStep].Next = append(builder.steps[builder.currentStep].Next, name)
	}

	for i, branchFn := range branches {
		sub := &Builder{
			name:              fmt.Sprintf("%s_branch_%d", builder.name, i+1),
			version:           builder.version,
			steps:             make(map[string]*StepDefinition),
			defaultMaxRetries: builder.defaultMaxRetries,
			nestingLevel:      builder.nestingLevel + 1,
		}

		branchFn(sub)

		if sub.startStep == "" {
			builder.err = fmt.Errorf("Fork %q: branch %d has no steps", name, i+1)

			return builder
		}
		if _, err := sub.Build(); err != nil {
			builder.err = fmt.Errorf("invalid branch %d for Fork %q: %v", i+1, name, err)

			return builder
		}

		builder.subBuilders = append(builder.subBuilders, sub)

		for stepName, stepDef := range sub.steps {
			if _, ok := builder.steps[stepName]; ok {
				builder.err = fmt.Errorf("duplicate step %q in fork flow %q", stepName, name)

				return builder
			}

			// Set the fork step as prev for the first step of each branch
			if stepName == sub.startStep {
				stepDef.Prev = name
			}

			builder.steps[stepName] = stepDef
		}

		forkStep.Parallel = append(forkStep.Parallel, sub.startStep)
	}

	builder.currentStep = name

	return builder
}

// JoinStep should be used without Condition. Use Join instead, which automatically handles
// dynamic step detection in fork branches. JoinStep with an explicit waitFor list does not
// account for dynamically created steps (e.g., Condition else branches).
func (builder *Builder) JoinStep(name string, waitFor []string, strategy JoinStrategy) *Builder {
	if builder.err != nil {
		return builder
	}

	if builder.currentStep == "" {
		builder.err = fmt.Errorf("JoinStep %q called with no step", name)

		return builder
	}

	if name == "" {
		builder.err = errors.New("JoinStep called with no name")

		return builder
	}

	if strategy == "" {
		strategy = JoinStrategyAll
	}

	step := &StepDefinition{
		Name:         name,
		Type:         StepTypeJoin,
		WaitFor:      waitFor,
		JoinStrategy: strategy,
		Prev:         builder.currentStep,
		Metadata:     make(map[string]any),
	}

	builder.steps[name] = step

	if builder.currentStep != "" && builder.currentStep != name {
		builder.steps[builder.currentStep].Next = append(builder.steps[builder.currentStep].Next, name)
	}

	builder.currentStep = name

	return builder
}

func (builder *Builder) Join(name string, strategy JoinStrategy) *Builder {
	if builder.err != nil {
		return builder
	}

	if builder.currentStep == "" {
		builder.err = fmt.Errorf("Join %q called with no step", name)

		return builder
	}

	if name == "" {
		builder.err = errors.New("Join called with no name")

		return builder
	}

	if strategy == "" {
		strategy = JoinStrategyAll
	}

	// Find the fork/parallel step before this join
	prevStep, ok := builder.steps[builder.currentStep]
	if !ok {
		builder.err = fmt.Errorf("previous step %q not found", builder.currentStep)

		return builder
	}

	var waitFor []string

	// Check if previous step is a Fork or Parallel
	if prevStep.Type == StepTypeFork || prevStep.Type == StepTypeParallel {
		// For each branch, check if there's a Condition step
		for _, branchStartStep := range prevStep.Parallel {
			// Traverse the branch to find the first Condition step
			conditionStepName := builder.findFirstConditionInBranch(branchStartStep)
			if conditionStepName != "" {
				// Add a virtual step for this condition
				virtualStep := fmt.Sprintf("cond#%s", conditionStepName)
				waitFor = append(waitFor, virtualStep)
			} else {
				// No condition, use the branch start step (or find terminal step)
				// For now, we'll use the branch start step, but the engine will handle
				// dynamic detection of terminal steps
				terminalStep := builder.findTerminalStepInBranch(branchStartStep)
				if terminalStep != "" {
					waitFor = append(waitFor, terminalStep)
				} else {
					waitFor = append(waitFor, branchStartStep)
				}
			}
		}
	} else {
		// Not a fork/parallel, use current step
		waitFor = []string{builder.currentStep}
	}

	step := &StepDefinition{
		Name:         name,
		Type:         StepTypeJoin,
		WaitFor:      waitFor,
		JoinStrategy: strategy,
		Prev:         builder.currentStep,
		Metadata:     make(map[string]any),
	}

	builder.steps[name] = step

	if builder.currentStep != "" && builder.currentStep != name {
		builder.steps[builder.currentStep].Next = append(builder.steps[builder.currentStep].Next, name)
	}

	builder.currentStep = name

	return builder
}

func (builder *Builder) ForkJoin(
	forkName string,
	branches []func(branch *Builder),
	joinName string,
	joinStrategy JoinStrategy,
) *Builder {
	if builder.err != nil {
		return builder
	}

	_ = builder.Fork(forkName, branches...)

	return builder.Join(joinName, joinStrategy)
}

func (builder *Builder) SavePoint(name string) *Builder {
	if builder.err != nil {
		return builder
	}

	if name == "" {
		builder.err = errors.New("SavePoint called with no name")

		return builder
	}

	if builder.nestingLevel > 1 {
		builder.err = fmt.Errorf("SavePoint %q called from within a subflow", name)

		return builder
	}

	step := &StepDefinition{
		Name:     name,
		Type:     StepTypeSavePoint,
		Prev:     builder.currentStep,
		Metadata: make(map[string]any),
	}

	builder.steps[name] = step

	if builder.currentStep != "" && builder.currentStep != name {
		builder.steps[builder.currentStep].Next = append(builder.steps[builder.currentStep].Next, name)
	}

	builder.currentStep = name

	return builder
}

func (builder *Builder) Condition(name, expr string, elseBranch func(elseBranchBuilder *Builder)) *Builder {
	if builder.err != nil {
		return builder
	}

	if name == "" {
		builder.err = errors.New("Condition called with no name")

		return builder
	}
	if expr == "" {
		builder.err = errors.New("Condition called with no expression")

		return builder
	}
	if builder.currentStep == "" {
		builder.err = fmt.Errorf("Condition %q called with no step", name)

		return builder
	}

	step := &StepDefinition{
		Name:      name,
		Condition: expr,
		Type:      StepTypeCondition,
		Next:      []string{},
		Prev:      builder.currentStep,
		Metadata:  make(map[string]any),
	}

	builder.steps[name] = step

	if elseBranch != nil {
		sub := &Builder{
			name:              builder.name + "_branch_else",
			version:           builder.version,
			steps:             make(map[string]*StepDefinition),
			defaultMaxRetries: builder.defaultMaxRetries,
			nestingLevel:      builder.nestingLevel, // do not +1
		}

		elseBranch(sub)

		if sub.startStep == "" {
			builder.err = fmt.Errorf("Condition %q: else branch has no steps", name)

			return builder
		}
		if _, err := sub.Build(); err != nil {
			builder.err = fmt.Errorf("invalid else branch for Condition %q: %w", name, err)

			return builder
		}

		step.Else = sub.startStep
		builder.steps[sub.startStep] = sub.steps[sub.startStep]
		builder.steps[sub.startStep].Prev = name

		for _, subStep := range sub.steps {
			builder.steps[subStep.Name] = subStep
		}

		builder.subBuilders = append(builder.subBuilders, sub)
	}

	if builder.currentStep != "" && builder.currentStep != name {
		builder.steps[builder.currentStep].Next = append(builder.steps[builder.currentStep].Next, name)
	}

	builder.currentStep = name

	return builder
}

func (builder *Builder) WaitHumanConfirm(name string, opts ...StepOption) *Builder {
	if builder.err != nil {
		return builder
	}

	if name == "" {
		builder.err = errors.New("WaitHumanConfirm called with no name")

		return builder
	}

	step := &StepDefinition{
		Name:          name,
		Type:          StepTypeHuman,
		MaxRetries:    builder.defaultMaxRetries,
		Prev:          builder.currentStep,
		Metadata:      make(map[string]any),
		Delay:         time.Minute,
		RetryStrategy: RetryStrategyFixed, // Default strategy
	}

	for _, opt := range opts {
		opt(step)
	}

	builder.steps[name] = step
	if builder.currentStep != "" && builder.currentStep != name {
		builder.steps[builder.currentStep].Next = append(builder.steps[builder.currentStep].Next, name)
	}

	builder.currentStep = name

	return builder
}

func (builder *Builder) Build() (*WorkflowDefinition, error) {
	if builder.err != nil {
		return nil, builder.err
	}

	if builder.name == "" {
		return nil, errors.New("workflow name is required")
	}

	if builder.startStep == "" {
		return nil, fmt.Errorf("builder %q: at least one step is required", builder.name)
	}

	id := fmt.Sprintf("%s-v%d", builder.name, builder.version)

	def := &WorkflowDefinition{
		ID:      id,
		Name:    builder.name,
		Version: builder.version,
		Definition: GraphDefinition{
			Start:               builder.startStep,
			Steps:               builder.steps,
			DLQEnabled:          builder.dlqEnabled,
			WorkflowLockTimeout: builder.workflowLockTimeout,
		},
	}

	if err := ValidateWorkflowDefinition(def); err != nil {
		return nil, err
	}

	for _, sub := range builder.subBuilders {
		if _, err := sub.Build(); err != nil {
			return nil, err
		}
	}

	return def, nil
}

func ValidateWorkflowDefinition(def *WorkflowDefinition) error {
	for stepName, stepDef := range def.Definition.Steps {
		if err := validateStepName(stepName); err != nil {
			return fmt.Errorf("def %q: %w", def.Name, err)
		}

		for _, nextStep := range stepDef.Next {
			if _, ok := def.Definition.Steps[nextStep]; !ok {
				return fmt.Errorf("def %q: step %q references unknown step: %q",
					def.Name, stepName, nextStep)
			}
		}

		if stepDef.OnFailure != "" {
			if _, ok := def.Definition.Steps[stepDef.OnFailure]; !ok {
				return fmt.Errorf("def %q: step %q references unknown compensation step: %q",
					def.Name, stepName, stepDef.OnFailure)
			}
		}

		if stepDef.Else != "" {
			if _, ok := def.Definition.Steps[stepDef.Else]; !ok {
				return fmt.Errorf("def %q: step %q references unknown else step: %q",
					def.Name, stepName, stepDef.Else)
			}
		}

		for _, parallelStep := range stepDef.Parallel {
			// Skip validation for virtual steps (cond#...)
			if isVirtualStep(parallelStep) {
				continue
			}
			if _, ok := def.Definition.Steps[parallelStep]; !ok {
				return fmt.Errorf("def %q: step %q references unknown parallel step: %q",
					def.Name, stepName, parallelStep)
			}
		}

		// Validate WaitFor for Join steps, but skip virtual steps
		if stepDef.Type == StepTypeJoin {
			for _, waitForStep := range stepDef.WaitFor {
				// Skip validation for virtual steps (cond#...)
				if isVirtualStep(waitForStep) {
					continue
				}
				if _, ok := def.Definition.Steps[waitForStep]; !ok {
					return fmt.Errorf("def %q: join step %q references unknown step in waitFor: %q",
						def.Name, stepName, waitForStep)
				}
			}
		}

		if stepDef.Type == StepTypeTask && stepDef.Handler == "" {
			return fmt.Errorf("def %q: task step %q must have a handler", def.Name, stepName)
		}
	}

	visited := make(map[string]bool)
	err := detectCycles(def.Definition.Start, def.Definition.Steps, visited, make(map[string]bool))
	if err != nil {
		return fmt.Errorf("def %q: %w", def.Name, err)
	}

	return nil
}

func detectCycles(
	current string,
	steps map[string]*StepDefinition,
	visited, recStack map[string]bool,
) error {
	visited[current] = true
	recStack[current] = true

	step, ok := steps[current]
	if !ok {
		recStack[current] = false

		return nil
	}

	// collect all outgoing targets
	var outs []string
	outs = append(outs, step.Next...)
	if step.OnFailure != "" {
		outs = append(outs, step.OnFailure)
	}
	if step.Else != "" {
		outs = append(outs, step.Else)
	}
	outs = append(outs, step.Parallel...)
	outs = append(outs, step.WaitFor...)

	for _, next := range outs {
		if next == "" {
			continue
		}
		// Skip virtual steps in cycle detection
		if isVirtualStep(next) {
			continue
		}
		if _, exists := steps[next]; !exists {
			continue
		}

		if !visited[next] {
			if err := detectCycles(next, steps, visited, recStack); err != nil {
				return err
			}
		} else if recStack[next] {
			return fmt.Errorf("cycle detected: %s -> %s", current, next)
		}
	}

	recStack[current] = false

	return nil
}

func NewTask(name, handler string, opts ...StepOption) *StepDefinition {
	step := &StepDefinition{
		Name:          name,
		Handler:       handler,
		Type:          StepTypeTask,
		Prev:          "", // NewTask doesn't have currentStep context
		Metadata:      make(map[string]any),
		MaxRetries:    defaultMaxRetries,
		RetryStrategy: RetryStrategyFixed, // Default strategy
	}

	for _, opt := range opts {
		opt(step)
	}

	return step
}

func validateStepName(name string) error {
	if name == "" {
		return errors.New("step name is required")
	}
	if name == rootStepName {
		return errors.New("step name cannot be _root_")
	}
	if isVirtualStep(name) {
		return errors.New("step name cannot start with 'cond#'")
	}

	return nil
}

// isVirtualStep checks if a step name is a virtual step (starts with "cond#").
// Virtual steps are placeholders for Condition steps that will be replaced
// with actual terminal steps during workflow execution.
func isVirtualStep(stepName string) bool {
	return len(stepName) > 5 && stepName[:5] == "cond#"
}

// findFirstConditionInBranch traverses a branch starting from startStepName
// and returns the first Condition step found, or empty string if none found.
func (builder *Builder) findFirstConditionInBranch(startStepName string) string {
	visited := make(map[string]bool)

	return builder.traverseBranchForCondition(startStepName, visited)
}

// traverseBranchForCondition is a helper that traverses the branch looking for a Condition step.
func (builder *Builder) traverseBranchForCondition(stepName string, visited map[string]bool) string {
	if stepName == "" {
		return ""
	}

	if visited[stepName] {
		return "" // Already visited, prevent cycles
	}
	visited[stepName] = true

	stepDef, ok := builder.steps[stepName]
	if !ok {
		return ""
	}

	// Check if this is a Condition step
	if stepDef.Type == StepTypeCondition {
		return stepName
	}

	// Traverse Next steps
	for _, nextStep := range stepDef.Next {
		if result := builder.traverseBranchForCondition(nextStep, visited); result != "" {
			return result
		}
	}

	// Also check Else branch if exists
	if stepDef.Else != "" {
		if result := builder.traverseBranchForCondition(stepDef.Else, visited); result != "" {
			return result
		}
	}

	return ""
}

// findTerminalStepInBranch finds the terminal step (no Next steps) in a branch.
// Returns the last step that has no Next steps.
func (builder *Builder) findTerminalStepInBranch(startStepName string) string {
	visited := make(map[string]bool)
	terminalSteps := builder.traverseBranchForTerminal(startStepName, visited)

	// Return the first terminal step found (or the start step if no terminal found)
	if len(terminalSteps) > 0 {
		return terminalSteps[0]
	}

	// If no terminal step found, return the start step
	return startStepName
}

// traverseBranchForTerminal traverses the branch and collects terminal steps.
func (builder *Builder) traverseBranchForTerminal(stepName string, visited map[string]bool) []string {
	if stepName == "" {
		return nil
	}

	if visited[stepName] {
		return nil // Already visited, prevent cycles
	}
	visited[stepName] = true

	stepDef, ok := builder.steps[stepName]
	if !ok {
		return nil
	}

	// If this step has no Next steps, it's a terminal step
	if len(stepDef.Next) == 0 {
		return []string{stepName}
	}

	var terminalSteps []string

	// Traverse Next steps
	for _, nextStep := range stepDef.Next {
		terminals := builder.traverseBranchForTerminal(nextStep, visited)
		terminalSteps = append(terminalSteps, terminals...)
	}

	// Also check Else branch if exists
	if stepDef.Else != "" {
		terminals := builder.traverseBranchForTerminal(stepDef.Else, visited)
		terminalSteps = append(terminalSteps, terminals...)
	}

	return terminalSteps
}
