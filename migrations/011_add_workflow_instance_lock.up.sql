ALTER TABLE workflows.workflow_instances
    ADD COLUMN IF NOT EXISTS locked_until TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_workflow_instances_locked_until
    ON workflows.workflow_instances(locked_until)
    WHERE locked_until IS NOT NULL;

COMMENT ON COLUMN workflows.workflow_instances.locked_until IS
    'Workflow instance lock expiration; workers skip running instances locked until now or later';
