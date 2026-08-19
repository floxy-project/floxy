ALTER TABLE workflows.workflow_queue
    ADD COLUMN IF NOT EXISTS locked_until TIMESTAMPTZ;

UPDATE workflows.workflow_queue q
SET locked_until = wi.locked_until
FROM workflows.workflow_instances wi
WHERE q.instance_id = wi.id
    AND q.attempted_at IS NOT NULL
    AND q.locked_until IS NULL
    AND wi.locked_until IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_workflow_queue_locked_until
    ON workflows.workflow_queue(locked_until)
    WHERE locked_until IS NOT NULL;

COMMENT ON COLUMN workflows.workflow_queue.locked_until IS
    'Queue item lease expiration; workers may reclaim attempted items after this timestamp';
