ALTER TABLE workflows.workflow_queue
    ADD COLUMN IF NOT EXISTS lock_token TEXT;

COMMENT ON COLUMN workflows.workflow_queue.lock_token IS
    'Opaque token identifying the worker lease owner for heartbeat and stale completion protection';
