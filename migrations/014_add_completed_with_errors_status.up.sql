ALTER TABLE workflows.workflow_instances DROP CONSTRAINT IF EXISTS workflow_instances_status_check;
ALTER TABLE workflows.workflow_instances ADD CONSTRAINT workflow_instances_status_check
    CHECK (status IN ('pending', 'running', 'completed', 'completed_with_errors', 'failed', 'rolling_back', 'cancelled', 'cancelling', 'aborted', 'dlq'));

CREATE OR REPLACE FUNCTION workflows.cleanup_old_workflows(days_to_keep INT DEFAULT 30)
    RETURNS TABLE(deleted_count BIGINT) AS $$
DECLARE
    result BIGINT;
BEGIN
    DELETE FROM workflows.workflow_instances
    WHERE status IN ('completed', 'completed_with_errors', 'failed', 'cancelled', 'aborted')
      AND completed_at < NOW() - INTERVAL '1 day' * days_to_keep;

    GET DIAGNOSTICS result = ROW_COUNT;
    RETURN QUERY SELECT result;
END;
$$ LANGUAGE plpgsql;
