ALTER TABLE queue ADD COLUMN locked_until TIMESTAMP;

UPDATE queue
SET locked_until = (
    SELECT wi.locked_until
    FROM workflow_instances wi
    WHERE wi.id = queue.instance_id
)
WHERE attempted_at IS NOT NULL
    AND locked_until IS NULL
    AND EXISTS (
        SELECT 1
        FROM workflow_instances wi
        WHERE wi.id = queue.instance_id
            AND wi.locked_until IS NOT NULL
    );
