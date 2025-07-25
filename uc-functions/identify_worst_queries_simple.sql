-- ============================================================================
-- UC Function: identify_worst_queries_simple (Basic Version)
-- Purpose: Simple version to validate UC Function syntax and system table access
-- ============================================================================

CREATE OR REPLACE FUNCTION dwiltse.query_optimization.identify_worst_queries_simple(
  hours_back INT,
  query_limit INT
)
RETURNS TABLE(
  query_rank INT,
  query_id STRING,
  workspace_id BIGINT,
  statement_preview STRING,
  executed_by STRING,
  duration_seconds DOUBLE,
  data_read_gb DOUBLE,
  warehouse_id STRING,
  end_time TIMESTAMP
)
RETURN

SELECT 
  ROW_NUMBER() OVER (ORDER BY execution_duration_ms DESC) AS query_rank,
  query_id,
  workspace_id,
  LEFT(query_text, 200) AS statement_preview,
  executed_by_user_id AS executed_by,
  ROUND(execution_duration_ms / 1000.0, 2) AS duration_seconds,
  ROUND(read_bytes / 1024.0 / 1024.0 / 1024.0, 3) AS data_read_gb,
  compute.warehouse_id,
  end_time

FROM system.query.history 

WHERE created_time >= CURRENT_TIMESTAMP - INTERVAL hours_back HOUR
  AND execution_status = 'FINISHED'
  AND execution_duration_ms > 0

ORDER BY execution_duration_ms DESC
LIMIT query_limit;

-- ============================================================================
-- Usage Examples:
-- SELECT * FROM dwiltse.query_optimization.identify_worst_queries_simple(24, 10);
-- ============================================================================