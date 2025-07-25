-- ============================================================================
-- Databricks Views: Query Performance Analysis (View-Based Approach)
-- Purpose: Smart bad query detection using views instead of table-valued functions
-- Author: Claude Code AI Assistant  
-- Version: 4.0 (View-based solution for Databricks compatibility)
-- ============================================================================

-- Base view with smart scoring algorithm
CREATE OR REPLACE VIEW dwiltse.query_optimization.query_performance_base AS
SELECT 
  query_id,
  workspace_id,
  created_time,
  end_time,
  execution_duration_ms,
  read_bytes,
  read_rows,
  spilled_local_bytes,
  shuffle_read_bytes,
  read_io_cache_percent,
  waiting_for_compute_duration_ms,
  executed_by_user_id,
  compute.warehouse_id,
  query_text,
  
  -- Smart badness scoring calculation
  (
    -- Memory spill penalty (biggest performance killer)
    CASE 
      WHEN COALESCE(spilled_local_bytes, 0) > 0 
      THEN 50.0 + LOG(2, GREATEST(1, COALESCE(spilled_local_bytes, 0) / 1048576.0))
      ELSE 0 
    END +
    
    -- Execution time penalty
    CASE 
      WHEN execution_duration_ms > 300000 -- 5+ minutes
      THEN 30.0 + LOG(2, GREATEST(1, execution_duration_ms / 60000.0))
      ELSE execution_duration_ms / 10000.0 
    END +
    
    -- Cache miss penalty
    CASE 
      WHEN COALESCE(read_io_cache_percent, 1.0) < 0.5 -- <50% cache hit
      THEN 20.0 * (1 - COALESCE(read_io_cache_percent, 1.0))
      ELSE 0 
    END +
    
    -- Data inefficiency penalty
    CASE 
      WHEN read_bytes > 0 AND read_rows > 0 
           AND (read_bytes / CAST(read_rows AS DOUBLE)) > 10000 -- >10KB per row
      THEN 15.0
      ELSE 0 
    END +
    
    -- Shuffle penalty
    CASE 
      WHEN COALESCE(shuffle_read_bytes, 0) > 0 AND read_bytes > 0
           AND (shuffle_read_bytes / CAST(read_bytes AS DOUBLE)) > 0.3 -- >30% shuffle
      THEN 10.0
      ELSE 0 
    END +
    
    -- Queue time penalty
    CASE 
      WHEN COALESCE(waiting_for_compute_duration_ms, 0) > 30000 -- >30 seconds
      THEN 10.0
      ELSE 0 
    END
    
  ) AS badness_score,
  
  -- Primary issue identification
  CASE 
    WHEN COALESCE(spilled_local_bytes, 0) > 1073741824 -- >1GB spill
      THEN 'MEMORY_SPILL_CRITICAL'
    WHEN execution_duration_ms > 1800000 -- >30 minutes
      THEN 'EXECUTION_TOO_SLOW'
    WHEN COALESCE(read_io_cache_percent, 1.0) < 0.3 -- <30% cache hit
      THEN 'POOR_CACHE_UTILIZATION'
    WHEN read_bytes > 0 AND read_rows > 0 
         AND (read_bytes / CAST(read_rows AS DOUBLE)) > 50000 -- >50KB per row
      THEN 'DATA_INEFFICIENT'
    WHEN COALESCE(shuffle_read_bytes, 0) > 0 AND read_bytes > 0
         AND (shuffle_read_bytes / CAST(read_bytes AS DOUBLE)) > 0.5 -- >50% shuffle
      THEN 'SHUFFLE_HEAVY'
    WHEN COALESCE(waiting_for_compute_duration_ms, 0) > 60000 -- >1 min wait
      THEN 'INFRASTRUCTURE_BOTTLENECK'
    ELSE 'GENERAL_PERFORMANCE'
  END AS primary_issue,
  
  -- Human-readable metrics
  ROUND(execution_duration_ms / 1000.0, 2) AS duration_seconds,
  ROUND(COALESCE(spilled_local_bytes, 0) / 1073741824.0, 3) AS spill_gb,
  ROUND(COALESCE(read_io_cache_percent, 0) * 100, 1) AS cache_hit_percent,
  ROUND(read_bytes / 1073741824.0, 3) AS data_read_gb

FROM system.query.history 
WHERE execution_status = 'FINISHED'
  AND execution_duration_ms > 5000 -- Skip very fast queries
  AND query_text IS NOT NULL;

-- ============================================================================
-- Specific Time-Window Views
-- ============================================================================

-- Last 2 hours - for immediate issues
CREATE OR REPLACE VIEW dwiltse.query_optimization.worst_queries_2h AS
SELECT 
  ROW_NUMBER() OVER (ORDER BY badness_score DESC) AS query_rank,
  query_id,
  badness_score,
  primary_issue,
  duration_seconds,
  spill_gb,
  cache_hit_percent,
  data_read_gb,
  executed_by_user_id AS executed_by,
  warehouse_id,
  LEFT(query_text, 200) AS statement_preview,
  end_time
FROM dwiltse.query_optimization.query_performance_base
WHERE created_time >= CURRENT_TIMESTAMP - INTERVAL 2 HOUR
  AND badness_score > 10.0
ORDER BY badness_score DESC
LIMIT 10;

-- Last 24 hours - standard analysis
CREATE OR REPLACE VIEW dwiltse.query_optimization.worst_queries_24h AS
SELECT 
  ROW_NUMBER() OVER (ORDER BY badness_score DESC) AS query_rank,
  query_id,
  badness_score,
  primary_issue,
  duration_seconds,
  spill_gb,
  cache_hit_percent,
  data_read_gb,
  executed_by_user_id AS executed_by,
  warehouse_id,
  LEFT(query_text, 200) AS statement_preview,
  end_time
FROM dwiltse.query_optimization.query_performance_base
WHERE created_time >= CURRENT_TIMESTAMP - INTERVAL 24 HOUR
  AND badness_score > 10.0
ORDER BY badness_score DESC
LIMIT 20;

-- Last week - trend analysis
CREATE OR REPLACE VIEW dwiltse.query_optimization.worst_queries_week AS
SELECT 
  ROW_NUMBER() OVER (ORDER BY badness_score DESC) AS query_rank,
  query_id,
  badness_score,
  primary_issue,
  duration_seconds,
  spill_gb,
  cache_hit_percent,
  data_read_gb,
  executed_by_user_id AS executed_by,
  warehouse_id,
  LEFT(query_text, 200) AS statement_preview,
  end_time,
  DATE(created_time) AS query_date
FROM dwiltse.query_optimization.query_performance_base
WHERE created_time >= CURRENT_TIMESTAMP - INTERVAL 7 DAY
  AND badness_score > 10.0
ORDER BY badness_score DESC
LIMIT 50;

-- Critical issues only - memory spills and major problems
CREATE OR REPLACE VIEW dwiltse.query_optimization.critical_queries AS
SELECT 
  ROW_NUMBER() OVER (ORDER BY badness_score DESC) AS query_rank,
  query_id,
  badness_score,
  primary_issue,
  duration_seconds,
  spill_gb,
  cache_hit_percent,
  data_read_gb,
  executed_by_user_id AS executed_by,
  warehouse_id,
  LEFT(query_text, 200) AS statement_preview,
  end_time
FROM dwiltse.query_optimization.query_performance_base
WHERE created_time >= CURRENT_TIMESTAMP - INTERVAL 24 HOUR
  AND (
    primary_issue = 'MEMORY_SPILL_CRITICAL' OR
    primary_issue = 'EXECUTION_TOO_SLOW' OR
    badness_score > 50.0
  )
ORDER BY badness_score DESC;

-- ============================================================================
-- Usage Examples:
-- 
-- Immediate issues (last 2 hours):
-- SELECT * FROM dwiltse.query_optimization.worst_queries_2h;
--
-- Daily analysis (last 24 hours):
-- SELECT * FROM dwiltse.query_optimization.worst_queries_24h;
--
-- Weekly trends:
-- SELECT * FROM dwiltse.query_optimization.worst_queries_week;
--
-- Critical problems only:
-- SELECT * FROM dwiltse.query_optimization.critical_queries;
--
-- Custom time window using base view:
-- SELECT * FROM dwiltse.query_optimization.query_performance_base 
-- WHERE created_time >= '2025-01-20' AND badness_score > 15
-- ORDER BY badness_score DESC;
-- ============================================================================