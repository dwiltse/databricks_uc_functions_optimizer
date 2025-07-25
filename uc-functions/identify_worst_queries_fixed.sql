-- ============================================================================
-- UC Function: identify_worst_queries (Fixed Databricks Version)
-- Purpose: Smart bad query detection with simplified structure for Databricks
-- Author: Claude Code AI Assistant  
-- Version: 3.1 (Fixed for Databricks syntax requirements)
-- ============================================================================

CREATE OR REPLACE FUNCTION dwiltse.query_optimization.identify_worst_queries(
  hours_back INT,
  query_limit INT
)
RETURNS TABLE(
  query_rank INT,
  query_id STRING,
  badness_score DOUBLE,
  primary_issue STRING,
  duration_seconds DOUBLE,
  spill_gb DOUBLE,
  cache_hit_percent DOUBLE,
  data_read_gb DOUBLE,
  executed_by STRING,
  warehouse_id STRING,
  statement_preview STRING,
  end_time TIMESTAMP
)
RETURN

SELECT 
  ROW_NUMBER() OVER (ORDER BY badness_score DESC) AS query_rank,
  query_id,
  badness_score,
  primary_issue,
  duration_seconds,
  spill_gb,
  cache_hit_percent,
  data_read_gb,
  executed_by,
  warehouse_id,
  statement_preview,
  end_time

FROM (
  SELECT 
    query_id,
    
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
    
    -- Output fields
    ROUND(execution_duration_ms / 1000.0, 2) AS duration_seconds,
    ROUND(COALESCE(spilled_local_bytes, 0) / 1073741824.0, 3) AS spill_gb,
    ROUND(COALESCE(read_io_cache_percent, 0) * 100, 1) AS cache_hit_percent,
    ROUND(read_bytes / 1073741824.0, 3) AS data_read_gb,
    executed_by_user_id AS executed_by,
    compute.warehouse_id,
    LEFT(query_text, 200) AS statement_preview,
    end_time
    
  FROM system.query.history 
  WHERE created_time >= CURRENT_TIMESTAMP - INTERVAL hours_back HOUR
    AND execution_status = 'FINISHED'
    AND execution_duration_ms > 5000 -- Skip very fast queries
    AND query_text IS NOT NULL
) ranked

WHERE badness_score > 10.0 -- Only meaningful performance issues
ORDER BY badness_score DESC
LIMIT query_limit;

-- ============================================================================
-- Usage Examples:
-- 
-- Basic usage (last 24 hours, top 10 worst):
-- SELECT * FROM dwiltse.query_optimization.identify_worst_queries(24, 10);
--
-- Focus on recent issues (last 2 hours, top 5):  
-- SELECT * FROM dwiltse.query_optimization.identify_worst_queries(2, 5);
-- ============================================================================