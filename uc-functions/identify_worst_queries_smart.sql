-- ============================================================================
-- UC Function: identify_worst_queries_smart (Intelligent Version)
-- Purpose: Smart bad query detection using Databricks performance principles
-- Author: Claude Code AI Assistant  
-- Version: 1.0 (Smart algorithm based on real performance impact)
-- ============================================================================

CREATE OR REPLACE FUNCTION dwiltse.query_optimization.identify_worst_queries_smart(
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

WITH performance_thresholds AS (
  SELECT 
    -- Scoring weights (centralized for easy tuning)
    50.0 AS memory_spill_base_penalty,
    30.0 AS execution_time_base_penalty, 
    20.0 AS cache_miss_max_penalty,
    15.0 AS data_inefficiency_penalty,
    10.0 AS shuffle_penalty,
    10.0 AS queue_time_penalty,
    
    -- Performance thresholds
    300000 AS slow_execution_threshold_ms,    -- 5 minutes
    1073741824 AS critical_spill_threshold,   -- 1GB
    1800000 AS very_slow_threshold_ms,        -- 30 minutes
    0.5 AS poor_cache_threshold,              -- 50% cache hit
    0.3 AS very_poor_cache_threshold,         -- 30% cache hit
    10000 AS inefficient_bytes_per_row,       -- 10KB per row
    50000 AS very_inefficient_bytes_per_row,  -- 50KB per row
    0.3 AS high_shuffle_ratio,                -- 30% shuffle ratio
    0.5 AS very_high_shuffle_ratio,           -- 50% shuffle ratio
    30000 AS high_queue_time_ms,              -- 30 seconds
    60000 AS very_high_queue_time_ms,         -- 1 minute
    5000 AS min_execution_time_ms,            -- 5 seconds
    10.0 AS min_badness_threshold
),

query_metrics AS (
  SELECT 
    *,
    -- Pre-calculated unit conversions
    ROUND(execution_duration_ms / 1000.0, 2) AS duration_seconds,
    ROUND(COALESCE(spilled_local_bytes, 0) / 1073741824.0, 3) AS spill_gb,
    ROUND(COALESCE(read_io_cache_percent, 0) * 100, 1) AS cache_hit_percent,
    ROUND(read_bytes / 1073741824.0, 3) AS data_read_gb,
    
    -- Pre-calculated performance indicators
    COALESCE(spilled_local_bytes, 0) / 1048576.0 AS spill_mb,
    execution_duration_ms / 60000.0 AS duration_minutes,
    COALESCE(read_io_cache_percent, 1.0) AS cache_hit_ratio,
    CASE 
      WHEN read_bytes > 0 AND read_rows > 0 
      THEN read_bytes / CAST(read_rows AS DOUBLE)
      ELSE 0 
    END AS bytes_per_row,
    CASE 
      WHEN COALESCE(shuffle_read_bytes, 0) > 0 AND read_bytes > 0
      THEN shuffle_read_bytes / CAST(read_bytes AS DOUBLE)
      ELSE 0 
    END AS shuffle_ratio,
    COALESCE(waiting_for_compute_duration_ms, 0) AS queue_time_ms
    
  FROM system.query.history 
  WHERE start_time >= CURRENT_TIMESTAMP - INTERVAL hours_back HOUR
    AND execution_status = 'FINISHED'
    AND execution_duration_ms > (SELECT min_execution_time_ms FROM performance_thresholds)
    AND statement_text IS NOT NULL
),

scored_queries AS (
  SELECT 
    qm.*,
    pt.*,
    
    -- Modular badness score calculation
    (
      -- Memory spill scoring
      CASE 
        WHEN spill_mb > 0 
        THEN pt.memory_spill_base_penalty + LOG(2, GREATEST(1, spill_mb))
        ELSE 0 
      END +
      
      -- Execution time scoring  
      CASE 
        WHEN qm.execution_duration_ms > pt.slow_execution_threshold_ms
        THEN pt.execution_time_base_penalty + LOG(2, GREATEST(1, duration_minutes))
        ELSE qm.execution_duration_ms / 10000.0 
      END +
      
      -- Cache utilization scoring
      CASE 
        WHEN qm.cache_hit_ratio < pt.poor_cache_threshold
        THEN pt.cache_miss_max_penalty * (1 - qm.cache_hit_ratio)
        ELSE 0 
      END +
      
      -- Data efficiency scoring
      CASE 
        WHEN qm.bytes_per_row > pt.inefficient_bytes_per_row
        THEN pt.data_inefficiency_penalty
        ELSE 0 
      END +
      
      -- Network shuffle scoring
      CASE 
        WHEN qm.shuffle_ratio > pt.high_shuffle_ratio
        THEN pt.shuffle_penalty
        ELSE 0 
      END +
      
      -- Infrastructure bottleneck scoring
      CASE 
        WHEN qm.queue_time_ms > pt.high_queue_time_ms
        THEN pt.queue_time_penalty
        ELSE 0 
      END
      
    ) AS badness_score,
    
    -- Structured primary issue identification
    CASE 
      WHEN COALESCE(spilled_local_bytes, 0) > pt.critical_spill_threshold 
        THEN 'MEMORY_SPILL_CRITICAL'
      WHEN qm.execution_duration_ms > pt.very_slow_threshold_ms 
        THEN 'EXECUTION_TOO_SLOW'
      WHEN qm.cache_hit_ratio < pt.very_poor_cache_threshold 
        THEN 'POOR_CACHE_UTILIZATION'
      WHEN qm.bytes_per_row > pt.very_inefficient_bytes_per_row 
        THEN 'DATA_INEFFICIENT'
      WHEN qm.shuffle_ratio > pt.very_high_shuffle_ratio 
        THEN 'SHUFFLE_HEAVY'
      WHEN qm.queue_time_ms > pt.very_high_queue_time_ms 
        THEN 'INFRASTRUCTURE_BOTTLENECK'
      ELSE 'GENERAL_PERFORMANCE'
    END AS primary_issue
    
  FROM query_metrics qm
  CROSS JOIN performance_thresholds pt
)

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
  compute.warehouse_id,
  LEFT(statement_text, 200) AS statement_preview,
  end_time

FROM scored_queries
WHERE badness_score > (SELECT min_badness_threshold FROM performance_thresholds)
ORDER BY badness_score DESC
LIMIT query_limit;

-- ============================================================================
-- Usage Examples:
-- 
-- Basic usage (last 24 hours, top 10 worst):
-- SELECT * FROM dwiltse.query_optimization.identify_worst_queries_smart(24, 10);
--
-- Focus on recent issues (last 2 hours, top 5):  
-- SELECT * FROM dwiltse.query_optimization.identify_worst_queries_smart(2, 5);
--
-- Extended analysis (last week, top 20):
-- SELECT * FROM dwiltse.query_optimization.identify_worst_queries_smart(168, 20);
-- ============================================================================