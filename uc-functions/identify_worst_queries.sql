-- ============================================================================
-- UC Function: identify_worst_queries
-- Purpose: Identify worst performing queries using improved algorithm with real cost data
-- Author: Claude Code AI Assistant  
-- Version: 2.0 (Enhanced with proper cost calculation and Databricks-specific metrics)
-- ============================================================================

CREATE OR REPLACE FUNCTION dwiltse.query_optimization.identify_worst_queries(
  hours_back INT,
  query_limit INT
)
RETURNS TABLE(
  query_rank STRING,
  query_id STRING,
  workspace_id BIGINT,
  statement_preview STRING,
  executed_by STRING,
  duration_seconds DOUBLE,
  execution_seconds DOUBLE,
  queue_seconds DOUBLE,
  data_read_gb DOUBLE,
  rows_processed BIGINT,
  actual_cost_usd DOUBLE,
  dbu_consumed DOUBLE,
  cost_per_minute DOUBLE,
  cost_per_gb DOUBLE,
  performance_impact_score DOUBLE,
  execution_score DOUBLE,
  resource_score DOUBLE,
  data_access_score DOUBLE,
  pattern_score DOUBLE,
  priority_score DOUBLE,
  optimization_category STRING,
  optimization_recommendation STRING,
  estimated_savings_percentage INT,
  implementation_effort STRING,
  warehouse_id STRING,
  end_time TIMESTAMP
)
RETURN

WITH query_costs AS (
  SELECT 
    qh.query_id,
    qh.workspace_id,
    qh.query_text,
    qh.executed_by_user_id,
    qh.created_time,
    qh.end_time,
    qh.total_duration_ms,
    qh.execution_duration_ms,
    qh.waiting_for_compute_duration_ms,
    qh.compilation_duration_ms,
    qh.read_rows,
    qh.read_bytes,
    qh.read_io_cache_percent,
    qh.spilled_local_bytes,
    qh.shuffle_read_bytes,
    qh.compute.warehouse_id,
    qh.client_application,
    
    -- Join with actual billing usage for real cost calculation
    bu.usage_quantity as dbu_consumed,
    bu.usage_unit,
    bu.sku_name,
    bu.billing_origin_product,
    
    -- Get actual list prices  
    lp.pricing.default as price_per_dbu,
    
    -- Calculate actual cost (not estimated!)
    COALESCE(bu.usage_quantity * lp.pricing.default, 0) as actual_cost_usd,
    
    -- Cost efficiency metrics
    CASE 
      WHEN qh.execution_duration_ms > 0 
      THEN COALESCE(bu.usage_quantity * lp.pricing.default, 0) / (qh.execution_duration_ms / 1000.0 / 60.0)
      ELSE 0 
    END as cost_per_minute,
    
    CASE 
      WHEN qh.read_bytes > 0
      THEN COALESCE(bu.usage_quantity * lp.pricing.default, 0) / (qh.read_bytes / 1024.0 / 1024.0 / 1024.0)
      ELSE 0
    END as cost_per_gb_processed

  FROM system.query.history qh
  
  -- Critical: Join with actual billing data for real costs
  LEFT JOIN system.billing.usage bu 
    ON qh.workspace_id = bu.workspace_id
    AND qh.compute.warehouse_id = bu.usage_metadata.warehouse_id
    AND qh.created_time >= bu.usage_start_time 
    AND qh.end_time <= bu.usage_end_time
    AND bu.usage_unit = 'DBU'
  
  LEFT JOIN system.billing.list_prices lp 
    ON bu.sku_name = lp.sku_name
    AND lp.price_start_time <= qh.created_time
    AND (lp.price_end_time IS NULL OR lp.price_end_time > qh.created_time)
    
  WHERE qh.created_time >= CURRENT_TIMESTAMP - INTERVAL hours_back HOUR
    AND qh.execution_status = 'FINISHED'
    AND qh.execution_duration_ms > 0
),

performance_metrics AS (
  SELECT *,
    
    -- Execution Efficiency Score (0-25 points)
    LEAST(25, 
      -- Queue time penalty (high queue time = infrastructure problem)
      CASE WHEN total_duration_ms > 0 
           THEN (waiting_for_compute_duration_ms / CAST(total_duration_ms AS DOUBLE)) * 10
           ELSE 0 END +
      
      -- Compilation overhead penalty (high compilation = metadata issues)
      CASE WHEN execution_duration_ms > 0 
           THEN (compilation_duration_ms / CAST(execution_duration_ms AS DOUBLE)) * 5
           ELSE 0 END +
      
      -- Duration percentile within time window (relative performance)
      PERCENT_RANK() OVER (ORDER BY execution_duration_ms) * 10
    ) AS execution_score,
    
    -- Resource Efficiency Score (0-30 points) - Databricks-specific
    LEAST(30,
      -- Disk spill penalty (major performance killer in Databricks)
      CASE WHEN spilled_local_bytes > 0 
           THEN LEAST(15, LOG(10, GREATEST(1, spilled_local_bytes / 1024.0 / 1024.0))) -- MB scale logarithmic
           ELSE 0 END +
      
      -- Shuffle inefficiency (network bottleneck indicator)
      CASE WHEN read_bytes > 0 AND shuffle_read_bytes > read_bytes * 0.1 
           THEN LEAST(10, (shuffle_read_bytes / CAST(read_bytes AS DOUBLE)) * 10)
           ELSE 0 END +
      
      -- Cache miss penalty (Delta Lake cache efficiency)
      (1 - COALESCE(read_io_cache_percent, 0)) * 5
    ) AS resource_score,
    
    -- Data Access Score (0-25 points) - Data lake efficiency
    LEAST(25,
      -- Large scan without proper filtering
      CASE WHEN read_bytes > 1073741824 AND read_rows > 0 -- 1GB+
           AND (read_bytes / CAST(read_rows AS DOUBLE)) > 10000 -- >10KB per row average
           THEN 15 ELSE 0 END +
      
      -- Poor selectivity (reading too many rows)
      CASE WHEN read_rows > 10000000 -- 10M+ rows
           THEN LEAST(10, LOG(10, read_rows / 1000000.0)) -- Logarithmic penalty
           ELSE 0 END
    ) AS data_access_score,
    
    -- Query Pattern Score (0-20 points) - Anti-pattern detection
    (CASE WHEN UPPER(query_text) LIKE '%SELECT\s*\*%' THEN 5 ELSE 0 END +
     CASE WHEN UPPER(query_text) LIKE '%ORDER\s+BY%' AND UPPER(query_text) NOT LIKE '%LIMIT%' THEN 8 ELSE 0 END +
     CASE WHEN UPPER(query_text) LIKE '%DISTINCT%' AND UPPER(query_text) LIKE '%GROUP\s+BY%' THEN 4 ELSE 0 END +
     CASE WHEN UPPER(query_text) LIKE '%UNION\s+%' AND UPPER(query_text) NOT LIKE '%UNION\s+ALL%' THEN 3 ELSE 0 END
    ) AS pattern_score

  FROM query_costs
),

query_analysis AS (
  SELECT *,
    -- Total performance impact score
    execution_score + resource_score + data_access_score + pattern_score AS performance_impact_score,
    
    -- Cost-weighted priority score (key improvement!)
    (execution_score + resource_score + data_access_score + pattern_score) *
    (1 + LOG(10, GREATEST(1, actual_cost_usd + 0.01))) AS priority_score, -- Log scale cost weighting
    
    -- Optimization categorization (more nuanced)
    CASE 
      WHEN spilled_local_bytes > 1073741824 THEN 'MEMORY_SPILL_CRITICAL' -- 1GB+ spill
      WHEN execution_duration_ms > 1800000 THEN 'SLOW_EXECUTION' -- 30+ minutes
      WHEN actual_cost_usd > 10 THEN 'HIGH_COST_QUERY' -- $10+ per query
      WHEN shuffle_read_bytes > read_bytes * 0.5 THEN 'SHUFFLE_HEAVY' -- 50%+ shuffle
      WHEN read_bytes > 0 AND (read_bytes / CAST(read_rows AS DOUBLE)) > 100000 THEN 'DATA_INEFFICIENT' -- 100KB+ per row
      WHEN UPPER(query_text) LIKE '%SELECT\s*\*%' THEN 'SELECT_ALL_ANTIPATTERN'
      WHEN UPPER(query_text) LIKE '%ORDER\s+BY%' AND UPPER(query_text) NOT LIKE '%LIMIT%' THEN 'UNBOUNDED_SORT'
      WHEN COALESCE(read_io_cache_percent, 0) < 0.5 THEN 'POOR_CACHE_UTILIZATION' -- <50% cache hit
      ELSE 'GENERAL_PERFORMANCE'
    END AS optimization_category,
    
    -- Enhanced recommendations with Databricks-specific guidance
    CASE 
      WHEN spilled_local_bytes > 1073741824 THEN 
        'CRITICAL: Memory spill detected. Increase warehouse size or optimize query to reduce memory usage'
      WHEN actual_cost_usd > 10 AND execution_duration_ms > 900000 THEN 
        'HIGH PRIORITY: Expensive slow query. Review execution plan and consider query rewrite'
      WHEN shuffle_read_bytes > read_bytes * 0.5 THEN 
        'Optimize joins and reduce data movement. Consider broadcast joins or partitioning'
      WHEN execution_duration_ms > 1800000 AND UPPER(query_text) LIKE '%SELECT\s*\*%' THEN 
        'Replace SELECT * with specific columns and review execution plan for optimization opportunities'
      WHEN execution_duration_ms > 900000 THEN 
        'Review query execution plan, consider indexing, and check for unnecessary data processing'
      WHEN read_bytes > 0 AND (read_bytes / CAST(read_rows AS DOUBLE)) > 100000 THEN 
        'Optimize data access - query reading too many bytes per row. Check column selection and filtering'
      WHEN UPPER(query_text) LIKE '%SELECT\s*\*%' THEN 
        'Replace SELECT * with specific column names to reduce data transfer and improve performance'
      WHEN UPPER(query_text) LIKE '%ORDER\s+BY%' AND UPPER(query_text) NOT LIKE '%LIMIT%' THEN 
        'Add LIMIT clause to ORDER BY queries to prevent unnecessary full dataset sorting'
      WHEN COALESCE(read_io_cache_percent, 0) < 0.5 THEN 
        'Poor cache utilization. Consider query patterns, data partitioning, or warehouse warm-up strategies'
      ELSE 
        'Consider query optimization techniques like predicate pushdown, column pruning, and join optimization'
    END AS optimization_recommendation,
    
    -- Savings estimation based on actual costs and patterns
    CASE 
      WHEN spilled_local_bytes > 1073741824 THEN 70 -- Memory spill fixes have huge impact
      WHEN shuffle_read_bytes > read_bytes * 0.5 THEN 60 -- Shuffle reduction
      WHEN UPPER(query_text) LIKE '%ORDER\s+BY%' AND UPPER(query_text) NOT LIKE '%LIMIT%' THEN 55
      WHEN UPPER(query_text) LIKE '%SELECT\s*\*%' THEN 40
      WHEN read_bytes > 0 AND (read_bytes / CAST(read_rows AS DOUBLE)) > 100000 THEN 45
      WHEN execution_duration_ms > 1800000 THEN 35 -- Very slow queries
      WHEN execution_duration_ms > 900000 THEN 25 -- Slow queries
      WHEN COALESCE(read_io_cache_percent, 0) < 0.5 THEN 30 -- Cache optimization
      ELSE 15
    END AS estimated_savings_percentage,
    
    -- Implementation effort based on config.yaml business rules
    CASE 
      WHEN optimization_category IN ('SELECT_ALL_ANTIPATTERN', 'UNBOUNDED_SORT') THEN 'Low'
      WHEN optimization_category IN ('DATA_INEFFICIENT', 'POOR_CACHE_UTILIZATION') THEN 'Medium'  
      WHEN optimization_category IN ('MEMORY_SPILL_CRITICAL', 'SHUFFLE_HEAVY') THEN 'High'
      ELSE 'Medium'
    END AS implementation_effort

  FROM performance_metrics
)

SELECT 
  'Query Rank: #' || ROW_NUMBER() OVER (ORDER BY priority_score DESC) AS query_rank,
  query_id,
  workspace_id,
  LEFT(query_text, 200) || '...' AS statement_preview,
  CAST(executed_by_user_id AS STRING) AS executed_by,
  
  -- Performance metrics
  ROUND(total_duration_ms / 1000.0, 2) AS duration_seconds,
  ROUND(execution_duration_ms / 1000.0, 2) AS execution_seconds,
  ROUND(waiting_for_compute_duration_ms / 1000.0, 2) AS queue_seconds,
  ROUND(read_bytes / 1024.0 / 1024.0 / 1024.0, 3) AS data_read_gb,
  read_rows AS rows_processed,
  
  -- Real cost metrics (major improvement!)
  ROUND(actual_cost_usd, 4) AS actual_cost_usd,
  ROUND(COALESCE(dbu_consumed, 0), 3) AS dbu_consumed,
  ROUND(cost_per_minute, 4) AS cost_per_minute,
  ROUND(cost_per_gb_processed, 4) AS cost_per_gb,
  
  -- Performance scores
  ROUND(performance_impact_score, 1) AS performance_impact_score,
  ROUND(execution_score, 1) AS execution_score,
  ROUND(resource_score, 1) AS resource_score,
  ROUND(data_access_score, 1) AS data_access_score,
  ROUND(pattern_score, 1) AS pattern_score,
  ROUND(priority_score, 1) AS priority_score,
  
  -- Optimization guidance
  optimization_category,
  optimization_recommendation,
  estimated_savings_percentage,
  implementation_effort,
  
  -- Context
  warehouse_id,
  end_time

FROM query_analysis
WHERE performance_impact_score > 10 -- Only meaningful impact queries
ORDER BY priority_score DESC
LIMIT query_limit;

-- ============================================================================
-- Usage Examples:
-- 
-- Basic usage (last 24 hours, top 10):
-- SELECT * FROM mcp.query_optimization.identify_worst_queries(24, 10);
--
-- Extended analysis (last week, top 20):  
-- SELECT * FROM mcp.query_optimization.identify_worst_queries(168, 20);
--
-- Focus on high-cost queries:
-- SELECT * FROM mcp.query_optimization.identify_worst_queries(24, 50) 
-- WHERE actual_cost_usd > 5.0;
-- ============================================================================