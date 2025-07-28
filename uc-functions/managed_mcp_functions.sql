-- ============================================================================
-- Managed MCP UC Functions: Complete Query Optimization Toolkit
-- Purpose: JSON-returning UC functions for managed MCP integration
-- Author: Claude Code AI Assistant  
-- Version: 1.0 (Based on working get_critical_queries pattern)
-- ============================================================================

-- ============================================================================
-- Function 1: get_worst_queries - Flexible time window analysis
-- ============================================================================

CREATE OR REPLACE FUNCTION dwiltse.query_optimization.get_worst_queries(
  hours_back INT,
  query_limit INT
)
RETURNS STRING
COMMENT 'Returns worst performing queries as JSON for managed MCP LLM analysis'
RETURN (
  SELECT 
    CASE 
      WHEN COUNT(*) = 0 THEN CONCAT('{"message": "No queries found in last ', CAST(hours_back AS STRING), ' hours", "queries": []}')
      ELSE CONCAT(
        '{"message": "Found ', CAST(COUNT(*) AS STRING), ' worst queries in last ', CAST(hours_back AS STRING), ' hours", "total_analyzed": ', 
        CAST((SELECT COUNT(*) FROM dwiltse.query_optimization.query_performance_base 
         WHERE start_time >= DATEADD(HOUR, -hours_back, CURRENT_TIMESTAMP())) AS STRING), 
        ', "queries": [',
        COALESCE(
          ARRAY_JOIN(
            ARRAY_AGG(
              CONCAT(
                '{"query_rank": ', CAST(query_rank AS STRING), 
                ', "query_id": "', query_id, '"',
                ', "badness_score": ', CAST(ROUND(badness_score, 1) AS STRING),
                ', "primary_issue": "', primary_issue, '"',
                ', "duration_seconds": ', CAST(duration_seconds AS STRING),
                ', "spill_gb": ', CAST(spill_gb AS STRING),
                ', "cache_hit_percent": ', CAST(cache_hit_percent AS STRING),
                ', "data_read_gb": ', CAST(data_read_gb AS STRING),
                ', "executed_by": "', executed_by, '"',
                ', "warehouse_id": "', COALESCE(warehouse_id, 'unknown'), '"',
                ', "statement_preview": "', REPLACE(REPLACE(statement_preview, '"', '\\"'), '\n', '\\n'), '"',
                ', "end_time": "', CAST(end_time AS STRING), '"',
                ', "hours_ago": ', CAST(ROUND((UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) - UNIX_TIMESTAMP(end_time)) / 3600.0, 1) AS STRING), '}'
              )
            ), 
            ', '
          ), 
          ''
        ),
        ']}'
      )
    END
  FROM (
    SELECT 
      query_rank,
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
        LEFT(statement_text, 200) AS statement_preview,
        end_time
      FROM dwiltse.query_optimization.query_performance_base
      WHERE start_time >= DATEADD(HOUR, -hours_back, CURRENT_TIMESTAMP())
        AND badness_score > 10.0
    ) ranked_all
    WHERE query_rank <= query_limit
  ) ranked_queries
);

-- ============================================================================
-- Function 2: get_query_recommendations - Optimization suggestions
-- ============================================================================

CREATE OR REPLACE FUNCTION dwiltse.query_optimization.get_query_recommendations(
  query_id STRING
)
RETURNS STRING
COMMENT 'Returns optimization recommendations for a specific query as JSON'
RETURN (
  SELECT 
    CASE 
      WHEN COUNT(*) = 0 THEN CONCAT('{"message": "Query not found: ', query_id, '", "recommendations": []}')
      ELSE CONCAT(
        '{"message": "Analysis for query ', query_id, '", "query_details": {',
        '"badness_score": ', ROUND(badness_score, 1),
        ', "primary_issue": "', primary_issue, '"',
        ', "duration_seconds": ', duration_seconds,
        ', "spill_gb": ', spill_gb,
        ', "cache_hit_percent": ', cache_hit_percent,
        ', "data_read_gb": ', data_read_gb,
        ', "warehouse_id": "', COALESCE(warehouse_id, 'unknown'), '"',
        '}, "recommendations": [',
        CASE 
          WHEN primary_issue = 'MEMORY_SPILL_CRITICAL' THEN 
            '{"priority": "HIGH", "category": "MEMORY", "issue": "Memory spilling to disk", "recommendation": "Increase cluster memory or reduce data volume per partition", "impact": "High performance gain"}'
          WHEN primary_issue = 'EXECUTION_TOO_SLOW' THEN 
            '{"priority": "HIGH", "category": "PERFORMANCE", "issue": "Slow execution time", "recommendation": "Add WHERE clauses, optimize joins, or consider partitioning", "impact": "Significant time savings"}'
          WHEN primary_issue = 'POOR_CACHE_UTILIZATION' THEN 
            '{"priority": "MEDIUM", "category": "CACHING", "issue": "Low cache hit rate", "recommendation": "Enable auto-optimize, use CACHE TABLE, or query frequently accessed data", "impact": "Moderate performance improvement"}'
          WHEN primary_issue = 'DATA_INEFFICIENT' THEN 
            '{"priority": "MEDIUM", "category": "DATA_DESIGN", "issue": "Reading too much data per row", "recommendation": "Use columnar formats like Delta, select specific columns only", "impact": "Reduced I/O and faster queries"}'
          WHEN primary_issue = 'SHUFFLE_HEAVY' THEN 
            '{"priority": "MEDIUM", "category": "JOINS", "issue": "Excessive data shuffling", "recommendation": "Optimize join order, use broadcast joins for small tables, ensure proper partitioning", "impact": "Network and performance improvement"}'
          WHEN primary_issue = 'INFRASTRUCTURE_BOTTLENECK' THEN 
            '{"priority": "HIGH", "category": "INFRASTRUCTURE", "issue": "Long wait times for compute", "recommendation": "Scale up cluster, use serverless compute, or optimize scheduling", "impact": "Reduced wait times"}'
          ELSE 
            '{"priority": "LOW", "category": "GENERAL", "issue": "General performance optimization", "recommendation": "Review query patterns and consider standard optimizations", "impact": "Minor improvements"}'
        END,
        '], "next_steps": [',
        '"Analyze the query execution plan for bottlenecks",',
        '"Review table partitioning and indexing strategies",',
        '"Consider using Delta Lake optimizations like Z-ORDER",',
        '"Monitor query performance after implementing changes"',
        ']}'
      )
    END
  FROM dwiltse.query_optimization.query_performance_base
  WHERE query_id = query_id
);

-- ============================================================================
-- Function 3: get_performance_trends - Performance trending analysis
-- ============================================================================

CREATE OR REPLACE FUNCTION dwiltse.query_optimization.get_performance_trends(
  days_back INT
)
RETURNS STRING
COMMENT 'Returns performance trends over time as JSON for managed MCP analysis'
RETURN (
  WITH daily_stats AS (
    SELECT 
      DATE(start_time) AS query_date,
      COUNT(*) AS total_queries,
      AVG(badness_score) AS avg_badness_score,
      COUNT(CASE WHEN badness_score > 30 THEN 1 END) AS critical_queries,
      AVG(duration_seconds) AS avg_duration_seconds,
      SUM(spill_gb) AS total_spill_gb,
      AVG(cache_hit_percent) AS avg_cache_hit_percent,
      SUM(data_read_gb) AS total_data_read_gb
    FROM dwiltse.query_optimization.query_performance_base
    WHERE start_time >= CURRENT_DATE - INTERVAL days_back DAY
    GROUP BY DATE(start_time)
    ORDER BY query_date DESC
  )
  SELECT 
    CONCAT(
      '{"message": "Performance trends for last ', days_back, ' days", "summary": {',
      '"total_days_analyzed": ', COUNT(*),
      ', "total_queries": ', SUM(total_queries),
      ', "avg_daily_queries": ', ROUND(AVG(total_queries), 1),
      ', "avg_badness_score": ', ROUND(AVG(avg_badness_score), 1),
      ', "total_critical_queries": ', SUM(critical_queries),
      '}, "daily_trends": [',
      STRING_AGG(
        CONCAT(
          '{"date": "', query_date, '"',
          ', "total_queries": ', total_queries,
          ', "avg_badness_score": ', ROUND(avg_badness_score, 1),
          ', "critical_queries": ', critical_queries,
          ', "avg_duration_seconds": ', ROUND(avg_duration_seconds, 1),
          ', "total_spill_gb": ', ROUND(total_spill_gb, 2),
          ', "avg_cache_hit_percent": ', ROUND(avg_cache_hit_percent, 1),
          ', "total_data_read_gb": ', ROUND(total_data_read_gb, 2), '}'
        ), 
        ', '
      ),
      '], "insights": [',
      CASE 
        WHEN AVG(avg_badness_score) > 25 THEN '"Overall performance is concerning - high badness scores detected"'
        WHEN AVG(avg_badness_score) > 15 THEN '"Performance is moderate - some optimization opportunities exist"'
        ELSE '"Performance appears healthy - low badness scores"'
      END,
      ', ',
      CASE 
        WHEN SUM(critical_queries) > days_back * 5 THEN '"High number of critical queries - immediate attention needed"'
        WHEN SUM(critical_queries) > days_back THEN '"Moderate critical query volume - monitor trends"'
        ELSE '"Low critical query volume - performance is stable"'
      END,
      ']}'
    )
  FROM daily_stats
);

-- ============================================================================
-- Function 4: analyze_warehouse_performance - Warehouse-specific analysis
-- ============================================================================

CREATE OR REPLACE FUNCTION dwiltse.query_optimization.analyze_warehouse_performance(
  warehouse_id STRING
)
RETURNS STRING
COMMENT 'Returns warehouse-specific performance analysis as JSON'
RETURN (
  WITH warehouse_stats AS (
    SELECT 
      COUNT(*) AS total_queries,
      AVG(badness_score) AS avg_badness_score,
      COUNT(CASE WHEN badness_score > 30 THEN 1 END) AS critical_queries,
      AVG(duration_seconds) AS avg_duration_seconds,
      SUM(spill_gb) AS total_spill_gb,
      AVG(cache_hit_percent) AS avg_cache_hit_percent,
      SUM(data_read_gb) AS total_data_read_gb,
      COUNT(DISTINCT executed_by_user_id) AS unique_users,
      MAX(start_time) AS last_query_time,
      MIN(start_time) AS first_query_time
    FROM dwiltse.query_optimization.query_performance_base
    WHERE warehouse_id = warehouse_id
      AND start_time >= CURRENT_DATE - INTERVAL 7 DAY
  ),
  top_issues AS (
    SELECT 
      primary_issue,
      COUNT(*) AS issue_count,
      AVG(badness_score) AS avg_issue_score
    FROM dwiltse.query_optimization.query_performance_base
    WHERE warehouse_id = warehouse_id
      AND start_time >= CURRENT_DATE - INTERVAL 7 DAY
      AND badness_score > 10.0
    GROUP BY primary_issue
    ORDER BY issue_count DESC
    LIMIT 3
  )
  SELECT 
    CASE 
      WHEN ws.total_queries = 0 THEN 
        CONCAT('{"message": "No queries found for warehouse: ', warehouse_id, '", "analysis": {}}')
      ELSE CONCAT(
        '{"message": "Analysis for warehouse ', warehouse_id, '", "summary": {',
        '"total_queries": ', ws.total_queries,
        ', "avg_badness_score": ', ROUND(ws.avg_badness_score, 1),
        ', "critical_queries": ', ws.critical_queries,
        ', "avg_duration_seconds": ', ROUND(ws.avg_duration_seconds, 1),
        ', "total_spill_gb": ', ROUND(ws.total_spill_gb, 2),
        ', "avg_cache_hit_percent": ', ROUND(ws.avg_cache_hit_percent, 1),
        ', "total_data_read_gb": ', ROUND(ws.total_data_read_gb, 2),
        ', "unique_users": ', ws.unique_users,
        ', "last_query_time": "', ws.last_query_time, '"',
        '}, "top_issues": [',
        COALESCE(
          (SELECT STRING_AGG(
            CONCAT(
              '{"issue": "', primary_issue, '"',
              ', "count": ', issue_count,
              ', "avg_score": ', ROUND(avg_issue_score, 1), '}'
            ), ', '
          ) FROM top_issues),
          ''
        ),
        '], "recommendations": [',
        CASE 
          WHEN ws.avg_badness_score > 25 THEN 
            '"HIGH PRIORITY: This warehouse has severe performance issues requiring immediate attention"'
          WHEN ws.avg_badness_score > 15 THEN 
            '"MEDIUM PRIORITY: This warehouse has moderate performance issues - optimization recommended"'
          ELSE 
            '"LOW PRIORITY: This warehouse performance is acceptable - monitor for trends"'
        END,
        ', ',
        CASE 
          WHEN ws.total_spill_gb > 10 THEN 
            '"Consider increasing warehouse size due to high memory spilling"'
          WHEN ws.avg_cache_hit_percent < 50 THEN 
            '"Poor cache utilization - review data access patterns"'
          ELSE 
            '"Resource utilization appears normal"'
        END,
        ']}'
      )
    END
  FROM warehouse_stats ws
);

-- ============================================================================
-- Usage Examples:
-- 
-- Flexible time window analysis:
-- SELECT dwiltse.query_optimization.get_worst_queries(24, 10);
-- SELECT dwiltse.query_optimization.get_worst_queries(2, 5);
--
-- Query-specific recommendations:
-- SELECT dwiltse.query_optimization.get_query_recommendations('your-query-id-here');
--
-- Performance trends:
-- SELECT dwiltse.query_optimization.get_performance_trends(7);
-- SELECT dwiltse.query_optimization.get_performance_trends(30);
--
-- Warehouse analysis:
-- SELECT dwiltse.query_optimization.analyze_warehouse_performance('your-warehouse-id');
--
-- All functions return JSON for managed MCP LLM consumption
-- ============================================================================