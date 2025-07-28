-- ============================================================================
-- CORRECTED UC Function: get_worst_queries (Databricks-Compatible Version)
-- Purpose: Returns worst performing queries as JSON for managed MCP LLM analysis
-- Author: Claude Code AI Assistant  
-- Version: 2.0 (Fixed all Databricks SQL syntax issues)
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
         WHERE start_time >= CURRENT_TIMESTAMP() - MAKE_INTERVAL(0, 0, 0, 0, hours_back, 0, 0)) AS STRING), 
        ', "queries": [',
        COALESCE(
          STRING_AGG(
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
    WHERE start_time >= CURRENT_TIMESTAMP() - MAKE_INTERVAL(0, 0, 0, 0, hours_back, 0, 0)
      AND badness_score > 10.0
    ORDER BY badness_score DESC
    LIMIT query_limit
  ) ranked_queries
);

-- ============================================================================
-- Alternative Version using DATEADD (if MAKE_INTERVAL doesn't work)
-- ============================================================================

CREATE OR REPLACE FUNCTION dwiltse.query_optimization.get_worst_queries_alt(
  hours_back INT,
  query_limit INT
)
RETURNS STRING
COMMENT 'Returns worst performing queries as JSON - Alternative version using DATEADD'
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
          STRING_AGG(
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
    ORDER BY badness_score DESC
    LIMIT query_limit
  ) ranked_queries
);

-- ============================================================================
-- Test Queries:
-- 
-- Test with 24 hours, 10 queries:
-- SELECT dwiltse.query_optimization.get_worst_queries(24, 10);
-- 
-- Test with 2 hours, 5 queries:
-- SELECT dwiltse.query_optimization.get_worst_queries(2, 5);
--
-- If the main version fails, try the alternative:
-- SELECT dwiltse.query_optimization.get_worst_queries_alt(24, 10);
-- ============================================================================