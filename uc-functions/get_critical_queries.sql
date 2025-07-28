-- ============================================================================
-- UC Function: get_critical_queries (Your Working Function from Friday)
-- Purpose: JSON wrapper around critical_queries view for managed MCP integration
-- Author: Claude Code AI Assistant  
-- Version: 1.0 (Extracted from your working Databricks script)
-- ============================================================================

CREATE OR REPLACE FUNCTION dwiltse.query_optimization.get_critical_queries()
RETURNS STRING
COMMENT 'Returns critical query issues as JSON for managed MCP LLM analysis'
RETURN (
  SELECT 
    CASE 
      WHEN COUNT(*) = 0 THEN '{"message": "No critical queries found", "queries": []}'
      ELSE CONCAT(
        '{"message": "Found ', COUNT(*), ' critical queries", "queries": [',
        STRING_AGG(
          CONCAT(
            '{"query_rank": ', query_rank, 
            ', "query_id": "', query_id, '"',
            ', "badness_score": ', ROUND(badness_score, 1),
            ', "primary_issue": "', primary_issue, '"',
            ', "duration_seconds": ', duration_seconds,
            ', "spill_gb": ', spill_gb,
            ', "cache_hit_percent": ', cache_hit_percent,
            ', "data_read_gb": ', data_read_gb,
            ', "executed_by": "', executed_by, '"',
            ', "warehouse_id": "', warehouse_id, '"',
            ', "statement_preview": "', REPLACE(REPLACE(statement_preview, '"', '\\"'), '\n', '\\n'), '"',
            ', "end_time": "', end_time, '"}'
          ), 
          ', '
        ),
        ']}'
      )
    END
  FROM dwiltse.query_optimization.critical_queries
);

-- ============================================================================
-- Usage Examples:
-- 
-- Test the function:
-- SELECT dwiltse.query_optimization.get_critical_queries();
--
-- For managed MCP integration:
-- This function returns JSON that can be consumed by LLMs via managed MCP
-- ============================================================================