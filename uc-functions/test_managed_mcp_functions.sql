-- ============================================================================
-- Test Suite: Managed MCP UC Functions
-- Purpose: Test all UC functions in Databricks playground
-- Author: Claude Code AI Assistant  
-- Version: 1.0 (Ready for Databricks testing)
-- ============================================================================

-- ============================================================================
-- Test 1: get_critical_queries (Your working function)
-- ============================================================================

SELECT 'Testing get_critical_queries...' AS test_name;
SELECT dwiltse.query_optimization.get_critical_queries() AS result;

-- ============================================================================
-- Test 2: get_worst_queries with different time windows
-- ============================================================================

SELECT 'Testing get_worst_queries - Last 2 hours, top 5...' AS test_name;
SELECT dwiltse.query_optimization.get_worst_queries(2, 5) AS result;

SELECT 'Testing get_worst_queries - Last 24 hours, top 10...' AS test_name;
SELECT dwiltse.query_optimization.get_worst_queries(24, 10) AS result;

SELECT 'Testing get_worst_queries - Last week, top 20...' AS test_name;
SELECT dwiltse.query_optimization.get_worst_queries(168, 20) AS result;

-- ============================================================================
-- Test 3: get_query_recommendations 
-- Note: Replace 'sample-query-id' with actual query ID from your results
-- ============================================================================

-- First, get a query ID to test with
SELECT 'Getting sample query ID for recommendations test...' AS test_name;
SELECT query_id FROM dwiltse.query_optimization.critical_queries LIMIT 1;

-- Test recommendations (update query_id below with actual ID from above)
SELECT 'Testing get_query_recommendations...' AS test_name;
-- SELECT dwiltse.query_optimization.get_query_recommendations('your-actual-query-id-here') AS result;

-- ============================================================================
-- Test 4: get_performance_trends 
-- ============================================================================

SELECT 'Testing get_performance_trends - Last 7 days...' AS test_name;
SELECT dwiltse.query_optimization.get_performance_trends(7) AS result;

SELECT 'Testing get_performance_trends - Last 30 days...' AS test_name;
SELECT dwiltse.query_optimization.get_performance_trends(30) AS result;

-- ============================================================================
-- Test 5: analyze_warehouse_performance
-- Note: Replace 'sample-warehouse-id' with actual warehouse ID from your results
-- ============================================================================

-- First, get a warehouse ID to test with
SELECT 'Getting sample warehouse ID for analysis test...' AS test_name;
SELECT DISTINCT warehouse_id FROM dwiltse.query_optimization.query_performance_base WHERE warehouse_id IS NOT NULL LIMIT 3;

-- Test warehouse analysis (update warehouse_id below with actual ID from above)
SELECT 'Testing analyze_warehouse_performance...' AS test_name;
-- SELECT dwiltse.query_optimization.analyze_warehouse_performance('your-actual-warehouse-id-here') AS result;

-- ============================================================================
-- Test 6: Verify all views are working
-- ============================================================================

SELECT 'Testing query_performance_base view...' AS test_name;
SELECT COUNT(*) AS total_queries FROM dwiltse.query_optimization.query_performance_base;

SELECT 'Testing critical_queries view...' AS test_name;
SELECT COUNT(*) AS critical_count FROM dwiltse.query_optimization.critical_queries;

SELECT 'Testing worst_queries_24h view...' AS test_name;
SELECT COUNT(*) AS worst_24h_count FROM dwiltse.query_optimization.worst_queries_24h;

-- ============================================================================
-- Test 7: Error handling tests
-- ============================================================================

SELECT 'Testing get_worst_queries with no results (0 hours)...' AS test_name;
SELECT dwiltse.query_optimization.get_worst_queries(0, 10) AS result;

SELECT 'Testing get_query_recommendations with invalid ID...' AS test_name;
SELECT dwiltse.query_optimization.get_query_recommendations('non-existent-query-id') AS result;

SELECT 'Testing analyze_warehouse_performance with invalid warehouse...' AS test_name;
SELECT dwiltse.query_optimization.analyze_warehouse_performance('non-existent-warehouse') AS result;

-- ============================================================================
-- Test 8: JSON validation (optional - to verify JSON structure)
-- ============================================================================

SELECT 'Validating JSON structure of get_critical_queries...' AS test_name;
SELECT 
  CASE 
    WHEN dwiltse.query_optimization.get_critical_queries() LIKE '%"message":%' 
      AND dwiltse.query_optimization.get_critical_queries() LIKE '%"queries":%'
    THEN 'JSON structure valid'
    ELSE 'JSON structure invalid'
  END AS json_validation;

-- ============================================================================
-- Instructions for Manual Testing:
--
-- 1. Run the entire script in Databricks SQL playground
-- 2. Look for any error messages in the results
-- 3. Verify that JSON outputs are properly formatted
-- 4. Update the commented sections with actual IDs from your data:
--    - Replace 'your-actual-query-id-here' with real query ID
--    - Replace 'your-actual-warehouse-id-here' with real warehouse ID
-- 5. Re-run those specific tests after updating the IDs
--
-- Expected Results:
-- - All functions should return valid JSON
-- - No syntax errors or null results
-- - JSON should contain expected fields (message, queries, etc.)
-- - Error handling should return appropriate messages for invalid inputs
-- ============================================================================