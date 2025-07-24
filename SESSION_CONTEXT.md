# Session Context - UC Functions Optimizer Project

## Project Status: Asset Migration Complete ✅

### What Was Accomplished
- **All 9 planned assets successfully copied** from databricks-query-optimizer project
- **Project structure created** with organized folders
- **Ready for Phase 1 implementation** (UC Functions development)

### Assets Successfully Copied
1. ✅ `config.yaml` - Business logic foundation (thresholds, scoring parameters)
2. ✅ `top_3_worst_queries_analysis.sql` - Proven badness scoring algorithm  
3. ✅ `query_optimization_uc_functions.sql` - UC Functions templates
4. ✅ `system-tables.md` - System table field mappings and patterns
5. ✅ `mcp_manager.py` - Working MCP integration patterns
6. ✅ `simple_app.py` - Minimal Streamlit UI foundation
7. ✅ `UC_FUNCTIONS_ARCHITECTURE_PLAN_2025-01-24.md` - Implementation blueprint
8. ✅ `app-architecture.md` - LLM prompt patterns

### Next Steps (When Session Resumes)
**Phase 1: UC Functions Implementation** (Week 1)
- Implement 5 UC Functions using config.yaml business logic:
  1. `identify_worst_queries(hours_back, limit)`
  2. `calculate_query_badness_score(query_metrics)`  
  3. `analyze_query_patterns(statement_text, execution_plan)`
  4. `calculate_optimization_roi(current_metrics, pattern_analysis)`
  5. `get_query_context_data(query_id, workspace_id)`

### Key References for Implementation
- **Business Logic**: `config/config.yaml` contains all thresholds and parameters
- **SQL Patterns**: `sql/reference/top_3_worst_queries_analysis.sql` has working algorithm
- **Function Templates**: `uc-functions/templates/query_optimization_uc_functions.sql`
- **System Tables**: `docs/system-tables.md` for field mappings
- **Architecture Plan**: `docs/UC_FUNCTIONS_ARCHITECTURE_PLAN_2025-01-24.md`

### Implementation Approach
1. Convert SQL logic from `top_3_worst_queries_analysis.sql` into UC Functions
2. Use config.yaml thresholds in function parameters
3. Reference system-tables.md for correct field names
4. Follow architecture plan's function definitions exactly
5. Test against system.query.history table

### MCP Integration (Phase 3)
- Reference `src/reference/mcp_manager.py` for connection patterns
- Use Managed MCP servers (Unity Catalog Functions server)
- Follow `/home/dwiltse/claude-docs/databricks/reference-links.md` for MCP guidance

### Timeline
- **Total**: 3 weeks vs 8 weeks for Genie approach
- **Current Status**: Phase 1 ready to begin
- **Expected**: 10x faster, 10x cheaper than original approach

## Project Context
This project implements the UC Functions architecture plan to create a simplified, high-performance query optimization app that identifies top 3 worst queries, analyzes them with LLM, and generates optimization scripts - all while being 10x faster and cheaper than the original Genie Spaces approach.