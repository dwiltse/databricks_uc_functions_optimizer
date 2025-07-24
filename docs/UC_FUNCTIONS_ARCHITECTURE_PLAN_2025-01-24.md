# UC Functions Architecture Plan for Query Optimization App
**Date**: January 24, 2025  
**Status**: Final Design - Ready for Implementation

## **Executive Summary**
Simplified architecture using UC Functions instead of complex Genie Spaces for a Databricks App that identifies the top 3 worst queries, analyzes them with LLM, and provides specific optimization scripts for export.

## **Why UC Functions Over Genie Spaces**
- **10x Faster**: 50-200ms vs 3-10 seconds per analysis
- **10x Cheaper**: $0.01-0.05 vs $0.10-0.50 per analysis  
- **Unlimited Scalability**: Pure compute vs LLM rate limits
- **99%+ Reliability**: Deterministic vs natural language variability
- **3 weeks development** vs 8 weeks for Genie approach

## **Core App Workflow**
```
1. Query Detection → 2. UC Function Analysis → 3. LLM Deep Analysis → 4. Script Generation → 5. Export Package
```

---

## **UC Functions Design (Based on config.yaml)**

### **1. `identify_worst_queries(hours_back, limit)`**
**Purpose**: Find top 3 worst performing queries using composite badness scoring

**Logic from config.yaml**:
- Slow query thresholds: 300s warning, 900s critical, 1800s emergency
- Expensive query thresholds: 10 DBU warning, 20 high, 50 critical
- Large scan thresholds: 1GB warning, 5GB critical, 20GB very large

**System Tables Used**:
- `system.query.history` - Query execution details
- `system.billing.usage` - Cost analysis

**Returns**: JSON array with top 3 queries ranked by badness score

### **2. `calculate_query_badness_score(query_metrics)`**
**Purpose**: Composite scoring algorithm for ranking worst queries

**Inputs**: duration_ms, dbu_cost, bytes_scanned, failure_rate, frequency

**Scoring Logic**:
- Duration weight: Uses 3-tier threshold system from config.yaml
- Cost impact: DBU cost × frequency × business_hours_multiplier  
- Data efficiency: bytes_scanned / rows_returned ratio
- Success rate penalty: Based on min_acceptable_success_rate (90%)

**Returns**: Structured badness score with category classifications

### **3. `analyze_query_patterns(statement_text, execution_plan)`** 
**Purpose**: Detect optimization anti-patterns using config.yaml pattern library

**Pattern Detection** (from optimization.pattern_savings_estimates):
- `select_all`: 30% savings opportunity
- `unbounded_sort`: 50% savings
- `cartesian_join`: 80% savings  
- `unpartitioned_filter`: 40% savings
- `redundant_distinct`: 20% savings
- `large_scan_optimization`: 60% savings

**Returns**: Array of detected patterns with savings estimates and implementation effort

### **4. `calculate_optimization_roi(current_metrics, pattern_analysis)`**
**Purpose**: Business impact calculation using config.yaml business context

**Uses config.yaml**:
- Monthly savings thresholds: $1000 high, $500 medium, $100 low priority
- Implementation effort classifications: low/medium/high hour estimates
- Cost reduction target: 20% from business_context
- Pattern-based savings estimates

**Returns**: ROI analysis with priority ranking and effort estimation

### **5. `get_query_context_data(query_id, workspace_id)`**
**Purpose**: Gather comprehensive query context for LLM analysis

**Data Collection**:
- Full execution plan from system tables
- Resource utilization breakdown
- Historical performance baseline (using performance_baselines from config.yaml)
- Table lineage and schema information
- Similar query patterns for comparison

**Returns**: Rich context object for LLM prompt construction

---

## **Databricks App Architecture**

### **Technology Stack**
- **Platform**: Databricks Apps (secure, internal)
- **Backend**: UC Functions (fast, deterministic analysis)
- **Intelligence**: Managed MCP (LLM-powered recommendations)
- **Frontend**: Streamlit (rapid development)
- **Authentication**: Workspace-based (no tokens needed)

### **Hybrid Architecture Flow**
```python
# Step 1: UC Functions - Fast Query Detection (100ms)
worst_queries = workspace.functions.call("identify_worst_queries", hours_back=24, limit=3)

# Step 2: UC Functions - Pattern Analysis (200ms per query)
for query in worst_queries:
    pattern_analysis = workspace.functions.call("analyze_query_patterns",
        statement_text=query.sql, execution_plan=query.plan)
    
    roi_analysis = workspace.functions.call("calculate_optimization_roi",
        current_metrics=query.metrics, pattern_analysis=pattern_analysis)

# Step 3: Managed MCP - Intelligent Analysis (5 seconds per query)
    context_data = workspace.functions.call("get_query_context_data", query_id=query.id)
    
    llm_analysis = mcp_client.call_tool("analyze_query_optimization", {
        "query_text": query.statement_text,
        "performance_data": context_data,
        "pattern_analysis": pattern_analysis,
        "business_thresholds": config_yaml_data
    })

# Step 4: Generate Optimization Scripts
    optimization_package = mcp_client.call_tool("generate_optimization_script", {
        "analysis": llm_analysis,
        "query_text": query.statement_text,
        "recommended_patterns": pattern_analysis
    })

# Step 5: Export Complete Package
    export_zip_package(optimization_package)
```

### **App Interface Design**

#### **Landing Page**:
```
🎯 Query Optimization Assistant

┌─── Today's Top 3 Worst Queries ────┐
│ 1. Customer Analytics               │
│    💰 $45 DBUs | ⏱️ 8.2min | ❌ 15% fail │
│    Badness Score: 17/20            │
│    [🔍 Analyze & Fix]              │
│                                    │ 
│ 2. Sales Dashboard                 │
│    💰 $32 DBUs | ⏱️ 6.1min | ⚠️ 5% fail  │
│    Badness Score: 14/20            │
│    [🔍 Analyze & Fix]              │
│                                    │
│ 3. Inventory Report                │  
│    💰 $28 DBUs | ⏱️ 4.8min | ✅ 0% fail  │
│    Badness Score: 11/20            │
│    [🔍 Analyze & Fix]              │
└────────────────────────────────────┘

📊 Quick Stats:
• Total potential savings: $3,240/month
• Implementation effort: 12 hours total
• Expected improvement: 60% faster, 65% cheaper
```

#### **Analysis Results Page**:
```
🛠️ Query Analysis Results for: Customer Analytics

📊 Current Performance Impact:
• Cost: $45 DBUs/day ($1,350/month)
• Execution Time: 8.2 minutes average  
• Success Rate: 85% (below 90% target)
• Data Scanned: 2.1B rows, 850GB
• Badness Score: 17/20 (CRITICAL)

🔍 Detected Anti-Patterns:
• ❌ Cartesian JOIN (80% savings opportunity)
• ❌ Full table scan (60% savings opportunity)  
• ❌ Redundant DISTINCT (20% savings opportunity)
• ⚠️ Unbounded ORDER BY (50% savings opportunity)

🤖 AI Root Cause Analysis:
"This query has 3 critical performance bottlenecks:

1. **Cartesian Join Issue**: Lines 23-25 create an unintentional 
   cartesian product between orders and customers tables due to 
   missing JOIN condition on customer_id.

2. **Missing Index**: The WHERE clause filters on order_date 
   but there's no index on (customer_id, order_date), forcing 
   a full table scan of 2.1B rows.

3. **Redundant Processing**: The query uses DISTINCT twice 
   unnecessarily - once in the subquery and again in the main 
   query, doubling processing time.

Main bottleneck: The cartesian join is creating 450B intermediate 
rows instead of the expected 2.1M rows."

💡 Specific Optimization Recommendations:
1. **Fix JOIN Condition** → 80% time reduction
   - Add missing ON customers.id = orders.customer_id
   - Implementation: 15 minutes

2. **Create Composite Index** → 60% scan reduction  
   - CREATE INDEX orders_customer_date_idx ON orders (customer_id, order_date)
   - Implementation: 2 hours

3. **Remove Redundant DISTINCT** → 20% processing savings
   - Remove DISTINCT from subquery, keep only in main query
   - Implementation: 5 minutes

4. **Add LIMIT to ORDER BY** → 50% sort savings
   - Add LIMIT 1000 to prevent unbounded sorting
   - Implementation: 2 minutes

📈 Expected Results After Optimization:
• New execution time: 1.2 minutes (85% faster)
• New cost: $8 DBUs/day (82% savings)  
• Annual savings: $12,045
• Implementation effort: 2.5 hours (Medium complexity)
• Expected success rate: 99%+

📄 Generated Optimization Package:
[⬇️ Download Complete Fix Package (6 files)]
```

---

## **Export Package Structure**
Each optimization generates a complete implementation package:

```
query_optimization_package_[query_id]_[timestamp].zip
├── analysis_report.md          # Human-readable analysis with business context
├── original_query.sql          # Current problematic query
├── optimized_query.sql         # LLM-generated optimized version  
├── performance_test.sql        # A/B testing script with metrics collection
├── ddl_changes.sql            # Index/partition DDL statements
├── rollback_script.sql        # Complete undo script if optimization fails
└── implementation_guide.md     # Step-by-step implementation instructions
```

---

## **Development Timeline**

### **Phase 1: UC Functions Implementation** (Week 1)
- Implement 5 UC Functions using config.yaml business logic
- Test functions against system.query.history
- Validate badness scoring algorithm
- Unit test pattern detection logic

### **Phase 2: Streamlit App Development** (Week 2)  
- Build simple 2-page Streamlit interface
- Integrate direct UC Function calls
- Create result display components
- Test end-to-end query identification flow

### **Phase 3: Managed MCP Integration** (Week 3)
- Integrate Managed MCP for intelligent analysis
- Build LLM prompt templates based on pattern analysis
- Implement optimization script generation
- Create export package functionality

**Total Development Time**: 3 weeks (vs 8 weeks for Genie approach)

---

## **Key Technical Advantages**

### **Performance Benefits**
- **Sub-second Response**: UC Functions return results in 50-200ms
- **Parallel Processing**: Can analyze multiple queries simultaneously  
- **Deterministic Results**: Same inputs always produce same outputs
- **Unlimited Scalability**: Limited only by Databricks cluster size

### **Cost Optimization** 
- **10x Cheaper Analysis**: $0.01-0.05 vs $0.10-0.50 per query
- **Targeted LLM Usage**: Only use expensive LLM for final recommendations
- **Batch Processing Capable**: Can analyze 1000s of queries affordably

### **Reliability & Maintenance**
- **99%+ Uptime**: No natural language parsing failures
- **Structured Outputs**: Consistent JSON responses
- **Easy Testing**: Deterministic functions are simple to unit test
- **Version Control**: Business logic centralized in config.yaml

---

## **Integration with Existing Work**

### **What to Keep from Current Project** ✅
- **config.yaml**: All business logic and thresholds (CRITICAL)
- **System table research**: Connection patterns and query structures
- **LLM prompt strategies**: From app_architecture.md
- **Performance scoring algorithms**: Proven business rules

### **What to Simplify/Skip** 🎯
- **Skip Genie Spaces entirely**: Overkill for deterministic analysis
- **Skip MCP orchestration complexity**: Direct function calls are cleaner
- **Skip materialized views**: Functions query system tables directly
- **Skip complex ETL pipelines**: Functions handle their own data access

### **Migration Strategy**
1. Create new clean project: `uc_function_optimization/`
2. Copy essential files: `config.yaml`, key reference docs
3. Reference original project for: System table patterns, UI components
4. Ignore complex components: Genie definitions, orchestration, ETL

---

## **Success Criteria**

### **Performance Targets**
- **Query Identification**: < 500ms for top 3 worst queries
- **Pattern Analysis**: < 200ms per query for anti-pattern detection  
- **Complete Analysis**: < 30 seconds for full 3-query analysis + recommendations
- **Cost per Analysis**: < $0.25 total (UC Functions + MCP)

### **Business Impact Goals**
- **Average Query Improvement**: 50%+ faster execution
- **Cost Reduction**: 40%+ DBU savings per optimized query
- **Implementation Success**: 90%+ of generated scripts work without modification
- **User Adoption**: Business users can operate app without training

### **Quality Metrics**
- **Pattern Detection Accuracy**: 95%+ correct anti-pattern identification
- **ROI Prediction Accuracy**: Within 20% of actual savings
- **Script Generation Quality**: 90%+ syntactically correct SQL
- **Recommendation Relevance**: 85%+ actionable recommendations

---

## **Next Steps for Implementation**

1. **Create New Project Structure**: Clean `uc_function_optimization/` folder
2. **Copy Essential Assets**: config.yaml and key reference documents
3. **Implement UC Functions**: Start with `identify_worst_queries` function
4. **Build Minimal UI**: 2-page Streamlit app for testing
5. **Integrate Managed MCP**: Add intelligent analysis layer
6. **Test & Iterate**: Validate against real query workloads
7. **Deploy & Monitor**: Production deployment with usage tracking

---

**This plan represents the optimal balance of simplicity, performance, and intelligence for query optimization at scale. The UC Functions handle fast data processing while Managed MCP provides sophisticated analysis - giving you the best of both worlds in a clean, maintainable architecture.**