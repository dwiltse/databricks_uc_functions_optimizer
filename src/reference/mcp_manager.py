"""
MCP Connection Manager - The heart of our query optimization app!
Uses the proven pattern from your playground success
"""

from databricks_mcp import DatabricksMCPClient
from databricks.sdk import WorkspaceClient
import json
import streamlit as st
from datetime import datetime, timedelta

class MCPConnectionManager:
    """Manages connection to Genie space via MCP - your proven working pattern!"""
    
    def __init__(self, genie_space_id="system_table_mcp_test"):
        self.genie_space_id = genie_space_id
        self.workspace_client = None
        self.mcp_client = None
        self._initialize_connection()
    
    def _initialize_connection(self):
        """Initialize MCP client with your playground-proven pattern"""
        try:
            # Same pattern that worked in playground!
            self.workspace_client = WorkspaceClient()
            mcp_url = f"{self.workspace_client.config.host}/api/2.0/mcp/genie/{self.genie_space_id}"
            
            self.mcp_client = DatabricksMCPClient(
                server_url=mcp_url,
                workspace_client=self.workspace_client
            )
            
            print(f"🎯 Connected to Genie space: {self.genie_space_id}")
            print(f"🔗 MCP URL: {mcp_url}")
            
        except Exception as e:
            print(f"❌ MCP connection failed: {e}")
            self.mcp_client = None
    
    def test_connection(self):
        """Test if MCP connection is working with specific error diagnosis"""
        if not self.mcp_client:
            return {
                "status": "error", 
                "error_type": "initialization_failed",
                "message": "MCP client not initialized - check workspace authentication",
                "troubleshooting": [
                    "Verify you're running in a Databricks environment",
                    "Check workspace authentication is working",
                    "Ensure databricks-mcp package is installed"
                ]
            }
        
        try:
            # Try to list available tools
            tools = self.mcp_client.list_tools()
            return {
                "status": "success", 
                "message": f"Connected! Found {len(tools)} tools",
                "tools": [{"name": tool.name, "description": tool.description} for tool in tools]
            }
        except Exception as e:
            error_msg = str(e).lower()
            
            # Specific error diagnosis
            if "404" in error_msg or "not found" in error_msg:
                return {
                    "status": "error",
                    "error_type": "genie_space_not_found", 
                    "message": f"Genie space '{self.genie_space_id}' not found",
                    "troubleshooting": [
                        f"Create Genie space with ID: {self.genie_space_id}",
                        "Verify the space name is correct",
                        "Check you have access to the space"
                    ]
                }
            elif "403" in error_msg or "forbidden" in error_msg:
                return {
                    "status": "error",
                    "error_type": "access_denied",
                    "message": "Access denied to Genie space",
                    "troubleshooting": [
                        "Request access to the Genie space",
                        "Check your workspace permissions",
                        "Verify you're authenticated correctly"
                    ]
                }
            elif "401" in error_msg or "unauthorized" in error_msg:
                return {
                    "status": "error", 
                    "error_type": "authentication_failed",
                    "message": "Authentication failed",
                    "troubleshooting": [
                        "Check workspace authentication",
                        "Verify your token/credentials",
                        "Try re-authenticating to Databricks"
                    ]
                }
            elif "beta" in error_msg or "not enabled" in error_msg:
                return {
                    "status": "error",
                    "error_type": "mcp_not_enabled", 
                    "message": "MCP Beta features not enabled",
                    "troubleshooting": [
                        "Contact your Databricks admin",
                        "Request MCP Beta feature enablement", 
                        "Ensure serverless compute is enabled"
                    ]
                }
            else:
                return {
                    "status": "error",
                    "error_type": "unknown",
                    "message": f"Connection test failed: {str(e)}",
                    "troubleshooting": [
                        "Check Databricks workspace connectivity",
                        "Verify MCP service is running",
                        "Review error details for specific issues"
                    ]
                }
    
    def query_genie_space(self, question):
        """Query the Genie space - same as playground!"""
        if not self.mcp_client:
            return {"error": "MCP client not connected"}
        
        try:
            print(f"🤖 Querying Genie: {question}")
            response = self.mcp_client.call_tool("query", {"question": question})
            
            # Extract text content from response
            result = "".join([c.text for c in response.content])
            
            return {"success": True, "data": result}
            
        except Exception as e:
            return {"error": f"Query failed: {str(e)}"}
    
    def get_worst_queries(self, hours_back=24, min_duration_seconds=30, limit=10):
        """Find the worst performing queries - the money maker!"""
        
        query_text = f"""
        Find the {limit} worst performing queries in the last {hours_back} hours.
        
        Criteria:
        - Execution time longer than {min_duration_seconds} seconds
        - Include query_id, statement_text, execution_duration_ms, user_name, warehouse_id
        - Order by execution time (slowest first)
        - Include the actual SQL text so we can optimize it
        
        Format the results clearly with each query's performance metrics.
        """
        
        return self.query_genie_space(query_text)
    
    def get_expensive_queries(self, hours_back=24, limit=10):
        """Find the most expensive queries by DBU cost"""
        
        query_text = f"""
        Find the {limit} most expensive queries by DBU consumption in the last {hours_back} hours.
        
        Include:
        - Query ID and SQL statement text
        - DBU cost and execution time
        - User and warehouse information
        - Data volume processed (rows_read, bytes_read)
        
        Order by cost (most expensive first).
        Show the actual SQL so we can analyze and optimize it.
        """
        
        return self.query_genie_space(query_text)
    
    def get_query_details(self, query_id):
        """Get detailed analysis for a specific query"""
        
        query_text = f"""
        For query_id '{query_id}', provide detailed analysis:
        
        Performance metrics:
        - Execution time, queue time, compilation time
        - Resource usage (CPU, memory, I/O)
        - Data scan statistics
        
        Query structure:
        - Tables accessed and JOIN patterns
        - WHERE clause complexity
        - Aggregation and sorting operations
        
        Optimization opportunities:
        - Missing indexes that could help
        - Inefficient JOIN orders
        - Partition pruning opportunities
        - Caching potential
        
        Provide specific, actionable recommendations.
        """
        
        return self.query_genie_space(query_text)
    
    def analyze_query_patterns(self, hours_back=168):  # 1 week default
        """Analyze overall query patterns for systemic issues"""
        
        query_text = f"""
        Analyze query patterns over the last {hours_back} hours to identify:
        
        Common performance issues:
        - Most frequently scanned tables
        - Repeated inefficient query patterns
        - Users with consistently slow queries
        
        Optimization opportunities:
        - Tables that would benefit from indexing
        - Common JOINs that could use materialized views
        - Partitioning strategies for large tables
        
        Provide a prioritized list of systemic improvements that would impact multiple queries.
        """
        
        return self.query_genie_space(query_text)
    
    def get_query_optimization_recommendations(self, query_details):
        """
        Get LLM-powered optimization recommendations for a specific bad query
        
        Args:
            query_details: Dict with query info from UC functions
                - query_id: str
                - statement_text: str
                - badness_score: float
                - primary_issue: str
                - performance_metrics: dict (duration, spill_gb, cache_hit_percent, etc.)
        
        Returns:
            Dict with structured optimization recommendations
        """
        
        # Extract key information
        query_id = query_details.get('query_id', 'unknown')
        sql_text = query_details.get('statement_text', '')
        badness_score = query_details.get('badness_score', 0)
        primary_issue = query_details.get('primary_issue', 'UNKNOWN')
        
        # Performance context
        duration = query_details.get('duration_seconds', 0)
        spill_gb = query_details.get('spill_gb', 0)
        cache_hit = query_details.get('cache_hit_percent', 0)
        data_read_gb = query_details.get('data_read_gb', 0)
        
        optimization_prompt = f"""
        **QUERY OPTIMIZATION ANALYSIS**
        
        **Query Context:**
        - Query ID: {query_id}
        - Badness Score: {badness_score}/100
        - Primary Issue: {primary_issue}
        - Duration: {duration} seconds
        - Memory Spill: {spill_gb} GB
        - Cache Hit Rate: {cache_hit}%
        - Data Read: {data_read_gb} GB
        
        **SQL Query to Optimize:**
        ```sql
        {sql_text}
        ```
        
        **Analysis Required:**
        
        1. **Root Cause Analysis:**
           - What specifically makes this query perform poorly?
           - Which parts of the SQL are the biggest bottlenecks?
        
        2. **Specific Optimization Recommendations:**
           - Provide EXACT SQL rewrites, not just general advice
           - Show before/after code examples
           - Focus on the primary issue: {primary_issue}
        
        3. **Prioritized Action Items:**
           - Rank fixes by potential impact (High/Medium/Low)
           - Estimate expected performance improvement
           - Note any trade-offs or risks
        
        4. **Implementation Notes:**
           - Prerequisites (indexes, partitioning, etc.)
           - Validation steps to confirm improvement
           - Monitoring recommendations
        
        **Focus Areas Based on Primary Issue:**
        {self._get_issue_specific_guidance(primary_issue)}
        
        Please provide specific, actionable SQL optimizations with concrete code examples.
        """
        
        return self.query_genie_space(optimization_prompt)
    
    def _get_issue_specific_guidance(self, primary_issue):
        """Get issue-specific guidance for LLM prompting"""
        
        guidance_map = {
            'MEMORY_SPILL_CRITICAL': """
            - Focus on reducing memory usage per partition
            - Look for opportunities to filter data earlier
            - Consider breaking complex queries into stages
            - Examine JOIN order and strategies
            """,
            'EXECUTION_TOO_SLOW': """
            - Identify missing WHERE clause filters
            - Look for inefficient JOINs and subqueries
            - Consider query restructuring opportunities  
            - Examine aggregation patterns
            """,
            'POOR_CACHE_UTILIZATION': """
            - Look for opportunities to cache intermediate results
            - Identify repeatedly accessed data patterns
            - Consider materialized view opportunities
            - Examine data access patterns
            """,
            'DATA_INEFFICIENT': """
            - Focus on column pruning (SELECT specific columns)
            - Look for unnecessary data scanning
            - Consider partitioning and filtering strategies
            - Examine data format optimizations
            """,
            'SHUFFLE_HEAVY': """
            - Focus on JOIN optimization and broadcast opportunities
            - Look for partitioning key alignment
            - Consider query restructuring to reduce shuffling
            - Examine aggregation placement
            """,
            'INFRASTRUCTURE_BOTTLENECK': """
            - Focus on resource utilization efficiency
            - Look for query timing and scheduling opportunities
            - Consider cluster sizing recommendations
            - Examine concurrency patterns
            """
        }
        
        return guidance_map.get(primary_issue, "Analyze general query optimization opportunities")
    
    def get_integrated_query_analysis(self, query_id_or_rank=1, hours_back=24):
        """
        Integrated workflow: UC Functions → LLM Analysis
        
        1. Get worst queries from UC functions  
        2. Get detailed LLM optimization recommendations
        3. Return combined analysis
        
        Args:
            query_id_or_rank: Either specific query_id or rank (1=worst, 2=second worst, etc.)
            hours_back: Hours to look back for query analysis
        
        Returns:
            Dict with comprehensive analysis combining rule-based + LLM insights
        """
        
        # Step 1: Get query details from UC functions
        if isinstance(query_id_or_rank, str):
            # Specific query ID requested
            uc_query = f"SELECT dwiltse.query_optimization.get_query_recommendations('{query_id_or_rank}')"
        else:
            # Get Nth worst query  
            uc_query = f"SELECT dwiltse.query_optimization.get_worst_queries({hours_back}, {query_id_or_rank})"
        
        print(f"🔍 Getting query details from UC functions...")
        uc_result = self.query_genie_space(f"Execute this query and return the JSON result: {uc_query}")
        
        if not uc_result.get('success'):
            return {"error": "Failed to get query details from UC functions", "details": uc_result}
        
        try:
            # Parse UC function JSON response
            uc_data = json.loads(uc_result['data']) if isinstance(uc_result['data'], str) else uc_result['data']
            
            if not uc_data.get('queries') or len(uc_data['queries']) == 0:
                return {"error": "No queries found matching criteria"}
            
            # Get the target query (first one if getting worst queries)
            target_query = uc_data['queries'][query_id_or_rank - 1] if isinstance(query_id_or_rank, int) else uc_data['queries'][0]
            
            print(f"🎯 Analyzing Query {target_query.get('query_id', 'unknown')} with LLM...")
            
            # Step 2: Get full SQL text for this query_id
            sql_query = f"""
            SELECT 
                query_id,
                statement_text,  
                badness_score,
                primary_issue,
                duration_seconds,
                spill_gb,
                cache_hit_percent,
                data_read_gb
            FROM dwiltse.query_optimization.query_performance_base
            WHERE query_id = '{target_query['query_id']}'
            LIMIT 1
            """
            
            sql_result = self.query_genie_space(f"Execute this query: {sql_query}")
            
            # Step 3: Combine UC data with full query text
            query_details = {
                **target_query,
                'statement_text': 'Full SQL query text will be extracted...'  # Placeholder
            }
            
            # Step 4: Get LLM optimization recommendations
            llm_analysis = self.get_query_optimization_recommendations(query_details)
            
            # Step 5: Return integrated analysis
            return {
                "success": True,
                "query_id": target_query['query_id'],
                "rule_based_analysis": uc_data,
                "llm_optimization_recommendations": llm_analysis,
                "analysis_timestamp": datetime.now().isoformat(),
                "methodology": "Hybrid: Rule-based identification + LLM optimization analysis"
            }
            
        except json.JSONDecodeError as e:
            return {"error": "Failed to parse UC function response", "details": str(e)}
        except Exception as e:
            return {"error": "Analysis failed", "details": str(e)}

# Streamlit integration helpers
@st.cache_resource
def get_mcp_manager():
    """Get MCP manager with resource caching for Streamlit (maintains connections)"""
    return MCPConnectionManager()

def cleanup_mcp_connections():
    """Cleanup MCP connections when app shuts down"""
    try:
        # Clear streamlit cache to trigger cleanup
        st.cache_resource.clear()
        print("🧹 MCP connections cleaned up")
    except Exception as e:
        print(f"⚠️ Cleanup warning: {e}")

def display_mcp_status():
    """Display MCP connection status in Streamlit"""
    mcp = get_mcp_manager()
    status = mcp.test_connection()
    
    if status["status"] == "success":
        st.success(f"✅ MCP Connected: {status['message']}")
        
        with st.expander("Available Tools"):
            for tool in status.get("tools", []):
                st.write(f"**{tool['name']}**: {tool['description']}")
    else:
        st.error(f"❌ MCP Connection Failed: {status['message']}")
        st.write("**Troubleshooting:**")
        st.write("- Check that your Genie space 'system_table_mcp_test' exists")
        st.write("- Verify MCP Beta features are enabled")
        st.write("- Ensure you have access to the Genie space")
    
    return status["status"] == "success"

# Test function for development
def test_mcp_connection():
    """Test the MCP connection - run this to validate setup"""
    print("🧪 Testing MCP Connection...")
    
    mcp = MCPConnectionManager()
    
    # Test 1: Connection
    status = mcp.test_connection()
    print(f"Connection Status: {status}")
    
    if status["status"] != "success":
        print("❌ Connection failed - check your setup!")
        return False
    
    # Test 2: Simple query
    print("\n🤖 Testing simple query...")
    result = mcp.query_genie_space("How many queries were executed in the last hour?")
    print(f"Query Result: {result}")
    
    # Test 3: Worst queries
    print("\n🐌 Testing worst queries detection...")
    worst = mcp.get_worst_queries(hours_back=24, limit=3)
    print(f"Worst Queries: {worst}")
    
    print("\n✅ MCP Connection Test Complete!")
    return True

def test_integrated_query_optimization():
    """Test the integrated query optimization workflow"""
    print("🚀 Testing Integrated Query Optimization Workflow...")
    
    mcp = MCPConnectionManager()
    
    # Test connection first
    status = mcp.test_connection()
    if status["status"] != "success":
        print("❌ MCP connection failed - cannot test optimization workflow")
        return False
    
    print("\n📊 Testing integrated analysis for worst query...")
    
    try:
        # Test the integrated workflow
        analysis = mcp.get_integrated_query_analysis(
            query_id_or_rank=1,  # Get worst query
            hours_back=24
        )
        
        print(f"Analysis Result: {analysis}")
        
        if analysis.get('success'):
            print("✅ Integrated analysis successful!")
            print(f"📋 Query ID: {analysis.get('query_id')}")
            print(f"🕒 Analysis Time: {analysis.get('analysis_timestamp')}")
            print(f"🔬 Methodology: {analysis.get('methodology')}")
            
            # Check if we got both rule-based and LLM analysis
            if analysis.get('rule_based_analysis') and analysis.get('llm_optimization_recommendations'):
                print("✅ Both rule-based and LLM analysis completed!")
                return True
            else:
                print("⚠️ Analysis incomplete - missing components")
                return False
        else:
            print(f"❌ Analysis failed: {analysis.get('error')}")
            return False
            
    except Exception as e:
        print(f"❌ Test failed with exception: {str(e)}")
        return False
    
    print("\n✅ Integrated Query Optimization Test Complete!")

if __name__ == "__main__":
    # Run connection test
    if test_mcp_connection():
        # If connection works, test the integrated optimization workflow
        test_integrated_query_optimization()