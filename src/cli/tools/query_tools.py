#!/usr/bin/env python3
"""
Query function tools for the Agentic Query CLI
"""

import sys
import os
from typing import Optional
from agents import function_tool

# Add the project root to the path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from src.functions.query_functions import generate_sql_only, execute_sql_only, generate_query_summary


def generate_sql_impl(agent_context, query: str, table_name: Optional[str] = None) -> str:
    """Generate SQL from natural language query"""
    if not agent_context.connection_id:
        return "❌ No connection established. Please connect first."
    
    if not agent_context.yaml_content:
        return "❌ No YAML file loaded. Please load a data dictionary first."
    
    # Use first table if not specified
    if not table_name and agent_context.tables:
        table_name = agent_context.tables[0]['name']
    
    if not table_name:
        return "❌ No table specified and no tables available."
    
    result = generate_sql_only(agent_context.connection_id, query, table_name, agent_context.yaml_content)
    
    if result["status"] == "error":
        return f"❌ SQL generation failed: {result['error']}"
    
    intent = result.get("intent", "unknown")
    if intent != "SQL_QUERY":
        return f"💡 Intent: {intent} - {result.get('message', 'Non-SQL query detected')}"
    
    sql = result.get("sql", "")
    if not sql:
        return "❌ No SQL generated"
    
    return f"✅ Generated SQL: {sql}"


def execute_sql_impl(agent_context, sql: str, table_name: Optional[str] = None) -> str:
    """Execute SQL query and return results"""
    if not agent_context.connection_id:
        return "❌ No connection established. Please connect first."
    
    if not table_name and agent_context.tables:
        table_name = agent_context.tables[0]['name']
    
    result = execute_sql_only(agent_context.connection_id, sql, table_name or "unknown")
    
    if result["status"] == "error":
        return f"❌ SQL execution failed: {result['error']}"
    
    if result["status"] == "success":
        row_count = result.get("row_count", 0)
        response = f"✅ Query executed successfully! Returned {row_count} rows."
        
        if "result" in result and result["result"]:
            results = result["result"]
            response += f"\n📋 Sample results (first 3 rows):\n"
            for i, row in enumerate(results[:3]):
                row_items = list(row.items())[:3]  # First 3 columns
                row_display = ", ".join([f"{k}: {v}" for k, v in row_items])
                response += f"  Row {i+1}: {row_display}\n"
        
        return response
    else:
        return f"❌ Query execution failed: {result.get('sql_error', 'Unknown error')}"


def generate_summary_impl(agent_context, query: str, sql: str, results: str) -> str:
    """Generate AI summary of query results"""
    if not agent_context.connection_id:
        return "❌ No connection established. Please connect first."
    
    # Convert results string to list format expected by function
    try:
        results_list = eval(results) if isinstance(results, str) else results
    except:
        results_list = []
    
    result = generate_query_summary(agent_context.connection_id, query, sql, results_list)
    
    if result["status"] == "error":
        return f"❌ Summary generation failed: {result['error']}"
    
    if "summary" in result:
        return f"📝 AI Summary: {result['summary']}"
    else:
        return "⚠️ No summary generated"