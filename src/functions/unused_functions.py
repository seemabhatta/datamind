"""
Unused functions - Core logic extracted from unused_router.py
These functions are preserved for future use but are not currently used by any CLI applications.
"""

import sys
import os
from pathlib import Path as PathlibPath
from typing import Optional
import pandas as pd

# Add the project root to the path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from utils import llm_util

from src.core.connection_utils import get_snowflake_connection, get_connection, remove_connection
from src.functions.query_functions import process_nl_query


def check_connection_status(connection_id: str):
    """
    Check if connection is still active
    
    UNUSED: Not called by any CLI applications
    REASON: CLIs don't need to check connection status - they just use connections
    POTENTIAL USE: Health monitoring, connection debugging
    """
    connection_data = get_connection(connection_id)
    if not connection_data:
        return {
            "status": "error",
            "error": "Connection not found"
        }
    
    try:
        conn = connection_data["connection"]
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        cursor.close()
        
        return {
            "status": "connected",
            "connection_id": connection_id,
            "account": connection_data["account"],
            "user": connection_data["user"]
        }
    except Exception as e:
        # Clean up dead connection
        remove_connection(connection_id)
        return {
            "status": "error",
            "error": f"Connection dead: {str(e)}"
        }


def nl2sql_and_execute(connection_id: str, query: str, table_name: str, dictionary_content: str):
    """
    Complete NL2SQL pipeline: Process natural language → Generate SQL → Execute → Return results
    
    UNUSED: Not called by any CLI applications
    REASON: CLIs use separate generate-sql and execute-sql endpoints for better control
    POTENTIAL USE: Single-step NL2SQL execution for simple applications
    """
    try:
        # Use the existing query function to process NL2SQL
        query_result = process_nl_query(connection_id, query, table_name, dictionary_content)
        
        # If SQL was successfully generated and executed, return the complete result
        if query_result.get("execution_status") == "success":
            return query_result
        
        # If SQL was generated but execution failed, return the error
        elif query_result.get("execution_status") == "failed":
            return query_result
            
        # If it's not a SQL query, return the classification result
        else:
            return query_result
            
    except Exception as e:
        return {
            "status": "error",
            "error": f"Error in NL2SQL pipeline: {str(e)}"
        }


def execute_sql_simple(connection_id: str, sql: str, limit: Optional[int] = 1000):
    """
    Execute SQL query on Snowflake (simple version)
    
    UNUSED: Not called by any CLI applications  
    REASON: CLIs use the more comprehensive execute_sql_only endpoint
    POTENTIAL USE: Simple SQL execution without detailed error handling
    """
    try:
        conn = get_snowflake_connection(connection_id)
        
        # Add LIMIT if not present and limit is specified
        if limit and "LIMIT" not in sql.upper():
            sql = f"{sql.rstrip(';')} LIMIT {limit}"
        
        # Execute query using cursor instead of pandas to avoid SQLAlchemy warning
        cursor = conn.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        cursor.close()
        
        # Convert to list of dictionaries
        result = [dict(zip(columns, row)) for row in rows]
        
        return {
            "status": "success",
            "columns": columns,
            "result": result,
            "row_count": len(result)
        }
        
    except Exception as e:
        return {
            "status": "error",
            "error": f"Error executing SQL: {str(e)}"
        }