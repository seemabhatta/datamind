"""
Unused API endpoints - preserved for future use
These endpoints are not currently used by any CLI applications but may be useful later.

To reactivate any of these endpoints:
1. Import this router in the main snowflake_api.py
2. Add: app.include_router(unused_router, tags=["unused"])
"""

import sys
import os
from pathlib import Path as PathlibPath
from fastapi import APIRouter, HTTPException, Body
from typing import Optional
import pandas as pd

# Add the project root to the path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))
from utils import llm_util

from ..models.api_models import QueryRequest
from ..utils.connection_utils import get_snowflake_connection, get_connection

router = APIRouter()

@router.get("/connection/{connection_id}/status")
async def check_connection_status(connection_id: str):
    """
    Check if connection is still active
    
    UNUSED: Not called by any CLI applications
    REASON: CLIs don't need to check connection status - they just use connections
    POTENTIAL USE: Health monitoring, connection debugging
    """
    connection_data = get_connection(connection_id)
    if not connection_data:
        raise HTTPException(status_code=404, detail="Connection not found")
    
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
        from utils.connection_utils import remove_connection
        remove_connection(connection_id)
        raise HTTPException(status_code=400, detail=f"Connection dead: {str(e)}")

@router.post("/connection/{connection_id}/nl2sql-execute")
async def nl2sql_and_execute(connection_id: str, request: QueryRequest):
    """
    Complete NL2SQL pipeline: Process natural language → Generate SQL → Execute → Return results
    
    UNUSED: Not called by any CLI applications
    REASON: CLIs use separate generate-sql and execute-sql endpoints for better control
    POTENTIAL USE: Single-step NL2SQL execution for simple applications
    """
    try:
        # Use the existing query endpoint to process NL2SQL
        from routers.query_router import process_nl_query
        query_result = await process_nl_query(connection_id, request)
        
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
        raise HTTPException(status_code=500, detail=f"Error in NL2SQL pipeline: {str(e)}")

@router.post("/connection/{connection_id}/execute-sql-simple")
async def execute_sql_simple(
    connection_id: str,
    sql: str = Body(...),
    limit: Optional[int] = Body(1000, description="Row limit for results")
):
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
        
        # Execute query
        df = pd.read_sql_query(sql, conn)
        
        # Convert to JSON
        result = df.to_dict(orient="records")
        columns = list(df.columns)
        
        return {
            "status": "success",
            "columns": columns,
            "result": result,
            "row_count": len(result)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error executing SQL: {str(e)}")

# Note: analyze_tables() is moved to dictionary_router.py since it's used by generate_data_dictionary()
# Note: health_check() is moved to main API file as it's a core endpoint