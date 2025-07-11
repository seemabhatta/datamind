from fastapi import APIRouter, HTTPException
from ..models.api_models import SnowflakeConnectionResponse
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from src.core.connection_utils import (
    create_snowflake_connection, 
    get_connection, 
    get_snowflake_connection,
    remove_connection
)

router = APIRouter()

@router.post("/connect", response_model=SnowflakeConnectionResponse)
async def connect_to_snowflake():
    """Establish connection to Snowflake using environment variables"""
    try:
        connection_id, connection_data = create_snowflake_connection()
        
        return SnowflakeConnectionResponse(
            connection_id=connection_id,
            account=connection_data["account"],
            user=connection_data["user"],
            warehouse=connection_data.get("warehouse"),
            database=connection_data.get("database"),
            schema=connection_data.get("schema"),
            role=connection_data.get("role")
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Connection failed: {str(e)}")

@router.get("/connection/{connection_id}/status")
async def check_connection_status(connection_id: str):
    """Check if connection is still active"""
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
        remove_connection(connection_id)
        raise HTTPException(status_code=400, detail=f"Connection dead: {str(e)}")

@router.delete("/connection/{connection_id}")
async def disconnect(connection_id: str):
    """Close Snowflake connection"""
    connection_data = get_connection(connection_id)
    if not connection_data:
        raise HTTPException(status_code=404, detail="Connection not found")
    
    try:
        conn = connection_data["connection"]
        conn.close()
        remove_connection(connection_id)
        
        return {"status": "success", "message": "Connection closed"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error closing connection: {str(e)}")