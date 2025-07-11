from fastapi import APIRouter, HTTPException, Query
from ..models.api_models import TableInfo, StageFileInfo
from ..utils.connection_utils import get_snowflake_connection

router = APIRouter()

@router.get("/connection/{connection_id}/databases")
async def list_databases(connection_id: str):
    """List all databases"""
    try:
        conn = get_snowflake_connection(connection_id)
        cursor = conn.cursor()
        cursor.execute("SHOW DATABASES")
        databases = [row[1] for row in cursor.fetchall()]  # Database name is in column 1
        cursor.close()
        
        return {"databases": databases}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing databases: {str(e)}")

@router.get("/connection/{connection_id}/schemas")
async def list_schemas(connection_id: str, database: str = Query(...)):
    """List schemas in a database"""
    try:
        conn = get_snowflake_connection(connection_id)
        cursor = conn.cursor()
        cursor.execute(f"SHOW SCHEMAS IN DATABASE {database}")
        schemas = [row[1] for row in cursor.fetchall()]  # Schema name is in column 1
        cursor.close()
        
        return {"schemas": schemas}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing schemas: {str(e)}")

@router.get("/connection/{connection_id}/tables")
async def list_tables(
    connection_id: str, 
    database: str = Query(...), 
    schema: str = Query(...)
):
    """List tables in a schema"""
    try:
        conn = get_snowflake_connection(connection_id)
        cursor = conn.cursor()
        cursor.execute(f"SHOW TABLES IN SCHEMA {database}.{schema}")
        
        tables = []
        for row in cursor.fetchall():
            tables.append(TableInfo(
                database=row[2],  # Database name
                schema=row[3],    # Schema name
                table=row[1],     # Table name
                table_type=row[4] # Table type
            ))
        cursor.close()
        
        return {"tables": tables}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing tables: {str(e)}")

@router.get("/connection/{connection_id}/stages")
async def list_stages(
    connection_id: str,
    database: str = Query(...),
    schema: str = Query(...)
):
    """List stages in a schema"""
    try:
        conn = get_snowflake_connection(connection_id)
        cursor = conn.cursor()
        cursor.execute(f"SHOW STAGES IN SCHEMA {database}.{schema}")
        
        stages = []
        for row in cursor.fetchall():
            stages.append({
                "name": row[1],      # Stage name
                "database": row[2],  # Database name
                "schema": row[3],    # Schema name
                "type": row[4]       # Stage type
            })
        cursor.close()
        
        return {"stages": stages}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing stages: {str(e)}")

@router.get("/connection/{connection_id}/stage-files")
async def list_stage_files(
    connection_id: str,
    stage_name: str = Query(..., description="Full stage name (e.g., @database.schema.stage_name)")
):
    """List files in a stage"""
    try:
        conn = get_snowflake_connection(connection_id)
        cursor = conn.cursor()
        cursor.execute(f"LIST {stage_name}")
        
        files = []
        for row in cursor.fetchall():
            files.append(StageFileInfo(
                name=row[0],                    # File name
                size=int(row[1]),              # File size
                last_modified=str(row[2])      # Last modified timestamp
            ))
        cursor.close()
        
        return {"files": files}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing stage files: {str(e)}")