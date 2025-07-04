import os
import uuid
import json
from typing import Dict, List, Optional, Any
from fastapi import FastAPI, HTTPException, Body, Query, Path, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
import sys
from pathlib import Path as PathlibPath

# Load environment variables
load_dotenv()

# Add the project root to the path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from utils import llm_util

app = FastAPI(title="Snowflake Cortex Analyst API", description="API for Snowflake-native NL2SQL")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Store active connections
snowflake_connections: Dict[str, Dict[str, Any]] = {}

# Pydantic models
class SnowflakeConnectionResponse(BaseModel):
    connection_id: str
    account: str
    user: str
    warehouse: Optional[str] = None
    database: Optional[str] = None
    schema: Optional[str] = None
    role: Optional[str] = None

class TableInfo(BaseModel):
    database: str
    schema: str
    table: str
    table_type: str

class StageFileInfo(BaseModel):
    name: str
    size: int
    last_modified: str

class QueryRequest(BaseModel):
    query: str
    connection_id: str
    table_name: str
    dictionary_content: Optional[str] = None

@app.post("/connect", response_model=SnowflakeConnectionResponse)
async def connect_to_snowflake():
    """Establish connection to Snowflake using environment variables"""
    try:
        # Get connection parameters from environment
        conn_params = {
            "account": os.getenv("SNOWFLAKE_ACCOUNT"),
            "user": os.getenv("SNOWFLAKE_USER"),
            "password": os.getenv("SNOWFLAKE_PASSWORD"),
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
            "database": os.getenv("SNOWFLAKE_DATABASE"),
            "schema": os.getenv("SNOWFLAKE_SCHEMA"),
            "role": os.getenv("SNOWFLAKE_ROLE")
        }
        
        # Validate required parameters
        if not all([conn_params["account"], conn_params["user"], conn_params["password"]]):
            raise HTTPException(
                status_code=400, 
                detail="Missing required Snowflake credentials in environment variables"
            )
        
        # Remove None values
        conn_params = {k: v for k, v in conn_params.items() if v is not None}
        
        # Create connection
        conn = snowflake.connector.connect(**conn_params)
        
        # Test connection
        cursor = conn.cursor()
        cursor.execute("SELECT current_version()")
        version = cursor.fetchone()[0]
        cursor.close()
        
        # Generate connection ID
        connection_id = str(uuid.uuid4())
        
        # Store connection
        snowflake_connections[connection_id] = {
            "connection": conn,
            "version": version,
            **conn_params
        }
        
        return SnowflakeConnectionResponse(
            connection_id=connection_id,
            account=conn_params["account"],
            user=conn_params["user"],
            warehouse=conn_params.get("warehouse"),
            database=conn_params.get("database"),
            schema=conn_params.get("schema"),
            role=conn_params.get("role")
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Connection failed: {str(e)}")

@app.get("/connection/{connection_id}/status")
async def check_connection_status(connection_id: str):
    """Check if connection is still active"""
    if connection_id not in snowflake_connections:
        raise HTTPException(status_code=404, detail="Connection not found")
    
    try:
        conn = snowflake_connections[connection_id]["connection"]
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        cursor.close()
        
        return {
            "status": "connected",
            "connection_id": connection_id,
            "account": snowflake_connections[connection_id]["account"],
            "user": snowflake_connections[connection_id]["user"]
        }
    except Exception as e:
        # Clean up dead connection
        if connection_id in snowflake_connections:
            del snowflake_connections[connection_id]
        raise HTTPException(status_code=400, detail=f"Connection dead: {str(e)}")

@app.get("/connection/{connection_id}/databases")
async def list_databases(connection_id: str):
    """List all databases"""
    if connection_id not in snowflake_connections:
        raise HTTPException(status_code=404, detail="Connection not found")
    
    try:
        conn = snowflake_connections[connection_id]["connection"]
        cursor = conn.cursor()
        cursor.execute("SHOW DATABASES")
        databases = [row[1] for row in cursor.fetchall()]  # Database name is in column 1
        cursor.close()
        
        return {"databases": databases}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing databases: {str(e)}")

@app.get("/connection/{connection_id}/schemas")
async def list_schemas(connection_id: str, database: str = Query(...)):
    """List schemas in a database"""
    if connection_id not in snowflake_connections:
        raise HTTPException(status_code=404, detail="Connection not found")
    
    try:
        conn = snowflake_connections[connection_id]["connection"]
        cursor = conn.cursor()
        cursor.execute(f"SHOW SCHEMAS IN DATABASE {database}")
        schemas = [row[1] for row in cursor.fetchall()]  # Schema name is in column 1
        cursor.close()
        
        return {"schemas": schemas}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing schemas: {str(e)}")

@app.get("/connection/{connection_id}/tables")
async def list_tables(
    connection_id: str, 
    database: str = Query(...), 
    schema: str = Query(...)
):
    """List tables in a schema"""
    if connection_id not in snowflake_connections:
        raise HTTPException(status_code=404, detail="Connection not found")
    
    try:
        conn = snowflake_connections[connection_id]["connection"]
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

@app.get("/connection/{connection_id}/stages")
async def list_stages(
    connection_id: str,
    database: str = Query(...),
    schema: str = Query(...)
):
    """List stages in a schema"""
    if connection_id not in snowflake_connections:
        raise HTTPException(status_code=404, detail="Connection not found")
    
    try:
        conn = snowflake_connections[connection_id]["connection"]
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

@app.get("/connection/{connection_id}/stage-files")
async def list_stage_files(
    connection_id: str,
    stage_name: str = Query(..., description="Full stage name (e.g., @database.schema.stage_name)")
):
    """List files in a stage"""
    if connection_id not in snowflake_connections:
        raise HTTPException(status_code=404, detail="Connection not found")
    
    try:
        conn = snowflake_connections[connection_id]["connection"]
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

@app.get("/connection/{connection_id}/load-stage-file")
async def load_stage_file(
    connection_id: str,
    stage_name: str = Query(..., description="Full stage name"),
    file_name: str = Query(..., description="File name in stage")
):
    """Load content from a stage file (assumes it's a text file like YAML)"""
    if connection_id not in snowflake_connections:
        raise HTTPException(status_code=404, detail="Connection not found")
    
    try:
        conn = snowflake_connections[connection_id]["connection"]
        cursor = conn.cursor()
        
        # Create a temporary table to load the file
        temp_table = f"temp_file_load_{uuid.uuid4().hex[:8]}"
        
        # Create temporary table
        cursor.execute(f"""
            CREATE OR REPLACE TEMPORARY TABLE {temp_table} (
                content VARCHAR(16777216)
            )
        """)
        
        # Copy file content to temp table
        cursor.execute(f"""
            COPY INTO {temp_table}
            FROM '{stage_name}/{file_name}'
            FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = '|' RECORD_DELIMITER = '\\n' SKIP_HEADER = 0)
        """)
        
        # Read content
        cursor.execute(f"SELECT content FROM {temp_table}")
        rows = cursor.fetchall()
        content = "\n".join([row[0] for row in rows])
        
        # Clean up
        cursor.execute(f"DROP TABLE {temp_table}")
        cursor.close()
        
        return {"content": content}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error loading stage file: {str(e)}")

@app.post("/connection/{connection_id}/query")
async def process_nl_query(connection_id: str, request: QueryRequest):
    """Process natural language query using NL2SQL and execute on Snowflake"""
    if connection_id not in snowflake_connections:
        raise HTTPException(status_code=404, detail="Connection not found")
    
    try:
        # Step 1: Classify intent using existing LLM utility
        intent = llm_util.classify_intent(request.query)
        
        if intent.strip() != "SQL_QUERY":
            return {
                "status": "success", 
                "intent": intent, 
                "message": "Non-SQL query detected",
                "query": request.query
            }
        
        # Step 2: Generate SQL using existing NL2SQL function
        if request.dictionary_content:
            # Debug logging
            print(f"DEBUG: Processing query: {request.query}")
            print(f"DEBUG: Table name: {request.table_name}")
            print(f"DEBUG: Dictionary length: {len(request.dictionary_content)} characters")
            
            # Use provided dictionary for SQL generation
            generated_sql = llm_util.create_sql_from_nl(
                request.query, 
                request.dictionary_content, 
                request.table_name
            )
            
            print(f"DEBUG: Generated SQL: {generated_sql}")
        else:
            # No dictionary provided - throw error
            raise HTTPException(
                status_code=400, 
                detail="Data dictionary is required for NL2SQL processing. Please load a dictionary file from the stage first."
            )
        
        # Step 3: Clean and validate the generated SQL
        sql_cleaned = generated_sql.strip()
        
        # Remove markdown code blocks if present
        if sql_cleaned.startswith("```sql"):
            sql_cleaned = sql_cleaned[6:]
        if sql_cleaned.startswith("```"):
            sql_cleaned = sql_cleaned[3:]
        if sql_cleaned.endswith("```"):
            sql_cleaned = sql_cleaned[:-3]
        
        sql_cleaned = sql_cleaned.strip()
        
        # Step 4: Execute the generated SQL on Snowflake
        conn = snowflake_connections[connection_id]["connection"]
        
        try:
            # Add reasonable limit if not present
            if "LIMIT" not in sql_cleaned.upper():
                sql_cleaned = f"{sql_cleaned.rstrip(';')} LIMIT 100"
            
            # Execute on Snowflake
            df = pd.read_sql_query(sql_cleaned, conn)
            
            # Convert results to JSON
            result = df.to_dict(orient="records")
            columns = list(df.columns)
            
            return {
                "status": "success",
                "intent": intent,
                "query": request.query,
                "sql": sql_cleaned,
                "table_name": request.table_name,
                "execution_status": "success",
                "columns": columns,
                "result": result,
                "row_count": len(result),
                "message": f"Successfully executed query and returned {len(result)} rows"
            }
            
        except Exception as sql_error:
            # SQL execution failed, return SQL but with error
            return {
                "status": "partial_success",
                "intent": intent,
                "query": request.query,
                "sql": sql_cleaned,
                "table_name": request.table_name,
                "execution_status": "failed",
                "sql_error": str(sql_error),
                "message": "Generated SQL but execution failed. Please review the SQL."
            }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing NL2SQL query: {str(e)}")

@app.post("/connection/{connection_id}/nl2sql-execute")
async def nl2sql_and_execute(connection_id: str, request: QueryRequest):
    """Complete NL2SQL pipeline: Process natural language → Generate SQL → Execute → Return results"""
    if connection_id not in snowflake_connections:
        raise HTTPException(status_code=404, detail="Connection not found")
    
    try:
        # Use the existing query endpoint to process NL2SQL
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

@app.post("/connection/{connection_id}/execute-sql")
async def execute_sql(
    connection_id: str,
    sql: str = Body(...),
    limit: Optional[int] = Body(1000, description="Row limit for results")
):
    """Execute SQL query on Snowflake"""
    if connection_id not in snowflake_connections:
        raise HTTPException(status_code=404, detail="Connection not found")
    
    try:
        conn = snowflake_connections[connection_id]["connection"]
        
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

@app.delete("/connection/{connection_id}")
async def disconnect(connection_id: str):
    """Close Snowflake connection"""
    if connection_id not in snowflake_connections:
        raise HTTPException(status_code=404, detail="Connection not found")
    
    try:
        conn = snowflake_connections[connection_id]["connection"]
        conn.close()
        del snowflake_connections[connection_id]
        
        return {"status": "success", "message": "Connection closed"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error closing connection: {str(e)}")

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "active_connections": len(snowflake_connections)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("snowflake_api:app", host="0.0.0.0", port=8001, reload=True)