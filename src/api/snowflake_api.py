import os
import uuid
import json
import yaml
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
import config

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

class TableSelectionRequest(BaseModel):
    connection_id: str
    tables: List[str]  # List of table names to analyze

class DataDictionaryRequest(BaseModel):
    connection_id: str
    tables: List[str]
    database_name: str
    schema_name: str

class SaveDictionaryRequest(BaseModel):
    connection_id: str
    stage_name: str
    file_name: str
    yaml_content: str

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
    """Load YAML data dictionary from Snowflake stage file"""
    if connection_id not in snowflake_connections:
        raise HTTPException(status_code=404, detail="Connection not found")
    
    try:
        conn = snowflake_connections[connection_id]["connection"]
        cursor = conn.cursor()
        
        print(f"DEBUG: Loading stage file {file_name} from {stage_name}")
        
        # Read file content directly from stage as plain text
        select_sql = f"""
        SELECT $1 as content 
        FROM '{stage_name}/{file_name}'
        """
        
        cursor.execute(select_sql)
        rows = cursor.fetchall()
        content = "\n".join([row[0] for row in rows if row[0]])
        
        cursor.close()
        
        print(f"DEBUG: Loaded {len(content)} characters from stage file")
        
        # Validate that it's YAML content
        if not content.strip():
            raise HTTPException(status_code=400, detail="Stage file is empty")
        
        # Basic YAML validation - check for common YAML indicators
        if not any(indicator in content for indicator in [':', '-', 'fields:', 'tables:', 'columns:']):
            raise HTTPException(status_code=400, detail="File does not appear to be a valid YAML data dictionary")
        
        return {"content": content}
        
    except Exception as e:
        print(f"DEBUG: Error loading stage file: {e}")
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
        
        # Step 2: Generate SQL using proper NL2SQL processing like nl2sql_api.py
        if request.dictionary_content:
            # Debug logging
            print(f"DEBUG: Processing query: {request.query}")
            print(f"DEBUG: Table name: {request.table_name}")
            print(f"DEBUG: Dictionary length: {len(request.dictionary_content)} characters")
            
            # Get connection for sample data
            conn = snowflake_connections[connection_id]["connection"]
            conn_details = snowflake_connections[connection_id]
            database = conn_details.get("database", "")
            schema = conn_details.get("schema", "")
            
            # Construct full table name if needed
            if "." not in request.table_name and database and schema:
                full_table_name = f"{database}.{schema}.{request.table_name}"
            else:
                full_table_name = request.table_name
            
            # Create enriched prompt like the original NL2SQL API
            try:
                # Load system prompt from file (like nl2sql_api does)
                system_prompt_file_path = config.NL2SQL_SYSTEM_PROMPT_FILE
                system_prompt = llm_util.load_prompt_file(system_prompt_file_path)
                
                # Get sample data from Snowflake table (equivalent to CSV sample data)
                sample_sql = f"SELECT * FROM {full_table_name} LIMIT 5"
                sample_df = pd.read_sql_query(sample_sql, conn)
                sample_data = sample_df.to_string(max_rows=5)
                
                # Build comprehensive prompt like create_nl2sqlchat_pompt() does
                enriched_prompt = f"""
                {system_prompt}
                ## Database Dictionary -  
                {request.dictionary_content}  
                ## Table Name
                {full_table_name}
                ## Sample Data
                {sample_data}
                """
                
                print(f"DEBUG: Using enriched prompt with sample data")
                
                # Call LLM with enriched prompt
                nl2sql_user_prompt = f"Convert the following natural language question to SQL: {request.query}"
                response = llm_util.call_response_api(llm_util.llm_model, enriched_prompt, nl2sql_user_prompt)
                generated_sql = response.choices[0].message.content
                
            except Exception as sample_error:
                print(f"DEBUG: Could not get sample data: {sample_error}")
                # Fallback to basic dictionary without sample data
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

@app.post("/connection/{connection_id}/analyze-tables")
async def analyze_tables(connection_id: str, request: TableSelectionRequest):
    """Analyze selected tables and generate sample data for data dictionary creation"""
    if connection_id not in snowflake_connections:
        raise HTTPException(status_code=404, detail="Connection not found")
    
    try:
        conn = snowflake_connections[connection_id]["connection"]
        conn_details = snowflake_connections[connection_id]
        database = conn_details.get("database", "")
        schema = conn_details.get("schema", "")
        
        table_analysis = {}
        
        for table_name in request.tables:
            print(f"DEBUG: Analyzing table {table_name}")
            
            # Construct full table name if needed
            if "." not in table_name and database and schema:
                full_table_name = f"{database}.{schema}.{table_name}"
            else:
                full_table_name = table_name
            
            try:
                # Get table schema information
                cursor = conn.cursor()
                print(f"DEBUG: Getting schema for {full_table_name}")
                cursor.execute(f"DESCRIBE TABLE {full_table_name}")
                schema_info = cursor.fetchall()
                print(f"DEBUG: Found {len(schema_info)} columns")
                
                # Get sample data
                print(f"DEBUG: Getting sample data from {full_table_name}")
                sample_sql = f"SELECT * FROM {full_table_name} LIMIT 10"
                sample_df = pd.read_sql_query(sample_sql, conn)
                print(f"DEBUG: Got {len(sample_df)} sample rows")
                
                # Get table statistics
                print(f"DEBUG: Getting row count for {full_table_name}")
                stats_sql = f"SELECT COUNT(*) as row_count FROM {full_table_name}"
                stats_df = pd.read_sql_query(stats_sql, conn)
                print(f"DEBUG: Stats columns: {list(stats_df.columns)}")
                # Handle case sensitivity - try both uppercase and lowercase
                if 'ROW_COUNT' in stats_df.columns:
                    row_count_raw = stats_df.iloc[0]['ROW_COUNT']
                elif 'row_count' in stats_df.columns:
                    row_count_raw = stats_df.iloc[0]['row_count']
                else:
                    row_count_raw = stats_df.iloc[0, 0]  # First column
                
                # Convert numpy type to native Python int
                row_count = int(row_count_raw) if pd.notna(row_count_raw) else 0
                print(f"DEBUG: Row count: {row_count}")
                
                # Analyze each column
                columns_info = []
                for col_info in schema_info:
                    col_name = col_info[0]
                    col_type = col_info[1]
                    col_nullable = col_info[2]
                    
                    # Get column statistics if it's numeric
                    if 'NUMBER' in col_type.upper() or 'FLOAT' in col_type.upper():
                        try:
                            print(f"DEBUG: Getting numeric stats for column {col_name}")
                            col_stats_sql = f"""
                            SELECT 
                                MIN({col_name}) as min_val,
                                MAX({col_name}) as max_val,
                                AVG({col_name}) as avg_val,
                                COUNT(DISTINCT {col_name}) as distinct_count
                            FROM {full_table_name}
                            """
                            col_stats_df = pd.read_sql_query(col_stats_sql, conn)
                            col_stats_raw = col_stats_df.iloc[0].to_dict()
                            # Convert numpy types to native Python types for JSON serialization
                            col_stats = {k: float(v) if pd.notna(v) and str(type(v)).startswith('<class \'numpy.') else v for k, v in col_stats_raw.items()}
                            print(f"DEBUG: Numeric stats for {col_name}: {col_stats}")
                        except Exception as col_error:
                            print(f"DEBUG: Error getting numeric stats for {col_name}: {col_error}")
                            col_stats = {}
                    else:
                        # For string columns, get distinct count and sample values
                        try:
                            print(f"DEBUG: Getting string stats for column {col_name}")
                            col_stats_sql = f"""
                            SELECT 
                                COUNT(DISTINCT {col_name}) as distinct_count,
                                COUNT({col_name}) as non_null_count
                            FROM {full_table_name}
                            """
                            col_stats_df = pd.read_sql_query(col_stats_sql, conn)
                            col_stats_raw = col_stats_df.iloc[0].to_dict()
                            # Convert numpy types to native Python types for JSON serialization
                            col_stats = {k: int(v) if pd.notna(v) and str(type(v)).startswith('<class \'numpy.') else v for k, v in col_stats_raw.items()}
                            print(f"DEBUG: String stats for {col_name}: {col_stats}")
                        except Exception as col_error:
                            print(f"DEBUG: Error getting string stats for {col_name}: {col_error}")
                            col_stats = {}
                    
                    # Get sample values and convert numpy types
                    if col_name in sample_df.columns:
                        sample_values_raw = sample_df[col_name].head(5).tolist()
                        sample_values = [
                            float(v) if pd.notna(v) and str(type(v)).startswith('<class \'numpy.float') else
                            int(v) if pd.notna(v) and str(type(v)).startswith('<class \'numpy.int') else
                            str(v) if pd.notna(v) else None
                            for v in sample_values_raw
                        ]
                    else:
                        sample_values = []
                    
                    columns_info.append({
                        "name": col_name,
                        "type": col_type,
                        "nullable": col_nullable == "Y",
                        "statistics": col_stats,
                        "sample_values": sample_values
                    })
                
                cursor.close()
                
                # Convert sample data and handle numpy types
                sample_data_raw = sample_df.head(5).to_dict(orient="records")
                sample_data = []
                for record in sample_data_raw:
                    converted_record = {}
                    for k, v in record.items():
                        if pd.notna(v):
                            if str(type(v)).startswith('<class \'numpy.float'):
                                converted_record[k] = float(v)
                            elif str(type(v)).startswith('<class \'numpy.int'):
                                converted_record[k] = int(v)
                            else:
                                converted_record[k] = str(v)
                        else:
                            converted_record[k] = None
                    sample_data.append(converted_record)
                
                table_analysis[table_name] = {
                    "full_name": full_table_name,
                    "row_count": row_count,
                    "columns": columns_info,
                    "sample_data": sample_data
                }
                
                print(f"DEBUG: Successfully analyzed table {table_name} with {len(columns_info)} columns")
                
            except Exception as table_error:
                print(f"DEBUG: Error analyzing table {table_name}: {table_error}")
                table_analysis[table_name] = {
                    "error": str(table_error),
                    "full_name": full_table_name
                }
        
        return {
            "status": "success",
            "connection_id": connection_id,
            "database": database,
            "schema": schema,
            "tables_analyzed": len(request.tables),
            "analysis": table_analysis
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error analyzing tables: {str(e)}")

@app.post("/connection/{connection_id}/generate-data-dictionary")
async def generate_data_dictionary(connection_id: str, request: DataDictionaryRequest):
    """Generate YAML data dictionary from analyzed table data using LLM"""
    if connection_id not in snowflake_connections:
        raise HTTPException(status_code=404, detail="Connection not found")
    
    try:
        # First analyze the tables
        table_request = TableSelectionRequest(
            connection_id=connection_id,
            tables=request.tables
        )
        analysis_result = await analyze_tables(connection_id, table_request)
        
        if analysis_result["status"] != "success":
            raise HTTPException(status_code=500, detail="Failed to analyze tables")
        
        # Prepare data for LLM processing
        table_analysis = analysis_result["analysis"]
        
        # Create a multi-table data dictionary using a direct LLM call instead of the single-table utility
        all_tables_info = []
        
        for table_name, table_info in table_analysis.items():
            if "error" in table_info:
                continue
            
            # Create table information for the prompt
            table_prompt_info = f"\nTable: {table_name}\n"
            table_prompt_info += f"Full Name: {table_info.get('full_name', f'{request.database_name}.{request.schema_name}.{table_name}')}\n"
            table_prompt_info += f"Row Count: {table_info.get('row_count', 'Unknown')}\n"
            table_prompt_info += "Columns:\n"
            
            columns_list = table_info.get("columns", [])
            table_prompt_info += f"Total Columns: {len(columns_list)}\n"
            
            for i, col in enumerate(columns_list, 1):
                col_name = col["name"]
                col_type = col["type"]
                nullable = "nullable" if col["nullable"] else "not null"
                sample_values = col.get("sample_values", [])[:5]  # First 5 sample values for better context
                stats = col.get("statistics", {})
                
                table_prompt_info += f"  {i}. {col_name} ({col_type}, {nullable})"
                if sample_values:
                    table_prompt_info += f" - samples: {sample_values}"
                
                # Add statistical context for better descriptions
                if stats:
                    if 'distinct_count' in stats:
                        table_prompt_info += f" - distinct values: {stats['distinct_count']}"
                    if 'min_val' in stats and 'max_val' in stats:
                        table_prompt_info += f" - range: {stats['min_val']} to {stats['max_val']}"
                    if 'avg_val' in stats:
                        table_prompt_info += f" - average: {stats['avg_val']:.2f}"
                        
                table_prompt_info += "\n"
            
            all_tables_info.append(table_prompt_info)
        
        if all_tables_info:
            # Create a comprehensive prompt for multiple tables
            tables_description = "\n".join(all_tables_info)
            
            # Load the enhanced data dictionary system prompt
            system_prompt_file = "enhancedDDSystemPrompt_v2.txt"
            system_prompt = llm_util.load_prompt_file(system_prompt_file)
            
            # Create user prompt for multiple tables using the correct protobuf schema structure
            user_prompt = f"""
Here are the details for {len(all_tables_info)} database tables from {request.database_name}.{request.schema_name}:

{tables_description}

Please generate a comprehensive YAML data dictionary that follows this EXACT structure for Snowflake Cortex Analyst:

tables:
  - name: "TABLE_NAME"
    description: "Concise business description of table purpose (max 15 words)"
    base_table:
      database: "{request.database_name}"
      schema: "{request.schema_name}"
      table: "TABLE_NAME"
    dimensions:
      - name: "COLUMN_NAME"
        description: "Concise business meaning of column (max 15 words)"
        expr: "COLUMN_NAME"
        dataType: "varchar/number/date/etc"
        unique: false
        sampleValues: ["sample1", "sample2"]
    measures:
      - name: "NUMERIC_COLUMN_NAME"  
        description: "Concise description of what this measures (max 15 words)"
        expr: "NUMERIC_COLUMN_NAME"
        dataType: "number"
        default_aggregation: "sum"
        sampleValues: ["123", "456"]

CRITICAL REQUIREMENTS:
1. Include ALL {len(all_tables_info)} tables listed above
2. For each table, include EVERY SINGLE COLUMN listed (do not skip any columns)
3. Use "dimensions" for non-numeric columns (varchar, text, date, timestamp, boolean, etc.)
4. Use "measures" for numeric columns that can be aggregated (number, integer, float, decimal)
5. Set dataType to: varchar, number, date, timestamp, boolean, etc. (based on the column type shown)
6. Use the exact field names: name, description, expr, dataType, unique, sampleValues
7. Do NOT use "category" or "fields" - they are not valid
8. ALWAYS use database: "{request.database_name}" and schema: "{request.schema_name}" for ALL tables
9. The "expr" field should always be the same as the column name
10. Include sample values from the data provided

DESCRIPTION REQUIREMENTS - Generate CONCISE, MEANINGFUL descriptions:
IMPORTANT: ALL descriptions must be 15 words or less.

For TABLE descriptions (max 15 words):
- Focus on primary business purpose and data type
- Example: "Customer demographic and contact information for marketing analysis"

For COLUMN descriptions (max 15 words):
- Explain business meaning concisely
- Include key context from sample values
- Examples:
  * "Customer unique identifier for tracking and relationship management"
  * "Annual income amount in USD for demographic analysis"
  * "Account creation date for customer lifecycle tracking"
  * "Primary email address for communication and login authentication"

Use sample values and statistics to infer business context but keep descriptions under 15 words.

VERIFICATION CHECKLIST - Ensure your YAML includes:
- All {len(all_tables_info)} tables
- Every column from each table (check the "Total Columns" count for each table)
- Proper categorization as dimensions or measures based on data type
- Concise descriptions (15 words or less) for all tables and columns
"""
            
            print(f"DEBUG: Generating YAML for {len(all_tables_info)} tables using structured output")
            
            # Create comprehensive prompt for structured generation
            complete_prompt = f"{system_prompt}\n\n{user_prompt}"
            
            # Generate YAML using structured output (guaranteed valid)
            yaml_text = llm_util.generate_structured_yaml(complete_prompt)
            parsed_yaml = yaml.safe_load(yaml_text)  # Safe to parse - guaranteed valid
            
            # Verify that all columns are included in the generated YAML
            try:
                verification_results = []
                
                for table_name, table_info in table_analysis.items():
                    if "error" in table_info:
                        continue
                    
                    expected_columns = [col["name"] for col in table_info.get("columns", [])]
                    
                    # Find this table in the generated YAML
                    yaml_table = None
                    for yaml_tbl in parsed_yaml.get("tables", []):
                        if yaml_tbl.get("name") == table_name:
                            yaml_table = yaml_tbl
                            break
                    
                    if yaml_table:
                        # Collect all column names from dimensions and measures
                        yaml_columns = []
                        yaml_columns.extend([dim.get("name") for dim in yaml_table.get("dimensions", [])])
                        yaml_columns.extend([meas.get("name") for meas in yaml_table.get("measures", [])])
                        
                        missing_columns = [col for col in expected_columns if col not in yaml_columns]
                        if missing_columns:
                            verification_results.append(f"Table {table_name}: Missing columns {missing_columns}")
                        else:
                            verification_results.append(f"Table {table_name}: ✓ All {len(expected_columns)} columns included")
                    else:
                        verification_results.append(f"Table {table_name}: ❌ Table not found in YAML")
                
                print("DEBUG: Column verification results:")
                for result in verification_results:
                    print(f"  {result}")
                    
            except Exception as verify_error:
                print(f"DEBUG: Could not verify column completeness: {verify_error}")
            
            # Validate YAML against protobuf schema
            is_valid, error = llm_util.validate_yaml_with_proto(yaml_text)
            if not is_valid:
                print(f"WARNING: Generated YAML failed protobuf validation: {error}")
                # Continue anyway, but note the warning
            
            return {
                "status": "success",
                "connection_id": connection_id,
                "database": request.database_name,
                "schema": request.schema_name,
                "tables": request.tables,
                "yaml_dictionary": yaml_text,
                "parsed_dictionary": parsed_yaml,
                "validation_status": "valid" if is_valid else "invalid",
                "validation_error": error if not is_valid else None,
                "tables_processed": len([t for t in table_analysis.values() if "error" not in t])
            }
            
        else:
            raise HTTPException(status_code=400, detail="No valid table data found to generate dictionary")
        
    except Exception as e:
        print(f"DEBUG: Error generating data dictionary: {e}")
        raise HTTPException(status_code=500, detail=f"Error generating data dictionary: {str(e)}")

@app.post("/connection/{connection_id}/save-dictionary-to-stage")
async def save_dictionary_to_stage(connection_id: str, request: SaveDictionaryRequest):
    """Save YAML data dictionary to a Snowflake stage"""
    if connection_id not in snowflake_connections:
        raise HTTPException(status_code=404, detail="Connection not found")
    
    try:
        conn = snowflake_connections[connection_id]["connection"]
        cursor = conn.cursor()
        
        # Write YAML content to a temporary local file with the desired name
        import tempfile
        import os
        
        # Create temp file with the desired filename
        temp_dir = tempfile.gettempdir()
        temp_file_path = os.path.join(temp_dir, request.file_name)
        
        with open(temp_file_path, 'w') as temp_file:
            temp_file.write(request.yaml_content)
        
        try:
            # Use Snowflake's PUT command to upload the file to the stage
            # Convert Windows path to proper format and escape backslashes
            normalized_path = temp_file_path.replace('\\', '/')
            put_command = f"PUT 'file://{normalized_path}' {request.stage_name} OVERWRITE=TRUE AUTO_COMPRESS=FALSE"
            print(f"DEBUG: Executing PUT command: {put_command}")
            cursor.execute(put_command)
            
            # Since we created the temp file with the desired name, it should upload with the correct name
            actual_filename = request.file_name
            
            # Verify the upload by listing the stage
            cursor.execute(f"LIST {request.stage_name}")
            files = cursor.fetchall()
            print(f"DEBUG: Files in stage after upload: {[f[0] for f in files]}")
            
            cursor.close()
            
            return {
                "status": "success", 
                "message": f"YAML dictionary uploaded to {request.stage_name}/{actual_filename}",
                "stage_name": request.stage_name,
                "file_name": actual_filename,
                "content_size": len(request.yaml_content)
            }
            
        finally:
            # Clean up temporary file
            if os.path.exists(temp_file_path):
                os.unlink(temp_file_path)
        
    except Exception as e:
        print(f"DEBUG: Error saving dictionary to stage: {e}")
        raise HTTPException(status_code=500, detail=f"Error saving dictionary to stage: {str(e)}")

@app.post("/connection/{connection_id}/generate-sql")
async def generate_sql_only(connection_id: str, request: QueryRequest):
    """Generate SQL from natural language query without executing it"""
    if connection_id not in snowflake_connections:
        raise HTTPException(status_code=404, detail="Connection not found")
    
    try:
        # Step 1: Classify intent
        intent = llm_util.classify_intent(request.query)
        
        if intent.strip() != "SQL_QUERY":
            return {
                "status": "success", 
                "intent": intent, 
                "message": "Non-SQL query detected",
                "query": request.query
            }
        
        # Step 2: Generate SQL only (no execution)
        if not request.dictionary_content:
            raise HTTPException(status_code=400, detail="Dictionary content is required for SQL generation")
        
        # Get connection for sample data
        conn = snowflake_connections[connection_id]["connection"]
        conn_details = snowflake_connections[connection_id]
        database = conn_details.get("database", "")
        schema = conn_details.get("schema", "")
        
        # Construct full table name if needed
        if "." not in request.table_name and database and schema:
            full_table_name = f"{database}.{schema}.{request.table_name}"
        else:
            full_table_name = request.table_name
        
        # Create enriched prompt with sample data
        try:
            system_prompt_file_path = config.NL2SQL_SYSTEM_PROMPT_FILE
            system_prompt = llm_util.load_prompt_file(system_prompt_file_path)
            
            # Get sample data from Snowflake table
            sample_sql = f"SELECT * FROM {full_table_name} LIMIT 5"
            sample_df = pd.read_sql_query(sample_sql, conn)
            sample_data = sample_df.to_string(max_rows=5)
            
            # Build comprehensive prompt
            enriched_prompt = f"""
            {system_prompt}
            ## Database Dictionary -  
            {request.dictionary_content}  
            ## Table Name
            {full_table_name}
            ## Sample Data
            {sample_data}
            """
            
            # Call LLM with enriched prompt
            nl2sql_user_prompt = f"Convert the following natural language question to SQL: {request.query}"
            response = llm_util.call_response_api(llm_util.llm_model, enriched_prompt, nl2sql_user_prompt)
            generated_sql = response.choices[0].message.content
            
        except Exception as sample_error:
            print(f"DEBUG: Could not get sample data: {sample_error}")
            # Fallback to basic dictionary without sample data
            generated_sql = llm_util.create_sql_from_nl(
                request.query, 
                request.dictionary_content, 
                request.table_name
            )
        
        # Clean up SQL (remove markdown, etc.)
        import re
        sql_clean = re.sub(r'```sql\s*', '', generated_sql)
        sql_clean = re.sub(r'```\s*', '', sql_clean)
        sql_clean = sql_clean.strip()
        
        # Remove trailing semicolon if present
        if sql_clean.endswith(';'):
            sql_clean = sql_clean[:-1].strip()
        
        # Add LIMIT if not present
        if "LIMIT" not in sql_clean.upper():
            sql_clean += " LIMIT 100"
        
        return {
            "status": "success",
            "intent": intent,
            "query": request.query,
            "sql": sql_clean,
            "table_name": request.table_name,
            "full_table_name": full_table_name
        }
        
    except Exception as e:
        print(f"DEBUG: Error generating SQL: {e}")
        raise HTTPException(status_code=500, detail=f"Error generating SQL: {str(e)}")

class ExecuteSQLRequest(BaseModel):
    connection_id: str
    sql: str
    table_name: Optional[str] = None

@app.post("/connection/{connection_id}/execute-sql")
async def execute_sql_only(connection_id: str, request: ExecuteSQLRequest):
    """Execute a SQL query on Snowflake and return results"""
    if connection_id not in snowflake_connections:
        raise HTTPException(status_code=404, detail="Connection not found")
    
    try:
        conn = snowflake_connections[connection_id]["connection"]
        
        print(f"DEBUG: Executing SQL: {request.sql}")
        
        # Execute SQL using pandas
        df = pd.read_sql_query(request.sql, conn)
        
        # Convert to JSON-serializable format
        result = df.to_dict(orient="records")
        columns = list(df.columns)
        row_count = len(df)
        
        print(f"DEBUG: SQL executed successfully, returned {row_count} rows")
        
        return {
            "status": "success",
            "sql": request.sql,
            "execution_status": "success",
            "columns": columns,
            "result": result,
            "row_count": row_count,
            "message": f"Successfully executed query and returned {row_count} rows"
        }
        
    except Exception as e:
        print(f"DEBUG: SQL execution error: {e}")
        return {
            "status": "error",
            "sql": request.sql,
            "execution_status": "failed",
            "sql_error": str(e),
            "message": "SQL execution failed. Please review the SQL."
        }

class SummaryRequest(BaseModel):
    connection_id: str
    query: str
    sql: str
    results: List[Dict[str, Any]]

@app.post("/connection/{connection_id}/generate-summary")
async def generate_query_summary(connection_id: str, request: SummaryRequest):
    """Generate AI summary of query results"""
    if connection_id not in snowflake_connections:
        raise HTTPException(status_code=404, detail="Connection not found")
    
    try:
        # Convert results back to DataFrame for summary generation
        df = pd.DataFrame(request.results)
        
        # Generate summary using existing LLM utility
        summary = llm_util.create_summary(df)
        
        return {
            "status": "success",
            "query": request.query,
            "sql": request.sql,
            "summary": summary,
            "row_count": len(request.results)
        }
        
    except Exception as e:
        print(f"DEBUG: Error generating summary: {e}")
        raise HTTPException(status_code=500, detail=f"Error generating summary: {str(e)}")

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