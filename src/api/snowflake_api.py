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
        
        # Read file content directly from stage using SELECT
        select_sql = f"""
        SELECT $1 as content 
        FROM '{stage_name}/{file_name}'
        (FILE_FORMAT => (TYPE='CSV' FIELD_DELIMITER=NONE RECORD_DELIMITER='\\n' SKIP_HEADER=0))
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
                system_prompt_file_path = "nl2sqlSystemPrompt.txt"
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
        
        # Create a temporary CSV-like structure for the LLM utility
        combined_data = []
        all_columns = []
        
        for table_name, table_info in table_analysis.items():
            if "error" in table_info:
                continue
                
            # Add sample data to combined dataset
            for row in table_info.get("sample_data", []):
                # Prefix column names with table name to avoid conflicts
                prefixed_row = {f"{table_name}.{k}": v for k, v in row.items()}
                combined_data.append(prefixed_row)
            
            # Track all columns with metadata
            for col in table_info.get("columns", []):
                col_info = {
                    "table": table_name,
                    "column": col["name"],
                    "full_name": f"{table_name}.{col['name']}",
                    "type": col["type"],
                    "nullable": col["nullable"],
                    "statistics": col.get("statistics", {}),
                    "sample_values": col.get("sample_values", [])
                }
                all_columns.append(col_info)
        
        # Create a DataFrame from combined data for the LLM utility
        if combined_data:
            combined_df = pd.DataFrame(combined_data)
            
            # Save to temporary CSV for processing
            temp_csv_path = f"/tmp/snowflake_tables_{connection_id[:8]}.csv"
            combined_df.to_csv(temp_csv_path, index=False)
            
            print(f"DEBUG: Created temporary CSV with {len(combined_df)} rows and {len(combined_df.columns)} columns")
            
            # Use the existing LLM utility to generate enhanced data dictionary
            yaml_text = llm_util.generate_enhanced_data_dictionary(temp_csv_path)
            
            # Validate YAML against protobuf schema
            is_valid, error = llm_util.validate_yaml_with_proto(yaml_text)
            if not is_valid:
                print(f"WARNING: Generated YAML failed protobuf validation: {error}")
                # Continue anyway, but note the warning
            
            # Clean up temporary file
            import os
            if os.path.exists(temp_csv_path):
                os.remove(temp_csv_path)
            
            return {
                "status": "success",
                "connection_id": connection_id,
                "database": request.database_name,
                "schema": request.schema_name,
                "tables": request.tables,
                "yaml_dictionary": yaml_text,
                "parsed_dictionary": yaml.safe_load(yaml_text),
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
        
        # Create a temporary table to hold the YAML content
        temp_table = f"temp_yaml_save_{uuid.uuid4().hex[:8]}"
        
        # Create temporary table with the YAML content
        cursor.execute(f"""
            CREATE OR REPLACE TEMPORARY TABLE {temp_table} (
                content STRING
            )
        """)
        
        # Insert YAML content line by line
        yaml_lines = request.yaml_content.split('\n')
        for line in yaml_lines:
            # Escape single quotes in the content
            escaped_line = line.replace("'", "''")
            cursor.execute(f"INSERT INTO {temp_table} VALUES ('{escaped_line}')")
        
        # Copy from temp table to stage
        copy_sql = f"""
            COPY INTO '{request.stage_name}/{request.file_name}'
            FROM (SELECT content FROM {temp_table})
            FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = '|' RECORD_DELIMITER = '\\n' SKIP_HEADER = FALSE)
            OVERWRITE = TRUE
            SINGLE = TRUE
        """
        
        cursor.execute(copy_sql)
        
        # Clean up temp table
        cursor.execute(f"DROP TABLE {temp_table}")
        cursor.close()
        
        return {
            "status": "success",
            "message": f"YAML dictionary saved to {request.stage_name}/{request.file_name}",
            "stage_name": request.stage_name,
            "file_name": request.file_name,
            "content_size": len(request.yaml_content)
        }
        
    except Exception as e:
        print(f"DEBUG: Error saving dictionary to stage: {e}")
        raise HTTPException(status_code=500, detail=f"Error saving dictionary to stage: {str(e)}")

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