import sys
import os
import re
from pathlib import Path as PathlibPath
from fastapi import APIRouter, HTTPException, Body
from typing import Optional
import pandas as pd

# Add the project root to the path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))
from utils import llm_util
import config

from ..models.api_models import QueryRequest, ExecuteSQLRequest, SummaryRequest
from ..utils.connection_utils import get_snowflake_connection, get_connection

router = APIRouter()

@router.post("/connection/{connection_id}/query")
async def process_nl_query(connection_id: str, request: QueryRequest):
    """Process natural language query using NL2SQL and execute on Snowflake"""
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
            conn = get_snowflake_connection(connection_id)
            conn_details = get_connection(connection_id)
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
        conn = get_snowflake_connection(connection_id)
        
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

@router.post("/connection/{connection_id}/generate-sql")
async def generate_sql_only(connection_id: str, request: QueryRequest):
    """Generate SQL from natural language query without executing it"""
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
        conn = get_snowflake_connection(connection_id)
        conn_details = get_connection(connection_id)
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

@router.post("/connection/{connection_id}/execute-sql")
async def execute_sql_only(connection_id: str, request: ExecuteSQLRequest):
    """Execute a SQL query on Snowflake and return results"""
    try:
        conn = get_snowflake_connection(connection_id)
        
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

@router.post("/connection/{connection_id}/generate-summary")
async def generate_query_summary(connection_id: str, request: SummaryRequest):
    """Generate AI summary of query results"""
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