import sys
import os
import yaml
from pathlib import Path as PathlibPath
from fastapi import APIRouter, HTTPException
import pandas as pd

# Add the project root to the path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))
from utils import llm_util

from ..models.api_models import DataDictionaryRequest, TableSelectionRequest
from ..utils.connection_utils import get_snowflake_connection, get_connection

router = APIRouter()

@router.post("/connection/{connection_id}/analyze-tables")
async def analyze_tables(connection_id: str, request: TableSelectionRequest):
    """Analyze selected tables and generate sample data for data dictionary creation"""
    try:
        conn = get_snowflake_connection(connection_id)
        conn_details = get_connection(connection_id)
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

@router.post("/connection/{connection_id}/generate-data-dictionary")
async def generate_data_dictionary(connection_id: str, request: DataDictionaryRequest):
    """Generate YAML data dictionary from analyzed table data using LLM"""
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