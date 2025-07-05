#!/usr/bin/env python3
"""
Natural Query CLI - Interactive natural language querying of Snowflake data
"""

import click
import requests
import json
import sys
import yaml

BASE_URL = "http://localhost:8001"

class APIClient:
    def __init__(self):
        self.connection_id = None
        
    def post(self, endpoint, data=None):
        response = requests.post(f"{BASE_URL}{endpoint}", json=data)
        return response.json() if response.status_code == 200 else {"error": response.text}
    
    def get(self, endpoint, params=None):
        response = requests.get(f"{BASE_URL}{endpoint}", params=params)
        return response.json() if response.status_code == 200 else {"error": response.text}
    
    def delete(self, endpoint):
        response = requests.delete(f"{BASE_URL}{endpoint}")
        return response.json() if response.status_code == 200 else {"error": response.text}

client = APIClient()

@click.group()
def cli():
    """Natural Language Query CLI for Snowflake"""
    pass

@cli.command()
def workflow():
    """Interactive workflow: connect -> select YAML -> query with natural language"""
    click.echo("🚀 Natural Language Query Workflow")
    click.echo("=" * 50)
    
    # Step 1: Connect to Snowflake
    click.echo("\n📋 Step 1: Connecting to Snowflake...")
    result = client.post("/connect")
    if "connection_id" not in result:
        click.echo(f"❌ Connection failed: {result}")
        return
    
    client.connection_id = result["connection_id"]
    click.echo(f"✅ Connected to {result['account']} as {result['user']}")
    click.echo(f"🔗 Connection ID: {result['connection_id'][:8]}...")
    
    # Step 1a: Select Database
    click.echo("\n📋 Step 1a: Select Database")
    db_result = client.get(f"/connection/{client.connection_id}/databases")
    if "databases" not in db_result:
        click.echo(f"❌ Failed to get databases: {db_result}")
        return
    
    databases = db_result["databases"]
    if not databases:
        click.echo("❌ No databases found")
        return
    
    click.echo("Available databases:")
    for i, db in enumerate(databases, 1):
        click.echo(f"  {i}. {db}")
    
    while True:
        try:
            choice = click.prompt("\nSelect database number", type=int)
            if 1 <= choice <= len(databases):
                selected_database = databases[choice - 1]
                break
            else:
                click.echo("❌ Invalid choice. Please try again.")
        except (ValueError, click.Abort):
            click.echo("❌ Invalid input. Please enter a number.")
    
    click.echo(f"Selected database: {selected_database}")
    
    # Step 1b: Select Schema
    click.echo(f"\n📋 Step 1b: Select Schema from {selected_database}")
    schema_result = client.get(f"/connection/{client.connection_id}/schemas", 
                             params={"database": selected_database})
    if "schemas" not in schema_result:
        click.echo(f"❌ Failed to get schemas: {schema_result}")
        return
    
    schemas = schema_result["schemas"]
    if not schemas:
        click.echo("❌ No schemas found")
        return
    
    click.echo("Available schemas:")
    for i, schema in enumerate(schemas, 1):
        click.echo(f"  {i}. {schema}")
    
    while True:
        try:
            choice = click.prompt("\nSelect schema number", type=int)
            if 1 <= choice <= len(schemas):
                selected_schema = schemas[choice - 1]
                break
            else:
                click.echo("❌ Invalid choice. Please try again.")
        except (ValueError, click.Abort):
            click.echo("❌ Invalid input. Please enter a number.")
    
    click.echo(f"Selected schema: {selected_schema}")
    
    # Step 1c: Select Stage
    click.echo(f"\n📋 Step 1c: Select Stage from {selected_database}.{selected_schema}")
    stages_result = client.get(f"/connection/{client.connection_id}/stages", 
                             params={"database": selected_database, "schema": selected_schema})
    if "stages" not in stages_result:
        click.echo(f"❌ Failed to get stages: {stages_result}")
        return
    
    stages = stages_result["stages"]
    if not stages:
        click.echo("❌ No stages found")
        return
    
    click.echo("Available stages:")
    for i, stage in enumerate(stages, 1):
        click.echo(f"  {i}. {stage['name']} ({stage['type']})")
    
    while True:
        try:
            choice = click.prompt("\nSelect stage number", type=int)
            if 1 <= choice <= len(stages):
                selected_stage = stages[choice - 1]
                stage_name = f"@{selected_database}.{selected_schema}.{selected_stage['name']}"
                break
            else:
                click.echo("❌ Invalid choice. Please try again.")
        except (ValueError, click.Abort):
            click.echo("❌ Invalid input. Please enter a number.")
    
    click.echo(f"Selected stage: {stage_name}")
    
    # Step 1d: Select YAML File
    click.echo(f"\n📋 Step 1d: Select YAML File from {stage_name}")
    files_result = client.get(f"/connection/{client.connection_id}/stage-files", 
                            params={"stage_name": stage_name})
    if "files" not in files_result:
        click.echo(f"❌ Failed to get files: {files_result}")
        return
    
    files = files_result["files"]
    if not files:
        click.echo("❌ No files found in stage")
        return
    
    # Filter for YAML files
    yaml_files = [f for f in files if f["name"].endswith(('.yaml', '.yml'))]
    if not yaml_files:
        click.echo("❌ No YAML files found in stage")
        click.echo(f"Available files: {[f['name'] for f in files]}")
        return
    
    click.echo("Available YAML files:")
    for i, file in enumerate(yaml_files, 1):
        # Extract just the filename without stage prefix
        filename = file["name"].split('/')[-1] if '/' in file["name"] else file["name"]
        click.echo(f"  {i}. {filename} ({file['size']} bytes)")
    
    # Step 2: Load and Parse YAML (with retry loop)
    click.echo(f"\n📋 Step 2: Loading and Parsing YAML File")
    
    yaml_data = None
    yaml_content = None
    selected_filename = None
    
    while yaml_data is None:
        # File selection
        while True:
            try:
                choice = click.prompt("\nSelect YAML file number (or 'q' to quit)", type=str)
                if choice.lower() == 'q':
                    click.echo("👋 Goodbye!")
                    return
                
                choice_int = int(choice)
                if 1 <= choice_int <= len(yaml_files):
                    selected_file = yaml_files[choice_int - 1]
                    selected_filename = selected_file["name"].split('/')[-1] if '/' in selected_file["name"] else selected_file["name"]
                    break
                else:
                    click.echo("❌ Invalid choice. Please try again.")
            except ValueError:
                click.echo("❌ Invalid input. Please enter a number or 'q' to quit.")
            except click.Abort:
                click.echo("\n👋 Goodbye!")
                return
        
        click.echo(f"Selected YAML file: {selected_filename}")
        
        # Load YAML content from stage
        click.echo(f"📄 Loading {selected_filename} from stage...")
        yaml_result = client.get(f"/connection/{client.connection_id}/load-stage-file", 
                               params={"stage_name": stage_name, "file_name": selected_filename})
        
        if "content" not in yaml_result:
            click.echo(f"❌ Failed to load YAML content: {yaml_result}")
            retry = click.confirm("💭 Try a different file?", default=True)
            if not retry:
                return
            continue
        
        yaml_content = yaml_result["content"]
        click.echo(f"✅ Loaded YAML content ({len(yaml_content)} characters)")
        
        # Parse YAML
        try:
            yaml_data = yaml.safe_load(yaml_content)
            click.echo("✅ YAML parsed successfully")
        except yaml.YAMLError as e:
            click.echo(f"❌ Failed to parse YAML: {e}")
            retry = click.confirm("💭 Try a different file?", default=True)
            if not retry:
                return
            # Continue loop to select another file
    
    # Extract database and schema information from YAML
    click.echo("\n🔍 Extracting database and schema information...")
    
    databases_found = set()
    schemas_found = set()
    tables_found = []
    
    if "tables" in yaml_data:
        for table in yaml_data["tables"]:
            if "base_table" in table:
                base_table = table["base_table"]
                if "database" in base_table:
                    databases_found.add(base_table["database"])
                if "schema" in base_table:
                    schemas_found.add(base_table["schema"])
                
                # Store table info for validation later
                tables_found.append({
                    "name": table.get("name", "Unknown"),
                    "database": base_table.get("database", ""),
                    "schema": base_table.get("schema", ""),
                    "full_name": f"{base_table.get('database', '')}.{base_table.get('schema', '')}.{table.get('name', '')}"
                })
    
    if not databases_found or not schemas_found:
        click.echo("❌ Could not extract database/schema information from YAML")
        click.echo("Expected YAML structure: tables[].base_table.{database, schema}")
        return
    
    # Display found information
    click.echo(f"📊 Found {len(databases_found)} database(s): {', '.join(databases_found)}")
    click.echo(f"📂 Found {len(schemas_found)} schema(s): {', '.join(schemas_found)}")
    click.echo(f"📋 Found {len(tables_found)} table(s):")
    for table in tables_found:
        click.echo(f"  - {table['name']} ({table['full_name']})")
    
    # Store parsed data for next steps
    yaml_context = {
        "content": yaml_content,
        "data": yaml_data,
        "databases": list(databases_found),
        "schemas": list(schemas_found),
        "tables": tables_found,
        "stage_database": selected_database,
        "stage_schema": selected_schema,
        "stage_name": stage_name,
        "filename": selected_filename
    }
    
    # Step 3: Database/Schema Confirmation
    click.echo(f"\n📋 Step 3: Database/Schema Confirmation")
    click.echo("🔍 The YAML file contains references to the following:")
    click.echo(f"🗄️  Database(s): {', '.join(databases_found)}")
    click.echo(f"📂 Schema(s): {', '.join(schemas_found)}")
    click.echo(f"📋 Table(s): {len(tables_found)} tables")
    for table in tables_found:
        click.echo(f"   - {table['name']} ({table['full_name']})")
    
    click.echo(f"\n💭 We will use this database and schema for querying:")
    
    # Convert sets to lists for indexing
    databases_list = list(databases_found)
    schemas_list = list(schemas_found)
    
    # Handle multiple databases/schemas (though usually should be one)
    if len(databases_list) > 1:
        click.echo(f"⚠️  Multiple databases found: {', '.join(databases_list)}")
        click.echo("Please select which database to use:")
        for i, db in enumerate(databases_list, 1):
            click.echo(f"  {i}. {db}")
        
        while True:
            try:
                choice = click.prompt("Select database number", type=int)
                if 1 <= choice <= len(databases_list):
                    target_database = databases_list[choice - 1]
                    break
                else:
                    click.echo("❌ Invalid choice. Please try again.")
            except (ValueError, click.Abort):
                click.echo("❌ Invalid input. Please enter a number.")
    else:
        target_database = databases_list[0]
    
    if len(schemas_list) > 1:
        click.echo(f"⚠️  Multiple schemas found: {', '.join(schemas_list)}")
        click.echo("Please select which schema to use:")
        for i, schema in enumerate(schemas_list, 1):
            click.echo(f"  {i}. {schema}")
        
        while True:
            try:
                choice = click.prompt("Select schema number", type=int)
                if 1 <= choice <= len(schemas_list):
                    target_schema = schemas_list[choice - 1]
                    break
                else:
                    click.echo("❌ Invalid choice. Please try again.")
            except (ValueError, click.Abort):
                click.echo("❌ Invalid input. Please enter a number.")
    else:
        target_schema = schemas_list[0]
    
    click.echo(f"\n🎯 Target Context:")
    click.echo(f"   🗄️  Database: {target_database}")
    click.echo(f"   📂 Schema: {target_schema}")
    click.echo(f"   📋 Tables: {len(tables_found)} table(s)")
    for table in tables_found:
        click.echo(f"      - {table['name']}")
    
    # Confirmation
    proceed = click.confirm(f"\n💭 Do you want to proceed with database '{target_database}' and schema '{target_schema}'?", default=True)
    if not proceed:
        click.echo("👋 Operation cancelled. Goodbye!")
        return
    
    # Update yaml_context with confirmed targets
    yaml_context["target_database"] = target_database
    yaml_context["target_schema"] = target_schema
    
    # Step 5: Natural Language Query Loop
    click.echo(f"\n📋 Step 5: Natural Language Query Interface")
    click.echo("🚀 You can now ask questions about your data in natural language!")
    click.echo("💡 Tips:")
    click.echo("   - Ask questions like: 'What is the total revenue?'")
    click.echo("   - Use table/column names from the YAML if needed")
    click.echo("   - Type 'quit', 'exit', or 'q' to stop")
    click.echo("   - Press Ctrl+C to exit anytime")
    
    click.echo(f"\n🎯 Query Context:")
    click.echo(f"   🗄️  Database: {target_database}")
    click.echo(f"   📂 Schema: {target_schema}")
    click.echo(f"   📋 Available Tables:")
    for table in tables_found:
        click.echo(f"      - {table['name']}")
    
    click.echo("\n" + "=" * 50)
    click.echo("🎤 Ready for your questions!")
    click.echo("=" * 50)
    
    # Query loop
    query_count = 0
    while True:
        try:
            # Get user input
            user_query = click.prompt(f"\n💬 Ask a question", type=str).strip()
            
            # Check for exit commands
            if user_query.lower() in ['quit', 'exit', 'q', 'stop']:
                click.echo("👋 Thanks for using the Natural Query CLI!")
                break
            
            if not user_query:
                click.echo("❌ Please enter a question.")
                continue
            
            query_count += 1
            click.echo(f"\n🔍 Processing Query #{query_count}: {user_query}")
            
            # For now, we'll prepare the data for the API call
            # We'll use the first table for queries (can be enhanced later)
            primary_table = tables_found[0]['name'] if tables_found else None
            
            if not primary_table:
                click.echo("❌ No tables available for querying.")
                continue
            
            click.echo(f"📋 Using table: {primary_table}")
            click.echo(f"📄 Using dictionary: {selected_filename}")
            
            # Step 6: SQL Generation
            click.echo("🔄 Generating SQL...")
            
            # Prepare request for SQL generation
            sql_request = {
                "query": user_query,
                "connection_id": client.connection_id,
                "table_name": primary_table,
                "dictionary_content": yaml_content
            }
            
            # Call the SQL generation endpoint
            sql_result = client.post(f"/connection/{client.connection_id}/generate-sql", sql_request)
            
            # Handle API errors
            if "error" in sql_result:
                click.echo(f"❌ API Error: {sql_result['error']}")
                continue
            
            # Check intent
            intent = sql_result.get("intent", "unknown")
            if intent != "SQL_QUERY":
                click.echo(f"💡 Intent: {intent}")
                click.echo(f"📄 {sql_result.get('message', 'Non-SQL query detected')}")
                
                # Debug: Show more details about why it was classified as non-SQL
                click.echo(f"🔧 Debug: Full API response: {sql_result}")
                
                # Suggest the user to be more specific
                if intent == "AMBIGUOUS_QUERY":
                    click.echo("💡 Try being more specific. Examples:")
                    click.echo("   - 'How many rows are in the DAILY_REVENUE table?'")
                    click.echo("   - 'What is the sum of revenue from DAILY_REVENUE?'")
                    click.echo("   - 'Show me the count of distinct products in DAILY_REVENUE'")
                continue
            
            # Display generated SQL
            generated_sql = sql_result.get("sql", "")
            if not generated_sql:
                click.echo("❌ No SQL generated")
                continue
                
            click.echo(f"✅ SQL Generated:")
            click.echo(f"🔧 {generated_sql}")
            
            # Ask user if they want to execute the SQL
            execute_sql = click.confirm(f"\n💭 Execute this SQL query?", default=True)
            if not execute_sql:
                click.echo("⏭️  Skipping execution")
                continue
            
            # Step 7: SQL Execution
            click.echo("⚡ Executing SQL...")
            
            # Prepare request for SQL execution
            exec_request = {
                "connection_id": client.connection_id,
                "sql": generated_sql,
                "table_name": primary_table
            }
            
            # Call the SQL execution endpoint
            exec_result = client.post(f"/connection/{client.connection_id}/execute-sql", exec_request)
            
            # Handle execution results
            if "error" in exec_result:
                click.echo(f"❌ Execution Error: {exec_result['error']}")
                continue
            
            execution_status = exec_result.get("execution_status", "unknown")
            
            if execution_status == "success":
                click.echo(f"✅ Query executed successfully!")
                
                # Display row count
                row_count = exec_result.get("row_count", 0)
                click.echo(f"📊 Returned {row_count} rows")
                
                # Display results
                if "result" in exec_result and exec_result["result"]:
                    results = exec_result["result"]
                    
                    # Show column headers
                    if "columns" in exec_result:
                        columns = exec_result["columns"]
                        click.echo(f"📋 Columns: {', '.join(columns)}")
                    
                    # Display sample results
                    click.echo(f"\n📋 Sample Results:")
                    max_rows = min(5, len(results))
                    for i in range(max_rows):
                        row_items = list(results[i].items())[:3]  # Show first 3 columns
                        row_display = ", ".join([f"{k}: {v}" for k, v in row_items])
                        click.echo(f"   Row {i+1}: {row_display}")
                    
                    if len(results) > max_rows:
                        click.echo(f"   ... and {len(results) - max_rows} more rows")
                    
                    # Ask if user wants a summary
                    if row_count > 0:
                        generate_summary = click.confirm(f"\n💭 Generate AI summary of results?", default=True)
                        if generate_summary:
                            click.echo("🤖 Generating summary...")
                            
                            # Prepare request for summary generation
                            summary_request = {
                                "connection_id": client.connection_id,
                                "query": user_query,
                                "sql": generated_sql,
                                "results": results
                            }
                            
                            # Call the summary generation endpoint
                            summary_result = client.post(f"/connection/{client.connection_id}/generate-summary", summary_request)
                            
                            if "error" in summary_result:
                                click.echo(f"❌ Summary Error: {summary_result['error']}")
                            elif "summary" in summary_result:
                                click.echo(f"✅ AI Summary:")
                                click.echo(f"📝 {summary_result['summary']}")
                            else:
                                click.echo("⚠️  No summary generated")
                        
                elif row_count == 0:
                    click.echo("📋 No data returned")
                    
            elif execution_status == "failed":
                click.echo(f"❌ Query execution failed!")
                if "sql_error" in exec_result:
                    click.echo(f"💥 SQL Error: {exec_result['sql_error']}")
                    
            else:
                click.echo(f"⚠️  Unknown execution status: {execution_status}")
                
            # Show success message if available
            if "message" in exec_result:
                click.echo(f"💬 {exec_result['message']}")
                
            click.echo("")  # Add spacing between queries
            
        except click.Abort:
            click.echo("\n👋 Thanks for using the Natural Query CLI!")
            break
        except KeyboardInterrupt:
            click.echo("\n👋 Thanks for using the Natural Query CLI!")
            break
        except Exception as e:
            click.echo(f"❌ An error occurred: {e}")
            continue_querying = click.confirm("💭 Continue querying?", default=True)
            if not continue_querying:
                break
    
    # Final Summary
    click.echo("\n" + "=" * 50)
    click.echo("🎉 Session Complete!")
    click.echo(f"📊 Account: {result['account']}")
    click.echo(f"👤 User: {result['user']}")
    click.echo(f"📄 YAML File: {selected_filename}")
    click.echo(f"🗄️ Database: {target_database}")
    click.echo(f"📂 Schema: {target_schema}")
    click.echo(f"💬 Queries Processed: {query_count}")
    click.echo("✅ Thank you for using Natural Query CLI!")
    click.echo("=" * 50)

@cli.command()
def connect():
    """Simple connection test"""
    click.echo("🔗 Testing Snowflake connection...")
    result = client.post("/connect")
    if "connection_id" in result:
        client.connection_id = result["connection_id"]
        click.echo(f"✅ Connected: {result['connection_id'][:8]}...")
        click.echo(f"Account: {result['account']}, User: {result['user']}")
    else:
        click.echo(f"❌ Failed: {result}")

if __name__ == '__main__':
    cli()