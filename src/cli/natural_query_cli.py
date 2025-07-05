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
    
    # Handle multiple databases/schemas (though usually should be one)
    if len(databases_found) > 1:
        click.echo(f"⚠️  Multiple databases found: {', '.join(databases_found)}")
        click.echo("Please select which database to use:")
        for i, db in enumerate(databases_found, 1):
            click.echo(f"  {i}. {db}")
        
        while True:
            try:
                choice = click.prompt("Select database number", type=int)
                if 1 <= choice <= len(databases_found):
                    target_database = databases_found[choice - 1]
                    break
                else:
                    click.echo("❌ Invalid choice. Please try again.")
            except (ValueError, click.Abort):
                click.echo("❌ Invalid input. Please enter a number.")
    else:
        target_database = databases_found[0]
    
    if len(schemas_found) > 1:
        click.echo(f"⚠️  Multiple schemas found: {', '.join(schemas_found)}")
        click.echo("Please select which schema to use:")
        for i, schema in enumerate(schemas_found, 1):
            click.echo(f"  {i}. {schema}")
        
        while True:
            try:
                choice = click.prompt("Select schema number", type=int)
                if 1 <= choice <= len(schemas_found):
                    target_schema = schemas_found[choice - 1]
                    break
                else:
                    click.echo("❌ Invalid choice. Please try again.")
            except (ValueError, click.Abort):
                click.echo("❌ Invalid input. Please enter a number.")
    else:
        target_schema = schemas_found[0]
    
    click.echo(f"\n🎯 Target Context:")
    click.echo(f"   🗄️  Database: {target_database}")
    click.echo(f"   📂 Schema: {target_schema}")
    
    # Confirmation
    proceed = click.confirm(f"\n💭 Do you want to proceed with database '{target_database}' and schema '{target_schema}'?", default=True)
    if not proceed:
        click.echo("👋 Operation cancelled. Goodbye!")
        return
    
    # Update yaml_context with confirmed targets
    yaml_context["target_database"] = target_database
    yaml_context["target_schema"] = target_schema
    
    # Summary
    click.echo("\n" + "=" * 50)
    click.echo("🎉 Step 3 Complete!")
    click.echo(f"📊 Account: {result['account']}")
    click.echo(f"👤 User: {result['user']}")
    click.echo(f"🔗 Connection: {result['connection_id'][:8]}...")
    click.echo(f"📄 YAML File: {selected_filename}")
    click.echo(f"🗄️ Target Database: {target_database}")
    click.echo(f"📂 Target Schema: {target_schema}")
    click.echo(f"📋 Tables: {len(tables_found)} tables")
    click.echo("✅ Ready for next step!")
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