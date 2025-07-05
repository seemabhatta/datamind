#!/usr/bin/env python3
"""
Natural Query CLI - Interactive natural language querying of Snowflake data
"""

import click
import requests
import json
import sys

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
    
    while True:
        try:
            choice = click.prompt("\nSelect YAML file number", type=int)
            if 1 <= choice <= len(yaml_files):
                selected_file = yaml_files[choice - 1]
                break
            else:
                click.echo("❌ Invalid choice. Please try again.")
        except (ValueError, click.Abort):
            click.echo("❌ Invalid input. Please enter a number.")
    
    selected_filename = selected_file["name"].split('/')[-1] if '/' in selected_file["name"] else selected_file["name"]
    click.echo(f"Selected YAML file: {selected_filename}")
    
    # Summary
    click.echo("\n" + "=" * 50)
    click.echo("🎉 Step 1 Complete!")
    click.echo(f"📊 Account: {result['account']}")
    click.echo(f"👤 User: {result['user']}")
    click.echo(f"🔗 Connection: {result['connection_id'][:8]}...")
    click.echo(f"🗄️ Database: {selected_database}")
    click.echo(f"📂 Schema: {selected_schema}")
    click.echo(f"🏪 Stage: {selected_stage['name']}")
    click.echo(f"📄 YAML File: {selected_filename}")
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