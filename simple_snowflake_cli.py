#!/usr/bin/env python3
"""
Super Simple Snowflake CLI - Auto-generated from OpenAPI
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
    """Simple Snowflake API CLI"""
    pass

@cli.command()
def connect():
    """Connect to Snowflake"""
    result = client.post("/connect")
    if "connection_id" in result:
        client.connection_id = result["connection_id"]
        click.echo(f"✅ Connected: {result['connection_id'][:8]}...")
        click.echo(f"Account: {result['account']}, User: {result['user']}")
    else:
        click.echo(f"❌ Failed: {result}")

@cli.command()
@click.argument('database')
@click.argument('schema')
def list_tables(database, schema):
    """List tables in database.schema"""
    if not client.connection_id:
        result = client.post("/connect")
        client.connection_id = result.get("connection_id")
    
    result = client.get(f"/connection/{client.connection_id}/tables", 
                       params={"database": database, "schema": schema})
    
    if "tables" in result:
        click.echo(f"Tables in {database}.{schema}:")
        for table in result["tables"]:
            click.echo(f"  - {table['table']}")
    else:
        click.echo(f"❌ Error: {result}")

@cli.command()
@click.argument('database')
@click.argument('schema')
@click.argument('tables', nargs=-1, required=True)
@click.option('--output', '-o', default='data_dict.yaml', help='Output file')
def create_dict(database, schema, tables, output):
    """Create data dictionary from tables"""
    if not client.connection_id:
        result = client.post("/connect")
        client.connection_id = result.get("connection_id")
    
    # Generate dictionary
    data = {
        "connection_id": client.connection_id,
        "tables": list(tables),
        "database_name": database,
        "schema_name": schema
    }
    
    click.echo(f"🔍 Analyzing {len(tables)} tables...")
    result = client.post(f"/connection/{client.connection_id}/generate-data-dictionary", data)
    
    if "yaml_dictionary" in result:
        with open(output, 'w') as f:
            f.write(result["yaml_dictionary"])
        click.echo(f"✅ Dictionary saved to {output}")
        click.echo(f"Tables processed: {result.get('tables_processed', 0)}")
    else:
        click.echo(f"❌ Error: {result}")

@cli.command()
@click.argument('query')
@click.argument('table')
@click.option('--dict-file', '-d', required=True, help='YAML dictionary file')
def query(query, table, dict_file):
    """Execute natural language query"""
    if not client.connection_id:
        result = client.post("/connect")
        client.connection_id = result.get("connection_id")
    
    try:
        with open(dict_file, 'r') as f:
            dict_content = f.read()
    except FileNotFoundError:
        click.echo(f"❌ Dictionary file not found: {dict_file}")
        return
    
    data = {
        "query": query,
        "connection_id": client.connection_id,
        "table_name": table,
        "dictionary_content": dict_content
    }
    
    click.echo(f"💬 Processing: {query}")
    result = client.post(f"/connection/{client.connection_id}/query", data)
    
    if "sql" in result:
        click.echo(f"Generated SQL: {result['sql']}")
        if result.get("execution_status") == "success":
            click.echo(f"✅ Executed successfully: {result.get('row_count', 0)} rows")
        elif result.get("execution_status") == "failed":
            click.echo(f"❌ Execution failed: {result.get('sql_error')}")
    else:
        click.echo(f"Result: {result}")

# Interactive workflow command
@cli.command()
@click.option('--database', help='Database name (optional - will prompt if not provided)')
@click.option('--schema', help='Schema name (optional - will prompt if not provided)')
@click.option('--tables', help='Comma-separated table names (optional - will show options)')
def workflow(database, schema, tables):
    """Interactive workflow: connect -> select database -> select schema -> select tables -> generate dict"""
    click.echo("🚀 Interactive Snowflake Data Dictionary Workflow")
    click.echo("=" * 50)
    
    # Step 1: Connect
    click.echo("\n📋 Step 1: Connecting to Snowflake...")
    result = client.post("/connect")
    if "connection_id" not in result:
        click.echo(f"❌ Connection failed: {result}")
        return
    
    client.connection_id = result["connection_id"]
    click.echo(f"✅ Connected to {result['account']} as {result['user']}")
    
    # Step 2: Select Database
    click.echo("\n📋 Step 2: Select Database")
    if not database:
        db_result = client.get(f"/connection/{client.connection_id}/databases")
        if "databases" in db_result:
            click.echo("Available databases:")
            for i, db in enumerate(db_result["databases"], 1):
                click.echo(f"  {i}. {db}")
            
            while True:
                try:
                    choice = click.prompt("\nSelect database number", type=int)
                    if 1 <= choice <= len(db_result["databases"]):
                        database = db_result["databases"][choice - 1]
                        break
                    else:
                        click.echo("❌ Invalid choice. Please try again.")
                except (ValueError, click.Abort):
                    click.echo("❌ Invalid input. Please enter a number.")
        else:
            click.echo(f"❌ Failed to get databases: {db_result}")
            return
    
    click.echo(f"Selected database: {database}")
    
    # Step 3: Select Schema  
    click.echo(f"\n📋 Step 3: Select Schema from {database}")
    if not schema:
        schema_result = client.get(f"/connection/{client.connection_id}/schemas", 
                                 params={"database": database})
        if "schemas" in schema_result:
            click.echo("Available schemas:")
            for i, sch in enumerate(schema_result["schemas"], 1):
                click.echo(f"  {i}. {sch}")
            
            while True:
                try:
                    choice = click.prompt("\nSelect schema number", type=int)
                    if 1 <= choice <= len(schema_result["schemas"]):
                        schema = schema_result["schemas"][choice - 1]
                        break
                    else:
                        click.echo("❌ Invalid choice. Please try again.")
                except (ValueError, click.Abort):
                    click.echo("❌ Invalid input. Please enter a number.")
        else:
            click.echo(f"❌ Failed to get schemas: {schema_result}")
            return
    
    click.echo(f"Selected schema: {schema}")
    
    # Step 4: Select Tables
    click.echo(f"\n📋 Step 4: Select Tables from {database}.{schema}")
    
    tables_result = client.get(f"/connection/{client.connection_id}/tables",
                              params={"database": database, "schema": schema})
    
    if "tables" not in tables_result:
        click.echo(f"❌ Failed to get tables: {tables_result}")
        return
    
    available_tables = tables_result["tables"]
    if not available_tables:
        click.echo("❌ No tables found in this schema")
        return
    
    click.echo("Available tables:")
    for i, table in enumerate(available_tables, 1):
        click.echo(f"  {i}. {table['table']} ({table['table_type']})")
    
    # Table selection options
    click.echo("\n💭 Table selection options:")
    click.echo("  1. Select specific tables by number")
    click.echo("  2. Select all tables")
    
    while True:
        try:
            selection_type = click.prompt("\nChoose selection method (1 or 2)", type=int)
            if selection_type in [1, 2]:
                break
            else:
                click.echo("❌ Please enter 1 or 2")
        except (ValueError, click.Abort):
            click.echo("❌ Invalid input. Please enter 1 or 2.")
    
    selected_tables = []
    
    if selection_type == 1:
        # Individual selection
        click.echo("\nEnter table numbers (comma-separated, e.g., 1,3,5):")
        while not selected_tables:
            try:
                table_input = click.prompt("Table numbers")
                indices = [int(x.strip()) for x in table_input.split(',')]
                selected_tables = [
                    available_tables[i-1]['table'] 
                    for i in indices 
                    if 1 <= i <= len(available_tables)
                ]
                if selected_tables:
                    click.echo(f"Selected tables: {', '.join(selected_tables)}")
                else:
                    click.echo("❌ No valid tables selected. Please try again.")
            except (ValueError, click.Abort):
                click.echo("❌ Invalid input. Please enter numbers separated by commas.")
    
    elif selection_type == 2:
        # Select all
        selected_tables = [table['table'] for table in available_tables]
        click.echo(f"Selected all {len(selected_tables)} tables")
    
    # Step 5: Generate Dictionary
    click.echo(f"\n📋 Step 5: Generating Data Dictionary")
    click.echo(f"Processing {len(selected_tables)} tables...")
    
    data = {
        "connection_id": client.connection_id,
        "tables": selected_tables,
        "database_name": database,
        "schema_name": schema
    }
    
    result = client.post(f"/connection/{client.connection_id}/generate-data-dictionary", data)
    
    if "yaml_dictionary" in result:
        # Ask for output filename
        default_filename = f"{database}_{schema}_dict.yaml"
        output_file = click.prompt(f"\nOutput filename", default=default_filename)
        
        with open(output_file, 'w') as f:
            f.write(result["yaml_dictionary"])
        
        # Summary
        click.echo("\n" + "=" * 50)
        click.echo("🎉 Workflow Complete!")
        click.echo(f"📊 Database: {database}")
        click.echo(f"📂 Schema: {schema}")
        click.echo(f"📋 Tables: {len(selected_tables)} ({', '.join(selected_tables)})")
        click.echo(f"📄 Output: {output_file}")
        click.echo(f"✅ Status: Success")
        click.echo("=" * 50)
    else:
        click.echo(f"❌ Failed to generate dictionary: {result}")

# Quick workflow for when you know what you want
@cli.command()
@click.argument('database')
@click.argument('schema')  
@click.argument('tables', nargs=-1, required=True)
def quick_workflow(database, schema, tables):
    """Quick workflow when you know database, schema, and tables"""
    click.echo("🚀 Quick workflow...")
    
    # Connect
    result = client.post("/connect")
    if "connection_id" not in result:
        click.echo(f"❌ Connection failed: {result}")
        return
    
    client.connection_id = result["connection_id"]
    click.echo(f"✅ Connected to {result['account']}")
    
    # Generate dictionary
    data = {
        "connection_id": client.connection_id,
        "tables": list(tables),
        "database_name": database,
        "schema_name": schema
    }
    
    click.echo(f"🔍 Processing {len(tables)} tables...")
    result = client.post(f"/connection/{client.connection_id}/generate-data-dictionary", data)
    
    if "yaml_dictionary" in result:
        output_file = f"{database}_{schema}_dict.yaml"
        with open(output_file, 'w') as f:
            f.write(result["yaml_dictionary"])
        
        click.echo(f"✅ Complete! Dictionary saved to {output_file}")
    else:
        click.echo(f"❌ Failed: {result}")

if __name__ == '__main__':
    cli()