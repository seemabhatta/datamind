#!/usr/bin/env python3
"""
Agentic Natural Query CLI - AI agent for interactive Snowflake querying
Built with OpenAI Agent SDK
"""

import click
import requests
import json
import yaml
import os
from typing import Optional, Dict, Any, List
from agents import Agent, Runner, function_tool
from dataclasses import dataclass

BASE_URL = "http://localhost:8001"

@dataclass
class AgentContext:
    """Stores agent context and state"""
    connection_id: Optional[str] = None
    current_database: Optional[str] = None
    current_schema: Optional[str] = None
    current_stage: Optional[str] = None
    yaml_content: Optional[str] = None
    yaml_data: Optional[Dict] = None
    tables: List[Dict] = None
    
    def __post_init__(self):
        if self.tables is None:
            self.tables = []

# Global context for the agent
agent_context = AgentContext()

class APIClient:
    """Wrapper for existing API calls"""
    
    @staticmethod
    def post(endpoint: str, data: Optional[Dict] = None) -> Dict:
        response = requests.post(f"{BASE_URL}{endpoint}", json=data)
        return response.json() if response.status_code == 200 else {"error": response.text}
    
    @staticmethod
    def get(endpoint: str, params: Optional[Dict] = None) -> Dict:
        response = requests.get(f"{BASE_URL}{endpoint}", params=params)
        return response.json() if response.status_code == 200 else {"error": response.text}

# Tool Functions for Agent SDK
@function_tool
def connect_to_snowflake() -> str:
    """Connect to Snowflake and establish a connection"""
    result = APIClient.post("/connect")
    if "connection_id" in result:
        agent_context.connection_id = result["connection_id"]
        return f"✅ Connected to {result['account']} as {result['user']} (Connection ID: {result['connection_id'][:8]}...)"
    else:
        return f"❌ Connection failed: {result.get('error', 'Unknown error')}"

@function_tool
def get_databases() -> str:
    """Get list of available databases"""
    if not agent_context.connection_id:
        return "❌ No connection established. Please connect first."
    
    result = APIClient.get(f"/connection/{agent_context.connection_id}/databases")
    if "databases" in result:
        databases = result["databases"]
        return f"📊 Found {len(databases)} databases: {', '.join(databases)}"
    else:
        return f"❌ Failed to get databases: {result.get('error', 'Unknown error')}"

@function_tool
def select_database(database_name: str) -> str:
    """Select a specific database to work with"""
    if not agent_context.connection_id:
        return "❌ No connection established. Please connect first."
    
    # Verify database exists
    result = APIClient.get(f"/connection/{agent_context.connection_id}/databases")
    if "databases" not in result:
        return f"❌ Failed to verify database: {result.get('error', 'Unknown error')}"
    
    if database_name not in result["databases"]:
        return f"❌ Database '{database_name}' not found. Available: {', '.join(result['databases'])}"
    
    agent_context.current_database = database_name
    return f"✅ Selected database: {database_name}"

@function_tool
def get_schemas(database_name: Optional[str] = None) -> str:
    """Get schemas for a database"""
    if not agent_context.connection_id:
        return "❌ No connection established. Please connect first."
    
    db_name = database_name or agent_context.current_database
    if not db_name:
        return "❌ No database specified. Please select a database first."
    
    result = APIClient.get(f"/connection/{agent_context.connection_id}/schemas", 
                          params={"database": db_name})
    if "schemas" in result:
        schemas = result["schemas"]
        return f"📂 Found {len(schemas)} schemas in {db_name}: {', '.join(schemas)}"
    else:
        return f"❌ Failed to get schemas: {result.get('error', 'Unknown error')}"

@function_tool
def select_schema(schema_name: str) -> str:
    """Select a specific schema to work with"""
    if not agent_context.current_database:
        return "❌ No database selected. Please select a database first."
    
    # Verify schema exists
    result = APIClient.get(f"/connection/{agent_context.connection_id}/schemas", 
                          params={"database": agent_context.current_database})
    if "schemas" not in result:
        return f"❌ Failed to verify schema: {result.get('error', 'Unknown error')}"
    
    if schema_name not in result["schemas"]:
        return f"❌ Schema '{schema_name}' not found. Available: {', '.join(result['schemas'])}"
    
    agent_context.current_schema = schema_name
    return f"✅ Selected schema: {schema_name}"

@function_tool
def get_stages() -> str:
    """Get stages in the current database and schema"""
    if not agent_context.connection_id:
        return "❌ No connection established. Please connect first."
    
    if not agent_context.current_database or not agent_context.current_schema:
        return "❌ Database and schema must be selected first."
    
    result = APIClient.get(f"/connection/{agent_context.connection_id}/stages", 
                          params={"database": agent_context.current_database, 
                                "schema": agent_context.current_schema})
    if "stages" in result:
        stages = result["stages"]
        stage_info = [f"{s['name']} ({s['type']})" for s in stages]
        return f"📋 Found {len(stages)} stages: {', '.join(stage_info)}"
    else:
        return f"❌ Failed to get stages: {result.get('error', 'Unknown error')}"

@function_tool
def select_stage(stage_name: str) -> str:
    """Select a specific stage to work with"""
    if not agent_context.current_database or not agent_context.current_schema:
        return "❌ Database and schema must be selected first."
    
    # Verify stage exists
    result = APIClient.get(f"/connection/{agent_context.connection_id}/stages", 
                          params={"database": agent_context.current_database, 
                                "schema": agent_context.current_schema})
    if "stages" not in result:
        return f"❌ Failed to verify stage: {result.get('error', 'Unknown error')}"
    
    stage_names = [s['name'] for s in result["stages"]]
    if stage_name not in stage_names:
        return f"❌ Stage '{stage_name}' not found. Available: {', '.join(stage_names)}"
    
    agent_context.current_stage = f"@{agent_context.current_database}.{agent_context.current_schema}.{stage_name}"
    return f"✅ Selected stage: {agent_context.current_stage}"

@function_tool
def get_yaml_files() -> str:
    """Get YAML files from the current stage"""
    if not agent_context.current_stage:
        return "❌ No stage selected. Please select a stage first."
    
    result = APIClient.get(f"/connection/{agent_context.connection_id}/stage-files", 
                          params={"stage_name": agent_context.current_stage})
    if "files" in result:
        files = result["files"]
        yaml_files = [f for f in files if f["name"].endswith(('.yaml', '.yml'))]
        if yaml_files:
            file_info = [f"{f['name'].split('/')[-1]} ({f['size']} bytes)" for f in yaml_files]
            return f"📄 Found {len(yaml_files)} YAML files: {', '.join(file_info)}"
        else:
            return f"❌ No YAML files found. Available files: {[f['name'] for f in files]}"
    else:
        return f"❌ Failed to get files: {result.get('error', 'Unknown error')}"

@function_tool
def load_yaml_file(filename: str) -> str:
    """Load and parse a YAML file from the current stage"""
    if not agent_context.current_stage:
        return "❌ No stage selected. Please select a stage first."
    
    # Load YAML content
    result = APIClient.get(f"/connection/{agent_context.connection_id}/load-stage-file", 
                          params={"stage_name": agent_context.current_stage, 
                                "file_name": filename})
    
    if "content" not in result:
        return f"❌ Failed to load YAML file: {result.get('error', 'Unknown error')}"
    
    yaml_content = result["content"]
    
    # Parse YAML
    try:
        yaml_data = yaml.safe_load(yaml_content)
        agent_context.yaml_content = yaml_content
        agent_context.yaml_data = yaml_data
        
        # Extract table information
        tables = []
        if "tables" in yaml_data:
            for table in yaml_data["tables"]:
                if "base_table" in table:
                    base_table = table["base_table"]
                    tables.append({
                        "name": table.get("name", "Unknown"),
                        "database": base_table.get("database", ""),
                        "schema": base_table.get("schema", ""),
                        "full_name": f"{base_table.get('database', '')}.{base_table.get('schema', '')}.{table.get('name', '')}"
                    })
        
        agent_context.tables = tables
        
        return f"✅ Loaded and parsed {filename} ({len(yaml_content)} chars). Found {len(tables)} tables: {[t['name'] for t in tables]}"
        
    except yaml.YAMLError as e:
        return f"❌ Failed to parse YAML: {e}"

@function_tool
def generate_sql(query: str, table_name: Optional[str] = None) -> str:
    """Generate SQL from natural language query"""
    if not agent_context.connection_id:
        return "❌ No connection established. Please connect first."
    
    if not agent_context.yaml_content:
        return "❌ No YAML file loaded. Please load a data dictionary first."
    
    # Use first table if not specified
    if not table_name and agent_context.tables:
        table_name = agent_context.tables[0]['name']
    
    if not table_name:
        return "❌ No table specified and no tables available."
    
    sql_request = {
        "query": query,
        "connection_id": agent_context.connection_id,
        "table_name": table_name,
        "dictionary_content": agent_context.yaml_content
    }
    
    result = APIClient.post(f"/connection/{agent_context.connection_id}/generate-sql", sql_request)
    
    if "error" in result:
        return f"❌ SQL generation failed: {result['error']}"
    
    intent = result.get("intent", "unknown")
    if intent != "SQL_QUERY":
        return f"💡 Intent: {intent} - {result.get('message', 'Non-SQL query detected')}"
    
    sql = result.get("sql", "")
    if not sql:
        return "❌ No SQL generated"
    
    return f"✅ Generated SQL: {sql}"

@function_tool
def execute_sql(sql: str, table_name: Optional[str] = None) -> str:
    """Execute SQL query and return results"""
    if not agent_context.connection_id:
        return "❌ No connection established. Please connect first."
    
    if not table_name and agent_context.tables:
        table_name = agent_context.tables[0]['name']
    
    exec_request = {
        "connection_id": agent_context.connection_id,
        "sql": sql,
        "table_name": table_name or "unknown"
    }
    
    result = APIClient.post(f"/connection/{agent_context.connection_id}/execute-sql", exec_request)
    
    if "error" in result:
        return f"❌ SQL execution failed: {result['error']}"
    
    status = result.get("status", "unknown")
    if status == "success":
        row_count = result.get("row_count", 0)
        response = f"✅ Query executed successfully! Returned {row_count} rows."
        
        if "result" in result and result["result"]:
            results = result["result"]
            response += f"\n📋 Sample results (first 3 rows):\n"
            for i, row in enumerate(results[:3]):
                row_items = list(row.items())[:3]  # First 3 columns
                row_display = ", ".join([f"{k}: {v}" for k, v in row_items])
                response += f"  Row {i+1}: {row_display}\n"
        
        return response
    else:
        return f"❌ Query execution failed: {result.get('sql_error', 'Unknown error')}"

@function_tool
def generate_summary(query: str, sql: str, results: str) -> str:
    """Generate AI summary of query results"""
    if not agent_context.connection_id:
        return "❌ No connection established. Please connect first."
    
    summary_request = {
        "connection_id": agent_context.connection_id,
        "query": query,
        "sql": sql,
        "results": results
    }
    
    result = APIClient.post(f"/connection/{agent_context.connection_id}/generate-summary", summary_request)
    
    if "error" in result:
        return f"❌ Summary generation failed: {result['error']}"
    
    if "summary" in result:
        return f"📝 AI Summary: {result['summary']}"
    else:
        return "⚠️ No summary generated"

@function_tool
def get_current_context() -> str:
    """Get current agent context and state"""
    context_info = []
    
    if agent_context.connection_id:
        context_info.append(f"🔗 Connected (ID: {agent_context.connection_id[:8]}...)")
    else:
        context_info.append("❌ Not connected")
    
    if agent_context.current_database:
        context_info.append(f"🗄️ Database: {agent_context.current_database}")
    
    if agent_context.current_schema:
        context_info.append(f"📂 Schema: {agent_context.current_schema}")
    
    if agent_context.current_stage:
        context_info.append(f"📋 Stage: {agent_context.current_stage}")
    
    if agent_context.yaml_content:
        context_info.append(f"📄 YAML loaded ({len(agent_context.yaml_content)} chars)")
    
    if agent_context.tables:
        table_names = [t['name'] for t in agent_context.tables]
        context_info.append(f"📊 Tables: {', '.join(table_names)}")
    
    return "\n".join(context_info) if context_info else "No context available"

# Agent Instructions
AGENT_INSTRUCTIONS = """
You are a Snowflake Query Assistant that helps users interact with their Snowflake data using natural language.

Your capabilities:
1. Connect to Snowflake databases
2. Browse database structures (databases, schemas, stages)
3. Load and parse YAML data dictionaries
4. Convert natural language queries to SQL
5. Execute SQL queries and show results
6. Generate AI summaries of query results

Workflow:
1. Always start by connecting to Snowflake if not already connected
2. Help users navigate to the right database/schema/stage
3. Load the appropriate YAML data dictionary
4. Process their natural language queries
5. Generate and execute SQL
6. Provide clear, helpful results

Guidelines:
- Be conversational and helpful
- Always check current context before proceeding
- Guide users through the workflow step by step
- Handle errors gracefully and suggest solutions
- Provide clear feedback on what's happening
- Ask clarifying questions when needed

Use the available tools to help users accomplish their goals efficiently.
"""

# Create the agent
snowflake_agent = Agent(
    name="SnowflakeQueryAgent",
    instructions=AGENT_INSTRUCTIONS,
    tools=[
        connect_to_snowflake,
        get_databases,
        select_database,
        get_schemas,
        select_schema,
        get_stages,
        select_stage,
        get_yaml_files,
        load_yaml_file,
        generate_sql,
        execute_sql,
        generate_summary,
        get_current_context
    ]
)

@click.group()
def cli():
    """Agentic Natural Language Query CLI for Snowflake"""
    pass

@cli.command()
@click.option('--query', '-q', help='Initial query to process')
def agent(query):
    """Start the agentic query session"""
    click.echo("🤖 Agentic Snowflake Query Assistant")
    click.echo("=" * 50)
    click.echo("💡 I can help you query your Snowflake data using natural language!")
    click.echo("💬 Just tell me what you want to do, and I'll guide you through it.")
    click.echo("🔧 Type 'quit', 'exit', or press Ctrl+C to stop")
    click.echo("=" * 50)
    
    # Start with initial query if provided
    if query:
        click.echo(f"\n👤 User: {query}")
        result = Runner.run_sync(snowflake_agent, query)
        click.echo(f"🤖 Assistant: {result.final_output}")
    
    # Interactive loop
    while True:
        try:
            user_input = click.prompt("\n👤 You", type=str).strip()
            
            if user_input.lower() in ['quit', 'exit', 'q', 'stop']:
                click.echo("👋 Thanks for using the Agentic Query Assistant!")
                break
            
            if not user_input:
                click.echo("❌ Please enter a question or command.")
                continue
            
            click.echo(f"🤖 Assistant: ", nl=False)
            result = Runner.run_sync(snowflake_agent, user_input)
            click.echo(result.final_output)
            
        except click.Abort:
            click.echo("\n👋 Thanks for using the Agentic Query Assistant!")
            break
        except KeyboardInterrupt:
            click.echo("\n👋 Thanks for using the Agentic Query Assistant!")
            break
        except Exception as e:
            click.echo(f"❌ An error occurred: {e}")
            continue_session = click.confirm("💭 Continue session?", default=True)
            if not continue_session:
                break

if __name__ == '__main__':
    cli()