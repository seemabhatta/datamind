#!/usr/bin/env python3
"""
Multi-Agent Orchestration CLI using OpenAI Agents Handoffs
Built with OpenAI Agent SDK handoff capabilities
"""

import click
import json
import yaml
import os
import sys
from typing import Optional, Dict, Any, List
from agents import Agent, Runner, function_tool, SQLiteSession, handoff
from agents.extensions import handoff_filters
from dataclasses import dataclass
from pydantic import BaseModel

# Add the project root to the path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# Import existing agent components
from src.cli.tools import (
    connect_to_snowflake_impl as connect_tool,
    get_current_context_impl as context_tool,
    get_databases_impl as databases_tool,
    select_database_impl as select_db_tool,
    get_schemas_impl as schemas_tool,
    select_schema_impl as select_schema_tool,
    get_stages_impl as stages_tool,
    select_stage_impl as select_stage_tool,
    get_yaml_files_impl as yaml_files_tool,
    load_yaml_file_impl as load_yaml_tool,
    get_yaml_content_impl as yaml_content_tool,
    generate_sql_impl as generate_sql_tool,
    execute_sql_impl as execute_sql_tool,
    generate_summary_impl as summary_tool
)

# Import dictionary generation tools separately
from src.cli.tools.dictionary_tools import (
    get_tables_impl as get_tables_tool,
    select_tables_impl as select_table_tool,
    generate_yaml_dictionary_impl as generate_dict_tool,
    save_dictionary_impl as save_dict_tool,
    upload_to_stage_impl as upload_stage_tool
)

@dataclass
class SharedContext:
    """Shared context across all agents"""
    connection_id: Optional[str] = None
    current_database: Optional[str] = None
    current_schema: Optional[str] = None
    current_stage: Optional[str] = None
    yaml_content: Optional[str] = None
    yaml_data: Optional[Dict] = None
    tables: List[Dict] = None
    dictionary_content: Optional[str] = None
    
    def __post_init__(self):
        if self.tables is None:
            self.tables = []

# Global shared context
shared_context = SharedContext()


# Connection Management Agent Tools
@function_tool
def connect_to_snowflake() -> str:
    """Connect to Snowflake and establish a connection"""
    return connect_tool(shared_context)

@function_tool
def get_databases() -> str:
    """Get list of available databases"""
    return databases_tool(shared_context)

@function_tool
def select_database(database_name: str) -> str:
    """Select a specific database to work with"""
    return select_db_tool(shared_context, database_name)

@function_tool
def get_schemas(database_name: Optional[str] = None) -> str:
    """Get schemas for a database"""
    return schemas_tool(shared_context, database_name)

@function_tool
def select_schema(schema_name: str) -> str:
    """Select a specific schema to work with"""
    return select_schema_tool(shared_context, schema_name)

@function_tool
def get_stages() -> str:
    """Get stages in the current database and schema"""
    return stages_tool(shared_context)

@function_tool
def select_stage(stage_name: str) -> str:
    """Select a specific stage to work with"""
    return select_stage_tool(shared_context, stage_name)

@function_tool
def get_current_context() -> str:
    """Get current agent context and state"""
    return context_tool(shared_context)

# Create Connection Management Agent
connection_agent = Agent(
    name="ConnectionAgent",
    instructions="""
    You are a Connection Management Agent specialized in handling Snowflake database connections and navigation.
    
    Your responsibilities:
    - Connect to Snowflake databases
    - Navigate databases, schemas, and stages
    - Maintain connection state
    - Provide connection status information
    
    When a user needs to connect or navigate Snowflake resources, handle it efficiently and provide clear status updates.
    Always confirm successful connections and selections.
    """,
    tools=[
        connect_to_snowflake,
        get_databases,
        select_database,
        get_schemas,
        select_schema,
        get_stages,
        select_stage,
        get_current_context
    ]
)

# Dictionary Generation Agent Tools
@function_tool
def get_tables() -> str:
    """Get tables in the current database and schema"""
    return get_tables_tool(shared_context)

@function_tool
def select_tables(table_selection: str) -> str:
    """Select specific tables to work with"""
    return select_table_tool(shared_context, table_selection)

@function_tool
def generate_dictionary(output_filename: Optional[str] = None) -> str:
    """Generate data dictionary for the selected tables"""
    return generate_dict_tool(shared_context, output_filename)

@function_tool
def save_dictionary(filename: str) -> str:
    """Save the generated dictionary to a local file"""
    return save_dict_tool(shared_context, filename)

@function_tool
def upload_to_stage(stage_name: str, filename: str) -> str:
    """Upload the generated dictionary to a Snowflake stage"""
    return upload_stage_tool(shared_context, stage_name, filename)

# Dictionary Generation Agent
dictionary_agent = Agent(
    name="DictionaryAgent",
    instructions="""
    You are a Data Dictionary Generation Agent specialized in creating comprehensive YAML data dictionaries.
    
    Your responsibilities:
    - Analyze database tables and generate detailed data dictionaries
    - Extract schema information, column statistics, and metadata
    - Save dictionaries locally or upload to Snowflake stages
    - Provide rich, business-friendly documentation
    
    When generating dictionaries:
    1. Analyze table structure and data patterns
    2. Generate comprehensive YAML documentation
    3. Offer to save locally or upload to stage
    4. Provide summary of generated content
    
    Always ensure dictionaries are complete and well-formatted.
    """,
    tools=[
        get_tables,
        select_tables,
        generate_dictionary,
        save_dictionary,
        upload_to_stage,
        get_current_context
    ]
)

# Query Processing Agent Tools
@function_tool
def get_yaml_files() -> str:
    """Get YAML files from the current stage"""
    return yaml_files_tool(shared_context)

@function_tool
def load_yaml_file(filename: str) -> str:
    """Load and parse a YAML file from the current stage"""
    return load_yaml_tool(shared_context, filename)

@function_tool
def get_yaml_content() -> str:
    """Get the loaded YAML data dictionary content for analysis"""
    return yaml_content_tool(shared_context)

@function_tool
def generate_sql(query: str, table_name: Optional[str] = None) -> str:
    """Generate SQL from natural language query"""
    return generate_sql_tool(shared_context, query, table_name)

@function_tool
def execute_sql(sql: str, table_name: Optional[str] = None) -> str:
    """Execute SQL query and return results"""
    return execute_sql_tool(shared_context, sql, table_name)

@function_tool
def generate_summary(query: str, sql: str, results: str) -> str:
    """Generate AI summary of query results"""
    return summary_tool(shared_context, query, sql, results)

# Query Processing Agent
query_agent = Agent(
    name="QueryAgent",
    instructions="""
    You are a Natural Language Query Agent specialized in converting business questions into SQL queries and executing them.
    
    Your responsibilities:
    - Load and understand YAML data dictionaries
    - Convert natural language queries to SQL
    - Execute SQL queries against Snowflake
    - Generate business-friendly summaries of results
    - Handle complex analytical questions
    
    When processing queries:
    1. Ensure appropriate YAML dictionary is loaded
    2. Generate accurate SQL based on the schema
    3. Execute queries and return results
    4. Provide clear, business-friendly explanations
    
    Always validate queries and provide helpful error messages if issues occur.
    """,
    tools=[
        get_yaml_files,
        load_yaml_file,
        get_yaml_content,
        generate_sql,
        execute_sql,
        generate_summary,
        get_current_context
    ]
)

# Handoff callback functions
async def on_connection_handoff(ctx):
    """Callback when handing off to connection agent"""
    print(f"🔗 Routing to Connection Agent")

async def on_dictionary_handoff(ctx):
    """Callback when handing off to dictionary agent"""
    print(f"📚 Routing to Dictionary Agent")

async def on_query_handoff(ctx):
    """Callback when handing off to query agent"""
    print(f"🔍 Routing to Query Agent")

# Triage Agent - Main orchestrator
triage_agent = Agent(
    name="TriageAgent",
    instructions="""
    You are the Triage Agent - the main orchestrator for a multi-agent Snowflake data analysis system.
    
    You coordinate between three specialized agents:
    1. **ConnectionAgent** - Handles Snowflake connections, database/schema navigation
    2. **DictionaryAgent** - Generates YAML data dictionaries for tables
    3. **QueryAgent** - Processes natural language queries and executes SQL
    
    ROUTING LOGIC:
    
    **Route to ConnectionAgent when:**
    - User wants to connect to Snowflake
    - User needs to select/navigate databases, schemas, or stages
    - User asks about connection status
    - Examples: "connect to database", "show me databases", "select schema X"
    
    **Route to DictionaryAgent when:**
    - User wants to generate data dictionaries
    - User needs to create YAML documentation for tables
    - User wants to save or upload dictionaries
    - Examples: "generate dictionary for table X", "create YAML for HMDA_SAMPLE", "save dictionary"
    
    **Route to QueryAgent when:**
    - User has data analysis questions
    - User wants to run SQL queries via natural language
    - User needs to load existing YAML dictionaries for querying
    - Examples: "show me loan approval rates", "what's the average loan amount", "load YAML file"
    
    **Handle directly when:**
    - General questions about the system
    - Requests for help or guidance
    - Multi-step workflows that need coordination
    
    IMPORTANT:
    - Always consider the user's intent and current context
    - Provide brief explanations of why you're routing to specific agents
    - If unsure, ask clarifying questions rather than guessing
    - Maintain conversation flow and context between handoffs
    """,
    handoffs=[
        handoff(
            agent=connection_agent,
            tool_name_override="route_to_connection_agent",
            tool_description_override="Route connection and navigation requests to the Connection Agent",
            on_handoff=on_connection_handoff
        ),
        handoff(
            agent=dictionary_agent,
            tool_name_override="route_to_dictionary_agent", 
            tool_description_override="Route dictionary generation requests to the Dictionary Agent",
            on_handoff=on_dictionary_handoff
        ),
        handoff(
            agent=query_agent,
            tool_name_override="route_to_query_agent",
            tool_description_override="Route data analysis and querying requests to the Query Agent", 
            on_handoff=on_query_handoff
        )
    ]
)

@click.group()
def cli():
    """Multi-Agent Orchestration CLI for Snowflake Data Analysis"""
    pass

@cli.command()
@click.option('--query', '-q', help='Initial query to process')
@click.option('--session-id', '-s', help='Session ID for conversation memory (default: auto-generated)')
def agent(query, session_id):
    """Start the multi-agent orchestration session"""
    click.echo("🤖 Multi-Agent Snowflake Analysis System")
    click.echo("=" * 60)
    click.echo("💡 Intelligent agent orchestration for data analysis!")
    click.echo("🔗 ConnectionAgent - Database connections & navigation")
    click.echo("📚 DictionaryAgent - YAML data dictionary generation") 
    click.echo("🔍 QueryAgent - Natural language querying")
    click.echo("🎯 TriageAgent - Smart routing & coordination")
    click.echo("🔧 Type 'quit', 'exit', or press Ctrl+C to stop")
    click.echo("=" * 60)
    
    # Create session for conversation memory
    if not session_id:
        import time
        session_id = f"multi_agent_session_{int(time.time())}"
    
    session = SQLiteSession(session_id)
    click.echo(f"📝 Session ID: {session_id}")
    
    # Start with initial query if provided
    if query:
        click.echo(f"\n👤 User: {query}")
        result = Runner.run_sync(triage_agent, query, session=session)
        click.echo(f"🤖 Assistant: {result.final_output}")
    else:
        # Initial greeting and setup
        click.echo("\n🔄 Multi-agent system ready!")
        initial_prompt = "Hello! I'm ready to help with Snowflake data analysis. I can connect to databases, generate data dictionaries, and answer your data questions. What would you like to do?"
        result = Runner.run_sync(triage_agent, initial_prompt, session=session)
        click.echo(f"🤖 Assistant: {result.final_output}")
    
    # Interactive loop
    while True:
        try:
            user_input = click.prompt("\n👤 You", type=str).strip()
            
            if user_input.lower() in ['quit', 'exit', 'q', 'stop']:
                click.echo("👋 Thanks for using the Multi-Agent Analysis System!")
                break
            
            if not user_input:
                click.echo("❌ Please enter a question or command.")
                continue
            
            click.echo(f"🤖 Assistant: ", nl=False)
            result = Runner.run_sync(triage_agent, user_input, session=session)
            click.echo(result.final_output)
            
        except click.Abort:
            click.echo("\n👋 Thanks for using the Multi-Agent Analysis System!")
            break
        except KeyboardInterrupt:
            click.echo("\n👋 Thanks for using the Multi-Agent Analysis System!")
            break
        except Exception as e:
            click.echo(f"❌ An error occurred: {e}")
            continue_session = click.confirm("💭 Continue session?", default=True)
            if not continue_session:
                break

if __name__ == '__main__':
    cli()