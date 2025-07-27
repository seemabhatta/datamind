#!/usr/bin/env python3
"""
Agentic YAML Dictionary Generator CLI - AI agent for interactive Snowflake dictionary generation
Built with OpenAI Agent SDK
"""

import click
import json
import yaml
import os
import sys
from typing import Optional, Dict, Any, List
from agents import Agent, Runner, function_tool, SQLiteSession
from dataclasses import dataclass

# Add the project root to the path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# Import our function tools
from src.cli.tools import (
    connect_to_snowflake_impl as connect_tool,
    get_current_context_impl as context_tool,
    get_databases_impl as databases_tool,
    select_database_impl as select_db_tool,
    get_schemas_impl as schemas_tool,
    select_schema_impl as select_schema_tool,
    get_stages_impl as stages_tool,
    select_stage_impl as select_stage_tool,
)

# Import dictionary-specific tools
from src.cli.tools.dictionary_tools import (
    get_tables_impl as get_tables_tool,
    select_tables_impl as select_tables_tool,
    generate_yaml_dictionary_impl as generate_dict_tool,
    save_dictionary_impl as save_dict_tool,
    upload_to_stage_impl as upload_tool,
    show_dictionary_preview_impl as preview_tool,
)

@dataclass
class AgentContext:
    """Stores agent context and state"""
    connection_id: Optional[str] = None
    current_database: Optional[str] = None
    current_schema: Optional[str] = None
    current_stage: Optional[str] = None
    selected_tables: List[str] = None
    dictionary_content: Optional[str] = None
    
    def __post_init__(self):
        if self.selected_tables is None:
            self.selected_tables = []

# Global context for the agent
agent_context = AgentContext()

# Tool Functions for Agent SDK - wrapper functions with agent_context

@function_tool
def connect_to_snowflake() -> str:
    """Connect to Snowflake and establish a connection"""
    return connect_tool(agent_context)

@function_tool
def get_databases() -> str:
    """Get list of available databases"""
    return databases_tool(agent_context)

@function_tool
def select_database(database_name: str) -> str:
    """Select a specific database to work with"""
    print(f"DEBUG: select_database() called with database_name='{database_name}'")
    result = select_db_tool(agent_context, database_name)
    print(f"DEBUG: select_database() result: {result}")
    return result

@function_tool
def get_schemas(database_name: Optional[str] = None) -> str:
    """Get schemas for a database"""
    print(f"DEBUG: get_schemas() called with database_name='{database_name}'")
    result = schemas_tool(agent_context, database_name)
    print(f"DEBUG: get_schemas() result: {result}")
    return result

@function_tool
def select_schema(schema_name: str) -> str:
    """Select a specific schema to work with"""
    print(f"DEBUG: select_schema() called with schema_name='{schema_name}'")
    result = select_schema_tool(agent_context, schema_name)
    print(f"DEBUG: select_schema() result: {result}")
    return result

@function_tool
def get_tables() -> str:
    """Get tables in the current database and schema"""
    print("DEBUG: get_tables() wrapper called")
    result = get_tables_tool(agent_context)
    print(f"DEBUG: get_tables() result: {result}")
    return result

@function_tool
def select_tables(table_selection: str) -> str:
    """Select tables for dictionary generation. Use 'all' for all tables, or comma-separated numbers like '1,3,5'"""
    print(f"DEBUG: select_tables() wrapper called with table_selection='{table_selection}'")
    result = select_tables_tool(agent_context, table_selection)
    print(f"DEBUG: select_tables() result: {result}")
    return result

@function_tool
def generate_yaml_dictionary(output_filename: Optional[str] = None) -> str:
    """Generate YAML data dictionary from selected tables"""
    return generate_dict_tool(agent_context, output_filename)

@function_tool
def save_dictionary(filename: str) -> str:
    """Save the generated dictionary to a file"""
    return save_dict_tool(agent_context, filename)

@function_tool
def upload_to_stage(stage_name: str, filename: str) -> str:
    """Upload the generated dictionary to a Snowflake stage"""
    return upload_tool(agent_context, stage_name, filename)

@function_tool
def get_stages() -> str:
    """Get stages in the current database and schema"""
    return stages_tool(agent_context)

@function_tool
def get_current_context() -> str:
    """Get current agent context and state"""
    return context_tool(agent_context)

@function_tool
def show_dictionary_preview() -> str:
    """Show a preview of the generated dictionary"""
    return preview_tool(agent_context)

# Agent Instructions
AGENT_INSTRUCTIONS = """
You are a Snowflake Data Dictionary Generator Assistant that helps users create YAML data dictionaries from their Snowflake tables.

Your capabilities:
1. Connect to Snowflake databases
2. Browse database structures (databases, schemas, tables)
3. Select tables for dictionary generation
4. Generate comprehensive YAML data dictionaries
5. Save dictionaries to local files
6. Upload dictionaries to Snowflake stages

IMPORTANT BEHAVIORAL GUIDELINES:
- Be conversational and flexible - users can express their intent in ANY way
- When you show a list of tables, users might say: "HMDA_SAMPLE", "the second one", "2", "table 2", "select HMDA", "generate dictionary for HMDA_SAMPLE", etc.
- ALWAYS interpret user intent intelligently based on context
- If user mentions a table name that exists, select it immediately
- If user says "generate" or "create" after seeing tables, proceed with generation
- Don't be rigid about format - be helpful and smart about what users mean
- Take action immediately when intent is clear

CRITICAL CONTEXTUAL RESPONSE RULES - FOLLOW THESE EXACTLY:
1. When you show a list of TABLES and user responds with a number, ONLY call select_tables()
2. When you show a list of DATABASES and user responds with a number, ONLY call select_database()
3. When you show a list of SCHEMAS and user responds with a number, ONLY call select_schema()
4. NEVER EVER call select_database() after showing tables
5. NEVER EVER call select_schema() after showing tables
6. NEVER EVER call get_tables() after showing tables
7. If user says "2" after you show tables, call select_tables("2") - DO NOT call anything else

CONTEXTUAL RESPONSE EXAMPLES:
Example 1:
Assistant: "I found 2 databases: 1. CORTES_DEMO_2  2. SNOWFLAKE. Which would you like to explore?"
User: "1"
Assistant: [calls select_database("CORTES_DEMO_2") immediately - because last message was about DATABASES]

Example 2:
Assistant: "Available tables: 1. DAILY_REVENUE  2. HMDA_SAMPLE  3. MORTGAGE_LENDING_RATES"
User: "2"
Assistant: [calls select_tables("2") immediately - because last message was about TABLES]

Example 3:
Assistant: "Available tables: 1. CUSTOMERS  2. ORDERS  3. PRODUCTS"
User: "HMDA_SAMPLE"
Assistant: [calls select_tables("HMDA_SAMPLE") then generate_yaml_dictionary() immediately]

Example 4:
Assistant: "Available tables: 1. CUSTOMERS  2. ORDERS  3. PRODUCTS"
User: "generate"
Assistant: [calls select_tables("all") then generate_yaml_dictionary() immediately]

WRONG EXAMPLES TO AVOID:
❌ If last message showed tables and user says "2", DO NOT call select_database()
❌ If last message showed databases and user says "2", DO NOT call select_tables()
❌ NEVER ignore the context of what you just presented to the user

SIMPLIFIED WORKFLOW - FOLLOW THIS EXACTLY:
1. Connect to Snowflake
2. Get databases and let user select ONE
3. Get schemas and let user select ONE  
4. Get tables and let user select which ones
5. Generate dictionary immediately after table selection
6. Save to file

IMPORTANT: After step 4 (showing tables), the ONLY valid next action is select_tables() followed by generate_yaml_dictionary()
DO NOT call any other database/schema tools after showing tables to user.

Auto-initialization Steps:
- Connect to Snowflake immediately
- Get databases, let user select or auto-select first one
- Get schemas, let user select or auto-select first one
- Get tables and present options for user selection
- Generate dictionary once tables are selected

EFFICIENCY RULES:
- Avoid duplicate API calls - don't verify selections that were just made
- Use the most direct path to complete the workflow
- Don't call the same endpoint multiple times unnecessarily
- Once connected, reuse the same connection for all operations
- NEVER call connect_to_snowflake() more than once per session

Dictionary Generation Guidelines:
- Always show progress when generating dictionaries
- Provide clear feedback on what tables are being processed
- Offer to save locally and upload to stage
- Show preview of generated content when helpful
- Handle errors gracefully and suggest solutions

Use the available tools to help users create comprehensive data dictionaries efficiently.
"""

# Create the agent
dictionary_agent = Agent(
    name="SnowflakeDictionaryAgent",
    instructions=AGENT_INSTRUCTIONS,
    tools=[
        connect_to_snowflake,
        get_databases,
        select_database,
        get_schemas,
        select_schema,
        get_tables,
        select_tables,
        generate_yaml_dictionary,
        save_dictionary,
        upload_to_stage,
        get_stages,
        get_current_context,
        show_dictionary_preview
    ]
)

@click.group()
def cli():
    """Agentic YAML Dictionary Generator CLI for Snowflake"""
    pass

@cli.command()
@click.option('--database', help='Database name to start with')
@click.option('--schema', help='Schema name to start with')
@click.option('--tables', help='Comma-separated table names to process')
def agent(database, schema, tables):
    """Start the agentic dictionary generation session"""
    click.echo("📚 Agentic Snowflake Dictionary Generator")
    click.echo("=" * 50)
    click.echo("💡 I can help you create YAML data dictionaries from your Snowflake tables!")
    click.echo("💬 Just tell me what you want to do, and I'll guide you through it.")
    click.echo("🔧 Type 'quit', 'exit', or press Ctrl+C to stop")
    click.echo("=" * 50)
    
    # Create persistent session for conversation history
    session = SQLiteSession("dictionary_session")
    
    # Build initialization prompt based on provided options
    if database and schema and tables:
        initialization_prompt = f"Please connect to Snowflake, select database '{database}', schema '{schema}', and generate a dictionary for tables: {tables}"
    elif database and schema:
        initialization_prompt = f"Please connect to Snowflake, select database '{database}' and schema '{schema}', then show me the available tables so I can select which ones to include in the dictionary"
    elif database:
        initialization_prompt = f"Please connect to Snowflake, select database '{database}', then show me the available schemas and tables so I can create a data dictionary"
    else:
        initialization_prompt = "Please connect to Snowflake and guide me through selecting a database, schema, and tables to create a data dictionary"
    
    # Auto-initialize the system
    click.echo("\n🔄 Initializing system...")
    result = Runner.run_sync(dictionary_agent, initialization_prompt, session=session)
    click.echo(f"🤖 Assistant: {result.final_output}")
    
    # Interactive loop
    while True:
        try:
            user_input = click.prompt("\n👤 You", type=str).strip()
            
            if user_input.lower() in ['quit', 'exit', 'q', 'stop']:
                click.echo("👋 Thanks for using the Agentic Dictionary Generator!")
                break
            
            if not user_input:
                click.echo("❌ Please enter a question or command.")
                continue
            
            click.echo(f"🤖 Assistant: ", nl=False)
            result = Runner.run_sync(dictionary_agent, user_input, session=session)
            click.echo(result.final_output)
            
        except click.Abort:
            click.echo("\n👋 Thanks for using the Agentic Dictionary Generator!")
            break
        except KeyboardInterrupt:
            click.echo("\n👋 Thanks for using the Agentic Dictionary Generator!")
            break
        except Exception as e:
            click.echo(f"❌ An error occurred: {e}")
            continue_session = click.confirm("💭 Continue session?", default=True)
            if not continue_session:
                break

if __name__ == '__main__':
    cli()