#!/usr/bin/env python3
"""
Agentic Natural Query CLI - AI agent for interactive Snowflake querying
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
    get_yaml_files_impl as yaml_files_tool,
    load_yaml_file_impl as load_yaml_tool,
    get_yaml_content_impl as yaml_content_tool,
    generate_sql_impl as generate_sql_tool,
    execute_sql_impl as execute_sql_tool,
    generate_summary_impl as summary_tool
)

from src.cli.tools.visualization_tools import (
    visualize_data_impl as visualize_tool,
    get_visualization_suggestions_impl as viz_suggestions_tool
)

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
    last_query_results: Optional[List[Dict]] = None
    last_query_columns: Optional[List[str]] = None
    last_query_sql: Optional[str] = None
    
    def __post_init__(self):
        if self.tables is None:
            self.tables = []

# Global context for the agent
agent_context = AgentContext()

# Removed APIClient class - using direct function calls instead

# Tool Functions for Agent SDK - wrapper functions with agent_context
# These are simple wrappers that pass agent_context to the actual tool functions

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
    return select_db_tool(agent_context, database_name)

@function_tool
def get_schemas(database_name: Optional[str] = None) -> str:
    """Get schemas for a database"""
    return schemas_tool(agent_context, database_name)

@function_tool
def select_schema(schema_name: str) -> str:
    """Select a specific schema to work with"""
    return select_schema_tool(agent_context, schema_name)

@function_tool
def get_stages() -> str:
    """Get stages in the current database and schema"""
    return stages_tool(agent_context)

@function_tool
def select_stage(stage_name: str) -> str:
    """Select a specific stage to work with"""
    return select_stage_tool(agent_context, stage_name)

@function_tool
def get_yaml_files() -> str:
    """Get YAML files from the current stage"""
    return yaml_files_tool(agent_context)

@function_tool
def load_yaml_file(filename: str) -> str:
    """Load and parse a YAML file from the current stage"""
    return load_yaml_tool(agent_context, filename)

@function_tool
def generate_sql(query: str, table_name: Optional[str] = None) -> str:
    """Generate SQL from natural language query"""
    return generate_sql_tool(agent_context, query, table_name)

@function_tool
def execute_sql(sql: str, table_name: Optional[str] = None) -> str:
    """Execute SQL query and return results"""
    return execute_sql_tool(agent_context, sql, table_name)

@function_tool
def generate_summary(query: str, sql: str, results: str) -> str:
    """Generate AI summary of query results"""
    return summary_tool(agent_context, query, sql, results)

@function_tool
def get_current_context() -> str:
    """Get current agent context and state"""
    return context_tool(agent_context)

@function_tool
def get_yaml_content() -> str:
    """Get the loaded YAML data dictionary content for analysis"""
    return yaml_content_tool(agent_context)

@function_tool
def visualize_data(user_request: str = "create a chart") -> str:
    """Create LLM-powered visualizations from query results. Describe what kind of chart you want."""
    return visualize_tool(agent_context, user_request)

@function_tool
def get_visualization_suggestions() -> str:
    """Get LLM-powered suggestions for visualizing the current query results"""
    return viz_suggestions_tool(agent_context)

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
7. Create LLM-powered interactive visualizations from query results
8. Provide intelligent visualization suggestions based on data analysis

IMPORTANT BEHAVIORAL GUIDELINES:
- Always consider the context of your previous message when interpreting user responses
- When you present options/lists to users, remember what you just showed them
- Be proactive in using tools when users give clear directives or selections
- If a user gives a brief response, consider it in context of what you just presented
- Don't ask for clarification if the user's intent is clear from context

CONTEXTUAL RESPONSE EXAMPLES:
Example 1:
Assistant: "I found 2 databases: 1. CORTES_DEMO_2  2. SNOWFLAKE. Which would you like to explore?"
User: "1"
Assistant: [calls select_database("CORTES_DEMO_2") immediately]

Example 2:
Assistant: "Here are the YAML files: 1. dict0.yaml  2. dict01.yaml  3. dict1.yaml"
User: "load the first one"
Assistant: [calls load_yaml_file("dict0.yaml") immediately]

Example 3:
Assistant: "I found 3 schemas: PUBLIC, STAGING, PROD"
User: "public"
Assistant: [calls select_schema("PUBLIC") immediately]

Example 4:
User: "give me sample queries"
Assistant: [calls get_yaml_content() first to analyze the data structure, then provides contextual sample queries based on actual tables and columns]

Example 5:
User: "load hmda_v4.yaml"
Assistant: [calls load_yaml_file("hmda_v4.yaml") directly - does NOT call connect_to_snowflake() again since already connected]

Workflow:
1. When asked to initialize, automatically: connect to Snowflake → get databases → select first database → get schemas → select first schema → get stages → select first stage → get YAML files → show available YAML files to user
2. When user selects a YAML file, load it and auto-connect to the database/schema specified in the YAML
3. Process their natural language queries using the loaded data dictionary
4. Generate and execute SQL based on the YAML table structure
5. Provide clear, helpful results

Auto-initialization Steps:
- Connect to Snowflake immediately
- Get databases, select the first one directly
- Get schemas, select the first one directly  
- Get stages, select the first one directly
- Present YAML files for user selection
- Once YAML is loaded, the system is ready for queries

EFFICIENCY RULES:
- Avoid duplicate API calls - don't verify selections that were just made
- Use the most direct path to get to YAML files
- Don't call the same endpoint multiple times unnecessarily
- Once connected, reuse the same connection for all operations
- NEVER call connect_to_snowflake() more than once per session
- Check connection status before attempting to reconnect

Guidelines:
- Be action-oriented and use tools proactively
- Guide users through the workflow step by step
- Handle errors gracefully and suggest solutions
- Provide clear feedback on what's happening
- When users ask for sample queries, analyze the actual YAML content to provide relevant examples

CRITICAL: QUERY EXECUTION BEHAVIOR
- If a YAML file is already loaded and user asks a data query, IMMEDIATELY use generate_sql() tool
- Do NOT suggest loading different files if you already have relevant data loaded
- Always check get_current_context() to see what data is available before suggesting alternatives
- If user asks a query that can be answered with current data, generate SQL and execute it immediately

Query Execution Examples:
User: "List the number of loans by agency"
Assistant: [calls generate_sql() immediately with the user's query, then execute_sql() with the result]

User: "Show me the top 10 customers"  
Assistant: [calls generate_sql() immediately, then execute_sql()]

User: "What's the average loan amount?"
Assistant: [calls generate_sql() immediately, then execute_sql()]

VISUALIZATION CAPABILITIES:
After executing queries, you can create visualizations:

User: "Show me a chart of this data"
Assistant: [calls visualize_data() with user request to create LLM-powered chart]

User: "What charts would work best for this data?"  
Assistant: [calls get_visualization_suggestions() to get LLM analysis and recommendations]

User: "Create a bar chart showing sales by region"
Assistant: [calls visualize_data("Create a bar chart showing sales by region")]

The LLM will:
- Analyze the data structure automatically
- Choose the most appropriate chart type
- Generate interactive plotly charts
- Provide explanations for visualization choices
- Create charts that open in the user's browser

VISUALIZATION WORKFLOW:
1. User runs a query (data gets stored automatically)
2. User requests visualization ("create a chart", "show me graphs", etc.)
3. You call visualize_data() with their request
4. LLM analyzes data and generates appropriate chart code
5. Interactive chart opens in browser

Do NOT ask for clarification or suggest loading different files if you have data that can answer the question.

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
        get_current_context,
        get_yaml_content,
        visualize_data,
        get_visualization_suggestions
    ]
)

@click.group()
def cli():
    """Agentic Natural Language Query CLI for Snowflake"""
    pass

@cli.command()
@click.option('--query', '-q', help='Initial query to process')
@click.option('--session-id', '-s', help='Session ID for conversation memory (default: auto-generated)')
def agent(query, session_id):
    """Start the agentic query session"""
    click.echo("🤖 Agentic Snowflake Query Assistant")
    click.echo("=" * 50)
    click.echo("💡 I can help you query your Snowflake data using natural language!")
    click.echo("💬 Just tell me what you want to do, and I'll guide you through it.")
    click.echo("🔧 Type 'quit', 'exit', or press Ctrl+C to stop")
    click.echo("=" * 50)
    
    # Create session for conversation memory
    if not session_id:
        import time
        session_id = f"query_session_{int(time.time())}"
    
    session = SQLiteSession(session_id)
    click.echo(f"📝 Session ID: {session_id}")
    
    # Auto-initialize the system
    click.echo("\n🔄 Initializing system...")
    initialization_prompt = "Please connect to Snowflake, navigate to the available databases and schemas, find the stage with YAML files, and show me the available YAML data dictionaries so I can select one to work with."
    
    result = Runner.run_sync(snowflake_agent, initialization_prompt, session=session)
    click.echo(f"🤖 Assistant: {result.final_output}")
    
    # Start with initial query if provided
    if query:
        click.echo(f"\n👤 User: {query}")
        result = Runner.run_sync(snowflake_agent, query, session=session)
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
            result = Runner.run_sync(snowflake_agent, user_input, session=session)
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