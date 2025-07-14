#!/usr/bin/env python3
"""
Database function tools for the Agentic Query CLI
"""

import sys
import os
from agents import function_tool

# Add the project root to the path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from src.functions.metadata_functions import list_databases, list_schemas


def get_databases_impl(agent_context) -> str:
    """Get list of available databases"""
    if not agent_context.connection_id:
        return "❌ No connection established. Please connect first."
    
    result = list_databases(agent_context.connection_id)
    if result["status"] == "success":
        databases = result["databases"]
        return f"📊 Found {len(databases)} databases: {', '.join(databases)}"
    else:
        return f"❌ Failed to get databases: {result.get('error', 'Unknown error')}"


def select_database_impl(agent_context, database_name: str) -> str:
    """Select a specific database to work with"""
    if not agent_context.connection_id:
        return "❌ No connection established. Please connect first."
    
    # Just set the database - skip verification to avoid duplicate calls
    agent_context.current_database = database_name
    return f"✅ Selected database: {database_name}"


def get_schemas_impl(agent_context, database_name: str = None) -> str:
    """Get schemas for a database"""
    if not agent_context.connection_id:
        return "❌ No connection established. Please connect first."
    
    db_name = database_name or agent_context.current_database
    if not db_name:
        return "❌ No database specified. Please select a database first."
    
    result = list_schemas(agent_context.connection_id, db_name)
    if result["status"] == "success":
        schemas = result["schemas"]
        return f"📂 Found {len(schemas)} schemas in {db_name}: {', '.join(schemas)}"
    else:
        return f"❌ Failed to get schemas: {result.get('error', 'Unknown error')}"


def select_schema_impl(agent_context, schema_name: str) -> str:
    """Select a specific schema to work with"""
    if not agent_context.current_database:
        return "❌ No database selected. Please select a database first."
    
    # Just set the schema - skip verification to avoid duplicate calls
    agent_context.current_schema = schema_name
    return f"✅ Selected schema: {schema_name}"