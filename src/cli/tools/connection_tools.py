#!/usr/bin/env python3
"""
Connection function tools for the Agentic Query CLI
"""

import sys
import os
from agents import function_tool

# Add the project root to the path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from src.functions.connection_functions import connect_to_snowflake as connect_func


def connect_to_snowflake_impl(agent_context) -> str:
    """Connect to Snowflake and establish a connection"""
    # Check if already connected
    if agent_context.connection_id:
        return f"✅ Already connected (Connection ID: {agent_context.connection_id[:8]}...)"
    
    result = connect_func()
    if result["status"] == "success":
        agent_context.connection_id = result["connection_id"]
        return f"✅ Connected to {result['account']} as {result['user']} (Connection ID: {result['connection_id'][:8]}...)"
    else:
        return f"❌ Connection failed: {result.get('error', 'Unknown error')}"


def get_current_context_impl(agent_context) -> str:
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