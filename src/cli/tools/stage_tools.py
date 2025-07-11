#!/usr/bin/env python3
"""
Stage function tools for the Agentic Query CLI
"""

import sys
import os
import yaml
from agents import function_tool

# Add the project root to the path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from src.functions.metadata_functions import list_stages, list_stage_files
from src.functions.stage_functions import load_stage_file as load_stage_func


def get_stages_impl(agent_context) -> str:
    """Get stages in the current database and schema"""
    if not agent_context.connection_id:
        return "❌ No connection established. Please connect first."
    
    if not agent_context.current_database or not agent_context.current_schema:
        return "❌ Database and schema must be selected first."
    
    result = list_stages(agent_context.connection_id, agent_context.current_database, agent_context.current_schema)
    if result["status"] == "success":
        stages = result["stages"]
        stage_info = [f"{s['name']} ({s['type']})" for s in stages]
        return f"📋 Found {len(stages)} stages: {', '.join(stage_info)}"
    else:
        return f"❌ Failed to get stages: {result.get('error', 'Unknown error')}"


def select_stage_impl(agent_context, stage_name: str) -> str:
    """Select a specific stage to work with"""
    if not agent_context.current_database or not agent_context.current_schema:
        return "❌ Database and schema must be selected first."
    
    # Just set the stage - skip verification to avoid duplicate calls
    agent_context.current_stage = f"@{agent_context.current_database}.{agent_context.current_schema}.{stage_name}"
    return f"✅ Selected stage: {agent_context.current_stage}"


def get_yaml_files_impl(agent_context) -> str:
    """Get YAML files from the current stage"""
    if not agent_context.current_stage:
        return "❌ No stage selected. Please select a stage first."
    
    result = list_stage_files(agent_context.connection_id, agent_context.current_stage)
    if result["status"] == "success":
        files = result["files"]
        yaml_files = [f for f in files if f["name"].endswith(('.yaml', '.yml'))]
        if yaml_files:
            file_info = [f"{f['name'].split('/')[-1]} ({f['size']} bytes)" for f in yaml_files]
            return f"📄 Found {len(yaml_files)} YAML files: {', '.join(file_info)}"
        else:
            return f"❌ No YAML files found. Available files: {[f['name'] for f in files]}"
    else:
        return f"❌ Failed to get files: {result.get('error', 'Unknown error')}"


def load_yaml_file_impl(agent_context, filename: str) -> str:
    """Load and parse a YAML file from the current stage"""
    if not agent_context.current_stage:
        return "❌ No stage selected. Please select a stage first."
    
    # Load YAML content
    result = load_stage_func(agent_context.connection_id, agent_context.current_stage, filename)
    
    if result["status"] != "success":
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
        
        # Auto-connect to database and schema from YAML
        if tables:
            first_table = tables[0]
            db_name = first_table.get('database')
            schema_name = first_table.get('schema')
            
            if db_name and db_name != agent_context.current_database:
                agent_context.current_database = db_name
                
            if schema_name and schema_name != agent_context.current_schema:
                agent_context.current_schema = schema_name
        
        return f"✅ Loaded and parsed {filename} ({len(yaml_content)} chars). Found {len(tables)} tables: {[t['name'] for t in tables]}. Auto-connected to database: {agent_context.current_database}, schema: {agent_context.current_schema}"
        
    except yaml.YAMLError as e:
        return f"❌ Failed to parse YAML: {e}"


def get_yaml_content_impl(agent_context) -> str:
    """Get the loaded YAML data dictionary content for analysis"""
    if not agent_context.yaml_content:
        return "❌ No YAML file loaded. Please load a data dictionary first."
    
    return f"📄 **YAML Data Dictionary Content:**\n\n{agent_context.yaml_content}"