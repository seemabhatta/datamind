#!/usr/bin/env python3
"""
Dictionary generation tools for the Agentic YAML CLI
"""

import sys
import os
from typing import List, Optional

# Add the project root to the path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from src.functions.metadata_functions import list_tables
from src.functions.dictionary_functions import generate_data_dictionary
from src.functions.stage_functions import save_dictionary_to_stage


def get_tables_impl(agent_context) -> str:
    """Get tables in the current database and schema"""
    if not agent_context.connection_id:
        return "❌ No connection established. Please connect first."
    
    if not agent_context.current_database or not agent_context.current_schema:
        return "❌ Database and schema must be selected first."
    
    result = list_tables(agent_context.connection_id, agent_context.current_database, agent_context.current_schema)
    if result["status"] == "success":
        tables = result["tables"]
        table_list = []
        for i, table in enumerate(tables, 1):
            table_list.append(f"{i}. {table['table']} ({table['table_type']})")
        
        # Store tables in context for later selection
        agent_context.available_tables = tables
        
        return f"📊 Found {len(tables)} tables in {agent_context.current_database}.{agent_context.current_schema}:\n" + "\n".join(table_list)
    else:
        return f"❌ Failed to get tables: {result.get('error', 'Unknown error')}"


def select_tables_impl(agent_context, table_selection: str) -> str:
    """Select tables for dictionary generation. Use 'all' for all tables, or comma-separated numbers like '1,3,5'"""
    if not agent_context.connection_id:
        return "❌ No connection established. Please connect first."
    
    if not agent_context.current_database or not agent_context.current_schema:
        return "❌ Database and schema must be selected first."
    
    # Get available tables if not already stored
    if not hasattr(agent_context, 'available_tables') or not agent_context.available_tables:
        result = list_tables(agent_context.connection_id, agent_context.current_database, agent_context.current_schema)
        if result["status"] != "success":
            return f"❌ Failed to get tables: {result.get('error', 'Unknown error')}"
        agent_context.available_tables = result["tables"]
    
    available_tables = agent_context.available_tables
    
    if table_selection.lower() == "all":
        agent_context.selected_tables = [table['table'] for table in available_tables]
        return f"✅ Selected all {len(agent_context.selected_tables)} tables: {', '.join(agent_context.selected_tables)}"
    
    try:
        # Parse comma-separated numbers
        indices = [int(x.strip()) for x in table_selection.split(',')]
        selected_tables = []
        
        for i in indices:
            if 1 <= i <= len(available_tables):
                selected_tables.append(available_tables[i-1]['table'])
            else:
                return f"❌ Invalid table number: {i}. Please use numbers 1-{len(available_tables)}"
        
        agent_context.selected_tables = selected_tables
        return f"✅ Selected {len(selected_tables)} tables: {', '.join(selected_tables)}"
        
    except ValueError:
        return "❌ Invalid selection format. Use 'all' or comma-separated numbers like '1,3,5'"


def generate_yaml_dictionary_impl(agent_context, output_filename: Optional[str] = None) -> str:
    """Generate YAML data dictionary from selected tables"""
    if not agent_context.connection_id:
        return "❌ No connection established. Please connect first."
    
    if not agent_context.current_database or not agent_context.current_schema:
        return "❌ Database and schema must be selected first."
    
    if not hasattr(agent_context, 'selected_tables') or not agent_context.selected_tables:
        return "❌ No tables selected. Please select tables first."
    
    try:
        # Generate dictionary using existing function
        result = generate_data_dictionary(
            agent_context.connection_id,
            agent_context.selected_tables,
            agent_context.current_database,
            agent_context.current_schema
        )
        
        if result["status"] == "success":
            agent_context.dictionary_content = result["yaml_dictionary"]
            
            # Save to file if filename provided
            if output_filename:
                with open(output_filename, 'w') as f:
                    f.write(result["yaml_dictionary"])
                
                return f"✅ Dictionary generated successfully!\n📄 Saved to: {output_filename}\n📊 Tables processed: {result.get('tables_processed', len(agent_context.selected_tables))}"
            else:
                return f"✅ Dictionary generated successfully!\n📊 Tables processed: {result.get('tables_processed', len(agent_context.selected_tables))}\n💡 Use save_dictionary() to save to file"
        else:
            return f"❌ Failed to generate dictionary: {result.get('error', 'Unknown error')}"
            
    except Exception as e:
        return f"❌ Error generating dictionary: {str(e)}"


def save_dictionary_impl(agent_context, filename: str) -> str:
    """Save the generated dictionary to a file"""
    if not hasattr(agent_context, 'dictionary_content') or not agent_context.dictionary_content:
        return "❌ No dictionary content available. Please generate a dictionary first."
    
    try:
        with open(filename, 'w') as f:
            f.write(agent_context.dictionary_content)
        return f"✅ Dictionary saved to: {filename}"
    except Exception as e:
        return f"❌ Failed to save dictionary: {str(e)}"


def upload_to_stage_impl(agent_context, stage_name: str, filename: str) -> str:
    """Upload the generated dictionary to a Snowflake stage"""
    if not agent_context.connection_id:
        return "❌ No connection established. Please connect first."
    
    if not hasattr(agent_context, 'dictionary_content') or not agent_context.dictionary_content:
        return "❌ No dictionary content available. Please generate a dictionary first."
    
    try:
        result = save_dictionary_to_stage(
            agent_context.connection_id,
            stage_name,
            filename,
            agent_context.dictionary_content
        )
        
        if result["status"] == "success":
            return f"✅ Dictionary uploaded to stage: {stage_name}/{filename}"
        else:
            return f"❌ Failed to upload to stage: {result.get('error', 'Unknown error')}"
            
    except Exception as e:
        return f"❌ Error uploading to stage: {str(e)}"


def show_dictionary_preview_impl(agent_context) -> str:
    """Show a preview of the generated dictionary"""
    if not hasattr(agent_context, 'dictionary_content') or not agent_context.dictionary_content:
        return "❌ No dictionary content available. Please generate a dictionary first."
    
    # Show first 500 characters
    preview = agent_context.dictionary_content[:500]
    if len(agent_context.dictionary_content) > 500:
        preview += "...\n\n[Content truncated - use save_dictionary() to save the full content]"
    
    return f"📋 Dictionary Preview:\n\n{preview}"