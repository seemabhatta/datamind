"""
Metadata functions - Core logic extracted from metadata_router.py
"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.core.connection_utils import get_snowflake_connection


def list_databases(connection_id: str):
    """List all databases"""
    try:
        conn = get_snowflake_connection(connection_id)
        cursor = conn.cursor()
        cursor.execute("SHOW DATABASES")
        databases = [row[1] for row in cursor.fetchall()]  # Database name is in column 1
        cursor.close()
        
        return {
            "status": "success",
            "databases": databases
        }
    except Exception as e:
        return {
            "status": "error",
            "error": f"Error listing databases: {str(e)}"
        }


def list_schemas(connection_id: str, database: str):
    """List schemas in a database"""
    try:
        conn = get_snowflake_connection(connection_id)
        cursor = conn.cursor()
        cursor.execute(f"SHOW SCHEMAS IN DATABASE {database}")
        schemas = [row[1] for row in cursor.fetchall()]  # Schema name is in column 1
        cursor.close()
        
        return {
            "status": "success",
            "schemas": schemas
        }
    except Exception as e:
        return {
            "status": "error",
            "error": f"Error listing schemas: {str(e)}"
        }


def list_tables(connection_id: str, database: str, schema: str):
    """List tables in a schema"""
    try:
        conn = get_snowflake_connection(connection_id)
        cursor = conn.cursor()
        cursor.execute(f"SHOW TABLES IN SCHEMA {database}.{schema}")
        
        tables = []
        for row in cursor.fetchall():
            tables.append({
                "database": row[2],  # Database name
                "schema": row[3],    # Schema name
                "table": row[1],     # Table name
                "table_type": row[4] # Table type
            })
        cursor.close()
        
        return {
            "status": "success",
            "tables": tables
        }
    except Exception as e:
        return {
            "status": "error",
            "error": f"Error listing tables: {str(e)}"
        }


def list_stages(connection_id: str, database: str, schema: str):
    """List stages in a schema"""
    try:
        conn = get_snowflake_connection(connection_id)
        cursor = conn.cursor()
        cursor.execute(f"SHOW STAGES IN SCHEMA {database}.{schema}")
        
        stages = []
        for row in cursor.fetchall():
            stages.append({
                "name": row[1],      # Stage name
                "database": row[2],  # Database name
                "schema": row[3],    # Schema name
                "type": row[4]       # Stage type
            })
        cursor.close()
        
        return {
            "status": "success",
            "stages": stages
        }
    except Exception as e:
        return {
            "status": "error",
            "error": f"Error listing stages: {str(e)}"
        }


def list_stage_files(connection_id: str, stage_name: str):
    """List files in a stage"""
    try:
        conn = get_snowflake_connection(connection_id)
        cursor = conn.cursor()
        cursor.execute(f"LIST {stage_name}")
        
        files = []
        for row in cursor.fetchall():
            files.append({
                "name": row[0],                    # File name
                "size": int(row[1]),              # File size
                "last_modified": str(row[2])      # Last modified timestamp
            })
        cursor.close()
        
        return {
            "status": "success",
            "files": files
        }
    except Exception as e:
        return {
            "status": "error",
            "error": f"Error listing stage files: {str(e)}"
        }