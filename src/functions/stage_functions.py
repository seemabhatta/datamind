"""
Stage functions - Core logic extracted from stage_router.py
"""

import tempfile
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.api.utils.connection_utils import get_snowflake_connection


def load_stage_file(connection_id: str, stage_name: str, file_name: str):
    """Load YAML data dictionary from Snowflake stage file"""
    try:
        conn = get_snowflake_connection(connection_id)
        cursor = conn.cursor()
        
        print(f"DEBUG: Loading stage file {file_name} from {stage_name}")
        
        # Read file content directly from stage as plain text
        select_sql = f"""
        SELECT $1 as content 
        FROM '{stage_name}/{file_name}'
        """
        
        cursor.execute(select_sql)
        rows = cursor.fetchall()
        content = "\n".join([row[0] for row in rows if row[0]])
        
        cursor.close()
        
        print(f"DEBUG: Loaded {len(content)} characters from stage file")
        
        # Validate that it's YAML content
        if not content.strip():
            return {
                "status": "error",
                "error": "Stage file is empty"
            }
        
        # Basic YAML validation - check for common YAML indicators
        if not any(indicator in content for indicator in [':', '-', 'fields:', 'tables:', 'columns:']):
            return {
                "status": "error",
                "error": "File does not appear to be a valid YAML data dictionary"
            }
        
        return {
            "status": "success",
            "content": content
        }
        
    except Exception as e:
        print(f"DEBUG: Error loading stage file: {e}")
        return {
            "status": "error",
            "error": f"Error loading stage file: {str(e)}"
        }


def save_dictionary_to_stage(connection_id: str, stage_name: str, file_name: str, yaml_content: str):
    """Save YAML data dictionary to a Snowflake stage"""
    try:
        conn = get_snowflake_connection(connection_id)
        cursor = conn.cursor()
        
        # Write YAML content to a temporary local file with the desired name
        temp_dir = tempfile.gettempdir()
        temp_file_path = os.path.join(temp_dir, file_name)
        
        with open(temp_file_path, 'w') as temp_file:
            temp_file.write(yaml_content)
        
        try:
            # Use Snowflake's PUT command to upload the file to the stage
            # Convert Windows path to proper format and escape backslashes
            normalized_path = temp_file_path.replace('\\', '/')
            put_command = f"PUT 'file://{normalized_path}' {stage_name} OVERWRITE=TRUE AUTO_COMPRESS=FALSE"
            print(f"DEBUG: Executing PUT command: {put_command}")
            cursor.execute(put_command)
            
            # Since we created the temp file with the desired name, it should upload with the correct name
            actual_filename = file_name
            
            # Verify the upload by listing the stage
            cursor.execute(f"LIST {stage_name}")
            files = cursor.fetchall()
            print(f"DEBUG: Files in stage after upload: {[f[0] for f in files]}")
            
            cursor.close()
            
            return {
                "status": "success", 
                "message": f"YAML dictionary uploaded to {stage_name}/{actual_filename}",
                "stage_name": stage_name,
                "file_name": actual_filename,
                "content_size": len(yaml_content)
            }
            
        finally:
            # Clean up temporary file
            if os.path.exists(temp_file_path):
                os.unlink(temp_file_path)
        
    except Exception as e:
        print(f"DEBUG: Error saving dictionary to stage: {e}")
        return {
            "status": "error",
            "error": f"Error saving dictionary to stage: {str(e)}"
        }