import tempfile
import os
from fastapi import APIRouter, HTTPException, Query
from ..models.api_models import SaveDictionaryRequest
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))
from src.core.connection_utils import get_snowflake_connection

router = APIRouter()

@router.get("/connection/{connection_id}/load-stage-file")
async def load_stage_file(
    connection_id: str,
    stage_name: str = Query(..., description="Full stage name"),
    file_name: str = Query(..., description="File name in stage")
):
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
            raise HTTPException(status_code=400, detail="Stage file is empty")
        
        # Basic YAML validation - check for common YAML indicators
        if not any(indicator in content for indicator in [':', '-', 'fields:', 'tables:', 'columns:']):
            raise HTTPException(status_code=400, detail="File does not appear to be a valid YAML data dictionary")
        
        return {"content": content}
        
    except Exception as e:
        print(f"DEBUG: Error loading stage file: {e}")
        raise HTTPException(status_code=500, detail=f"Error loading stage file: {str(e)}")

@router.post("/connection/{connection_id}/save-dictionary-to-stage")
async def save_dictionary_to_stage(connection_id: str, request: SaveDictionaryRequest):
    """Save YAML data dictionary to a Snowflake stage"""
    try:
        conn = get_snowflake_connection(connection_id)
        cursor = conn.cursor()
        
        # Write YAML content to a temporary local file with the desired name
        temp_dir = tempfile.gettempdir()
        temp_file_path = os.path.join(temp_dir, request.file_name)
        
        with open(temp_file_path, 'w') as temp_file:
            temp_file.write(request.yaml_content)
        
        try:
            # Use Snowflake's PUT command to upload the file to the stage
            # Convert Windows path to proper format and escape backslashes
            normalized_path = temp_file_path.replace('\\', '/')
            put_command = f"PUT 'file://{normalized_path}' {request.stage_name} OVERWRITE=TRUE AUTO_COMPRESS=FALSE"
            print(f"DEBUG: Executing PUT command: {put_command}")
            cursor.execute(put_command)
            
            # Since we created the temp file with the desired name, it should upload with the correct name
            actual_filename = request.file_name
            
            # Verify the upload by listing the stage
            cursor.execute(f"LIST {request.stage_name}")
            files = cursor.fetchall()
            print(f"DEBUG: Files in stage after upload: {[f[0] for f in files]}")
            
            cursor.close()
            
            return {
                "status": "success", 
                "message": f"YAML dictionary uploaded to {request.stage_name}/{actual_filename}",
                "stage_name": request.stage_name,
                "file_name": actual_filename,
                "content_size": len(request.yaml_content)
            }
            
        finally:
            # Clean up temporary file
            if os.path.exists(temp_file_path):
                os.unlink(temp_file_path)
        
    except Exception as e:
        print(f"DEBUG: Error saving dictionary to stage: {e}")
        raise HTTPException(status_code=500, detail=f"Error saving dictionary to stage: {str(e)}")