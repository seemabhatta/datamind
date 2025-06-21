import os
from pathlib import Path
import pandas
import yaml
import json
import sys
from fastapi import FastAPI, UploadFile, File, HTTPException, Body
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

# Add the project root to the path so we can import modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))  
import config
from utils import llm_util, file_utils

app = FastAPI(title="NL2SQL API", description="API for NL2SQL Chat")

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

# Set up base directory for relative paths
BASE_DIR = Path(__file__).parent.resolve()

@app.post("/upload-file/")
async def upload_file(file: UploadFile = File(...)):
    """
    Upload a CSV file and generate a data dictionary
    """
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Only CSV files are supported")
    
    try:
        # Save the uploaded file
        sample_data_path = file_utils.get_sample_data_path()
        sample_data_path.parent.mkdir(exist_ok=True)
        
        # Read the file content
        content = await file.read()
        
        # Write to file
        with open(sample_data_path, "wb") as f:
            f.write(content)
        
        # Generate enhanced data dictionary
        sample_data_filename = config.SAMPLE_DATA_FILENAME
        llm_util.save_enhanced_data_dictionary_to_yaml_file(sample_data_filename)
        
        # Look for data dictionary in project root
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
        generated_dict = Path(project_root) / config.DATA_DICT_FILENAME
        if generated_dict.exists():
            # Load the generated dictionary to return in the response
            with open(generated_dict, "r", encoding="utf-8") as f:
                data_dict = yaml.safe_load(f)
            return {
                "status": "success",
                "message": "File uploaded and data dictionary generated successfully",
                "data_dictionary": data_dict
            }
        else:
            raise HTTPException(status_code=500, detail="Failed to generate data dictionary")
    
    except Exception as e:
        # Clean up any partial files
        file_utils.cleanup_files()
        raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")

@app.post("/query/")
async def process_query(query: str):
    """
    Process a natural language query and return SQL and results
    """
    try:
        # Check if data dictionary exists
        data_dict_path = file_utils.get_data_dict_path()
        if not data_dict_path.exists():
            raise HTTPException(status_code=400, detail="No data dictionary found. Please upload a file first.")
        
        # Load data dictionary
        enriched_data_dict = file_utils.load_data_dict()
        
        # Classify intent
        intent = llm_util.classify_intent(query)
        
        if intent.strip() == "SQL_QUERY":
            # Generate SQL
            sql_result = llm_util.create_sql_from_nl(query, enriched_data_dict)
            
            # Execute SQL query (this would need to be implemented)
            # For now, just return the SQL
            return {
                "status": "success",
                "intent": intent,
                "sql": sql_result
            }
        else:
            return {
                "status": "success",
                "intent": intent,
                "message": "Non-SQL query detected"
            }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing query: {str(e)}")

@app.delete("/clear-data/")
async def clear_data():
    """
    Clear all uploaded files and data dictionaries
    """
    try:
        file_utils.cleanup_files()
        return {"status": "success", "message": "All data cleared successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error clearing data: {str(e)}")

@app.post("/generate-summary/")
async def generate_summary(data: dict = Body(...)):
    """
    Generate a summary of the query results
    """
    try:
        # Parse the JSON data back into a DataFrame
        df_json = data.get("data")
        if not df_json:
            raise HTTPException(status_code=400, detail="No data provided")
        
        # Convert JSON string to DataFrame
        df = pandas.read_json(df_json, orient='records')
        
        # Generate summary using llm_util
        summary = llm_util.create_summary(df)
        
        return {
            "status": "success",
            "summary": summary
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating summary: {str(e)}")

if __name__ == "__main__":
    uvicorn.run("nl2sql-api:app", host="0.0.0.0", port=8000, reload=True)
