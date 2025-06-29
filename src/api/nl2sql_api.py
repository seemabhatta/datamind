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
    Upload a CSV file and generate a data dictionary and SQLite DB.
    """
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Only CSV files are supported")
    
    base_name = Path(file.filename).stem

    try:
        # 1. Save uploaded file
        uploaded_file_path = file_utils.save_uploaded_file(file)
        
        # 2. Generate enhanced data dictionary as YAML text
        yaml_text = llm_util.generate_enhanced_data_dictionary(uploaded_file_path)

        # 3. Save YAML
        file_utils.save_dict_yaml(yaml_text, base_name)

        # 4. Save SQLite DB (read from the just-uploaded file)
        paths = file_utils.prepare_data_paths(base_name)
        df = pandas.read_csv(uploaded_file_path)
        file_path = paths["directory"] / file.filename
        df = pandas.read_csv(file_path)
        file_utils.save_dataframe_to_sqlite(df, base_name)

        # 5. Return parsed data dictionary
        return {
            "status": "success",
            "message": "File uploaded, data dictionary and database generated successfully",
            "data_dictionary": yaml.safe_load(yaml_text)
        }
    except Exception as e:
        file_utils.cleanup_files(base_name)
        raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")
       

@app.post("/query/")
async def process_query(query: str):
    """
    Process a natural language query and return SQL and results
    """
    try:
        # For simplicity, use the first available data dictionary in the data directory
        data_dir = Path("data")
        if not data_dir.exists() or not any(data_dir.iterdir()):
            raise HTTPException(status_code=400, detail="No data dictionary found. Please upload a file first.")
        # Pick the first subdirectory as the active dataset
        base_name = next((p.name for p in data_dir.iterdir() if p.is_dir()), None)
        if not base_name:
            raise HTTPException(status_code=400, detail="No data dictionary found. Please upload a file first.")
        enriched_data_dict = file_utils.get_data_dict(base_name)
        if not enriched_data_dict:
            raise HTTPException(status_code=400, detail="Data dictionary could not be loaded.")
        # Classify intent
        intent = llm_util.classify_intent(query)
        if intent.strip() == "SQL_QUERY":
            # Generate SQL
            sql_result = llm_util.create_sql_from_nl(query, enriched_data_dict)
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
