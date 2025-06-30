import os
from pathlib import Path as PathlibPath
import pandas as pd
import yaml
import sys
from fastapi import FastAPI, UploadFile, File, HTTPException, Body, Query, Path
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import shutil
import stat

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
import config
from utils import llm_util, file_utils

app = FastAPI(title="NL2SQL API", description="API for NL2SQL Chat")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
BASE_DIR = PathlibPath(__file__).parent.resolve()

@app.get("/list-datasets/")
async def list_datasets():
    """List all dataset folders in the data directory."""
    try:
        data_dir = BASE_DIR.parent.parent / "data"
        if not data_dir.exists():
            return {"status": "success", "datasets": []}
        datasets = [f.name for f in data_dir.iterdir() if f.is_dir()]
        return {"status": "success", "datasets": datasets}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing datasets: {str(e)}")
        
@app.get("/list-files/{dataset_name}/")
async def list_files(dataset_name: str):
    """List all files in a specific dataset folder."""
    try:
        data_dir = BASE_DIR.parent.parent / "data" / dataset_name
        if not data_dir.exists() or not data_dir.is_dir():
            return {"status": "success", "files": []}
        files = [f.name for f in data_dir.iterdir() if f.is_file()]
        return {"status": "success", "files": files}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing files: {str(e)}")
        
@app.post("/upload-file/")
async def upload_file(file: UploadFile = File(...)):
    """Upload a CSV file, generate a data dictionary and SQLite DB."""
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Only CSV files are supported")
    base_name = Path(file.filename).stem
    try:
        uploaded_file_path = file_utils.save_uploaded_file(file)
        yaml_text = llm_util.generate_enhanced_data_dictionary(uploaded_file_path)
        
        # Validate YAML against protobuf schema before saving
        is_valid, error = llm_util.validate_yaml_with_proto(yaml_text)
        if not is_valid:
            raise HTTPException(status_code=400, detail=f"Generated YAML is not protobuf-compatible: {error}")
        
        #save yaml to file
        file_utils.save_dict_yaml(yaml_text, base_name)
        df = pd.read_csv(uploaded_file_path)
        file_utils.save_dataframe_to_sqlite(df, base_name)
        return {
            "status": "success",
            "message": "File uploaded, data dictionary and database generated successfully",
            "data_dictionary": yaml.safe_load(yaml_text)
        }
    except Exception as e:
        file_utils.cleanup_files(base_name)
        raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")
       

@app.post("/query/")
async def process_query(
    query: str = Query(..., description="Natural language query"),
    base_name: str = Query(..., description="Dataset base name (required)")
):
    """Process a natural language query and return SQL."""
    try:
        data_dir = Path("data")
        if not data_dir.exists() or not any(data_dir.iterdir()):
            raise HTTPException(status_code=400, detail="No data dictionary found. Please upload a file first.")
        if not base_name:
            raise HTTPException(status_code=400, detail="No data dictionary found. Please upload a file first.")
        enriched_data_dict = file_utils.get_data_dict(base_name)
        if not enriched_data_dict:
            raise HTTPException(status_code=400, detail="Data dictionary could not be loaded.")
        intent = llm_util.classify_intent(query)
        if intent.strip() == "SQL_QUERY":
            sql_result = llm_util.create_sql_from_nl(query, enriched_data_dict, base_name)
            return {"status": "success", "intent": intent, "sql": sql_result}
        else:
            return {"status": "success", "intent": intent, "message": "Non-SQL query detected"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing query: {str(e)}")

@app.delete("/clear-data/")
async def clear_data():
    """Clear all uploaded files and data dictionaries."""
    try:
        file_utils.cleanup_files()
        return {"status": "success", "message": "All data cleared successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error clearing data: {str(e)}")

def remove_readonly(func, path, excinfo):
    os.chmod(path, stat.S_IWRITE)
    func(path)

@app.delete("/delete-dataset/{dataset_name}/")
async def delete_dataset(dataset_name: str = Path(..., description="Name of the dataset to delete")):
    """Delete a specific dataset folder."""
    try:
        data_dir = BASE_DIR.parent.parent / "data" / dataset_name
        if data_dir.exists() and data_dir.is_dir():
            shutil.rmtree(data_dir, onerror=remove_readonly)
            return {"status": "success", "detail": f"Dataset '{dataset_name}' deleted."}
        else:
            return {"status": "error", "detail": "Dataset not found."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error deleting dataset: {str(e)}")

@app.post("/generate-summary/")
async def generate_summary(data: dict = Body(...)):
    """Generate a summary of the query results."""
    try:
        df_json = data.get("data")
        if not df_json:
            raise HTTPException(status_code=400, detail="No data provided")
        df = pd.read_json(df_json, orient='records')
        summary = llm_util.create_summary(df)
        return {"status": "success", "summary": summary}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating summary: {str(e)}")



@app.post("/execute-sql/")
async def execute_sql(sql: str = Body(...), base_name: str = Body(...)):
    """Execute a SQL query against the uploaded SQLite DB and return the results."""
    try:
        conn = file_utils.get_db_connection(base_name)
        if conn is None:
            raise HTTPException(status_code=404, detail="Database not found.")
        df = pd.read_sql_query(sql, conn)
        conn.close()
        result = df.to_dict(orient="records")
        columns = list(df.columns)
        return {"status": "success", "columns": columns, "result": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error executing SQL: {str(e)}")

if __name__ == "__main__":
    uvicorn.run("nl2sql-api:app", host="0.0.0.0", port=8000, reload=True)
