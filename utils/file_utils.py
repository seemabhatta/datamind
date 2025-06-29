import os
from pathlib import Path
import sys
import yaml
import shutil
import config
import pandas as pd
import sqlite3

from utils import cache_utils

# Add the project root to the path so we can import modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))  

def prepare_data_paths(base_name: str, data_dir: str = "data") -> dict[str, Path]:
    """
    Given a base filename (without extension), ensure the /data/<base_name> directory exists,
    and return a dict with Path objects for .csv, .db, and .yaml files.
    """
    base_dir = Path(data_dir) / base_name
    base_dir.mkdir(parents=True, exist_ok=True)

    paths = {
        "directory": base_dir.resolve(),
        "data_file": (base_dir / f"{base_name}.csv").resolve(),
        "db_file": (base_dir / f"{base_name}.db").resolve(),
        "dict_file": (base_dir / f"{base_name}.yaml").resolve()
    }
    return paths

def save_uploaded_file(uploaded_file) -> Path:
    """
    Save the uploaded file in /data/<base_name>/<original_filename>,
    where <base_name> is the uploaded file's name without extension.
    Returns the Path to the saved file.
    """
    filename = uploaded_file.name
    base_name, ext = os.path.splitext(filename)
    paths = prepare_data_paths(base_name)
    # Save with original filename and extension in the correct subdirectory
    file_path = paths["directory"] / filename
    with open(file_path, "wb") as f:
        f.write(uploaded_file.getbuffer())
    return file_path

def save_dict_yaml(yaml_content: str, base_name: str) -> Path:
    """
    Save the YAML string as /data/<base_name>/<base_name>.yaml.
    Returns the Path to the saved YAML file.
    """
    paths = prepare_data_paths(base_name)
    dict_file_path = paths["dict_file"]
    with open(dict_file_path, "w", encoding="utf-8") as f:
        f.write(yaml_content)
    return dict_file_path

def save_dataframe_to_sqlite(df: pd.DataFrame, base_name: str, table_name: str = None) -> Path:
    """
    Save a pandas DataFrame to a SQLite database in /data/<base_name>/<base_name>.db.
    The table name defaults to base_name if not provided.
    Returns the Path to the created database.
    """
    paths = prepare_data_paths(base_name)
    db_path = paths["db_file"]
    if table_name is None:
        table_name = base_name

    conn = sqlite3.connect(db_path)
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()
    return db_path

def get_data_dict(base_name: str) -> Optional[dict]:
    """
    Load and return the data dictionary YAML as a Python dict.
    Returns None if not found.
    """
    paths = prepare_data_paths(base_name)
    dict_file_path = paths["dict_file"]
    if not dict_file_path.exists():
        return None
    with open(dict_file_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def get_db_connection(base_name: str) -> Optional[sqlite3.Connection]:
    """
    Return a sqlite3.Connection object for /data/<base_name>/<base_name>.db.
    Caller is responsible for closing the connection.
    Returns None if the DB file does not exist.
    """
    paths = prepare_data_paths(base_name)
    db_file_path = paths["db_file"]
    if not db_file_path.exists():
        return None
    return sqlite3.connect(db_file_path)


def cleanup_files(base_name=None):
    """
    Delete all files and folders related to the provided base_name from /data,
    and clear session cache. If base_name is None, do nothing.
    """
    if base_name:
        data_dir = Path("data") / base_name
        if data_dir.exists() and data_dir.is_dir():
            shutil.rmtree(data_dir)
            print(f"Deleted data directory: {data_dir}")
    cache_utils.clear_session_cache()
    print("Sample data file and both data dictionaries deleted.")


def load_data_dict(base_name=None):
    """
    Load the data dictionary YAML file from /data/<base_name>/<base_name>.yaml.
    If base_name is None, returns None.
    """
    if not base_name:
        return None
    paths = prepare_data_paths(base_name)
    data_dict_file_path = paths["dict_file"]
    if not Path(data_dict_file_path).exists():
        return None
    with open(data_dict_file_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)        
