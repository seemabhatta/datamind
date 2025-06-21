import os
from pathlib import Path
import config
import yaml
import cache_utils

def get_sample_data_path():
    return Path(config.SAMPLE_DATA_DIR) / config.SAMPLE_DATA_FILENAME

def get_data_dict_path():
    return Path(config.DATA_DICT_FILENAME)

def save_uploaded_file(uploaded_file):
    """Save the uploaded file to the sample-data directory."""
    sample_data_path = get_sample_data_path()
    sample_data_path.parent.mkdir(exist_ok=True)
    with open(sample_data_path, "wb") as f:
        f.write(uploaded_file.getbuffer())
    return sample_data_path

def delete_sample_file():
    sample_data_path = get_sample_data_path()
    if sample_data_path.exists():
        sample_data_path.unlink()

def delete_data_dict():
    data_dict_path = get_data_dict_path()
    if data_dict_path.exists():
        data_dict_path.unlink()

def cleanup_files():
    delete_sample_file()
    delete_data_dict()
    cache_utils.clear_session_cache()
    print("Sample data file and both data dictionaries deleted.")

def load_data_dict(data_dict_file_path=None):
    """Load the data dictionary YAML file."""
    if data_dict_file_path is None:
        data_dict_file_path = get_data_dict_path()
    if not Path(data_dict_file_path).exists():
        return None
    with open(data_dict_file_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)
