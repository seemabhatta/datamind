import pandas
import streamlit as st
import sqlite3
import os
import requests
import json
import sys
from dotenv import load_dotenv
import pathlib
import atexit
import re

# Add the project root to the path so we can import modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
import config
from utils import cache_utils, file_utils
from src.ui import ui_components

load_dotenv()

# Set up base directory for relative paths
BASE_DIR = pathlib.Path(__file__).parent.resolve()

# Load configuration variables
db_path = config.DB_PATH
table_name = config.TABLE_NAME
sample_data_filename = os.path.join(config.SAMPLE_DATA_DIR, config.SAMPLE_DATA_FILENAME)
data_dict_filename = config.DATA_DICT_FILENAME

# API configuration
API_URL = "http://localhost:8000"  # Default API URL

# Cache configuration
#CACHE_SIZE = 100

#def cleanup_on_refresh():
#    """Cleanup function for browser refresh/close."""
#    requests.delete(f"{API_URL}/clear-data/")
#    cache_utils.clear_session_cache()
#    print("Cleanup performed on browser refresh/close")

def cleanup_files():
    """Clean up sample data and data dictionary files via API"""
    try:
        response = requests.delete(f"{API_URL}/clear-data/")
        response.raise_for_status()
        print("API cleanup successful")
    except Exception as e:
        print(f"API cleanup failed: {e}")
        # Fallback to local cleanup
        file_utils.cleanup_files()

# Register cleanup for normal Python process termination
atexit.register(cleanup_files)

def normalize_user_input(user_input):
    """Normalize user input for consistent caching"""
    if not user_input:
        return ""
    
    normalized = user_input.lower()
    normalized = re.sub(r'\s+', ' ', normalized.strip())
    normalized = re.sub(r'[.!?/-_@#$%^&*()]+$', '', normalized)
    
    return normalized

def main():
    st.set_page_config(page_title="NL2SQL Chat", layout="centered")   
    # Show title and clear cache in sidebar
    ui_components.show_title_and_clear_cache(cache_utils.clear_session_cache)

    # Initialize session states
    if 'data_dict_ready' not in st.session_state:
        st.session_state['data_dict_ready'] = False
    if 'file_uploaded' not in st.session_state:
        st.session_state['file_uploaded'] = False

    # File upload handling in sidebar
    uploaded_file = ui_components.show_file_uploader()

    # Handle file removal
    if st.session_state['file_uploaded'] and uploaded_file is None:
        try:
            response = requests.delete(f"{API_URL}/clear-data/")
            response.raise_for_status()
            cache_utils.clear_session_cache()
            st.session_state['data_dict_ready'] = False
            st.session_state['file_uploaded'] = False
            print("File and data dictionary deleted via API, and cache cleared due to file removal")
        except Exception as e:
            st.error(f"Error clearing data: {e}")

    # Save the uploaded file and generate data dictionary via API
    if not st.session_state['data_dict_ready']:
        if uploaded_file is not None:
            with ui_components.show_spinner("Generating data dictionary. Please wait..."):
                try:
                    # Prepare the file for API upload
                    files = {"file": (uploaded_file.name, uploaded_file.getvalue(), "text/csv")}
                    
                    # Send to API
                    response = requests.post(f"{API_URL}/upload-file/", files=files)
                    response.raise_for_status()
                    
                    # Process response
                    result = response.json()
                    if result.get("status") == "success":
                        st.session_state['file_uploaded'] = True
                        st.session_state['data_dict_ready'] = True
                        print("File uploaded and data dictionary generated via API")
                    else:
                        st.error("API returned an error: " + result.get("detail", "Unknown error"))
                        return
                except Exception as e:
                    st.error(f"Error uploading file: {e}")
                    st.session_state['file_uploaded'] = False
                    st.session_state['data_dict_ready'] = False
                    return
        else:
            return
    else:
        print("Querying using existing data dictionary.")    # Show chat history
    ui_components.show_chat_messages()

    # User query input and cache logic
    with st.form(key="user_query_form"):
        user_input = st.text_input("Please enter your query here -", key="user_query")
        submitted = st.form_submit_button("Enter")
    if submitted and user_input.strip():
        # Normalize for cache check
        normalized_input = normalize_user_input(user_input)
        cached = cache_utils.get_cached_sql(normalized_input)
        
        if cached:
            print(f'cache hit for user query: {user_input} (normalized: {normalized_input})')
            ui_components.show_info_message("Cache hit: Returning cached intent and SQL query.")
            sql_result = cached['sql']
            intent = cached['intent']
        else:
            print(f'cache miss for user query: {user_input} (normalized: {normalized_input})')
            try:
                # Send query to API
                response = requests.post(
                    f"{API_URL}/query/",
                    params={"query": user_input}
                )
                response.raise_for_status()
                result = response.json()
                
                if result.get("status") == "success":
                    intent = result.get("intent", "")
                    sql_result = result.get("sql", "")
                    # Cache the result
                    cache_utils.set_cached_sql(normalized_input, intent, sql_result)
                else:
                    st.error("API returned an error: " + result.get("detail", "Unknown error"))
                    return
            except Exception as e:
                st.error(f"Error processing query: {e}")
                return
    
        ui_components.show_intent(intent)
        if sql_result:
            ui_components.show_sql_query(sql_result)
        
            # Determine the base_name from the uploaded file (strip extension)
            base_name = None
            if uploaded_file is not None:
                base_name = os.path.splitext(uploaded_file.name)[0]
            else:
                # fallback: try to get from session or config
                base_name = os.path.splitext(os.path.basename(sample_data_filename))[0]

            # Call the backend API to execute the SQL
            try:
                exec_response = requests.post(
                    f"{API_URL}/execute-sql/",
                    json={"sql": sql_result, "base_name": base_name}
                )
                exec_response.raise_for_status()
                exec_result = exec_response.json()
                if exec_result.get("status") == "success":
                    columns = exec_result.get("columns", [])
                    result = exec_result.get("result", [])
                    df = pandas.DataFrame(result, columns=columns)
                    ui_components.show_query_result(df)

                    # Convert DataFrame to JSON for API request
                    df_json = df.to_json(orient='records')
                    try:
                        # Send DataFrame to API for summary generation
                        response = requests.post(
                            f"{API_URL}/generate-summary/",
                            json={"data": df_json}
                        )
                        response.raise_for_status()
                        result = response.json()
                        if result.get("status") == "success":
                            summary = result.get("summary", "No summary available")
                            ui_components.show_summary(summary)
                        else:
                            st.warning("Could not generate summary: " + result.get("detail", "Unknown error"))
                    except Exception as e:
                        st.warning(f"Could not generate summary: {e}")
                        # Continue execution even if summary fails
                else:
                    st.error("SQL execution failed: " + exec_result.get("detail", "Unknown error"))
            except Exception as exp:
                st.error(f"An error occurred during SQL execution: {exp}")
        # Clear the input
        #st.session_state.user_query = ""
        #st.session_state["user_query"] = ""

if __name__ == "__main__":
    main()
