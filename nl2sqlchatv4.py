import pandas
from openai import OpenAI
import streamlit as st
import sqlite3
import os
from dotenv import load_dotenv
import pathlib
import config
import atexit
import cache_utils
import ui_components
import file_utils
import llm_util

load_dotenv()

# Set up base directory for relative paths
BASE_DIR = pathlib.Path(__file__).parent.resolve()

# Load configuration variables
db_path = config.DB_PATH
table_name = config.TABLE_NAME
sample_data_filename = os.path.join(config.SAMPLE_DATA_DIR, config.SAMPLE_DATA_FILENAME)
data_dict_filename = config.DATA_DICT_FILENAME
system_prompts_dir = config.SYSTEM_PROMPTS_DIR
nl2sql_system_prompt_file = config.NL2SQL_SYSTEM_PROMPT_FILE
summary_prompt_file = config.SUMMARY_PROMPT_FILE
intent_identifier_prompt_file = config.INTENT_IDENTIFIER_PROMPT_FILE
llm_model = config.LLM_MODEL
api_key = os.getenv("OPENAI_API_KEY")
client = OpenAI(api_key=api_key)

# Cache configuration
#CACHE_SIZE = 100

#def cleanup_on_refresh():
#    """Cleanup function for browser refresh/close."""
#    file_utils.cleanup_files()
#    cache_utils.clear_session_cache()
#    print("Cleanup performed on browser refresh/close")

def cleanup_files():
    """Clean up sample data and data dictionary files"""
    file_utils.cleanup_files()

# Register cleanup for normal Python process termination
atexit.register(cleanup_files)

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
        file_utils.delete_sample_file()
        file_utils.delete_data_dict()
        cache_utils.clear_session_cache()
        st.session_state['data_dict_ready'] = False
        st.session_state['file_uploaded'] = False
        #ui_components.add_message("assistant", "File removed. Please upload a new file to continue.")
        print("File and data dictionary deleted, and cache cleared due to file removal")

    # Save the uploaded file and generate data dictionary
    if not st.session_state['data_dict_ready']:
        if uploaded_file is not None:
            with ui_components.show_spinner("Generating data dictionary. Please wait..."):
                file_utils.save_uploaded_file(uploaded_file)
                st.session_state['file_uploaded'] = True
                print(f"Sample data file saved as {file_utils.get_sample_data_path()}")
                # Generate enhanced data dictionary
                llm_util.save_enhanced_data_dictionary_to_yaml_file(sample_data_filename)
                generated_dict = BASE_DIR / config.DATA_DICT_FILENAME
                if generated_dict.exists() and generated_dict != data_dict_filename:
                  generated_dict.replace(data_dict_filename)
                  print("Enhanced data dictionary generated and saved as data-dict.yaml.")
                  st.session_state['data_dict_ready'] = True
                else:
                  st.error("Something went wrong generating the data dictionary.")
                  return
                # Check if data dictionary was generated
                data_dict_path = file_utils.get_data_dict_path()
                if not data_dict_path.exists():
                    # a. Delete the uploaded file
                    file_utils.delete_sample_file()
                    # b. Show error message in frontend
                    ui_components.show_warning_message('Something went wrong while processing the file, please upload it once again')
                    # c. Remove uploaded file from session state
                    st.session_state['file_uploaded'] = False
                    st.session_state['data_dict_ready'] = False
                    st.experimental_rerun()
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
        normalized_input = llm_util.normalize_user_input(user_input)
        cached = cache_utils.get_cached_sql(normalized_input)
        if cached:
            print(f'cache hit for user query: {user_input} (normalized: {normalized_input})')
            ui_components.show_info_message("Cache hit: Returning cached intent and SQL query.")
            sql_result = cached['sql']
            intent = cached['intent']
        else:
            print(f'cache miss for user query: {user_input} (normalized: {normalized_input})')
            intent = llm_util.classify_intent(user_input)
            if intent.strip() == "SQL_QUERY":
                enriched_data_dict = file_utils.load_data_dict()
                sql_result = llm_util.create_sql_from_nl(user_input, enriched_data_dict)
                cache_utils.set_cached_sql(normalized_input, intent, sql_result)
            else:
                sql_result = ""
                cache_utils.set_cached_sql(normalized_input, intent, "")
    
        ui_components.show_intent(intent)
        if sql_result:
            ui_components.show_sql_query(sql_result)
        
            try:
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                cursor.execute(sql_result)
                query_result = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                df = pandas.DataFrame(query_result, columns=columns)
                conn.close()
                ui_components.show_query_result(df)
            
                summary = llm_util.create_summary(df)
                ui_components.show_summary(summary)
            except Exception as exp:
                st.error(f"An error occurred: {exp}")
        # Clear the input
        st.session_state.user_query = ""

if __name__ == "__main__":
    main()
