import pandas
from openai import OpenAI
import streamlit as st
import sqlite3
import re
import os
from dotenv import load_dotenv
import pathlib
import importlib.util
import config
import atexit
from collections import OrderedDict
import cache_utils
import ui_components
import file_utils

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

def call_response_api(llm_model, system_prompt, user_prompt):
    response = client.chat.completions.create(
        model=f"{llm_model}",
        messages=[
            {"role": "system", "content": f"{system_prompt}"},
            {"role": "user", "content": f"{user_prompt}"}
        ]
    )
    return response

def load_prompt_file(file_path):
    try:
        prompt_path = BASE_DIR / system_prompts_dir / file_path if not pathlib.Path(file_path).is_absolute() else pathlib.Path(file_path)
        with open(prompt_path, 'r', encoding='utf-8') as file:
            content = file.read()
        return content
    except FileNotFoundError:
        return f"Error: File not found at {prompt_path}"
    except Exception as e:
        return f"Error: {str(e)}"

def create_nl2sqlchat_pompt(enriched_data_dict):
    system_prompt_file_path = nl2sql_system_prompt_file
    system_prompt = load_prompt_file(system_prompt_file_path)
    sample_data_path = BASE_DIR / sample_data_filename
    try:
        with open(sample_data_path, "r", encoding="utf-8") as f:
            sample_data = f.read()
    except FileNotFoundError:
        sample_data = ""
    prompt = f"""
                {system_prompt}
                ## Database Dictionary -  
                {enriched_data_dict}  
                ## Table Name
                {table_name}
                ## Sample Data
                {sample_data}
            """
    return prompt

def create_summery_prompt():
    file_path = summary_prompt_file
    summary = load_prompt_file(file_path)
    prompt = f"""
            {summary}
            """
    return prompt

def normalize_user_input(user_input):
    """Normalize user input for consistent caching"""
    if not user_input:
        return ""
    
    normalized = user_input.lower()
    normalized = re.sub(r'\s+', ' ', normalized.strip())
    normalized = re.sub(r'[.!?]+$', '', normalized)
    
    return normalized

def create_sql_from_nl():
    
    sql_result = ""
    with st.form(key="user_query_form"):
        user_input = st.text_input("Please enter your query here -", key="user_query")
        submitted = st.form_submit_button("Enter")

    if not submitted or not user_input.strip():
        return ""

    normalized_input = normalize_user_input(user_input)

    # Check cache first
    cached = cache_utils.get_cached_sql(normalized_input)
    if cached:
        print(f'cache hit for user query: {user_input} (normalized: {normalized_input})')
        ui_components.show_info_message("Cache hit: Returning cached intent and SQL query.")
        st.write(f"Identified Intent: {cached['intent']}")
        return cached['sql']
    else:
        print(f'cache miss for user query: {user_input} (normalized: {normalized_input})')
        intent = classify_intent(user_input)
        if intent.strip() == "SQL_QUERY":
            data_dict_file_path = config.DATA_DICT_FILENAME
            enriched_data_dict = file_utils.load_data_dict(data_dict_file_path)
            nl2sql_system_prompt = create_nl2sqlchat_pompt(enriched_data_dict)
            nl2sql_user_prompt = f"Convert the following natural language question to SQL: {user_input}"
            response = call_response_api(llm_model, nl2sql_system_prompt, nl2sql_user_prompt)
            sql_result = response.choices[0].message.content

            cache_utils.set_cached_sql(normalized_input, intent, sql_result)
        else:
            sql_result = ""
            cache_utils.set_cached_sql(normalized_input, intent, "")

        st.write(f"Identified Intent: {intent}")
        return sql_result
    
def extract_sql(sql_response):
    sql_code = re.findall(r"``````", sql_response, re.DOTALL)
    if sql_code:
        sql_query = sql_code[0].strip()
    else:
        sql_query = sql_response.strip()
    return sql_query

def classify_intent(user_input):
    """Classifies the intent of the user's input using the LLM"""
    file_path = intent_identifier_prompt_file
    system_prompt = load_prompt_file(file_path)
    user_prompt = f"Classify the intent of the following user query: {user_input}"
    response = call_response_api(llm_model, system_prompt, user_prompt)
    intent_raw = response.choices[0].message.content.strip()
    match = re.search(r"intent\s*:\s*(\w+)", intent_raw, re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return intent_raw

def cleanup_on_refresh():
    """Cleanup function for browser refresh/close."""
    file_utils.cleanup_files()
    cache_utils.clear_session_cache()
    print("Cleanup performed on browser refresh/close")

def cleanup_files():
    """Clean up sample data and data dictionary files"""
    file_utils.cleanup_files()

# Register cleanup for normal Python process termination
atexit.register(cleanup_files)

def main():
    st.set_page_config(page_title="NL2SQL Chat", layout="centered")
    # Use modular UI for title and clear cache
    ui_components.show_title_and_clear_cache(cache_utils.clear_session_cache)

    # Initialize session state for tracking browser refresh/close
    if 'initialized' not in st.session_state:
        st.session_state['initialized'] = True
        # This will run on first load and browser refresh
        cleanup_on_refresh()

    # Use session state to track if data dictionary is already generated
    if 'data_dict_ready' not in st.session_state:
        st.session_state['data_dict_ready'] = False
    if 'file_uploaded' not in st.session_state:
        st.session_state['file_uploaded'] = False

    # File upload handling
    uploaded_file = ui_components.show_file_uploader()

    # Handle file removal: delete files and clear cache when user removes the file
    if st.session_state['file_uploaded'] and uploaded_file is None:
        file_utils.delete_sample_file()
        file_utils.delete_data_dict()
        cache_utils.clear_session_cache()  # Clear the cache when file is removed
        st.session_state['data_dict_ready'] = False
        st.session_state['file_uploaded'] = False
        print("File and data dictionary deleted, and cache cleared due to file removal")

    # Save the uploaded file and generate data dictionary
    if not st.session_state['data_dict_ready']:
        if uploaded_file is not None:
            with ui_components.show_spinner("Generating data dictionary. Please wait..."):
                file_utils.save_uploaded_file(uploaded_file)
                st.session_state['file_uploaded'] = True
                print(f"Sample data file saved as {file_utils.get_sample_data_path()}")

                try:
                    testEnhDD_path = BASE_DIR / "testEnhDD.py"
                    spec = importlib.util.spec_from_file_location("testEnhDD", str(testEnhDD_path))
                    testEnhDD = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(testEnhDD)
                    testEnhDD.save_enhanced_data_dictionary_to_yaml_file(config.SAMPLE_DATA_FILENAME)
                    generated_dict = BASE_DIR / config.DATA_DICT_FILENAME
                    if generated_dict.exists() and generated_dict != data_dict_filename:
                        generated_dict.replace(data_dict_filename)
                    print("Enhanced data dictionary generated and saved as data-dict.yaml.")
                    st.session_state['data_dict_ready'] = True
                except Exception as e:
                    st.error(f"Failed to generate data dictionary: {e}")
                    return
        else:
            return
    else:
        print("Querying using existing data dictionary.")

    sql_response = create_sql_from_nl()
    if sql_response:
        sql_query = extract_sql(sql_response)
        ui_components.show_sql_query(sql_query)
        
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute(sql_query)
            query_result = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            df = pandas.DataFrame(query_result, columns=columns)
            conn.close()
            ui_components.show_query_result(df)
            
            summery_system_propmt = create_summery_prompt()
            summery_user_prompt = f"analyze and provide a summery of the SQL query result - {df}"
            final_response = call_response_api(llm_model, summery_system_propmt, summery_user_prompt)
            summery = final_response.choices[0].message.content
            ui_components.show_summary(summery)
        except Exception as exp:
            st.error(f"An error occurred: {exp}")

if __name__ == "__main__":
    main()
