import os
import re
import streamlit as st
import pathlib
import sys
from openai import OpenAI
from dotenv import load_dotenv
import pandas as pd
import yaml
from google.protobuf.json_format import ParseDict
from utils.schema.semantic_model_pb2 import SemanticModel
from protobuf_to_pydantic import msg_to_pydantic_model
from utils.schema.semantic_model_pb2 import SemanticModel as ProtoSemanticModel  

# Add the project root to the path so we can import modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))  
import config
from utils import file_utils

load_dotenv()

BASE_DIR = pathlib.Path(__file__).parent.resolve()
llm_model = config.LLM_MODEL
api_key = os.getenv("OPENAI_API_KEY")
client = OpenAI(api_key=api_key)

# Auto-generate Pydantic model from protobuf schema
PydanticSemanticModel = msg_to_pydantic_model(ProtoSemanticModel)


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
        # Use project root for system prompts
        #project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        prompt_path = file_utils.resolve_prompt_path(config.SYSTEM_PROMPTS_DIR, file_path)
        with open(prompt_path, 'r', encoding='utf-8') as file:
            content = file.read()
        return content

    except FileNotFoundError:
        return f"Error: File not found at {prompt_path}"
    except Exception as e:
        return f"Error: {str(e)}"

def normalize_user_input(user_input):
   """Normalize user input for consistent caching"""
   if not user_input:
        return ""
    
   normalized = user_input.lower().strip()
   normalized = re.sub(r'\s+', ' ', normalized)
   normalized = re.sub(r'[.!?,/-]+$', '', normalized)

   return normalized

def classify_intent(user_input):
    """Classifies the intent of the user's input using the LLM"""
    file_path = config.INTENT_IDENTIFIER_PROMPT_FILE
    system_prompt = load_prompt_file(file_path)
    user_prompt = f"Classify the intent of the following user query: {user_input}"
    response = call_response_api(llm_model, system_prompt, user_prompt)
    intent_raw = response.choices[0].message.content.strip()
    match = re.search(r"intent\s*:\s*(\w+)", intent_raw, re.IGNORECASE)
    if match:
        return match.group(1)
    return intent_raw

def create_nl2sqlchat_pompt(enriched_data_dict, table_name):
    system_prompt_file_path = config.NL2SQL_SYSTEM_PROMPT_FILE
    system_prompt = load_prompt_file(system_prompt_file_path)
    paths = file_utils.prepare_data_paths(table_name)
    sample_data_path = paths["data_file"]
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

def create_sql_from_nl(user_input, enriched_data_dict, table_name):
    """
    Generate SQL from user input and enriched data dictionary. No caching logic here.
    """
    nl2sql_system_prompt = create_nl2sqlchat_pompt(enriched_data_dict, table_name)
    nl2sql_user_prompt = f"Convert the following natural language question to SQL: {user_input}"
    response = call_response_api(llm_model, nl2sql_system_prompt, nl2sql_user_prompt)
    sql_result = response.choices[0].message.content
    return sql_result

def create_summary(df):
    file_path = config.SUMMARY_PROMPT_FILE
    prompt = load_prompt_file(file_path)
    summery_system_prompt = f"{prompt}"
    summery_user_prompt = f"analyze and provide a summery of the SQL query result - {df}" 
    response = call_response_api(llm_model, summery_system_prompt, summery_user_prompt)
    summery = response.choices[0].message.content
    return summery


def generate_enhanced_data_dictionary(sample_data_path):
    """
    Loads the sample_data_file (CSV), loads the system prompt from enhancedDDSystemPrompt.txt,
    and calls the LLM to generate an enhanced data dictionary. Returns the LLM's response as a string.
    Uses a more efficient prompt by sending only column names, types, and 2 sample values per column.
    """
    print(f"Loading sample data from {sample_data_path}...")
    try:
        df = pd.read_csv(sample_data_path)
        print(f"Successfully loaded sample data from {sample_data_path} with {len(df)} rows and {len(df.columns)} columns")
    except Exception as e:
        print(f"Error loading sample data from CSV: {e}")
        return None

    # Efficient prompt: only column names, types, and 2 sample values per column
    sample_info = []
    for col in df.columns:
        dtype = str(df[col].dtype)
        samples = df[col].dropna().unique()[:2].tolist()
        sample_info.append(f"{col} ({dtype}): {samples}")
    user_prompt = (
        "Here are the columns, types, and sample values from the uploaded data:\n" +
        "\n".join(sample_info) +
        "\nPlease generate an enhanced YAML data dictionary for this dataset."
    )

    # Step 3: Load the system prompt from the txt file
    system_prompt_path = file_utils.resolve_prompt_path(
        config.SYSTEM_PROMPTS_DIR,
        config.ENHANCED_DD_SYSTEM_PROMPT_FILE
    )
    system_prompt = load_prompt_file(system_prompt_path)
    
    if not system_prompt:
        print("Failed to load system prompt")
        return None

    # Step 4: Call the LLM
    print("Calling OpenAI API...")
    response = call_response_api(llm_model, system_prompt, user_prompt)

    if response and hasattr(response, 'choices') and response.choices:
        content = response.choices[0].message.content
        # Use improved regex for YAML code block extraction (no double backslash)
        match = re.search(r"yaml\s*([\s\S]+?)```", content, re.IGNORECASE)
        if match:
            yaml_text = match.group(1).strip()
        else:
            yaml_text = content.strip()
            print("Warning: No YAML code block found in LLM response. Using full response as YAML.")

        # Validate YAML before returning
        try:
            yaml.safe_load(yaml_text)
        except yaml.YAMLError as e:
            print(f"YAML validation error: {e}")
            return None

        return yaml_text
    else:
        print("No valid response from LLM.")
        return None

def validate_semantic_model(yaml_str):
    """
    Validates a YAML string against the SemanticModel protobuf schema.
    """
    try:
        is_valid, error = validate_yaml_with_proto(yaml_str)
        if is_valid:
            return {"status": "success", "message": "YAML is valid against the schema."}
        else:
            return {"status": "error", "message": error}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Validation failed: {str(e)}")


import datetime

def convert_dates_to_strings(obj):
    if isinstance(obj, dict):
        return {k: convert_dates_to_strings(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_dates_to_strings(i) for i in obj]
    elif isinstance(obj, (datetime.date, datetime.datetime)):
        return obj.isoformat()
    else:
        return obj

def convert_sample_values_to_strings(data):
    if isinstance(data, dict):
        for k, v in data.items():
            if k == "sampleValues" and isinstance(v, list):
                data[k] = [str(item) for item in v]
            else:
                convert_sample_values_to_strings(v)
    elif isinstance(data, list):
        for item in data:
            convert_sample_values_to_strings(item)
    return data
def validate_yaml_with_proto(yaml_str):
    """
    Validates a YAML string against the SemanticModel protobuf schema.
    Returns (True, None) if valid, (False, error_message) if not.
    """
    try:
        data = yaml.safe_load(yaml_str)
        data = convert_dates_to_strings(data)
        data = convert_sample_values_to_strings(data)
        # Convert YAML dict to protobuf
        ParseDict(data, SemanticModel())
        return True, None
    except Exception as e:
        return False, str(e)

def generate_structured_yaml(table_data_prompt):
    """
    Generate YAML using OpenAI responses API with structured output.
    Uses auto-generated Pydantic model from protobuf schema to guarantee valid structure.
    """
    try:
        response = client.responses.parse(
            model="gpt-4o-mini",
            input=[
                {"role": "system", "content": "You are a data dictionary generator. Generate a structured semantic model based on the provided table data."},
                {"role": "user", "content": table_data_prompt}
            ],
            text_format=PydanticSemanticModel
        )
        
        # Convert Pydantic model to YAML
        semantic_model = response.output_parsed
        
        if semantic_model is None:
            raise Exception("Response output_parsed is None - structured parsing failed")
            
        yaml_text = yaml.dump(semantic_model.dict(), sort_keys=False)
        
        print(f"DEBUG: Generated structured YAML ({len(yaml_text)} characters)")
        return yaml_text
        
    except Exception as e:
        print(f"ERROR: Failed to generate structured YAML: {e}")
        raise Exception(f"Structured YAML generation failed: {str(e)}")