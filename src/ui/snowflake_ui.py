import streamlit as st
import requests
import pandas as pd
import json
import time
from typing import Optional, Dict, Any

# Configuration
API_BASE_URL = "http://localhost:8001"

# Initialize session state
if "connection_id" not in st.session_state:
    st.session_state.connection_id = None
if "connected" not in st.session_state:
    st.session_state.connected = False
if "selected_database" not in st.session_state:
    st.session_state.selected_database = None
if "selected_schema" not in st.session_state:
    st.session_state.selected_schema = None
if "selected_table" not in st.session_state:
    st.session_state.selected_table = None
if "chat_history" not in st.session_state:
    st.session_state.chat_history = []
if "dictionary_content" not in st.session_state:
    st.session_state.dictionary_content = None

def make_api_request(method: str, endpoint: str, **kwargs) -> requests.Response:
    """Make API request with error handling"""
    url = f"{API_BASE_URL}{endpoint}"
    try:
        if method.upper() == "GET":
            response = requests.get(url, **kwargs)
        elif method.upper() == "POST":
            response = requests.post(url, **kwargs)
        elif method.upper() == "DELETE":
            response = requests.delete(url, **kwargs)
        else:
            raise ValueError(f"Unsupported method: {method}")
        return response
    except requests.exceptions.ConnectionError:
        st.error("❌ Cannot connect to Snowflake API. Make sure the API server is running on port 8001.")
        st.stop()

def connect_to_snowflake():
    """Connect to Snowflake"""
    with st.spinner("Connecting to Snowflake..."):
        response = make_api_request("POST", "/connect")
        
        if response.status_code == 200:
            data = response.json()
            st.session_state.connection_id = data["connection_id"]
            st.session_state.connected = True
            st.success(f"✅ Connected to Snowflake!")
            st.info(f"**User:** {data['user']} | **Role:** {data['role']} | **Database:** {data['database']}")
            return True
        else:
            error_detail = response.json().get("detail", "Unknown error")
            st.error(f"❌ Connection failed: {error_detail}")
            return False

def disconnect_from_snowflake():
    """Disconnect from Snowflake"""
    if st.session_state.connection_id:
        response = make_api_request("DELETE", f"/connection/{st.session_state.connection_id}")
        if response.status_code == 200:
            st.session_state.connection_id = None
            st.session_state.connected = False
            st.session_state.selected_database = None
            st.session_state.selected_schema = None
            st.session_state.selected_table = None
            st.success("✅ Disconnected from Snowflake")
        else:
            st.error("❌ Failed to disconnect")

def get_databases():
    """Get list of databases"""
    if not st.session_state.connection_id:
        return []
    
    response = make_api_request("GET", f"/connection/{st.session_state.connection_id}/databases")
    if response.status_code == 200:
        return response.json()["databases"]
    return []

def get_schemas(database: str):
    """Get list of schemas for a database"""
    if not st.session_state.connection_id:
        return []
    
    response = make_api_request("GET", f"/connection/{st.session_state.connection_id}/schemas", 
                               params={"database": database})
    if response.status_code == 200:
        return response.json()["schemas"]
    return []

def get_tables(database: str, schema: str):
    """Get list of tables for a schema"""
    if not st.session_state.connection_id:
        return []
    
    response = make_api_request("GET", f"/connection/{st.session_state.connection_id}/tables",
                               params={"database": database, "schema": schema})
    if response.status_code == 200:
        return response.json()["tables"]
    return []

def get_stage_files(database: str, schema: str):
    """Get stage files for dictionary loading"""
    if not st.session_state.connection_id:
        return []
    
    # First get stages
    response = make_api_request("GET", f"/connection/{st.session_state.connection_id}/stages",
                               params={"database": database, "schema": schema})
    
    if response.status_code != 200:
        return []
    
    stages = response.json()["stages"]
    all_files = []
    
    for stage in stages:
        stage_name = f"@{database}.{schema}.{stage['name']}"
        file_response = make_api_request("GET", f"/connection/{st.session_state.connection_id}/stage-files",
                                        params={"stage_name": stage_name})
        
        if file_response.status_code == 200:
            files = file_response.json()["files"]
            yaml_files = [f for f in files if f["name"].endswith((".yaml", ".yml"))]
            for file in yaml_files:
                file["stage_name"] = stage_name
            all_files.extend(yaml_files)
    
    return all_files

def load_dictionary_file(stage_name: str, file_name: str):
    """Load dictionary file from stage"""
    if not st.session_state.connection_id:
        return None
    
    response = make_api_request("GET", f"/connection/{st.session_state.connection_id}/load-stage-file",
                               params={"stage_name": stage_name, "file_name": file_name})
    
    if response.status_code == 200:
        return response.json()["content"]
    return None

def execute_sql(sql: str, limit: int = 100):
    """Execute SQL query"""
    if not st.session_state.connection_id:
        return None
    
    response = make_api_request("POST", f"/connection/{st.session_state.connection_id}/execute-sql",
                               json={"sql": sql, "limit": limit})
    
    if response.status_code == 200:
        return response.json()
    else:
        error_detail = response.json().get("detail", "Unknown error")
        st.error(f"❌ SQL execution failed: {error_detail}")
        return None

def process_nl_query(query: str, table_name: str, dictionary_content: Optional[str] = None):
    """Process natural language query"""
    if not st.session_state.connection_id:
        return None
    
    query_data = {
        "query": query,
        "connection_id": st.session_state.connection_id,
        "table_name": table_name,
        "dictionary_content": dictionary_content
    }
    
    response = make_api_request("POST", f"/connection/{st.session_state.connection_id}/query",
                               json=query_data)
    
    if response.status_code == 200:
        return response.json()
    else:
        error_detail = response.json().get("detail", "Unknown error")
        st.error(f"❌ NL query processing failed: {error_detail}")
        return None

def main():
    st.set_page_config(
        page_title="Snowflake Cortex Analyst",
        page_icon="❄️",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    st.title("❄️ Snowflake Cortex Analyst")
    st.markdown("---")
    
    # Sidebar for connection and navigation
    with st.sidebar:
        st.header("🔗 Connection")
        
        # Connection status
        if st.session_state.connected:
            st.success("✅ Connected")
            if st.button("🔌 Disconnect", type="secondary"):
                disconnect_from_snowflake()
                st.rerun()
        else:
            st.warning("❌ Not Connected")
            if st.button("🔌 Connect to Snowflake", type="primary"):
                if connect_to_snowflake():
                    st.rerun()
        
        if st.session_state.connected:
            st.markdown("---")
            st.header("📊 Data Selection")
            
            # Database selection
            databases = get_databases()
            if databases:
                selected_db = st.selectbox(
                    "Select Database:",
                    options=databases,
                    index=databases.index(st.session_state.selected_database) if st.session_state.selected_database in databases else 0
                )
                
                if selected_db != st.session_state.selected_database:
                    st.session_state.selected_database = selected_db
                    st.session_state.selected_schema = None
                    st.session_state.selected_table = None
                    st.rerun()
            
            # Schema selection
            if st.session_state.selected_database:
                schemas = get_schemas(st.session_state.selected_database)
                if schemas:
                    selected_schema = st.selectbox(
                        "Select Schema:",
                        options=schemas,
                        index=schemas.index(st.session_state.selected_schema) if st.session_state.selected_schema in schemas else 0
                    )
                    
                    if selected_schema != st.session_state.selected_schema:
                        st.session_state.selected_schema = selected_schema
                        st.session_state.selected_table = None
                        st.rerun()
            
            # Table selection
            if st.session_state.selected_database and st.session_state.selected_schema:
                tables = get_tables(st.session_state.selected_database, st.session_state.selected_schema)
                if tables:
                    table_names = [t["table"] for t in tables]
                    selected_table = st.selectbox(
                        "Select Table:",
                        options=table_names,
                        index=table_names.index(st.session_state.selected_table) if st.session_state.selected_table in table_names else 0
                    )
                    
                    if selected_table != st.session_state.selected_table:
                        st.session_state.selected_table = selected_table
                        st.rerun()
            
            # Dictionary file selection
            if st.session_state.selected_database and st.session_state.selected_schema:
                st.markdown("---")
                st.subheader("📚 Data Dictionary")
                
                stage_files = get_stage_files(st.session_state.selected_database, st.session_state.selected_schema)
                if stage_files:
                    file_options = [f["name"] for f in stage_files]
                    selected_file = st.selectbox("Select Dictionary File:", options=["None"] + file_options)
                    
                    if selected_file != "None":
                        if st.button("📥 Load Dictionary"):
                            file_info = next(f for f in stage_files if f["name"] == selected_file)
                            content = load_dictionary_file(file_info["stage_name"], selected_file)
                            if content:
                                st.session_state.dictionary_content = content
                                st.success(f"✅ Loaded {selected_file}")
                            else:
                                st.error("❌ Failed to load dictionary")
                    
                    if st.session_state.dictionary_content:
                        st.info("✅ Dictionary loaded")
                        if st.button("🗑️ Clear Dictionary"):
                            st.session_state.dictionary_content = None
                            st.rerun()
                else:
                    st.info("No YAML dictionary files found in stages")
    
    # Main content area
    if not st.session_state.connected:
        st.info("👆 Please connect to Snowflake using the sidebar to get started.")
        return
    
    # Tab layout
    tab1, tab2, tab3, tab4 = st.tabs(["💬 Chat", "🔍 SQL Query", "📊 Data Explorer", "⚙️ Settings"])
    
    with tab1:
        st.header("💬 Natural Language Chat")
        
        if not st.session_state.selected_table:
            st.warning("Please select a table from the sidebar to start chatting.")
        else:
            st.info(f"Chatting about: **{st.session_state.selected_database}.{st.session_state.selected_schema}.{st.session_state.selected_table}**")
            
            # Chat history
            for i, chat in enumerate(st.session_state.chat_history):
                if chat["type"] == "user":
                    st.chat_message("user").write(chat["content"])
                else:
                    with st.chat_message("assistant"):
                        st.write(chat["content"])
                        if "sql" in chat:
                            st.code(chat["sql"], language="sql")
                        if "data" in chat:
                            st.dataframe(chat["data"], use_container_width=True)
            
            # Chat input
            if prompt := st.chat_input("Ask a question about your data..."):
                # Add user message
                st.session_state.chat_history.append({"type": "user", "content": prompt})
                
                # Process with NL2SQL
                with st.spinner("Processing your question..."):
                    nl_result = process_nl_query(prompt, st.session_state.selected_table, st.session_state.dictionary_content)
                    
                    if nl_result and nl_result.get("intent") == "SQL_QUERY" and "sql" in nl_result:
                        sql = nl_result["sql"]
                        
                        # Execute the generated SQL
                        sql_result = execute_sql(sql, limit=50)
                        
                        if sql_result:
                            df = pd.DataFrame(sql_result["result"])
                            response_content = f"I found {len(df)} rows. Here's the result:"
                            
                            chat_entry = {
                                "type": "assistant",
                                "content": response_content,
                                "sql": sql,
                                "data": df
                            }
                        else:
                            chat_entry = {
                                "type": "assistant", 
                                "content": "I generated SQL but couldn't execute it. Please check the query.",
                                "sql": sql
                            }
                    else:
                        chat_entry = {
                            "type": "assistant",
                            "content": "I couldn't generate a SQL query for that question. Try rephrasing or being more specific."
                        }
                    
                    st.session_state.chat_history.append(chat_entry)
                    st.rerun()
    
    with tab2:
        st.header("🔍 SQL Query Interface")
        
        col1, col2 = st.columns([3, 1])
        
        with col1:
            sql_query = st.text_area(
                "Enter SQL Query:",
                height=150,
                placeholder="SELECT * FROM your_table LIMIT 10;"
            )
        
        with col2:
            st.markdown("**Query Options:**")
            limit = st.number_input("Row Limit:", min_value=1, max_value=10000, value=100)
            
            if st.button("▶️ Execute Query", type="primary"):
                if sql_query.strip():
                    result = execute_sql(sql_query, limit)
                    if result:
                        st.success(f"✅ Query executed successfully! {result['row_count']} rows returned.")
                        
                        if result["result"]:
                            df = pd.DataFrame(result["result"])
                            st.dataframe(df, use_container_width=True)
                            
                            # Download option
                            csv = df.to_csv(index=False)
                            st.download_button(
                                "📥 Download CSV",
                                csv,
                                "query_results.csv",
                                "text/csv"
                            )
                        else:
                            st.info("Query executed but returned no data.")
                else:
                    st.warning("Please enter a SQL query.")
    
    with tab3:
        st.header("📊 Data Explorer")
        
        if st.session_state.selected_table:
            table_name = f"{st.session_state.selected_database}.{st.session_state.selected_schema}.{st.session_state.selected_table}"
            st.subheader(f"Table: {table_name}")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                if st.button("📋 Show Schema"):
                    sql = f"DESCRIBE TABLE {table_name}"
                    result = execute_sql(sql)
                    if result and result["result"]:
                        df = pd.DataFrame(result["result"])
                        st.subheader("Table Schema")
                        st.dataframe(df, use_container_width=True)
            
            with col2:
                if st.button("🔢 Row Count"):
                    sql = f"SELECT COUNT(*) as total_rows FROM {table_name}"
                    result = execute_sql(sql)
                    if result and result["result"]:
                        count = result["result"][0]["TOTAL_ROWS"]
                        st.metric("Total Rows", f"{count:,}")
            
            with col3:
                if st.button("👀 Sample Data"):
                    sql = f"SELECT * FROM {table_name} LIMIT 10"
                    result = execute_sql(sql)
                    if result and result["result"]:
                        df = pd.DataFrame(result["result"])
                        st.subheader("Sample Data (10 rows)")
                        st.dataframe(df, use_container_width=True)
        else:
            st.info("Select a table from the sidebar to explore its data.")
    
    with tab4:
        st.header("⚙️ Settings & Information")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("🔗 Connection Info")
            if st.session_state.connected:
                # Get connection status
                response = make_api_request("GET", f"/connection/{st.session_state.connection_id}/status")
                if response.status_code == 200:
                    status_data = response.json()
                    st.json(status_data)
                
                st.subheader("📊 API Health")
                health_response = make_api_request("GET", "/health")
                if health_response.status_code == 200:
                    health_data = health_response.json()
                    st.json(health_data)
        
        with col2:
            st.subheader("💾 Session Data")
            
            if st.button("🗑️ Clear Chat History"):
                st.session_state.chat_history = []
                st.success("Chat history cleared!")
            
            if st.button("🔄 Reset Session"):
                for key in ["selected_database", "selected_schema", "selected_table", "chat_history", "dictionary_content"]:
                    if key in st.session_state:
                        del st.session_state[key]
                st.success("Session reset!")
                st.rerun()
            
            st.subheader("📚 Dictionary Status")
            if st.session_state.dictionary_content:
                st.success("✅ Dictionary loaded")
                with st.expander("View Dictionary Content"):
                    st.text(st.session_state.dictionary_content[:1000] + "..." if len(st.session_state.dictionary_content) > 1000 else st.session_state.dictionary_content)
            else:
                st.info("No dictionary loaded")

if __name__ == "__main__":
    main()