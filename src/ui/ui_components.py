import streamlit as st
import re

def show_title_and_clear_cache(clear_cache_callback):
    """Display the app title and a Clear Cache button in the top right."""
    col1, col2 = st.columns([4, 1])
    with col1:
        st.title("NL2SQL Chat")
    with col2:
        if st.button("Clear Cache"):
            clear_cache_callback()
            st.success("Cache cleared successfully!")

def show_file_uploader():
    """Display the file uploader and return the uploaded file object."""
    return st.file_uploader("To get started upload your sample data file (CSV)", type=["csv"])

def show_spinner(message):
    """Display a spinner with the given message."""
    return st.spinner(message)

def show_info_message(message):
    st.info(message)

def show_warning_message(message):
    st.warning(message)

def show_success_message(message):
    st.success(message)

def show_sql_query(sql_result):
   sql_code = re.findall(r"``````", sql_result, re.DOTALL)
   if sql_code:
        sql_query = sql_code[0].strip()
   else:
        sql_query = sql_result.strip()
   st.write("\n---\n**SQL Query:**\n")
   st.code(sql_query, language='sql')

def show_query_result(df):
    st.write("\n---\n**Query Result:**\n")
    st.dataframe(df)

def show_summary(summary):
    st.write("\n---\n**Summary:**\n")
    st.write(summary)

def show_intent(intent):
    st.write(f"Identified Intent: {intent}")

def add_message(role, content):
    """Add a message to the Streamlit session state for chat or logging purposes."""
    if 'messages' not in st.session_state:
        st.session_state['messages'] = []
    st.session_state['messages'].append({'role': role, 'content': content})

def show_chat_messages():
    """Display chat messages from the session state."""
    if 'messages' in st.session_state:
        for msg in st.session_state['messages']:
            if msg['role'] == 'user':
                st.markdown(f"**User:** {msg['content']}")
            elif msg['role'] == 'assistant':
                st.markdown(f"**Assistant:** {msg['content']}")
            else:
                st.markdown(f"**{msg['role'].capitalize()}:** {msg['content']}")


