import streamlit as st
from upload_page import show_upload_process
from query_page import show_query

def main():
    st.set_page_config(page_title="NL2SQL Chat", layout="centered")
    st.sidebar.title("Navigation")

    # Sidebar navigation with buttons
    if 'page' not in st.session_state:
        st.session_state['page'] = 'upload'

    if st.sidebar.button("Upload / Process"):
        st.session_state['page'] = 'upload'
    if st.sidebar.button("Query"):
        st.session_state['page'] = 'query'

    # Show the selected page
    if st.session_state['page'] == 'upload':
        show_upload_process()
    elif st.session_state['page'] == 'query':
        show_query()

if __name__ == "__main__":
    main()