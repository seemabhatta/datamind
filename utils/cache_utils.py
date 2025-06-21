import streamlit as st

CACHE_SIZE = 100

def get_cached_sql(user_input):
    """Get cached SQL from session state."""
    if 'QUERY_CACHE' not in st.session_state:
        st.session_state.QUERY_CACHE = {}
    return st.session_state.QUERY_CACHE.get(user_input)

def set_cached_sql(user_input, intent, sql_response):
    """Set user query, intent, and SQL in cache."""
    if 'QUERY_CACHE' not in st.session_state:
        st.session_state.QUERY_CACHE = {}
    # Enforce cache size limit
    if len(st.session_state.QUERY_CACHE) >= CACHE_SIZE:
        # Remove the first inserted item (FIFO)
        first_key = next(iter(st.session_state.QUERY_CACHE))
        del st.session_state.QUERY_CACHE[first_key]
    st.session_state.QUERY_CACHE[user_input] = {'intent': intent, 'sql': sql_response}

def clear_session_cache():
    """Clear ONLY the cache from session state - DO NOT delete files."""
    if 'QUERY_CACHE' in st.session_state:
        st.session_state.QUERY_CACHE.clear()
        print('Session cache cleared - files preserved')

def print_cache_content():
    """Print the current cache content in a readable format."""
    if 'QUERY_CACHE' in st.session_state and st.session_state.QUERY_CACHE:
        for k, v in st.session_state.QUERY_CACHE.items():
            print(f'User Query: {k}\n  Intent: {v["intent"]}\n  SQL: {v["sql"]}\n')
    else:
        print('Current cache content: (empty)')
