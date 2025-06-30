import streamlit as st
import pandas as pd
import requests

API_URL = "http://localhost:8000"

def show_query():
    st.header("Query Dataset")

    user_query = st.text_input("Enter your question (natural language):")
    if st.button("Run Query") and user_query.strip():
        with st.spinner("Processing query..."):
            try:
                params = {"query": user_query}
                response = requests.post(f"{API_URL}/query/", params=params)
                response.raise_for_status()
                result = response.json()
                if result.get("status") != "success":
                    st.error("API returned an error: " + result.get("detail", "Unknown error"))
                    return
                intent = result.get("intent", "")
                sql_query = result.get("sql", "")
                st.info(f"Intent: {intent}")
                st.code(sql_query, language="sql")

                # Execute SQL if present
                if sql_query:
                    exec_response = requests.post(
                        f"{API_URL}/execute-sql/",
                        json={"sql": sql_query, "base_name": None}
                    )
                    exec_response.raise_for_status()
                    exec_result = exec_response.json()
                    if exec_result.get("status") == "success":
                        columns = exec_result.get("columns", [])
                        data = exec_result.get("result", [])
                        if data:
                            df = pd.DataFrame(data, columns=columns)
                            st.dataframe(df)
                            # Optionally, generate a summary
                            df_json = df.to_json(orient='records')
                            try:
                                summary_resp = requests.post(
                                    f"{API_URL}/generate-summary/",
                                    json={"data": df_json}
                                )
                                summary_resp.raise_for_status()
                                summary_result = summary_resp.json()
                                if summary_result.get("status") == "success":
                                    st.success("Summary: " + summary_result.get("summary", "No summary available"))
                                else:
                                    st.warning("Could not generate summary: " + summary_result.get("detail", "Unknown error"))
                            except Exception as e:
                                st.warning(f"Could not generate summary: {e}")
                        else:
                            st.info("No results returned for this query.")
                    else:
                        st.error("SQL execution failed: " + exec_result.get("detail", "Unknown error"))
            except Exception as e:
                st.error(f"Error processing query: {e}")