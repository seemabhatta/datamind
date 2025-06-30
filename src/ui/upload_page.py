import streamlit as st
import requests

API_URL = "http://localhost:8000"

def show_upload_process():
    st.header("Upload & Process Dataset")

    # List available datasets
    st.subheader("Available Datasets")
    try:
        resp = requests.get(f"{API_URL}/list-datasets/")
        resp.raise_for_status()
        datasets = resp.json().get("datasets", [])
        if datasets:
            st.write(datasets)
        else:
            st.info("No datasets found. Upload a CSV to get started.")
    except Exception as e:
        st.error(f"Could not fetch datasets: {e}")

    # File upload
    st.subheader("Upload a CSV File")
    uploaded_file = st.file_uploader("Choose a CSV file", type="csv")
    if uploaded_file is not None:
        with st.spinner("Uploading and processing file..."):
            try:
                files = {"file": (uploaded_file.name, uploaded_file.getvalue(), "text/csv")}
                response = requests.post(f"{API_URL}/upload-file/", files=files)
                response.raise_for_status()
                result = response.json()
                if result.get("status") == "success":
                    st.success("File uploaded and data dictionary generated!")
                    st.write("Data Dictionary Preview:", result.get("data_dictionary"))
                else:
                    st.error("API returned an error: " + result.get("detail", "Unknown error"))
            except Exception as e:
                st.error(f"Error uploading file: {e}")

    # Option to clear/reset data
    if st.button("Clear All Data"):
        try:
            response = requests.delete(f"{API_URL}/clear-data/")
            response.raise_for_status()
            st.success("All data cleared successfully.")
        except Exception as e:
            st.error(f"Error clearing data: {e}")