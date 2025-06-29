import sys
import os
import io
import asyncio
import importlib.util

# Add src to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))

# Dynamically import nl2sql-api.py as a module
api_path = os.path.join(os.path.dirname(__file__), '..', 'src', 'api', 'nl2sql-api.py')
spec = importlib.util.spec_from_file_location("nl2sql_api", api_path)
nl2sql_api = importlib.util.module_from_spec(spec)
spec.loader.exec_module(nl2sql_api)

class DummyUploadFile:
    def __init__(self, filename, content):
        self.filename = filename
        self.file = io.BytesIO(content)
    def read(self):
        return self.file.read()
    @property
    def name(self):
        return self.filename
    def getbuffer(self):
        return self.file.getbuffer()
        
# Path to your real CSV file
csv_path = r"C:\Users\ujjal\Downloads\hmda\hmda_sample_new.csv"
with open(csv_path, "rb") as f:
    content = f.read()
dummy_file = DummyUploadFile("hmda_sample_new.csv", content)

# Call the upload function directly
result = asyncio.run(nl2sql_api.upload_file(dummy_file))
print(result)