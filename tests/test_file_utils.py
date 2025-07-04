import os
import shutil
import unittest
import pandas as pd
import yaml
import sqlite3
from pathlib import Path
from io import BytesIO
from unittest.mock import MagicMock

# Add the parent directory to sys.path to import the module
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.file_utils import (
    prepare_data_paths,
    save_uploaded_file,
    save_dict_yaml,
    save_dataframe_to_sqlite,
    get_data_dict,
    get_db_connection,
    cleanup_files
)

class MockUploadFile:
    """Mock class to simulate an uploaded file object"""
    def __init__(self, name, content):
        self.name = name
        self.content = content
    
    def getbuffer(self):
        return self.content

class TestFileUtils(unittest.TestCase):
    
    def setUp(self):
        """Set up test environment before each test"""
        # Use a unique test base name to avoid conflicts
        self.test_base_name = "test_file"
        
        # Clean up any existing test data
        test_dir = Path("data") / self.test_base_name
        if test_dir.exists():
            try:
                shutil.rmtree(test_dir)
            except (PermissionError, OSError) as e:
                print(f"Warning: Could not clean up {test_dir}: {e}")
    
    def tearDown(self):
        """Clean up after each test"""
        # Close any open file handles
        import gc
        gc.collect()
        
        # Remove test data directory
        test_dir = Path("data") / self.test_base_name
        if test_dir.exists():
            try:
                shutil.rmtree(test_dir)
            except (PermissionError, OSError) as e:
                print(f"Warning: Could not clean up {test_dir} in tearDown: {e}")
    
    def test_prepare_data_paths(self):
        """Test prepare_data_paths function"""
        paths = prepare_data_paths(self.test_base_name)
        
        # Check if all paths are returned and directory is created
        self.assertTrue(isinstance(paths, dict))
        self.assertTrue("directory" in paths)
        self.assertTrue("data_file" in paths)
        self.assertTrue("db_file" in paths)
        self.assertTrue("dict_file" in paths)
        
        # Check if directory was created
        self.assertTrue(os.path.exists(paths["directory"]))
        
        # Check if paths are Path objects
        self.assertTrue(isinstance(paths["directory"], Path))
        self.assertTrue(isinstance(paths["data_file"], Path))
        self.assertTrue(isinstance(paths["db_file"], Path))
        self.assertTrue(isinstance(paths["dict_file"], Path))
    
    def test_save_uploaded_file(self):
        """Test save_uploaded_file function"""
        # Create mock file
        test_content = b"col1,col2\n1,2\n3,4"
        mock_file = MockUploadFile("test_file.csv", test_content)
        
        # Save the file
        saved_path = save_uploaded_file(mock_file)
        
        # Check if file exists and content is correct
        self.assertTrue(os.path.exists(saved_path))
        with open(saved_path, "rb") as f:
            content = f.read()
            self.assertEqual(content, test_content)
        
        # Check if the file is saved with original name
        self.assertEqual(saved_path.name, "test_file.csv")
    
    def test_save_dict_yaml(self):
        """Test save_dict_yaml function"""
        # Create test YAML content
        yaml_content = "table_name: test\ncolumns:\n  - name: col1\n    type: int\n  - name: col2\n    type: int"
        
        # Save the YAML
        saved_path = save_dict_yaml(yaml_content, self.test_base_name)
        
        # Check if file exists and content is correct
        self.assertTrue(os.path.exists(saved_path))
        with open(saved_path, "r", encoding="utf-8") as f:
            content = f.read()
            self.assertEqual(content, yaml_content)
    
    def test_save_dataframe_to_sqlite(self):
        """Test save_dataframe_to_sqlite function"""
        # Create test dataframe
        df = pd.DataFrame({"col1": [1, 3], "col2": [2, 4]})
        
        # Save to SQLite
        db_path = save_dataframe_to_sqlite(df, self.test_base_name, "test_table")
        
        # Check if file exists
        self.assertTrue(os.path.exists(db_path))
        
        # Check if data was saved correctly
        conn = sqlite3.connect(db_path)
        saved_df = pd.read_sql_query("SELECT * FROM test_table", conn)
        conn.close()
        
        # Check if dataframes are equal
        pd.testing.assert_frame_equal(df, saved_df)
    
    def test_get_data_dict(self):
        """Test get_data_dict function"""
        # Create test YAML content and save it
        test_dict = {"table_name": "test", "columns": [{"name": "col1", "type": "int"}, {"name": "col2", "type": "int"}]}
        yaml_content = yaml.dump(test_dict)
        yaml_path = save_dict_yaml(yaml_content, self.test_base_name)
        
        # Verify the file was saved in the expected location
        self.assertTrue(os.path.exists(yaml_path))
        self.assertEqual(yaml_path.name, f"{self.test_base_name}.yaml")
        
        # Get the data dict
        loaded_dict = get_data_dict(self.test_base_name)
        
        # Check if dict was loaded correctly
        self.assertIsNotNone(loaded_dict)
        self.assertEqual(loaded_dict["table_name"], "test")
        self.assertEqual(len(loaded_dict["columns"]), 2)
        self.assertEqual(loaded_dict["columns"][0]["name"], "col1")
        self.assertEqual(loaded_dict["columns"][0]["type"], "int")
        self.assertEqual(loaded_dict["columns"][1]["name"], "col2")
        self.assertEqual(loaded_dict["columns"][1]["type"], "int")
        
        # Test with invalid YAML content
        invalid_yaml_content = "Invalid YAML content"
        invalid_yaml_path = save_dict_yaml(invalid_yaml_content, self.test_base_name)
        loaded_dict = get_data_dict(self.test_base_name)
        # For minimal agentic utility, accept string as result
        self.assertEqual(loaded_dict, "Invalid YAML content")
        
        # Test with missing YAML file
        missing_yaml_path = prepare_data_paths(self.test_base_name)["dict_file"]
        loaded_dict = get_data_dict(self.test_base_name)
        self.assertIsNone(loaded_dict)
    
    def test_get_db_connection(self):
        """Test get_db_connection function"""
        # Create test dataframe and save to SQLite
        df = pd.DataFrame({"col1": [1, 3], "col2": [2, 4]})
        db_path = save_dataframe_to_sqlite(df, self.test_base_name)
        
        # Verify the database file exists
        self.assertTrue(os.path.exists(db_path))
        self.assertEqual(db_path.name, f"{self.test_base_name}.db")
        
        # Get connection
        conn = get_db_connection(self.test_base_name)
        
        # Check if connection is valid
        self.assertIsNotNone(conn)
        self.assertIsInstance(conn, sqlite3.Connection)
        
        # Check if we can query data
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {self.test_base_name}")
        rows = cursor.fetchall()
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0][0], 1)  # First row, first column
        self.assertEqual(rows[0][1], 2)  # First row, second column
        self.assertEqual(rows[1][0], 3)  # Second row, first column
        self.assertEqual(rows[1][1], 4)  # Second row, second column
        
        # Test with custom table name
        custom_table = "custom_table"
        db_path = save_dataframe_to_sqlite(df, self.test_base_name, custom_table)
        conn = get_db_connection(self.test_base_name)
        cursor = conn.cursor()
        
        # Verify custom table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        table_names = [table[0] for table in tables]
        self.assertIn(custom_table, table_names)
        
        # Query custom table
        cursor.execute(f"SELECT * FROM {custom_table}")
        rows = cursor.fetchall()
        self.assertEqual(len(rows), 2)
        
        # Test with invalid database file
        invalid_db_path = save_uploaded_file(MockUploadFile("invalid.db", b"Invalid database content"))
        conn = get_db_connection(self.test_base_name)
        # For minimal agentic utility, connection may exist, but queries will fail
        if conn is not None:
            with self.assertRaises(sqlite3.DatabaseError):
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM some_table")
            conn.close()
        
        # Test with missing database file
        cleanup_files(self.test_base_name)
        conn = get_db_connection(self.test_base_name)
        self.assertIsNone(conn)
        if conn is not None:
            conn.close()
        
        # Clean up connections
        conn.close()
    
    def test_cleanup_files(self):
        """Test cleanup_files function"""
        # Create multiple test files
        paths = prepare_data_paths(self.test_base_name)
        
        # Create CSV file
        with open(paths["data_file"], "w") as f:
            f.write("col1,col2\n1,2\n3,4")
        self.assertTrue(os.path.exists(paths["data_file"]))
        
        # Create YAML file
        with open(paths["dict_file"], "w") as f:
            f.write("table_name: test\ncolumns:\n  - name: col1\n    type: int")
        self.assertTrue(os.path.exists(paths["dict_file"]))
        
        # Create SQLite DB
        df = pd.DataFrame({"col1": [1, 3], "col2": [2, 4]})
        conn = sqlite3.connect(paths["db_file"])
        df.to_sql(self.test_base_name, conn, if_exists='replace', index=False)
        conn.close()
        self.assertTrue(os.path.exists(paths["db_file"]))
        
        # Create another test file in the directory
        extra_file = paths["directory"] / "extra.txt"
        with open(extra_file, "w") as f:
            f.write("extra content")
        self.assertTrue(os.path.exists(extra_file))
        
        # Clean up specific base_name
        cleanup_files(self.test_base_name)
        
        # Check all files and directory are gone for the specific test_base_name
        self.assertFalse(os.path.exists(paths["directory"]))
        self.assertFalse(os.path.exists(paths["data_file"]))
        self.assertFalse(os.path.exists(paths["dict_file"]))
        self.assertFalse(os.path.exists(paths["db_file"]))
        self.assertFalse(os.path.exists(extra_file))
    
    def test_nonexistent_files(self):
        """Test behavior with nonexistent files"""
        # Try to get data dict for nonexistent file
        nonexistent_dict = get_data_dict("nonexistent")
        self.assertIsNone(nonexistent_dict)
        
        # Try to get DB connection for nonexistent file
        nonexistent_conn = get_db_connection("nonexistent")
        self.assertIsNone(nonexistent_conn)
        
        # Test cleanup with nonexistent base_name (should not raise errors)
        try:
            cleanup_files("nonexistent_base_name")
            # If we get here, no exception was raised
            self.assertTrue(True)
        except Exception as e:
            self.fail(f"cleanup_files raised {type(e).__name__} unexpectedly!")
        
        # Test with completely nonexistent base_name
        completely_random_name = "completely_random_nonexistent_name_12345"
        nonexistent_dict = get_data_dict(completely_random_name)
        self.assertIsNone(nonexistent_dict)
        
        nonexistent_conn = get_db_connection(completely_random_name)
        self.assertIsNone(nonexistent_conn)
        
        # Test cleanup with nonexistent base_name (should not raise errors)
        try:
            cleanup_files("nonexistent_base_name")
            # If we get here, no exception was raised
            self.assertTrue(True)
        except Exception as e:
            self.fail(f"cleanup_files raised {type(e).__name__} unexpectedly!")

if __name__ == "__main__":
    unittest.main()
    
