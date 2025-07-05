#!/usr/bin/env python3
"""
Comprehensive test suite for Snowflake API endpoints
Tests both ACCOUNTADMIN and service user authentication
"""
import requests
import json
import time
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

BASE_URL = "http://localhost:8001"

class TestSnowflakeAPI:
    """Test class for Snowflake API functionality"""
    
    def setup_class(self):
        """Setup test class with connection details"""
        self.base_url = BASE_URL
        self.connection_id = None
        self.service_connection_id = None
    
    def test_health_endpoint(self):
        """Test API health check endpoint"""
        response = requests.get(f"{self.base_url}/health")
        assert response.status_code == 200
        data = response.json()
        assert "status" in data
        assert data["status"] == "healthy"
        assert "active_connections" in data
        print("✅ Health endpoint working")
    
    def test_accountadmin_connection(self):
        """Test connection with ACCOUNTADMIN role"""
        # Temporarily set ACCOUNTADMIN credentials
        original_user = os.getenv("SNOWFLAKE_USER")
        original_password = os.getenv("SNOWFLAKE_PASSWORD") 
        original_role = os.getenv("SNOWFLAKE_ROLE")
        
        # Set ACCOUNTADMIN credentials in environment
        os.environ["SNOWFLAKE_USER"] = "UJJALBFM2025"
        os.environ["SNOWFLAKE_PASSWORD"] = "Lajjukumar1234"
        os.environ["SNOWFLAKE_ROLE"] = "ACCOUNTADMIN"
        
        # Wait for env vars to be picked up
        time.sleep(1)
        
        response = requests.post(f"{self.base_url}/connect")
        assert response.status_code == 200
        
        data = response.json()
        assert "connection_id" in data
        assert data["user"] == "UJJALBFM2025"
        assert data["role"] == "ACCOUNTADMIN"
        assert data["account"] == "KIXUIIJ-MTC00254"
        
        self.connection_id = data["connection_id"]
        print(f"✅ ACCOUNTADMIN connection successful: {self.connection_id}")
        
        # Restore original credentials
        os.environ["SNOWFLAKE_USER"] = original_user
        os.environ["SNOWFLAKE_PASSWORD"] = original_password
        os.environ["SNOWFLAKE_ROLE"] = original_role
    
    def test_service_user_connection(self):
        """Test connection with service user role"""
        response = requests.post(f"{self.base_url}/connect")
        assert response.status_code == 200
        
        data = response.json()
        assert "connection_id" in data
        assert data["user"] == "nl2sql_service_user"
        assert data["role"] == "nl2sql_service_role"
        assert data["account"] == "KIXUIIJ-MTC00254"
        
        self.service_connection_id = data["connection_id"]
        print(f"✅ Service user connection successful: {self.service_connection_id}")
    
    def test_connection_status(self):
        """Test connection status endpoint"""
        if not self.service_connection_id:
            self.test_service_user_connection()
            
        response = requests.get(f"{self.base_url}/connection/{self.service_connection_id}/status")
        assert response.status_code == 200
        
        data = response.json()
        assert data["status"] == "connected"
        assert data["connection_id"] == self.service_connection_id
        print("✅ Connection status check working")
    
    def test_list_databases(self):
        """Test database listing"""
        if not self.service_connection_id:
            self.test_service_user_connection()
            
        response = requests.get(f"{self.base_url}/connection/{self.service_connection_id}/databases")
        assert response.status_code == 200
        
        data = response.json()
        assert "databases" in data
        databases = data["databases"]
        assert len(databases) > 0
        assert "CORTES_DEMO_2" in databases
        print(f"✅ Database listing working: {len(databases)} databases found")
    
    def test_list_schemas(self):
        """Test schema listing for CORTES_DEMO_2"""
        if not self.service_connection_id:
            self.test_service_user_connection()
            
        response = requests.get(
            f"{self.base_url}/connection/{self.service_connection_id}/schemas",
            params={"database": "CORTES_DEMO_2"}
        )
        assert response.status_code == 200
        
        data = response.json()
        assert "schemas" in data
        schemas = data["schemas"]
        assert "CORTEX_DEMO" in schemas
        print(f"✅ Schema listing working: {schemas}")
    
    def test_list_tables(self):
        """Test table listing for CORTEX_DEMO schema"""
        if not self.service_connection_id:
            self.test_service_user_connection()
            
        response = requests.get(
            f"{self.base_url}/connection/{self.service_connection_id}/tables",
            params={"database": "CORTES_DEMO_2", "schema": "CORTEX_DEMO"}
        )
        assert response.status_code == 200
        
        data = response.json()
        assert "tables" in data
        tables = data["tables"]
        
        # Check expected tables exist
        table_names = [table["table"] for table in tables]
        expected_tables = ["DAILY_REVENUE", "HMDA_SAMPLE", "PRODUCT", "REGION"]
        
        for expected_table in expected_tables:
            assert expected_table in table_names, f"Expected table {expected_table} not found"
        
        print(f"✅ Table listing working: {table_names}")
    
    def test_list_stages(self):
        """Test stage listing"""
        if not self.service_connection_id:
            self.test_service_user_connection()
            
        response = requests.get(
            f"{self.base_url}/connection/{self.service_connection_id}/stages",
            params={"database": "CORTES_DEMO_2", "schema": "CORTEX_DEMO"}
        )
        assert response.status_code == 200
        
        data = response.json()
        assert "stages" in data
        stages = data["stages"]
        
        # Check CORTEX_DEMO_V2_STAGE exists
        stage_names = [stage["name"] for stage in stages]
        assert "CORTEX_DEMO_V2_STAGE" in stage_names
        print(f"✅ Stage listing working: {stage_names}")
    
    def test_list_stage_files(self):
        """Test stage file listing"""
        if not self.service_connection_id:
            self.test_service_user_connection()
            
        stage_name = "@CORTES_DEMO_2.CORTEX_DEMO.CORTEX_DEMO_V2_STAGE"
        response = requests.get(
            f"{self.base_url}/connection/{self.service_connection_id}/stage-files",
            params={"stage_name": stage_name}
        )
        assert response.status_code == 200
        
        data = response.json()
        assert "files" in data
        files = data["files"]
        assert len(files) > 0
        
        # Check for expected files
        file_names = [file["name"] for file in files]
        yaml_files = [f for f in file_names if f.endswith(".yaml")]
        assert len(yaml_files) > 0, "No YAML dictionary files found"
        
        print(f"✅ Stage file listing working: {len(files)} files, {len(yaml_files)} YAML files")
    
    def test_load_stage_file(self):
        """Test loading a dictionary file from stage"""
        if not self.service_connection_id:
            self.test_service_user_connection()
            
        stage_name = "@CORTES_DEMO_2.CORTEX_DEMO.CORTEX_DEMO_V2_STAGE"
        file_name = "cortex_demo_v2_stage/hmda_model.yaml"
        
        response = requests.get(
            f"{self.base_url}/connection/{self.service_connection_id}/load-stage-file",
            params={"stage_name": stage_name, "file_name": file_name}
        )
        assert response.status_code == 200
        
        data = response.json()
        assert "content" in data
        content = data["content"]
        assert len(content) > 0
        assert "yaml" in content.lower() or "model" in content.lower()
        
        print(f"✅ Stage file loading working: {len(content)} characters loaded")
    
    def test_execute_sql(self):
        """Test SQL execution"""
        if not self.service_connection_id:
            self.test_service_user_connection()
            
        sql = "SELECT COUNT(*) as row_count FROM CORTES_DEMO_2.CORTEX_DEMO.DAILY_REVENUE"
        response = requests.post(
            f"{self.base_url}/connection/{self.service_connection_id}/execute-sql",
            json={"sql": sql}
        )
        assert response.status_code == 200
        
        data = response.json()
        assert "status" in data
        assert data["status"] == "success"
        assert "result" in data
        assert "columns" in data
        assert len(data["result"]) > 0
        
        # Check result structure
        row_count = data["result"][0]["ROW_COUNT"]
        assert row_count > 0
        
        print(f"✅ SQL execution working: {row_count} rows in DAILY_REVENUE")
    
    def test_execute_sql_with_limit(self):
        """Test SQL execution with row limit"""
        if not self.service_connection_id:
            self.test_service_user_connection()
            
        sql = "SELECT * FROM CORTES_DEMO_2.CORTEX_DEMO.DAILY_REVENUE"
        response = requests.post(
            f"{self.base_url}/connection/{self.service_connection_id}/execute-sql",
            json={"sql": sql, "limit": 5}
        )
        assert response.status_code == 200
        
        data = response.json()
        assert data["row_count"] == 5
        assert len(data["result"]) == 5
        
        # Check expected columns
        expected_columns = ["DATE", "REVENUE", "COGS", "FORECASTED_REVENUE", "PRODUCT_ID", "REGION_ID"]
        for col in expected_columns:
            assert col in data["columns"]
        
        print(f"✅ SQL execution with limit working: {data['row_count']} rows returned")
    
    def test_nl_query_processing(self):
        """Test natural language query processing"""
        if not self.service_connection_id:
            self.test_service_user_connection()
            
        query_data = {
            "query": "Show me the total revenue by region",
            "connection_id": self.service_connection_id,
            "table_name": "DAILY_REVENUE",
            "dictionary_content": None
        }
        
        response = requests.post(
            f"{self.base_url}/connection/{self.service_connection_id}/query",
            json=query_data
        )
        assert response.status_code == 200
        
        data = response.json()
        assert "status" in data
        assert data["status"] == "success"
        assert "intent" in data
        
        print(f"✅ NL query processing working: Intent = {data['intent']}")
    
    def test_disconnect(self):
        """Test connection disconnection"""
        if not self.service_connection_id:
            self.test_service_user_connection()
            
        response = requests.delete(f"{self.base_url}/connection/{self.service_connection_id}")
        assert response.status_code == 200
        
        data = response.json()
        assert data["status"] == "success"
        
        # Verify connection is actually closed
        response = requests.get(f"{self.base_url}/connection/{self.service_connection_id}/status")
        assert response.status_code == 404  # Connection should not be found
        
        print("✅ Disconnect working")


def run_performance_tests():
    """Run performance and load tests"""
    print("\n=== Performance Tests ===")
    
    # Test multiple concurrent connections
    start_time = time.time()
    connections = []
    
    for i in range(3):
        response = requests.post(f"{BASE_URL}/connect")
        if response.status_code == 200:
            connections.append(response.json()["connection_id"])
    
    connection_time = time.time() - start_time
    print(f"✅ Created {len(connections)} connections in {connection_time:.2f}s")
    
    # Clean up connections
    for conn_id in connections:
        requests.delete(f"{BASE_URL}/connection/{conn_id}")
    
    print("✅ Performance tests completed")


def run_error_handling_tests():
    """Test error handling scenarios"""
    print("\n=== Error Handling Tests ===")
    
    # Test invalid connection ID
    response = requests.get(f"{BASE_URL}/connection/invalid-id/status")
    assert response.status_code == 404
    print("✅ Invalid connection ID handled correctly")
    
    # Test invalid SQL
    response = requests.post(f"{BASE_URL}/connect")
    if response.status_code == 200:
        conn_id = response.json()["connection_id"]
        
        response = requests.post(
            f"{BASE_URL}/connection/{conn_id}/execute-sql",
            json={"sql": "INVALID SQL STATEMENT"}
        )
        assert response.status_code == 500
        print("✅ Invalid SQL handled correctly")
        
        # Clean up
        requests.delete(f"{BASE_URL}/connection/{conn_id}")
    
    print("✅ Error handling tests completed")


if __name__ == "__main__":
    """Run all tests when script is executed directly"""
    print("🧪 Starting Snowflake API Test Suite")
    print("=" * 50)
    
    # Initialize test class
    test_suite = TestSnowflakeAPI()
    test_suite.setup_class()
    
    # Run all tests
    tests = [
        test_suite.test_health_endpoint,
        test_suite.test_service_user_connection,
        test_suite.test_connection_status,
        test_suite.test_list_databases,
        test_suite.test_list_schemas,
        test_suite.test_list_tables,
        test_suite.test_list_stages,
        test_suite.test_list_stage_files,
        test_suite.test_load_stage_file,
        test_suite.test_execute_sql,
        test_suite.test_execute_sql_with_limit,
        test_suite.test_nl_query_processing,
        test_suite.test_disconnect,
    ]
    
    passed = 0
    failed = 0
    
    for test in tests:
        try:
            test()
            passed += 1
        except Exception as e:
            print(f"❌ {test.__name__} failed: {e}")
            failed += 1
            continue
    
    # Run additional test suites
    try:
        run_performance_tests()
        passed += 1
    except Exception as e:
        print(f"❌ Performance tests failed: {e}")
        failed += 1
    
    try:
        run_error_handling_tests()
        passed += 1
    except Exception as e:
        print(f"❌ Error handling tests failed: {e}")
        failed += 1
    
    # Print results
    print("\n" + "=" * 50)
    print(f"🧪 Test Results: {passed} passed, {failed} failed")
    
    if failed == 0:
        print("🎉 All tests passed! Snowflake API is working perfectly!")
    else:
        print(f"⚠️  {failed} tests failed. Check the output above.")
        exit(1)