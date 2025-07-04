#!/usr/bin/env python3
"""
Snowflake API Test CLI - Test tool for the enhanced Snowflake API
"""

import argparse
import requests
import json
import sys
import os
from typing import Optional, Dict, Any

# Default API base URL
DEFAULT_BASE_URL = "http://localhost:8001"

class SnowflakeAPITester:
    def __init__(self, base_url: str = DEFAULT_BASE_URL):
        self.base_url = base_url.rstrip('/')
        self.connection_id: Optional[str] = None
    
    def make_request(self, method: str, endpoint: str, data: Optional[Dict] = None, params: Optional[Dict] = None) -> Dict[str, Any]:
        """Make HTTP request to the API"""
        url = f"{self.base_url}{endpoint}"
        
        try:
            if method.upper() == "GET":
                response = requests.get(url, params=params)
            elif method.upper() == "POST":
                response = requests.post(url, json=data)
            elif method.upper() == "DELETE":
                response = requests.delete(url)
            else:
                raise ValueError(f"Unsupported method: {method}")
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            print(f"❌ API Request failed: {str(e)}")
            if hasattr(e, 'response') and e.response is not None:
                try:
                    error_detail = e.response.json()
                    print(f"   Error details: {error_detail}")
                except:
                    print(f"   Response text: {e.response.text}")
            return {"error": str(e)}
    
    def connect(self) -> bool:
        """Test connection to Snowflake"""
        print("🔗 Testing Snowflake connection...")
        
        response = self.make_request("POST", "/connect")
        
        if "error" in response:
            return False
        
        if response.get("connection_id"):
            self.connection_id = response["connection_id"]
            print(f"✅ Connected successfully!")
            print(f"   Connection ID: {self.connection_id}")
            print(f"   Account: {response.get('account')}")
            print(f"   User: {response.get('user')}")
            print(f"   Database: {response.get('database', 'Not set')}")
            print(f"   Schema: {response.get('schema', 'Not set')}")
            return True
        else:
            print("❌ Connection failed")
            return False
    
    def check_status(self) -> bool:
        """Check connection status"""
        if not self.connection_id:
            print("No connection found. Attempting to connect...")
            if not self.connect():
                return False
        
        print(f"🔍 Checking connection status...")
        response = self.make_request("GET", f"/connection/{self.connection_id}/status")
        
        if "error" in response:
            return False
        
        print(f"✅ Connection is active")
        print(f"   Status: {response.get('status')}")
        print(f"   Account: {response.get('account')}")
        print(f"   User: {response.get('user')}")
        return True
    
    def list_databases(self) -> bool:
        """List available databases"""
        if not self.connection_id:
            print("No connection found. Attempting to connect...")
            if not self.connect():
                return False
        
        print("📋 Listing databases...")
        response = self.make_request("GET", f"/connection/{self.connection_id}/databases")
        
        if "error" in response:
            return False
        
        databases = response.get("databases", [])
        print(f"✅ Found {len(databases)} databases:")
        for db in databases:
            print(f"   - {db}")
        return True
    
    def list_schemas(self, database: str) -> bool:
        """List schemas in a database"""
        if not self.connection_id:
            print("No connection found. Attempting to connect...")
            if not self.connect():
                return False
        
        print(f"📋 Listing schemas in {database}...")
        response = self.make_request("GET", f"/connection/{self.connection_id}/schemas", params={"database": database})
        
        if "error" in response:
            return False
        
        schemas = response.get("schemas", [])
        print(f"✅ Found {len(schemas)} schemas in {database}:")
        for schema in schemas:
            print(f"   - {schema}")
        return True
    
    def list_tables(self, database: str, schema: str) -> bool:
        """List tables in a schema"""
        if not self.connection_id:
            print("No connection found. Attempting to connect...")
            if not self.connect():
                return False
        
        print(f"📋 Listing tables in {database}.{schema}...")
        response = self.make_request("GET", f"/connection/{self.connection_id}/tables", 
                                   params={"database": database, "schema": schema})
        
        if "error" in response:
            return False
        
        tables = response.get("tables", [])
        print(f"✅ Found {len(tables)} tables in {database}.{schema}:")
        for table in tables:
            print(f"   - {table['table']} ({table['table_type']})")
        return True
    
    def analyze_tables(self, tables: list) -> Dict[str, Any]:
        """Analyze selected tables"""
        if not self.connection_id:
            print("❌ No active connection")
            return {}
        
        print(f"🔍 Analyzing {len(tables)} tables: {', '.join(tables)}")
        
        data = {
            "connection_id": self.connection_id,
            "tables": tables
        }
        
        response = self.make_request("POST", f"/connection/{self.connection_id}/analyze-tables", data=data)
        
        if "error" in response:
            return {}
        
        print(f"✅ Analysis completed for {response.get('tables_analyzed', 0)} tables")
        print(f"   Database: {response.get('database')}")
        print(f"   Schema: {response.get('schema')}")
        
        analysis = response.get("analysis", {})
        for table_name, table_info in analysis.items():
            if "error" in table_info:
                print(f"   ❌ {table_name}: {table_info['error']}")
            else:
                print(f"   ✅ {table_name}: {table_info['row_count']} rows, {len(table_info.get('columns', []))} columns")
        
        return response
    
    def generate_data_dictionary(self, tables: list, database: str, schema: str, save_to_file: Optional[str] = None) -> Dict[str, Any]:
        """Generate YAML data dictionary"""
        if not self.connection_id:
            print("❌ No active connection")
            return {}
        
        print(f"📝 Generating data dictionary for tables: {', '.join(tables)}")
        
        data = {
            "connection_id": self.connection_id,
            "tables": tables,
            "database_name": database,
            "schema_name": schema
        }
        
        response = self.make_request("POST", f"/connection/{self.connection_id}/generate-data-dictionary", data=data)
        
        if "error" in response:
            return {}
        
        print(f"✅ Data dictionary generated successfully")
        print(f"   Tables processed: {response.get('tables_processed', 0)}")
        print(f"   Validation status: {response.get('validation_status')}")
        
        if response.get("validation_status") != "valid":
            print(f"   ⚠️  Validation warning: {response.get('validation_error')}")
        
        yaml_content = response.get("yaml_dictionary", "")
        if save_to_file and yaml_content:
            try:
                with open(save_to_file, 'w') as f:
                    f.write(yaml_content)
                print(f"   💾 YAML saved to: {save_to_file}")
            except Exception as e:
                print(f"   ❌ Failed to save YAML: {str(e)}")
        
        return response
    
    def save_dictionary_to_stage(self, stage_name: str, file_name: str, yaml_content: str) -> bool:
        """Save dictionary to Snowflake stage"""
        if not self.connection_id:
            print("❌ No active connection")
            return False
        
        print(f"💾 Saving dictionary to stage {stage_name}/{file_name}")
        
        data = {
            "connection_id": self.connection_id,
            "stage_name": stage_name,
            "file_name": file_name,
            "yaml_content": yaml_content
        }
        
        response = self.make_request("POST", f"/connection/{self.connection_id}/save-dictionary-to-stage", data=data)
        
        if "error" in response:
            return False
        
        print(f"✅ Dictionary saved successfully")
        print(f"   Location: {response.get('stage_name')}/{response.get('file_name')}")
        print(f"   Size: {response.get('content_size')} characters")
        return True
    
    def query_nl2sql(self, query: str, table_name: str, dictionary_content: str) -> bool:
        """Test natural language to SQL conversion"""
        if not self.connection_id:
            print("❌ No active connection")
            return False
        
        print(f"💬 Processing NL query: {query}")
        
        data = {
            "query": query,
            "connection_id": self.connection_id,
            "table_name": table_name,
            "dictionary_content": dictionary_content
        }
        
        response = self.make_request("POST", f"/connection/{self.connection_id}/query", data=data)
        
        if "error" in response:
            return False
        
        print(f"✅ Query processed")
        print(f"   Intent: {response.get('intent')}")
        
        if response.get('sql'):
            print(f"   Generated SQL: {response.get('sql')}")
        
        if response.get('execution_status') == 'success':
            print(f"   ✅ Execution successful: {response.get('row_count', 0)} rows returned")
        elif response.get('execution_status') == 'failed':
            print(f"   ❌ Execution failed: {response.get('sql_error')}")
        
        return True
    
    def disconnect(self) -> bool:
        """Disconnect from Snowflake"""
        if not self.connection_id:
            print("❌ No active connection")
            return False
        
        print("🔌 Disconnecting...")
        response = self.make_request("DELETE", f"/connection/{self.connection_id}")
        
        if "error" in response:
            return False
        
        print("✅ Disconnected successfully")
        self.connection_id = None
        return True
    
    def interactive_workflow(self) -> bool:
        """AWS CLI-style interactive workflow for creating data dictionaries"""
        print("🚀 Snowflake Data Dictionary Creation Wizard")
        print("=" * 50)
        
        # Step 1: Connect
        print("\n📋 Step 1: Connection")
        if not self.connection_id:
            print("🔗 Connecting to Snowflake...")
            if not self.connect():
                return False
        else:
            print("✅ Already connected")
        
        # Step 2: Select Database
        print("\n📋 Step 2: Select Database")
        if not self.list_databases():
            return False
        
        while True:
            database = input("\n💭 Enter database name: ").strip().upper()
            if database:
                break
            print("❌ Database name cannot be empty")
        
        # Step 3: Select Schema
        print(f"\n📋 Step 3: Select Schema from {database}")
        if not self.list_schemas(database):
            return False
        
        while True:
            schema = input("\n💭 Enter schema name: ").strip().upper()
            if schema:
                break
            print("❌ Schema name cannot be empty")
        
        # Step 4: Select Tables
        print(f"\n📋 Step 4: Select Tables from {database}.{schema}")
        if not self.list_tables(database, schema):
            return False
        
        print("\n💭 Select tables (multiple options):")
        print("   1. Enter table names manually (comma-separated)")
        print("   2. Select all tables")
        print("   3. Interactive selection")
        
        while True:
            choice = input("\nChoose option (1/2/3): ").strip()
            if choice in ['1', '2', '3']:
                break
            print("❌ Please enter 1, 2, or 3")
        
        selected_tables = []
        
        if choice == '1':
            # Manual entry
            while not selected_tables:
                table_input = input("\n💭 Enter table names (comma-separated): ").strip()
                if table_input:
                    selected_tables = [t.strip().upper() for t in table_input.split(',')]
                else:
                    print("❌ Please enter at least one table name")
        
        elif choice == '2':
            # Get all tables
            response = self.make_request("GET", f"/connection/{self.connection_id}/tables", 
                                       params={"database": database, "schema": schema})
            if "error" not in response:
                selected_tables = [table['table'] for table in response.get('tables', [])]
                print(f"✅ Selected all {len(selected_tables)} tables")
        
        elif choice == '3':
            # Interactive selection
            response = self.make_request("GET", f"/connection/{self.connection_id}/tables", 
                                       params={"database": database, "schema": schema})
            if "error" not in response:
                tables = response.get('tables', [])
                print(f"\n📋 Available tables ({len(tables)}):")
                for i, table in enumerate(tables, 1):
                    print(f"   {i}. {table['table']} ({table['table_type']})")
                
                while not selected_tables:
                    indices_input = input("\n💭 Enter table numbers (comma-separated, e.g., 1,3,5): ").strip()
                    if indices_input:
                        try:
                            indices = [int(i.strip()) for i in indices_input.split(',')]
                            selected_tables = [tables[i-1]['table'] for i in indices if 1 <= i <= len(tables)]
                            if selected_tables:
                                print(f"✅ Selected tables: {', '.join(selected_tables)}")
                            else:
                                print("❌ No valid tables selected")
                        except (ValueError, IndexError):
                            print("❌ Invalid input. Please enter valid numbers.")
                    else:
                        print("❌ Please enter at least one table number")
        
        if not selected_tables:
            print("❌ No tables selected. Exiting.")
            return False
        
        # Step 5: Analyze Tables
        print(f"\n📋 Step 5: Analyzing {len(selected_tables)} Tables")
        analysis_result = self.analyze_tables(selected_tables)
        if not analysis_result:
            return False
        
        # Step 6: Generate Data Dictionary
        print(f"\n📋 Step 6: Generate Data Dictionary")
        output_file = input(f"\n💭 Output file name [data_dictionary.yaml]: ").strip()
        if not output_file:
            output_file = "data_dictionary.yaml"
        
        dict_result = self.generate_data_dictionary(selected_tables, database, schema, output_file)
        if not dict_result:
            return False
        
        # Step 7: Upload to Stage (Optional)
        print(f"\n📋 Step 7: Upload to Snowflake Stage (Optional)")
        upload_choice = input("💭 Do you want to upload to a Snowflake stage? (y/N): ").strip().lower()
        
        if upload_choice in ['y', 'yes']:
            stage_name = input("💭 Enter stage name (e.g., @DB.SCHEMA.STAGE): ").strip()
            if stage_name:
                stage_file = input(f"💭 File name in stage [{output_file}]: ").strip()
                if not stage_file:
                    stage_file = output_file
                
                yaml_content = dict_result.get("yaml_dictionary", "")
                if yaml_content:
                    if self.save_dictionary_to_stage(stage_name, stage_file, yaml_content):
                        print(f"✅ Dictionary uploaded to {stage_name}/{stage_file}")
                    else:
                        print("❌ Failed to upload to stage")
        
        # Summary
        print("\n" + "=" * 50)
        print("🎉 Data Dictionary Creation Complete!")
        print(f"   📊 Database: {database}")
        print(f"   📂 Schema: {schema}")
        print(f"   📋 Tables: {len(selected_tables)} ({', '.join(selected_tables)})")
        print(f"   📄 Output File: {output_file}")
        print(f"   ✅ Status: Success")
        print("=" * 50)
        
        return True


def main():
    parser = argparse.ArgumentParser(description="Snowflake API Test CLI")
    parser.add_argument("--url", default=DEFAULT_BASE_URL, help="API base URL")
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Connect
    connect_parser = subparsers.add_parser('connect', help='Connect to Snowflake')
    
    # Status
    status_parser = subparsers.add_parser('status', help='Check connection status')
    
    # List databases
    list_db_parser = subparsers.add_parser('list-databases', help='List databases')
    
    # List schemas
    list_schemas_parser = subparsers.add_parser('list-schemas', help='List schemas')
    list_schemas_parser.add_argument('database', help='Database name')
    
    # List tables
    list_tables_parser = subparsers.add_parser('list-tables', help='List tables')
    list_tables_parser.add_argument('database', help='Database name')
    list_tables_parser.add_argument('schema', help='Schema name')
    
    # Analyze tables
    analyze_parser = subparsers.add_parser('analyze-tables', help='Analyze tables')
    analyze_parser.add_argument('tables', nargs='+', help='Table names to analyze')
    
    # Generate dictionary
    gen_dict_parser = subparsers.add_parser('generate-dictionary', help='Generate data dictionary')
    gen_dict_parser.add_argument('database', help='Database name')
    gen_dict_parser.add_argument('schema', help='Schema name')
    gen_dict_parser.add_argument('tables', nargs='+', help='Table names')
    gen_dict_parser.add_argument('--save', help='Save YAML to file')
    
    # Save to stage
    save_stage_parser = subparsers.add_parser('save-to-stage', help='Save dictionary to stage')
    save_stage_parser.add_argument('stage', help='Stage name (e.g., @DB.SCHEMA.STAGE)')
    save_stage_parser.add_argument('filename', help='File name')
    save_stage_parser.add_argument('yaml_file', help='YAML file to upload')
    
    # Query
    query_parser = subparsers.add_parser('query', help='Test NL2SQL query')
    query_parser.add_argument('query', help='Natural language query')
    query_parser.add_argument('table', help='Table name')
    query_parser.add_argument('--dictionary', '-d', required=True, help='YAML dictionary file')
    
    # Disconnect
    disconnect_parser = subparsers.add_parser('disconnect', help='Disconnect')
    
    # Full workflow test
    workflow_parser = subparsers.add_parser('test-workflow', help='Test complete workflow')
    workflow_parser.add_argument('database', help='Database name')
    workflow_parser.add_argument('schema', help='Schema name')
    workflow_parser.add_argument('tables', nargs='+', help='Table names to test')
    
    # Interactive workflow command (AWS CLI style)
    interactive_parser = subparsers.add_parser('wizard', help='Interactive data dictionary creation wizard (AWS CLI style)')
    
    # Sequential workflow command (AWS CLI style)
    seq_workflow_parser = subparsers.add_parser('create-dictionary', help='Create data dictionary - AWS CLI style workflow')
    seq_workflow_parser.add_argument('--database', required=True, help='Database name')
    seq_workflow_parser.add_argument('--schema', required=True, help='Schema name')
    seq_workflow_parser.add_argument('--tables', nargs='+', help='Specific table names (optional - will prompt if not provided)')
    seq_workflow_parser.add_argument('--output', '-o', default='data_dictionary.yaml', help='Output YAML file name')
    seq_workflow_parser.add_argument('--stage', help='Snowflake stage to upload to (e.g., @DB.SCHEMA.STAGE)')
    seq_workflow_parser.add_argument('--stage-file', help='File name in stage (defaults to output filename)')
    seq_workflow_parser.add_argument('--interactive', '-i', action='store_true', help='Interactive table selection')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    # Create API tester
    tester = SnowflakeAPITester(args.url)
    
    # Execute commands
    if args.command == 'connect':
        success = tester.connect()
        return 0 if success else 1
    
    elif args.command == 'status':
        success = tester.check_status()
        return 0 if success else 1
    
    elif args.command == 'list-databases':
        success = tester.list_databases()
        return 0 if success else 1
    
    elif args.command == 'list-schemas':
        success = tester.list_schemas(args.database)
        return 0 if success else 1
    
    elif args.command == 'list-tables':
        success = tester.list_tables(args.database, args.schema)
        return 0 if success else 1
    
    elif args.command == 'analyze-tables':
        result = tester.analyze_tables(args.tables)
        return 0 if result else 1
    
    elif args.command == 'generate-dictionary':
        result = tester.generate_data_dictionary(args.tables, args.database, args.schema, args.save)
        return 0 if result else 1
    
    elif args.command == 'save-to-stage':
        if not os.path.exists(args.yaml_file):
            print(f"❌ YAML file not found: {args.yaml_file}")
            return 1
        
        with open(args.yaml_file, 'r') as f:
            yaml_content = f.read()
        
        success = tester.save_dictionary_to_stage(args.stage, args.filename, yaml_content)
        return 0 if success else 1
    
    elif args.command == 'query':
        if not os.path.exists(args.dictionary):
            print(f"❌ Dictionary file not found: {args.dictionary}")
            return 1
        
        with open(args.dictionary, 'r') as f:
            dictionary_content = f.read()
        
        success = tester.query_nl2sql(args.query, args.table, dictionary_content)
        return 0 if success else 1
    
    elif args.command == 'disconnect':
        success = tester.disconnect()
        return 0 if success else 1
    
    elif args.command == 'wizard':
        success = tester.interactive_workflow()
        return 0 if success else 1
    
    elif args.command == 'test-workflow':
        print("🚀 Starting complete workflow test...")
        
        # Step 1: Connect
        if not tester.connect():
            return 1
        
        # Step 2: Analyze tables
        print("\n" + "="*50)
        analysis_result = tester.analyze_tables(args.tables)
        if not analysis_result:
            return 1
        
        # Step 3: Generate dictionary
        print("\n" + "="*50)
        dict_result = tester.generate_data_dictionary(args.tables, args.database, args.schema, "test_workflow_dict.yaml")
        if not dict_result:
            return 1
        
        # Step 4: Test a sample query (if dictionary was generated)
        if dict_result.get("yaml_dictionary"):
            print("\n" + "="*50)
            sample_query = f"How many records are in {args.tables[0]}?"
            tester.query_nl2sql(sample_query, args.tables[0], dict_result["yaml_dictionary"])
        
        print("\n" + "="*50)
        print("🎉 Workflow test completed successfully!")
        
        return 0
    
    else:
        print(f"Unknown command: {args.command}")
        return 1


if __name__ == "__main__":
    sys.exit(main())