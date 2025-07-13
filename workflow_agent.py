#!/usr/bin/env python3
"""
Simple Workflow Agent for your 4-step YAML workflow:
1. Generate YAML file
2. Store YAML in stage  
3. Select YAML from stage
4. Use YAML to query database
"""

import asyncio
import yaml
import json
import os
from datetime import datetime
from typing import Dict, Any, List, Optional

# Your existing MCP components
from src.nl2sql_v2.mcp.client import MCPClientManager
from src.nl2sql_v2.core.config import get_config


class YAMLWorkflowAgent:
    """Simple agent focused on your 4-step workflow"""
    
    def __init__(self):
        self.config = get_config()
        self.mcp_manager = None
        self.current_yaml = None
        
    async def initialize(self):
        """Initialize MCP connections"""
        self.mcp_manager = MCPClientManager()
        await self.mcp_manager.initialize()
    
    async def step1_generate_yaml(self, table_name: str) -> Dict[str, Any]:
        """Step 1: Generate YAML file for table"""
        print(f"🔄 Step 1: Generating YAML for {table_name}...")
        
        try:
            # Get table metadata via MCP
            snowflake_client = self.mcp_manager.get_snowflake_client()
            if not snowflake_client:
                raise Exception("Snowflake client not available")
            
            # Connection config from environment
            connection_config = {
                "account": self.config.snowflake.account,
                "user": self.config.snowflake.user,
                "password": self.config.snowflake.password,
                "warehouse": self.config.snowflake.warehouse,
                "database": self.config.snowflake.database,
                "schema": self.config.snowflake.schema,
                "role": self.config.snowflake.role
            }
            
            # Get table structure
            response = await snowflake_client.describe_table(
                table_name, 
                self.config.snowflake.schema,
                self.config.snowflake.database,
                connection_config
            )
            
            if not response.success:
                raise Exception(f"Failed to get table metadata: {response.error_message}")
            
            # Build YAML structure
            yaml_data = {
                "table_name": table_name,
                "database": self.config.snowflake.database,
                "schema": self.config.snowflake.schema,
                "generated_at": datetime.now().isoformat(),
                "columns": response.data["columns"],
                "row_count": response.data.get("row_count"),
                "description": f"Data dictionary for {table_name}"
            }
            
            self.current_yaml = yaml_data
            print(f"✅ Step 1 Complete: Generated YAML with {len(yaml_data['columns'])} columns")
            return yaml_data
            
        except Exception as e:
            print(f"❌ Step 1 Failed: {e}")
            raise
    
    async def step2_store_in_stage(self, yaml_data: Dict[str, Any], stage_name: str = "YAML_STAGE") -> str:
        """Step 2: Store YAML in Snowflake stage"""
        print(f"🔄 Step 2: Storing YAML in stage {stage_name}...")
        
        try:
            snowflake_client = self.mcp_manager.get_snowflake_client()
            connection_config = {
                "account": self.config.snowflake.account,
                "user": self.config.snowflake.user,
                "password": self.config.snowflake.password,
                "warehouse": self.config.snowflake.warehouse,
                "database": self.config.snowflake.database,
                "schema": self.config.snowflake.schema,
                "role": self.config.snowflake.role
            }
            
            # Convert to YAML string
            yaml_content = yaml.dump(yaml_data, default_flow_style=False)
            filename = f"{yaml_data['table_name']}_dictionary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.yaml"
            
            # Create stage if it doesn't exist
            create_stage_sql = f"CREATE STAGE IF NOT EXISTS {stage_name}"
            await snowflake_client.execute_query(create_stage_sql, connection_config)
            
            # Store YAML content in stage using PUT command
            # Note: This is simplified - actual implementation would use PUT command
            store_sql = f"""
            INSERT INTO {stage_name}_METADATA (filename, content, created_at)
            VALUES ('{filename}', '{yaml_content.replace("'", "''")}', CURRENT_TIMESTAMP())
            """
            
            response = await snowflake_client.execute_query(store_sql, connection_config)
            
            if response.success:
                print(f"✅ Step 2 Complete: Stored {filename} in {stage_name}")
                return filename
            else:
                raise Exception(f"Failed to store in stage: {response.error_message}")
                
        except Exception as e:
            print(f"❌ Step 2 Failed: {e}")
            raise
    
    async def step3_select_from_stage(self, filename: str, stage_name: str = "YAML_STAGE") -> Dict[str, Any]:
        """Step 3: Select YAML from stage"""
        print(f"🔄 Step 3: Retrieving {filename} from {stage_name}...")
        
        try:
            snowflake_client = self.mcp_manager.get_snowflake_client()
            connection_config = {
                "account": self.config.snowflake.account,
                "user": self.config.snowflake.user,
                "password": self.config.snowflake.password,
                "warehouse": self.config.snowflake.warehouse,
                "database": self.config.snowflake.database,
                "schema": self.config.snowflake.schema,
                "role": self.config.snowflake.role
            }
            
            # Retrieve from stage
            select_sql = f"""
            SELECT content FROM {stage_name}_METADATA 
            WHERE filename = '{filename}'
            ORDER BY created_at DESC LIMIT 1
            """
            
            response = await snowflake_client.execute_query(select_sql, connection_config)
            
            if response.success and response.data["rows"]:
                yaml_content = response.data["rows"][0]["CONTENT"]
                yaml_data = yaml.safe_load(yaml_content)
                self.current_yaml = yaml_data
                print(f"✅ Step 3 Complete: Retrieved YAML for {yaml_data.get('table_name', 'unknown table')}")
                return yaml_data
            else:
                raise Exception(f"YAML file {filename} not found in stage")
                
        except Exception as e:
            print(f"❌ Step 3 Failed: {e}")
            raise
    
    async def step4_query_with_yaml(self, natural_query: str) -> Dict[str, Any]:
        """Step 4: Use YAML context to query database"""
        print(f"🔄 Step 4: Querying with YAML context...")
        
        try:
            if not self.current_yaml:
                raise Exception("No YAML loaded. Please complete steps 1-3 first.")
            
            llm_client = self.mcp_manager.get_llm_client()
            snowflake_client = self.mcp_manager.get_snowflake_client()
            
            # Build enhanced context from YAML
            table_context = f"""
Table: {self.current_yaml['table_name']}
Database: {self.current_yaml['database']}.{self.current_yaml['schema']}
Columns: {', '.join([col['name'] + ' (' + col['type'] + ')' for col in self.current_yaml['columns']])}
Row Count: {self.current_yaml.get('row_count', 'Unknown')}
"""
            
            # Generate SQL with enhanced context
            prompt = f"""
Given this table structure:
{table_context}

Convert this natural language to SQL:
"{natural_query}"

Return only the SQL query, no explanations.
"""
            
            sql_response = await llm_client.generate_response(prompt, {})
            sql = sql_response.strip()
            
            # Clean up SQL
            if sql.startswith("```sql"):
                sql = sql[6:]
            if sql.startswith("```"):
                sql = sql[3:]
            if sql.endswith("```"):
                sql = sql[:-3]
            sql = sql.strip()
            
            print(f"📝 Generated SQL: {sql}")
            
            # Execute query
            connection_config = {
                "account": self.config.snowflake.account,
                "user": self.config.snowflake.user,
                "password": self.config.snowflake.password,
                "warehouse": self.config.snowflake.warehouse,
                "database": self.config.snowflake.database,
                "schema": self.config.snowflake.schema,
                "role": self.config.snowflake.role
            }
            
            result = await snowflake_client.execute_query(sql, connection_config)
            
            if result.success:
                print(f"✅ Step 4 Complete: Query returned {len(result.data.get('rows', []))} rows")
                return {
                    "sql": sql,
                    "results": result.data,
                    "yaml_context": self.current_yaml['table_name']
                }
            else:
                raise Exception(f"Query failed: {result.error_message}")
                
        except Exception as e:
            print(f"❌ Step 4 Failed: {e}")
            raise
    
    async def run_full_workflow(self, table_name: str, query: str):
        """Run complete 4-step workflow"""
        print("🚀 Starting 4-Step YAML Workflow")
        print("=" * 50)
        
        try:
            # Step 1: Generate YAML
            yaml_data = await self.step1_generate_yaml(table_name)
            
            # Step 2: Store in stage
            filename = await self.step2_store_in_stage(yaml_data)
            
            # Step 3: Select from stage
            retrieved_yaml = await self.step3_select_from_stage(filename)
            
            # Step 4: Query with YAML context
            results = await self.step4_query_with_yaml(query)
            
            print("\n🎉 Workflow Complete!")
            print(f"Generated SQL: {results['sql']}")
            print(f"Results: {len(results['results'].get('rows', []))} rows")
            
            return results
            
        except Exception as e:
            print(f"\n💥 Workflow Failed: {e}")
            raise


async def main():
    """Test the workflow"""
    agent = YAMLWorkflowAgent()
    await agent.initialize()
    
    # Example usage
    results = await agent.run_full_workflow(
        table_name="HMDA_SAMPLE",
        query="show me the first 5 records"
    )
    
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    asyncio.run(main())