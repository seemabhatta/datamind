#!/usr/bin/env python3
"""
Simple CLI for your 4-step YAML workflow
Usage: python simple_cli.py
"""

import asyncio
import sys
from workflow_agent import YAMLWorkflowAgent


async def main():
    """Simple CLI for the workflow"""
    print("🚀 Simple YAML Workflow CLI")
    print("=" * 40)
    print("Workflow Steps:")
    print("1. Generate YAML file")
    print("2. Store YAML in stage")
    print("3. Select YAML from stage")  
    print("4. Use YAML to query database")
    print("=" * 40)
    
    agent = YAMLWorkflowAgent()
    
    try:
        print("🔄 Initializing...")
        await agent.initialize()
        print("✅ Connected to Snowflake and OpenAI")
        
        while True:
            print("\nOptions:")
            print("1. Run full workflow")
            print("2. Generate YAML only")
            print("3. Query with existing YAML")
            print("4. Exit")
            
            choice = input("\nChoose option (1-4): ").strip()
            
            if choice == "1":
                # Full workflow
                table_name = input("Enter table name: ").strip()
                query = input("Enter your query: ").strip()
                
                try:
                    results = await agent.run_full_workflow(table_name, query)
                    print(f"\n📊 Results:")
                    for i, row in enumerate(results['results'].get('rows', [])[:5]):
                        print(f"Row {i+1}: {row}")
                    if len(results['results'].get('rows', [])) > 5:
                        print(f"... and {len(results['results']['rows']) - 5} more rows")
                except Exception as e:
                    print(f"❌ Error: {e}")
            
            elif choice == "2":
                # Generate YAML only
                table_name = input("Enter table name: ").strip()
                try:
                    yaml_data = await agent.step1_generate_yaml(table_name)
                    print(f"\n📄 Generated YAML:")
                    print(f"Table: {yaml_data['table_name']}")
                    print(f"Columns: {len(yaml_data['columns'])}")
                    print(f"First 3 columns: {[col['name'] for col in yaml_data['columns'][:3]]}")
                except Exception as e:
                    print(f"❌ Error: {e}")
            
            elif choice == "3":
                # Query with existing YAML
                if not agent.current_yaml:
                    print("❌ No YAML loaded. Please generate one first.")
                    continue
                
                query = input("Enter your query: ").strip()
                try:
                    results = await agent.step4_query_with_yaml(query)
                    print(f"\n📊 Results:")
                    for i, row in enumerate(results['results'].get('rows', [])[:5]):
                        print(f"Row {i+1}: {row}")
                    if len(results['results'].get('rows', [])) > 5:
                        print(f"... and {len(results['results']['rows']) - 5} more rows")
                except Exception as e:
                    print(f"❌ Error: {e}")
            
            elif choice == "4":
                print("👋 Goodbye!")
                break
            
            else:
                print("❌ Invalid choice. Please enter 1-4.")
    
    except KeyboardInterrupt:
        print("\n👋 Goodbye!")
    except Exception as e:
        print(f"💥 Fatal error: {e}")
    finally:
        if agent.mcp_manager:
            await agent.mcp_manager.shutdown()


if __name__ == "__main__":
    asyncio.run(main())