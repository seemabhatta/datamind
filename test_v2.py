#!/usr/bin/env python3
"""
NL2SQL v2 Test Script
Quick tests to verify the system is working.
"""

import asyncio
import os
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

async def test_basic_imports():
    """Test that all imports work"""
    print("🧪 Testing imports...")
    
    try:
        from src.nl2sql_v2.core.config import get_config
        from src.nl2sql_v2.mcp.client import MCPClientManager, LLMDirectClient
        from src.nl2sql_v2.agents.orchestrator import AgentOrchestrator
        print("✅ All imports successful")
        return True
    except Exception as e:
        print(f"❌ Import failed: {e}")
        return False


async def test_config():
    """Test configuration loading"""
    print("🧪 Testing configuration...")
    
    try:
        from src.nl2sql_v2.core.config import get_config
        config = get_config()
        print(f"✅ Config loaded - Debug: {config.debug}")
        return True
    except Exception as e:
        print(f"❌ Config test failed: {e}")
        return False


async def test_llm_client():
    """Test OpenAI direct client (if API key available)"""
    print("🧪 Testing OpenAI client...")
    
    if not os.getenv('OPENAI_API_KEY'):
        print("⚠️ OPENAI_API_KEY not set - skipping LLM test")
        return True
    
    try:
        from src.nl2sql_v2.mcp.client import LLMDirectClient
        client = LLMDirectClient()
        
        # Test intent classification
        response = await client.classify_intent("connect to database", {
            "session_connected": False,
            "has_selected_tables": False,
            "has_yaml_content": False
        })
        
        if response.success:
            print(f"✅ LLM client working - Intent: {response.data.get('intent')}")
            return True
        else:
            print(f"❌ LLM test failed: {response.error_message}")
            return False
            
    except Exception as e:
        print(f"❌ LLM client test failed: {e}")
        return False


async def test_orchestrator():
    """Test orchestrator initialization"""
    print("🧪 Testing orchestrator...")
    
    try:
        from src.nl2sql_v2.mcp.client import MCPClientManager
        from src.nl2sql_v2.agents.orchestrator import AgentOrchestrator
        
        # Create manager (without connecting to external services)
        manager = MCPClientManager()
        
        # Test orchestrator creation
        orchestrator = AgentOrchestrator(manager)
        print("✅ Orchestrator created successfully")
        return True
        
    except Exception as e:
        print(f"❌ Orchestrator test failed: {e}")
        return False


async def test_cli_help():
    """Test CLI help command"""
    print("🧪 Testing CLI...")
    
    try:
        import subprocess
        result = subprocess.run([
            sys.executable, "-m", "src.nl2sql_v2.cli.main", "--help"
        ], capture_output=True, text=True, timeout=10)
        
        if result.returncode == 0:
            print("✅ CLI help working")
            return True
        else:
            print(f"❌ CLI test failed: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ CLI test failed: {e}")
        return False


async def main():
    """Run all tests"""
    print("🚀 NL2SQL v2 Test Suite")
    print("=" * 50)
    
    tests = [
        ("Basic Imports", test_basic_imports),
        ("Configuration", test_config),
        ("OpenAI Client", test_llm_client),
        ("Orchestrator", test_orchestrator),
        ("CLI Help", test_cli_help),
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        print(f"\n📋 {test_name}")
        try:
            success = await test_func()
            if success:
                passed += 1
        except Exception as e:
            print(f"❌ {test_name} failed with exception: {e}")
    
    print(f"\n📊 Test Results: {passed}/{total} passed")
    
    if passed == total:
        print("🎉 All tests passed! System is ready.")
        return 0
    else:
        print("⚠️ Some tests failed. Check the errors above.")
        print("\n🔧 Next steps:")
        print("1. Make sure you've run: pip install -r requirements_v2.txt")
        print("2. Set OPENAI_API_KEY environment variable")
        print("3. Install Snowflake MCP server: uvx install mcp_snowflake_server")
        return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))