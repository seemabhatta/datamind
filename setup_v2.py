#!/usr/bin/env python3
"""
NL2SQL v2 Setup Script
Automates the installation and configuration of the new system.
"""

import os
import subprocess
import sys
from pathlib import Path


def run_command(cmd, description=""):
    """Run a command and handle errors"""
    print(f"🔄 {description or cmd}")
    try:
        result = subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True)
        print(f"✅ {description or cmd}")
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed: {description or cmd}")
        print(f"Error: {e.stderr}")
        return None


def check_python_version():
    """Check Python version"""
    if sys.version_info < (3, 8):
        print("❌ Python 3.8+ required. Current version:", sys.version)
        return False
    print(f"✅ Python version: {sys.version.split()[0]}")
    return True


def install_dependencies():
    """Install Python dependencies"""
    print("\n📦 Installing Dependencies...")
    
    # Install core dependencies
    if not run_command("pip install -r requirements_v2.txt", "Installing Python packages"):
        return False
    
    # Install uv (package manager for MCP servers)
    print("\n🔧 Installing uv (Python package manager)...")
    if not run_command("pip install uv", "Installing uv"):
        return False
    
    # Install Snowflake MCP server - use working GitHub repository
    print("\n❄️ Installing Snowflake MCP Server...")
    
    # Try the working community implementation
    if run_command("pip install git+https://github.com/datawiz168/mcp-snowflake-service.git", "Installing datawiz168 Snowflake MCP"):
        print("✅ Installed datawiz168 Snowflake MCP server")
    elif run_command("pip install git+https://github.com/dynamike/snowflake-mcp-server.git", "Installing dynamike Snowflake MCP"):
        print("✅ Installed dynamike Snowflake MCP server")
    else:
        # Install required dependencies manually
        print("Installing Snowflake dependencies...")
        if run_command("pip install snowflake-connector-python mcp", "Installing Snowflake connector"):
            print("✅ Installed Snowflake connector - MCP server can be run manually")
        else:
            print("❌ Failed to install Snowflake dependencies")
            return False
    
    return True


def create_env_template():
    """Create environment variable template and .env file"""
    env_template = """# NL2SQL v2 Environment Variables
# Fill in your actual values below

# Snowflake Connection (Required)
SNOWFLAKE_ACCOUNT=your-account-here
SNOWFLAKE_USER=your-username
SNOWFLAKE_PASSWORD=your-password
SNOWFLAKE_WAREHOUSE=your-warehouse
SNOWFLAKE_DATABASE=your-database
SNOWFLAKE_SCHEMA=your-schema
SNOWFLAKE_ROLE=your-role

# OpenAI API (Required)
OPENAI_API_KEY=your-openai-api-key-here

# Optional Configuration
LLM_MODEL=gpt-4
LLM_TEMPERATURE=0.1
DEBUG=false
LOG_LEVEL=INFO
SESSION_TIMEOUT_MINUTES=60
MAX_QUERY_RESULTS=1000
"""
    
    # Create template file
    env_template_file = Path(".env.template")
    with open(env_template_file, "w") as f:
        f.write(env_template)
    print(f"✅ Created environment template: {env_template_file}")
    
    # Create .env file if it doesn't exist
    env_file = Path(".env")
    if not env_file.exists():
        with open(env_file, "w") as f:
            f.write(env_template)
        print(f"✅ Created .env file: {env_file}")
        print("📝 Please edit .env and fill in your credentials")
    else:
        print(f"ℹ️ .env file already exists: {env_file}")
        print("📝 Please make sure your credentials are set in .env")


def test_installation():
    """Test the installation"""
    print("\n🧪 Testing Installation...")
    
    # Test our CLI
    result = run_command("python -m src.nl2sql_v2.cli.main version", "Testing CLI")
    if not result:
        return False
    
    # Test Snowflake MCP server
    result = run_command("uvx mcp_snowflake_server --help", "Testing Snowflake MCP server")
    if not result:
        print("⚠️ Snowflake MCP server test failed")
        return False
    
    return True


def main():
    """Main setup function"""
    print("🚀 NL2SQL v2 Setup Script")
    print("=" * 50)
    
    # Check Python version
    if not check_python_version():
        return 1
    
    # Install dependencies
    if not install_dependencies():
        print("\n❌ Dependency installation failed")
        return 1
    
    # Create environment template
    create_env_template()
    
    # Test installation
    if not test_installation():
        print("\n⚠️ Some tests failed, but core installation is complete")
    
    print("\n🎉 Setup Complete!")
    print("\n📋 Next Steps:")
    print("1. Edit the .env file and fill in your credentials:")
    print("   - SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, etc.")
    print("   - OPENAI_API_KEY")
    print("2. Test the installation:")
    print("   python test_v2.py")
    print("3. Run the CLI:")
    print("   python -m src.nl2sql_v2.cli.main chat")
    print("\n📖 See QUICKSTART_v2.md for examples")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())