# 🚀 NL2SQL v2 Installation Guide

## **Simple 3-Step Setup Using Existing MCP Servers**

### **Step 1: Install Dependencies**
```bash
# Navigate to project directory
cd /mnt/c/Users/ujjal/OneDrive/Desktop/Seema/VS-workspace/OpenAI/nl2sqlchat

# Install Python dependencies
pip install -r requirements_v2.txt

# Install uv (Python package manager used by MCP servers)
pip install uv

# Install the existing Snowflake MCP server
uvx install mcp_snowflake_server
```

### **Step 2: Set Environment Variables**
```bash
# Required Snowflake credentials
export SNOWFLAKE_ACCOUNT="your-account"
export SNOWFLAKE_USER="your-username"
export SNOWFLAKE_PASSWORD="your-password" 
export SNOWFLAKE_WAREHOUSE="your-warehouse"
export SNOWFLAKE_DATABASE="your-database"
export SNOWFLAKE_SCHEMA="your-schema"
export SNOWFLAKE_ROLE="your-role"

# Required OpenAI API key
export OPENAI_API_KEY="your-openai-api-key"

# Optional settings
export LLM_MODEL="gpt-4"
export DEBUG="false"
```

### **Step 3: Run the New CLI**
```bash
# Start interactive session
python -m src.nl2sql_v2.cli.main chat

# Test with a single query
python -m src.nl2sql_v2.cli.main chat --query "connect to snowflake"

# Check system health
python -m src.nl2sql_v2.cli.main health
```

## **What We're Using:**

### **✅ Existing MCP Servers:**
- **Snowflake**: `isaacwasserman/mcp-snowflake-server` (via uvx)
- **OpenAI**: Direct API integration (OpenAI has built-in MCP support)

### **✅ Our Custom Architecture:**
- **Multi-Agent Orchestrator**: Intent classification and workflow management
- **Specialized Agents**: Connection, Query, Exploration, Dictionary
- **Session Management**: Proper context handling
- **Repository Pattern**: Clean, testable interfaces

## **Why This Approach:**

| Component | Why Reuse Existing | Benefits |
|-----------|-------------------|----------|
| **Snowflake MCP** | Mature, well-tested server | ✅ Production ready<br>✅ Full Snowflake feature support<br>✅ Active maintenance |
| **OpenAI Integration** | Built-in SDK support | ✅ Official support<br>✅ No extra server needed<br>✅ Lower complexity |
| **Our Orchestrator** | Custom business logic | ✅ NL2SQL-specific workflows<br>✅ Multi-agent coordination<br>✅ Intent classification |

## **Verification Steps:**

### **1. Test Snowflake MCP Server**
```bash
# Test if the server is installed
uvx mcp_snowflake_server --help

# Should show available options like:
# --account, --warehouse, --user, etc.
```

### **2. Test Environment Variables**
```bash
# Check if variables are set
echo $SNOWFLAKE_ACCOUNT
echo $OPENAI_API_KEY
```

### **3. Test Our CLI**
```bash
# Quick health check
python -m src.nl2sql_v2.cli.main health

# Should show:
# ✅ System healthy
```

## **Troubleshooting:**

### **If uvx command not found:**
```bash
pip install uv
# or
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### **If MCP server fails to start:**
```bash
# Check if all required env vars are set
uvx mcp_snowflake_server \
  --account $SNOWFLAKE_ACCOUNT \
  --user $SNOWFLAKE_USER \
  --password $SNOWFLAKE_PASSWORD \
  --warehouse $SNOWFLAKE_WAREHOUSE \
  --database $SNOWFLAKE_DATABASE \
  --schema $SNOWFLAKE_SCHEMA
```

### **If OpenAI API fails:**
```bash
# Test API key
python -c "import openai; openai.api_key='your-key'; print('API key works')"
```

## **Next Steps After Installation:**
1. **Connect**: `"connect to snowflake"`
2. **Explore**: `"show me available tables"`
3. **Query**: `"how many customers do we have?"`
4. **Dictionary**: `"generate data dictionary"`

---

**This approach combines the best of both worlds: battle-tested MCP servers + our modern multi-agent architecture!**