# 🚀 NL2SQL v2 - Quick Start Guide

## **3-Minute Setup**

### **Option 1: Automated Setup (Recommended)**
```bash
# Run the setup script
python setup_v2.py

# Follow the prompts and set your environment variables
```

### **Option 2: Manual Setup**

#### **Step 1: Install Dependencies**
```bash
cd /mnt/c/Users/ujjal/OneDrive/Desktop/Seema/VS-workspace/OpenAI/nl2sqlchat

# Install Python packages
pip install -r requirements_v2.txt

# Install uv package manager
pip install uv

# Install existing Snowflake MCP server
uvx install mcp_snowflake_server
```

#### **Step 2: Configure Credentials**
```bash
# Edit the .env file that was created
# Fill in your actual credentials:

# .env file contents:
SNOWFLAKE_ACCOUNT=your-account-here
SNOWFLAKE_USER=your-username
SNOWFLAKE_PASSWORD=your-password
SNOWFLAKE_WAREHOUSE=your-warehouse
SNOWFLAKE_DATABASE=your-database
SNOWFLAKE_SCHEMA=your-schema
OPENAI_API_KEY=your-openai-api-key-here
```

#### **Step 3: Test Installation**
```bash
# Run tests
python test_v2.py

# Start the CLI
python -m src.nl2sql_v2.cli.main chat
```

## **What You Get:**

### **✅ Modern Architecture**
- **Multi-Agent System**: Specialized agents for different tasks
- **Existing MCP Servers**: Battle-tested Snowflake integration
- **Direct OpenAI**: No custom LLM server needed
- **Smart Orchestration**: Intent-based routing

### **✅ Enterprise Features**
- **Session Management**: Persistent context across queries
- **Error Handling**: Intelligent error recovery and suggestions
- **Data Dictionaries**: AI-enhanced schema documentation
- **Security**: Environment-based credential management

### **✅ Simple Usage**
```bash
# Natural language examples:
"connect to snowflake"
"show me available tables"
"how many customers do we have?"
"generate data dictionary"
"save dictionary as schema.yaml"
```

## **Architecture Overview:**

```
┌─────────────────────────────────────┐
│         Your CLI Command            │
└─────────────┬───────────────────────┘
              │
┌─────────────▼───────────────────────┐
│      Agent Orchestrator             │
│   (Intent Classification)           │
└─────────────┬───────────────────────┘
              │
    ┌─────────┼─────────┐
    │         │         │
┌───▼────┐ ┌──▼──┐ ┌───▼────────┐
│Connect │ │Query│ │Exploration │
│Agent   │ │Agent│ │Agent       │
└───┬────┘ └──┬──┘ └───┬────────┘
    │         │         │
┌───▼─────────▼─────────▼────────┐
│     Existing MCP Servers       │
│  📊 Snowflake  🤖 OpenAI      │
└────────────────────────────────┘
```

## **Key Benefits:**

| **Aspect** | **v1 (Old)** | **v2 (New)** |
|------------|---------------|---------------|
| **Setup** | Custom servers + complex config | Existing servers + env vars |
| **Dependencies** | 15+ custom components | 2 external servers |
| **Maintenance** | High (custom MCP servers) | Low (reuse existing) |
| **Features** | Basic NL2SQL | Multi-agent + orchestration |
| **Reliability** | Custom implementation | Battle-tested servers |

## **Common Issues & Solutions:**

### **"uvx command not found"**
```bash
pip install uv
# or download from: https://astral.sh/uv/
```

### **"Snowflake connection failed"**
```bash
# Check your credentials
echo $SNOWFLAKE_ACCOUNT
echo $SNOWFLAKE_USER

# Test MCP server directly
uvx mcp_snowflake_server --account $SNOWFLAKE_ACCOUNT --user $SNOWFLAKE_USER
```

### **"OpenAI API error"**
```bash
# Check API key
echo $OPENAI_API_KEY

# Test API access
python -c "import openai; print('API key valid')"
```

## **Next Steps:**

1. **🎯 Try the Examples**: Follow the natural language examples above
2. **📚 Read the Docs**: See README_v2.md for detailed architecture info
3. **🔧 Customize**: Modify agents in `src/nl2sql_v2/agents/` for your needs
4. **🚀 Deploy**: Use the same architecture for production deployments

---

**Ready to go! This gives you a production-ready NL2SQL system in under 5 minutes.** 🎉