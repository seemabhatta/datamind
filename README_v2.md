# NL2SQL CLI v2 🚀

Modern Multi-Agent Natural Language to SQL System built with 2024/2025 best practices.

## 🏗️ Architecture Overview

This is a complete rewrite implementing cutting-edge patterns:

### **Multi-Agent Collaboration Pattern**
```
┌─────────────────────────────────────┐
│         Agent Orchestrator         │
│    (Intent Classification &        │
│     Workflow Coordination)         │
└─────────────┬───────────────────────┘
              │
    ┌─────────┼─────────┐
    │         │         │
┌───▼────┐ ┌──▼──┐ ┌───▼────┐ ┌────▼────┐
│Connect │ │Query│ │Explore │ │Dict     │
│Agent   │ │Agent│ │Agent   │ │Agent    │
└────────┘ └─────┘ └────────┘ └─────────┘
```

### **MCP (Model Context Protocol) Integration**
- **Snowflake MCP Server**: Database operations
- **LLM MCP Server**: AI/ML operations  
- **Orchestrator MCP Server**: Workflow management

### **Enterprise Patterns**
- **Repository Pattern**: Clean data access abstractions
- **Dependency Injection**: Testable, modular design
- **Domain-Driven Design**: Clear business logic separation
- **Reflection Pattern**: Query validation and improvement
- **Planning Pattern**: Multi-step workflow orchestration

## 🎯 Key Improvements Over v1

| Aspect | v1 (Legacy) | v2 (Modern) |
|--------|-------------|-------------|
| **Architecture** | Monolithic agents | Multi-agent collaboration |
| **Communication** | Direct function calls | MCP protocol |
| **State Management** | Global variables | Session contexts |
| **Error Handling** | Basic try/catch | Reflection & retry patterns |
| **Intent Classification** | Rule-based | LLM-powered with context |
| **Extensibility** | Tightly coupled | Loosely coupled interfaces |
| **Testing** | Limited | Full dependency injection |
| **Scalability** | Single process | Distributed MCP servers |

## 🚀 Quick Start

### 1. Install Dependencies
```bash
pip install -r requirements_v2.txt
```

### 2. Set Environment Variables
```bash
export SNOWFLAKE_ACCOUNT="your-account"
export SNOWFLAKE_USER="your-username"
export SNOWFLAKE_PASSWORD="your-password"
export SNOWFLAKE_WAREHOUSE="your-warehouse"
export SNOWFLAKE_DATABASE="your-database"
export SNOWFLAKE_SCHEMA="your-schema"
export OPENAI_API_KEY="your-openai-key"
```

### 3. Start Interactive Session
```bash
python -m src.nl2sql_v2.cli.main chat
```

### 4. Quick Test
```bash
python -m src.nl2sql_v2.cli.main chat --query "connect to snowflake"
```

## 💬 Usage Examples

### Connection Management
```bash
# Auto-connect with environment variables
"connect to snowflake"

# Connect with specific parameters
"connect to account ABC123 with user john@company.com"

# Check connection status
"show connection status"
```

### Database Exploration
```bash
# Explore databases
"show me the available databases"

# List tables in current schema
"what tables are available?"

# Describe a specific table
"describe the customers table"

# Select tables for querying
"select tables 1, 2, and 3"
"select all tables"
```

### Natural Language Querying
```bash
# Simple queries
"how many customers do we have?"
"show me the top 10 sales"

# Complex queries
"find customers who placed orders in the last 30 days"
"what's the average order value by month this year?"

# Data insights
"show me sales trends by region"
```

### Data Dictionary Management
```bash
# Generate dictionary
"generate data dictionary for all tables"
"create dictionary for selected tables"

# Load existing dictionary
"load dictionary from schema.yaml"

# Save dictionary
"save dictionary as customer_schema.yaml"

# Enhance with AI
"enhance dictionary with AI insights"
```

## 🛠️ Configuration

### Environment Variables
```bash
# Snowflake Connection
SNOWFLAKE_ACCOUNT=your-account
SNOWFLAKE_USER=your-user
SNOWFLAKE_PASSWORD=your-password
SNOWFLAKE_WAREHOUSE=your-warehouse
SNOWFLAKE_DATABASE=your-database
SNOWFLAKE_SCHEMA=your-schema
SNOWFLAKE_ROLE=your-role

# LLM Configuration
LLM_PROVIDER=openai  # openai, anthropic, azure
LLM_MODEL=gpt-4
OPENAI_API_KEY=your-key
LLM_MAX_TOKENS=4000
LLM_TEMPERATURE=0.1

# Application Settings
DEBUG=false
LOG_LEVEL=INFO
SESSION_TIMEOUT_MINUTES=60
MAX_QUERY_RESULTS=1000
```

### Configuration File (config.yaml)
```yaml
snowflake_mcp:
  name: "snowflake_server"
  command: "uv"
  args: ["run", "mcp-snowflake-server"]
  timeout: 60

llm_mcp:
  name: "llm_server"
  command: "uv" 
  args: ["run", "mcp-llm-server"]
  timeout: 30

debug: false
log_level: "INFO"
session_timeout_minutes: 60
max_query_results: 1000
```

## 🏛️ Architecture Deep Dive

### Agent Specialization

#### **ConnectionAgent**
- Database authentication
- Connection management
- Credential validation
- Connection status monitoring

#### **QueryAgent** 
- Natural language to SQL conversion
- Query execution and validation
- Result formatting and summarization
- Error handling with suggestions

#### **ExplorationAgent**
- Database/schema/table discovery
- Metadata browsing
- Table selection management
- Schema navigation

#### **DictionaryAgent**
- YAML dictionary generation
- Schema documentation
- AI-enhanced metadata
- Dictionary management (load/save)

### MCP Client Architecture

#### **SnowflakeMCPClient**
```python
await client.execute_query(sql, connection_config)
await client.get_databases(connection_config)
await client.get_tables(database, schema, connection_config)
```

#### **LLMMCPClient**
```python
await client.generate_sql(natural_language, schema_context)
await client.classify_intent(text, context)
await client.generate_response(prompt, context)
```

#### **OrchestratorMCPClient**
```python
await client.plan_workflow(user_request, session_context)
await client.validate_workflow_step(step_name, step_data)
```

### Session Management
```python
@dataclass
class SessionContext:
    session_id: str
    connection: Optional[DatabaseConnection]
    selected_tables: List[TableMetadata]
    query_history: List[QueryResult]
    yaml_content: Optional[Dict[str, Any]]
    user_preferences: Dict[str, Any]
```

## 🔧 Development

### Running Tests
```bash
pytest src/nl2sql_v2/tests/
```

### Code Quality
```bash
black src/nl2sql_v2/
isort src/nl2sql_v2/
mypy src/nl2sql_v2/
```

### MCP Server Development
Implement custom MCP servers following the official MCP specification:
```python
from mcp.server.fastmcp import FastMCP

mcp = FastMCP("custom_server")

@mcp.tool()
def custom_tool(param: str) -> str:
    return f"Processed: {param}"
```

## 🔐 Security Considerations

- **Credential Management**: Environment variables, no hardcoding
- **SQL Injection Prevention**: Parameterized queries via MCP
- **Input Validation**: All user inputs validated
- **Connection Security**: TLS encryption for all connections
- **Audit Logging**: Comprehensive query and action logging

## 📊 Performance Optimizations

- **Connection Pooling**: Reuse database connections
- **Query Caching**: Cache frequent query results
- **Async Operations**: Non-blocking I/O throughout
- **Schema Caching**: Cache table metadata
- **Lazy Loading**: Load data on demand

## 🔄 Migration from v1

The v2 system is a complete rewrite. To migrate:

1. **Export Data**: Save any existing dictionaries
2. **Update Dependencies**: Install v2 requirements
3. **Configure Environment**: Set new environment variables
4. **Start Fresh**: Begin with new v2 CLI

## 🤝 Contributing

1. **Architecture**: Follow the established patterns
2. **Testing**: Write tests for all new features
3. **Documentation**: Update README and code docs
4. **Code Style**: Use black, isort, mypy
5. **MCP Compliance**: Follow MCP specifications

## 📈 Roadmap

- [ ] **Multi-database Support**: PostgreSQL, MySQL, BigQuery
- [ ] **Advanced Analytics**: Statistical analysis agents
- [ ] **Data Visualization**: Chart generation agents
- [ ] **Collaborative Features**: Multi-user sessions
- [ ] **Cloud Deployment**: Kubernetes-ready containers
- [ ] **API Gateway**: REST API with same agents
- [ ] **Streaming Queries**: Real-time data processing

## 📝 License

Apache 2.0 License - See LICENSE file for details.

---

**Built with ❤️ using 2024/2025 AI Agent Best Practices**