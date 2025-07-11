# NL2SQL Chat Application

A comprehensive natural language to SQL application that supports multiple interfaces and backends. The application uses OpenAI's GPT model to convert natural language questions into SQL queries, execute them on Snowflake databases, and display results in user-friendly formats.

## Features

- **Multi-Interface Support**: Streamlit UI, CLI, and REST API
- **Snowflake Integration**: Direct connection to Snowflake databases
- **Agentic CLI**: AI-powered interactive command-line interface using OpenAI Agent SDK
- **YAML Data Dictionary**: Advanced schema understanding with semantic modeling
- **Stage File Management**: Load data dictionaries directly from Snowflake stages
- **Natural Language Processing**: Advanced NL2SQL conversion with context awareness
- **Caching & Performance**: Optimized query execution and result caching

## Prerequisites

- Python 3.8+
- pip (Python package manager)
- OpenAI API key
- Snowflake account and credentials
- OpenAI Agent SDK (for CLI agent functionality)

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd nl2sqlchat
```

2. Install the required packages:
```bash
pip install -r requirements.txt
```

3. Create a `.env` file in the project root and add your configuration:
```
# OpenAI Configuration
OPENAI_API_KEY=your_openai_api_key_here

# Snowflake Configuration
SNOWFLAKE_ACCOUNT=your_account.region
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema
SNOWFLAKE_ROLE=your_role
```

## Configuration

Edit the `config.py` file to customize the application settings:

```python
# System Prompts
NL2SQL_SYSTEM_PROMPT_FILE = "utils/system-prompts/nl2sqlSystemPrompt.txt"
INTENT_IDENTIFIER_SYSTEM_PROMPT_FILE = "utils/system-prompts/intentIndentifierSystemPrompt.txt"
SUMMARY_PROMPT_FILE = "utils/system-prompts/summaryPrompt.txt"

# LLM Configuration
LLM_MODEL = "gpt-4o-mini"  # or another supported model
TEMPERATURE = 0.1
MAX_TOKENS = 4096
```

## Data Dictionary

The application uses YAML-based data dictionaries stored in Snowflake stages to understand database schemas. Each field in the dictionary includes:

- **Type**: Data type (integer, string, float, etc.)
- **Description**: Human-readable field description
- **Validation**: Business rules and constraints
- **Category**: Field classification (categorical, continuous, identifier, etc.)
- **Business Rules**: Domain-specific logic
- **Source**: Data lineage information
- **Relationships**: Foreign key and entity relationships

Example YAML structure:
```yaml
tables:
  - name: HMDA_SAMPLE
    base_table:
      database: CORTES_DEMO_2
      schema: CORTEX_DEMO
    fields:
      - name: county_name
        type: string
        description: County name where property is located
        category: categorical
```

## Running the Application

### Option 1: Agentic CLI (Recommended)
```bash
python src/cli/agentic_query_cli.py agent
```

### Option 2: Streamlit UI
```bash
streamlit run src/ui/main_ui.py
```

### Option 3: REST API
```bash
uvicorn src.api.nl2sql_api:app --reload --port 8001
```

### Option 4: Traditional CLI
```bash
python src/cli/natural_query_cli.py
```

## Usage

### Agentic CLI Usage
1. Run the agentic CLI with: `python src/cli/agentic_query_cli.py agent`
2. The system will auto-initialize:
   - Connect to Snowflake
   - Browse available databases and schemas
   - Find stages with YAML data dictionaries
   - Present available files for selection
3. Select a YAML data dictionary file to load
4. Ask natural language questions about your data
5. The agent will generate SQL, execute it, and provide results

### Example Queries
- "Show me the total population per county"
- "List the number of loans by action taken"
- "What is the average loan amount by state?"
- "Count applications by applicant ethnicity"

## Project Structure

```
nl2sqlchat/
├── src/
│   ├── api/                    # REST API endpoints
│   │   ├── routers/           # API route handlers
│   │   ├── models/            # Pydantic models
│   │   └── utils/             # API utilities
│   ├── cli/                   # Command-line interfaces
│   │   ├── agentic_query_cli.py   # AI agent CLI
│   │   └── natural_query_cli.py   # Traditional CLI
│   ├── functions/             # Core business logic
│   │   ├── connection_functions.py
│   │   ├── query_functions.py
│   │   ├── metadata_functions.py
│   │   └── stage_functions.py
│   └── ui/                    # Streamlit UI components
├── utils/                     # Shared utilities
│   ├── llm_util.py           # LLM interaction utilities
│   ├── file_utils.py         # File handling utilities
│   ├── cache_utils.py        # Caching utilities
│   └── system-prompts/       # System prompts for LLM
├── tests/                     # Test files
├── config.py                  # Configuration settings
├── requirements.txt           # Python dependencies
└── README.md                  # This file
```

## Architecture

### Key Components

1. **Functions Layer**: Core business logic extracted from API routers
   - Direct function calls eliminate HTTP overhead
   - Shared by both CLI and API interfaces
   - Maintains clean separation of concerns

2. **API Layer**: REST endpoints for web integration
   - FastAPI-based with automatic OpenAPI documentation
   - Pydantic models for request/response validation
   - Modular router structure

3. **CLI Layer**: Command-line interfaces
   - Agentic CLI using OpenAI Agent SDK
   - Traditional CLI for batch processing
   - Direct function calls for optimal performance

4. **UI Layer**: Streamlit web interface
   - Interactive chat interface
   - Real-time query execution
   - Data visualization capabilities

### Recent Improvements

- **Performance**: Eliminated API calls from CLI, using direct function calls
- **Modularity**: Extracted reusable functions from API routers
- **Maintainability**: Clean separation between presentation and business logic
- **Scalability**: Modular architecture supports easy extension

## Customization

You can customize the application by:

1. Modifying system prompts in `utils/system-prompts/`
2. Creating custom YAML data dictionaries
3. Adjusting configuration in `config.py`
4. Adding new function modules for additional data sources

## Debugging

### Setting up VS Code for Debugging

1. Create a `.vscode` directory in the project root if it doesn't exist already
2. Create a `launch.json` file inside the `.vscode` directory with the following content:

```json
{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Streamlit: nl2sql-app.py",
            "type": "python",
            "request": "launch",
            "module": "streamlit",
            "args": [
                "run",
                "nl2sql-app.py"
            ],
            "justMyCode": true,
            "env": {
                "PYTHONPATH": "${workspaceFolder}"
            }
        }
    ]
}
```

### Debugging Steps

1. Set breakpoints in your code by clicking in the gutter next to the line numbers
2. Select the "Run and Debug" view from the VS Code sidebar (or press Ctrl+Shift+D)
3. Choose "Streamlit: nl2sql-app.py" from the dropdown menu at the top
4. Click the green play button or press F5 to start debugging
5. The Streamlit app will launch in your default browser
6. When execution reaches a breakpoint, VS Code will pause and allow you to:
   - Inspect variables in the Variables panel
   - Step through code using the debug controls
   - View the call stack
   - Evaluate expressions in the Debug Console

7. Use the following keyboard shortcuts during debugging:
   - F5: Continue execution
   - F10: Step over
   - F11: Step into
   - Shift+F11: Step out
   - Ctrl+Shift+F5: Restart
   - Shift+F5: Stop debugging

## License

[Specify your license here]

## Contributing

[Specify contribution guidelines if applicable]
