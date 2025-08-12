# Low-Level Design: Simple FastAPI Agent Wrapper

## Table of Contents
1. [System Architecture](#system-architecture)
2. [Simple File Structure](#simple-file-structure)
3. [Keyword-Based Agent Routing](#keyword-based-agent-routing)
4. [API Design](#api-design)
5. [Data Models](#data-models)
6. [Agent Integration](#agent-integration)

## System Architecture

### Simple Component Overview
```
┌─────────────────────────────────────────────────────┐
│                FastAPI Wrapper                     │
├─────────────────────────────────────────────────────┤
│                                                     │
│  ┌─────────────────┐    ┌─────────────────────┐     │
│  │   API Routes    │    │   Agent Functions   │     │
│  │                 │    │   Integration       │     │
│  │ - Connection    │    │                     │     │
│  │ - Query         │    │ - Direct calls      │     │
│  │ - Visualization │    │ - Simple state      │     │
│  │ - Dictionary    │    │ - Result formatting │     │
│  └─────────────────┘    └─────────────────────┘     │
│           │                       │                 │
│           └───────────────────────┘                 │
│                                                     │
└─────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────┐
│              Existing Agent Functions              │
├─────────────────────────────────────────────────────┤
│                                                     │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐ │
│  │Connection   │  │Query        │  │Visualization│ │
│  │Tools        │  │Tools        │  │Tools        │ │
│  └─────────────┘  └─────────────┘  └─────────────┘ │
│                                                     │
│  ┌─────────────┐  ┌─────────────┐                  │
│  │Dictionary   │  │Stage        │                  │
│  │Tools        │  │Tools        │                  │
│  └─────────────┘  └─────────────┘                  │
└─────────────────────────────────────────────────────┘
```

## Simple File Structure

**FastAPI Application Structure**
- **main.py** - FastAPI application entry point and configuration
- **models.py** - Pydantic data models for request/response validation
- **routes/** - API endpoint definitions organized by functionality
  - **connection.py** - Snowflake connection management endpoints
  - **query.py** - Query processing endpoints (routes to `agentic_query_cli.py`)
  - **visualization.py** - Chart creation and visualization endpoints  
  - **dictionary.py** - YAML processing endpoints (routes to `agentic_generate_yaml_cli.py`)
- **agent_integration.py** - Integration layer for direct calls to existing agent functions
- **requirements.txt** - Python dependencies specification

## Keyword-Based Agent Routing

### Routing Logic Design
**UI Chat Interface**: Detect keywords in user messages to determine appropriate agent routing

**Primary Keywords**:
- **"generate"** → Routes to YAML Dictionary Generation Agent (`agentic_generate_yaml_cli.py`)
- **"@query"** → Routes to Query Processing Agent (`agentic_query_cli.py`)

**Plus Button Integration**:
- **Generate Option** → Prepends "@generate " to chat input, ensuring routing to YAML agent
- **Query Option** → Prepends "@query " to chat input, ensuring routing to query agent

**Fallback Logic**:
- **SQL-related keywords** (select, from, where, database, table) → Route to query agent
- **YAML-related keywords** (yaml, dictionary, schema, model, create) → Route to YAML agent
- **Default behavior** → Route to query agent for general queries

**Implementation Strategy**:
- **Client-side detection** in React chat interface
- **Server-side validation** in agent service
- **Endpoint mapping** based on detected agent type
- **Session context** maintained throughout agent interactions

### Agent Communication Protocol
**Message Flow**: UI → Agent Service → FastAPI Wrapper → CLI Agent → Response Chain
**Session Management**: Consistent session IDs across agent calls for context preservation
**Error Handling**: Graceful fallback between agents when routing fails
**Response Formatting**: Standardized response format regardless of agent type

## API Design

### REST API Endpoints

#### 1. Connection Management
**Design**: RESTful endpoints for Snowflake connection lifecycle
- **POST /api/v1/connections** - Initialize new Snowflake connection using existing tools
- **GET /api/v1/databases** - Retrieve list of available databases from connected instance
- **POST /api/v1/databases/select** - Set active database for current session
- **Response Format**: Consistent JSON responses with status and message/data fields
- **Error Handling**: Standardized error responses for connection failures
- **Integration**: Direct calls to existing connection management functions

#### 2. YAML Dictionary Management (agentic_generate_yaml_cli.py)
**Design**: YAML file operations and dictionary generation via CLI agent
- **GET /api/v1/yaml-files** - List available YAML files in current Snowflake stage
- **POST /api/v1/yaml-files/load** - Load and parse existing YAML dictionary file
- **POST /api/v1/yaml-files/generate** - Generate new YAML dictionary via `agentic_generate_yaml_cli.py`
- **Agent Routing**: When "generate" keyword detected, route requests to YAML generation agent
- **Session Management**: Include session ID for stateful agent interactions
- **Response Handling**: Parse agent output and format for UI consumption
- **Error Management**: Handle agent failures and provide meaningful error messages

#### 3. Query Processing (agentic_query_cli.py)
**Design**: Natural language to SQL processing via CLI agent
- **POST /api/v1/queries/execute** - Process natural language queries via `agentic_query_cli.py`
- **POST /api/v1/queries/generate** - Generate SQL only (legacy endpoint for direct SQL operations)
- **Agent Routing**: When "@query" keyword detected, route requests to query processing agent
- **Natural Language Processing**: Convert user queries to executable SQL
- **Query Execution**: Execute generated SQL against Snowflake and return results
- **Session Context**: Maintain query history and context within sessions
- **Result Formatting**: Structure query results for UI visualization
- **Performance Handling**: Manage large result sets appropriately

#### 4. Visualization
**Design**: Chart creation from query results
- **POST /api/v1/visualizations/create** - Create charts from last query results
- **Chart Types**: Support multiple visualization types based on data structure
- **Data Integration**: Use results from previous query executions
- **Chart Configuration**: Allow customization of chart appearance and behavior
- **Response Format**: Return chart specifications suitable for UI rendering
- **Error Handling**: Handle visualization creation failures gracefully

#### 5. System Health
**Design**: Service monitoring and status endpoints
- **GET /health** - Basic service health check endpoint
- **Status Response**: Simple JSON response indicating service operational status
- **Monitoring Integration**: Enable external monitoring systems to verify service health

## Data Models

### Simple Pydantic Models
**Data Validation Strategy**: Use Pydantic for request/response validation and serialization

**Request Models**:
- **QueryRequest** - Natural language query string, optional table name, session ID
- **VisualizationRequest** - Chart description string for visualization creation
- **DatabaseSelectRequest** - Database name for connection management
- **YamlLoadRequest** - Filename for YAML dictionary operations
- **ConnectionRequest** - Basic connection parameters (uses defaults from config)

**Response Models**:
- **SimpleResponse** - Standard response with status, message, optional SQL query and title
- **QueryResponse** - Enhanced response including results data and execution metadata
- **ListResponse** - Response format for list operations (databases, files, etc.)
- **ErrorResponse** - Standardized error response format with error details



## Agent Integration

### Simple Agent Integration

**Integration Strategy**: Direct function calls to existing CLI agent tools with minimal wrapper layer

**State Management**:
- **SimpleState Class** - Track connection details, current database/schema/stage, YAML content, query results
- **Global State Instance** - Single state object for demo (session-based state for production)
- **State Properties** - Connection ID, current selections, cached data, query history

**Connection Functions**:
- **connect_to_snowflake()** - Initialize Snowflake connection using existing tools
- **list_databases()** - Retrieve available databases from connection
- **select_database()** - Set active database for subsequent operations

**Query Processing Functions**:
- **generate_sql()** - Convert natural language to SQL using existing tools
- **execute_query()** - Execute SQL queries against Snowflake
- **process_query_agent()** - Route to `agentic_query_cli.py` for full query processing

**YAML Management Functions**:
- **list_yaml_files()** - List YAML files in current Snowflake stage
- **load_yaml_file()** - Load and parse existing YAML dictionary
- **generate_yaml_dictionary()** - Route to `agentic_generate_yaml_cli.py` for generation

**Visualization Functions**:
- **create_visualization()** - Generate charts from last query results using existing tools

**Agent Routing Functions**:
- **call_generate_yaml_agent()** - Direct integration with YAML generation CLI agent
- **call_query_agent()** - Direct integration with query processing CLI agent






<function_calls>
<invoke name="TodoWrite">
<parameter name="todos">[{"id": "1", "content": "Create detailed low-level design for FastAPI wrapper", "status": "completed"}, {"id": "2", "content": "Design database schemas and data models", "status": "completed"}, {"id": "3", "content": "Design API request/response flows", "status": "completed"}, {"id": "4", "content": "Design WebSocket communication protocol", "status": "completed"}, {"id": "5", "content": "Design error handling and logging strategy", "status": "completed"}]</parameter>
</invoke>

<function_calls>
<invoke name="TodoWrite">
<parameter name="todos">[{"id": "1", "content": "Create detailed low-level design for FastAPI wrapper", "status": "completed"}, {"id": "2", "content": "Design database schemas and data models", "status": "in_progress"}, {"id": "3", "content": "Design API request/response flows", "status": "pending"}, {"id": "4", "content": "Design WebSocket communication protocol", "status": "pending"}, {"id": "5", "content": "Design error handling and logging strategy", "status": "pending"}]