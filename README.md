
---

## 🔍 **1. Discovery is Instant**

**Goal**: Enable users to find data assets using natural language

### 🔹 Tasks:

* **Metadata Ingestion**

  * Extract metadata from data sources (DBs, warehouses, APIs, etc.)
  * Normalize across sources (schema, table, column, lineage)

* **Semantic Annotation**

  * Add business terms, synonyms, tags, domains to technical metadata
  * Map assets to glossary entries or entities (e.g., “Customer”, “Loan”)

* **Embedding Generation**

  * Convert names, descriptions, tags into vector embeddings (OpenAI, BERT, etc.)

* **Vector Indexing & Search**

  * Store embeddings in vector DB (e.g., FAISS, Pinecone)
  * Implement similarity-based search using LLMs or search APIs

* **Search Interface**

  * Build CLI with smart autocomplete, filters, and search ranking
  * Optionally enrich search with knowledge graph traversal (e.g., show related assets)

---

## 📝 **2. Documentation is Automatic**

**Goal**: Auto-generate and maintain rich, business-friendly data documentation

### 🔹 Tasks:

* **Schema Profiling**

  * Analyze structure, types, value distributions, and example data

* **AI-Based Documentation**

  * Use LLMs to summarize datasets, columns, and joins in plain language
  * Auto-fill field descriptions using glossary matches and AI hints

* **Glossary Linking**

  * Connect assets to glossary terms (manual or AI-assisted mapping)

* **Change Detection & Versioning**

  * Track schema changes and automatically update affected documentation

* **Collaboration Layer**

  * Enable user feedback, ratings, edits, and tagging directly in UI

---

## 💬 **3. Analytics are Conversational**

**Goal**: Allow users to query data using plain English and receive answers or charts

### 🔹 Tasks:

* **NLQ Pipeline**

  * Translate natural language into SQL using schema-aware prompting
  * Validate and optimize queries for performance and safety

* **LLM Context Injection**

  * Use semantic model + data catalog to provide context to LLMs
  * Support join inference, metric resolution, and disambiguation

* **Query Execution & Visualization**

  * Execute validated queries and return tabular or visual results
  * Auto-select chart types and generate summaries

* **Conversational Flow Management**

  * Enable follow-ups, drill-downs, and clarifying questions
  * Optionally persist memory context per session

---

## 🛡️ **4. Governance is Seamless**

**Goal**: Automate data quality checks, access controls, and compliance policies

### 🔹 Tasks:

* **Data Classification**

  * Tag PII, PHI, financial fields using rules and ML models
  * Assign compliance levels (GDPR, CCPA, HIPAA, etc.)

* **Policy Enforcement Engine**

  * Define and enforce access controls, data usage rules, retention policies
  * Apply restrictions during search and query execution

* **Quality Monitoring**

  * Auto-generate quality rules (null checks, type drift, outliers)
  * Alert on failures and log lineage impact

* **Audit Trail and Lineage**

  * Track data usage, transformations, and access events
  * Visualize lineage from source to dashboard/report

* **Governance Dashboards**

  * Display data health, ownership, access logs, and remediation status

---

## ♾️ Optional: **Knowledge Graph Layer (Cross-Cutting)**

**Goal**: Enable deep relationship mapping and reasoning over assets

### 🔹 Tasks:

* **Entity Modeling**

  * Define entities (Customer, Product, Loan) and link to datasets/columns

* **Relationship Extraction**

  * Map joins, feature usage, report dependencies, business processes

* **Graph Construction**

  * Build graph database (Neo4j, Neptune) from semantic model
  * Support traversal, impact analysis, root cause analysis

* **LLM Integration with KG**

  * Use KG as context provider for LLMs or agents
  * Enable structured query generation (Cypher/SPARQL)

---

## 🚀 **Getting Started**

### Prerequisites

- Python 3.8+
- Snowflake account with appropriate permissions
- OpenAI API key

### Installation

1. **Clone the repository:**
   ```bash
   git clone <repository-url>
   cd nl2sqlchat
   ```

2. **Create and activate virtual environment:**
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Set up environment variables:**
   Create a `.env` file with your credentials:
   ```env
   OPENAI_API_KEY=your_openai_api_key
   SNOWFLAKE_USER=your_snowflake_user
   SNOWFLAKE_PASSWORD=your_snowflake_password
   SNOWFLAKE_ACCOUNT=your_snowflake_account
   SNOWFLAKE_WAREHOUSE=your_warehouse
   ```

### Usage

The system provides multiple interfaces for different use cases:

#### 1. Agentic Query Interface (Recommended)
Interactive AI agent for natural language querying with session memory:
```bash
python src/cli/agentic_query_cli.py agent
# Or with custom session ID:
python src/cli/agentic_query_cli.py agent --session-id my_session
```

#### 2. Dictionary Generation (Agentic)
AI-assisted data dictionary generation:
```bash
python src/cli/agentic_generate_yaml_cli.py agent
```

#### 3. Classic CLI Interfaces
Traditional command-line interfaces:
```bash
# Natural language querying
python src/cli/natural_query_cli.py

# Data dictionary generation
python src/cli/generate_yaml_cli.py
```

#### 4. Web Interface
Launch the Streamlit web application:
```bash
streamlit run src/ui/main_ui.py
```

### Key Dependencies

- **openai-agents**: OpenAI Agent SDK for conversational AI
- **snowflake-connector-python**: Snowflake database connectivity
- **openai**: OpenAI API client for LLM integration
- **streamlit**: Web interface framework
- **pyyaml**: YAML processing for data dictionaries
- **pandas**: Data manipulation and analysis
- **protobuf-to-pydantic**: Schema validation and conversion

