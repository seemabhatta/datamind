# Snowflake API Testing Guide

This guide shows how to test the enhanced Snowflake API using the CLI test tool.

## Prerequisites

1. **Start the Snowflake API server:**
   ```bash
   uvicorn src.api.snowflake_api:app --reload --port 8001
   ```

2. **Set environment variables:**
   ```bash
   export SNOWFLAKE_ACCOUNT=your_account
   export SNOWFLAKE_USER=your_username
   export SNOWFLAKE_PASSWORD=your_password
   export SNOWFLAKE_WAREHOUSE=your_warehouse
   export SNOWFLAKE_DATABASE=your_database
   export SNOWFLAKE_SCHEMA=your_schema
   ```

## CLI Test Tool Usage

### Basic Commands

#### 1. Connect to Snowflake
```bash
python test_snowflake_api.py connect
```

#### 2. Check Connection Status
```bash
python test_snowflake_api.py status
```

#### 3. Explore Database Structure
```bash
# List databases
python test_snowflake_api.py list-databases

# List schemas in a database
python test_snowflake_api.py list-schemas SALES_DB

# List tables in a schema
python test_snowflake_api.py list-tables SALES_DB PUBLIC
```

### Core Workflow Commands

#### 4. Analyze Tables
```bash
# Analyze one or more tables
python test_snowflake_api.py analyze-tables CUSTOMERS ORDERS PRODUCTS
```

#### 5. Generate Data Dictionary
```bash
# Generate YAML dictionary and save to file
python test_snowflake_api.py generate-dictionary SALES_DB PUBLIC CUSTOMERS ORDERS --save sales_dict.yaml
```

#### 6. Save Dictionary to Snowflake Stage
```bash
# Upload dictionary to a Snowflake stage
python test_snowflake_api.py save-to-stage @SALES_DB.PUBLIC.MY_STAGE sales_dict.yaml sales_dict.yaml
```

#### 7. Test Natural Language Queries
```bash
# Query using generated dictionary
python test_snowflake_api.py query "How many customers are there?" CUSTOMERS --dictionary sales_dict.yaml

python test_snowflake_api.py query "Show me the top 10 customers by revenue" CUSTOMERS --dictionary sales_dict.yaml
```

#### 8. Disconnect
```bash
python test_snowflake_api.py disconnect
```

## Complete Workflow Test

Test the entire workflow with one command:

```bash
python test_snowflake_api.py test-workflow SALES_DB PUBLIC CUSTOMERS ORDERS
```

This will:
1. Connect to Snowflake
2. Analyze the specified tables
3. Generate a data dictionary
4. Test a sample NL2SQL query
5. Save results to `test_workflow_dict.yaml`

## Example Output

### Successful Connection:
```
🔗 Testing Snowflake connection...
✅ Connected successfully!
   Connection ID: a1b2c3d4-e5f6-7890-abcd-ef1234567890
   Account: mycompany.snowflakecomputing.com
   User: john.doe
   Database: SALES_DB
   Schema: PUBLIC
```

### Table Analysis:
```
🔍 Analyzing 2 tables: CUSTOMERS, ORDERS
✅ Analysis completed for 2 tables
   Database: SALES_DB
   Schema: PUBLIC
   ✅ CUSTOMERS: 10000 rows, 8 columns
   ✅ ORDERS: 50000 rows, 6 columns
```

### Dictionary Generation:
```
📝 Generating data dictionary for tables: CUSTOMERS, ORDERS
✅ Data dictionary generated successfully
   Tables processed: 2
   Validation status: valid
   💾 YAML saved to: sales_dict.yaml
```

### NL2SQL Query:
```
💬 Processing NL query: How many customers are there?
✅ Query processed
   Intent: SQL_QUERY
   Generated SQL: SELECT COUNT(*) FROM SALES_DB.PUBLIC.CUSTOMERS
   ✅ Execution successful: 1 rows returned
```

## Troubleshooting

### Connection Issues:
- Verify environment variables are set correctly
- Check Snowflake account details
- Ensure API server is running on port 8001

### API Errors:
- Check API server logs for detailed error messages
- Verify table names exist in your database
- Ensure you have proper permissions

### Dictionary Generation Issues:
- Verify tables contain data
- Check OpenAI API key is configured
- Review LLM utility configuration

## Custom API URL

If running the API on a different host/port:
```bash
python test_snowflake_api.py --url http://your-host:8001 connect
```