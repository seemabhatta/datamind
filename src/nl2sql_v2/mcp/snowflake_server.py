#!/usr/bin/env python3
"""
Simple Snowflake MCP Server
Provides basic Snowflake database operations via MCP protocol.
"""

import asyncio
import json
import sys
import os
import logging
from typing import Any, Dict, List, Optional

try:
    # Suppress the deprecation warning for snowflake connector
    import warnings
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        import snowflake.connector
        from snowflake.connector import DictCursor
    SNOWFLAKE_AVAILABLE = True
except ImportError:
    SNOWFLAKE_AVAILABLE = False
    snowflake = None


logger = logging.getLogger(__name__)


class SnowflakeMCPServer:
    """Simple MCP server for Snowflake operations"""
    
    def __init__(self):
        self.tools = {
            "execute_query": {
                "name": "execute_query",
                "description": "Execute a SQL query",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "sql": {"type": "string"},
                        "connection": {"type": "object"}
                    },
                    "required": ["sql", "connection"]
                }
            },
            "list_databases": {
                "name": "list_databases", 
                "description": "List all databases",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "connection": {"type": "object"}
                    },
                    "required": ["connection"]
                }
            },
            "list_schemas": {
                "name": "list_schemas",
                "description": "List schemas in a database", 
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "database": {"type": "string"},
                        "connection": {"type": "object"}
                    },
                    "required": ["database", "connection"]
                }
            },
            "list_tables": {
                "name": "list_tables",
                "description": "List tables in a schema",
                "inputSchema": {
                    "type": "object", 
                    "properties": {
                        "database": {"type": "string"},
                        "schema": {"type": "string"},
                        "connection": {"type": "object"}
                    },
                    "required": ["database", "schema", "connection"]
                }
            },
            "describe_table": {
                "name": "describe_table",
                "description": "Get table structure and metadata",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "table": {"type": "string"},
                        "schema": {"type": "string"}, 
                        "database": {"type": "string"},
                        "connection": {"type": "object"}
                    },
                    "required": ["table", "schema", "database", "connection"]
                }
            }
        }
    
    def _get_connection(self, connection_config: Dict[str, Any]):
        """Create Snowflake connection"""
        if not SNOWFLAKE_AVAILABLE:
            raise Exception("Snowflake connector not installed. Please install with: pip install snowflake-connector-python")
        
        return snowflake.connector.connect(
            account=connection_config["account"],
            user=connection_config["user"],
            password=connection_config["password"],
            warehouse=connection_config.get("warehouse"),
            database=connection_config.get("database"),
            schema=connection_config.get("schema"),
            role=connection_config.get("role")
        )
    
    async def execute_query(self, sql: str, connection_config: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a SQL query"""
        try:
            conn = self._get_connection(connection_config)
            cursor = conn.cursor(DictCursor)
            cursor.execute(sql)
            
            if cursor.description:
                # Query returned results
                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                return {
                    "rows": rows,
                    "columns": columns,
                    "rowcount": len(rows)
                }
            else:
                # DDL/DML query
                return {
                    "rowcount": cursor.rowcount,
                    "message": "Query executed successfully"
                }
                
        except Exception as e:
            raise Exception(f"Query execution failed: {e}")
        finally:
            if 'conn' in locals():
                conn.close()
    
    async def list_databases(self, connection_config: Dict[str, Any]) -> Dict[str, Any]:
        """List all databases"""
        sql = "SHOW DATABASES"
        result = await self.execute_query(sql, connection_config)
        databases = [row["name"] for row in result["rows"]]
        return {"databases": databases}
    
    async def list_schemas(self, database: str, connection_config: Dict[str, Any]) -> Dict[str, Any]:
        """List schemas in a database"""
        sql = f"SHOW SCHEMAS IN DATABASE {database}"
        result = await self.execute_query(sql, connection_config)
        schemas = [row["name"] for row in result["rows"]]
        return {"schemas": schemas}
    
    async def list_tables(self, database: str, schema: str, connection_config: Dict[str, Any]) -> Dict[str, Any]:
        """List tables in a schema"""
        sql = f"SHOW TABLES IN SCHEMA {database}.{schema}"
        result = await self.execute_query(sql, connection_config)
        tables = []
        for row in result["rows"]:
            tables.append({
                "name": row["name"],
                "type": row.get("kind", "TABLE"),
                "database": database,
                "schema": schema
            })
        return {"tables": tables}
    
    async def describe_table(self, table: str, schema: str, database: str, connection_config: Dict[str, Any]) -> Dict[str, Any]:
        """Get table structure and metadata"""
        sql = f"DESCRIBE TABLE {database}.{schema}.{table}"
        result = await self.execute_query(sql, connection_config)
        columns = []
        for row in result["rows"]:
            columns.append({
                "name": row["name"],
                "type": row["type"],
                "nullable": row.get("null?", "Y") == "Y",
                "default": row.get("default"),
                "primary_key": row.get("primary key", "N") == "Y"
            })
        
        # Get row count
        try:
            count_sql = f"SELECT COUNT(*) as count FROM {database}.{schema}.{table}"
            count_result = await self.execute_query(count_sql, connection_config)
            row_count = count_result["rows"][0]["COUNT"]
        except:
            row_count = None
        
        return {
            "name": table,
            "schema": schema,
            "database": database,
            "columns": columns,
            "row_count": row_count
        }
    
    async def handle_request(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Handle incoming MCP request"""
        method = request.get("method")
        params = request.get("params", {})
        request_id = request.get("id")
        
        try:
            if method == "initialize":
                return {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {
                        "protocolVersion": "2024-11-05",
                        "capabilities": {
                            "tools": {"listChanged": True}
                        },
                        "serverInfo": {
                            "name": "snowflake-mcp-server",
                            "version": "1.0.0"
                        }
                    }
                }
            
            elif method == "tools/list":
                return {
                    "jsonrpc": "2.0", 
                    "id": request_id,
                    "result": {
                        "tools": list(self.tools.values())
                    }
                }
            
            elif method == "tools/call":
                tool_name = params.get("name")
                arguments = params.get("arguments", {})
                
                if tool_name == "execute_query":
                    result = await self.execute_query(arguments["sql"], arguments["connection"])
                elif tool_name == "list_databases":
                    result = await self.list_databases(arguments["connection"])
                elif tool_name == "list_schemas":
                    result = await self.list_schemas(arguments["database"], arguments["connection"])
                elif tool_name == "list_tables":
                    result = await self.list_tables(arguments["database"], arguments["schema"], arguments["connection"])
                elif tool_name == "describe_table":
                    result = await self.describe_table(arguments["table"], arguments["schema"], arguments["database"], arguments["connection"])
                else:
                    raise Exception(f"Unknown tool: {tool_name}")
                
                return {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": json.dumps(result)
                            }
                        ]
                    }
                }
            
            else:
                raise Exception(f"Unknown method: {method}")
        
        except Exception as e:
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {
                    "code": -32000,
                    "message": str(e)
                }
            }
    
    async def run(self):
        """Run the MCP server"""
        import sys
        import platform
        
        # On Windows, ensure we're using the right text mode for stdin/stdout
        if platform.system() == "Windows":
            # Ensure stdin/stdout are in text mode and properly encoded
            if hasattr(sys.stdin, 'reconfigure'):
                sys.stdin.reconfigure(encoding='utf-8')
            if hasattr(sys.stdout, 'reconfigure'):
                sys.stdout.reconfigure(encoding='utf-8')
        
        try:
            while True:
                try:
                    # Read request from stdin
                    line = sys.stdin.readline()
                    if not line:
                        break
                    
                    line = line.strip()
                    if not line:
                        continue
                    
                    request = json.loads(line)
                    response = await self.handle_request(request)
                    
                    # Write response to stdout
                    response_json = json.dumps(response)
                    print(response_json, flush=True)
                    
                except json.JSONDecodeError:
                    # Invalid JSON, skip
                    continue
                except Exception as e:
                    logger.error(f"Server error: {e}")
                    # Send error response for any unhandled exceptions
                    error_response = {
                        "jsonrpc": "2.0",
                        "id": None,
                        "error": {
                            "code": -32603,
                            "message": f"Internal error: {str(e)}"
                        }
                    }
                    print(json.dumps(error_response), flush=True)
        except KeyboardInterrupt:
            pass
        except Exception as e:
            logger.error(f"Fatal server error: {e}")
            sys.exit(1)


async def main():
    """Main entry point"""
    if not SNOWFLAKE_AVAILABLE:
        # Send an error response and exit
        error_response = {
            "jsonrpc": "2.0",
            "id": None,
            "error": {
                "code": -32001,
                "message": "Snowflake connector not available. Install with: pip install snowflake-connector-python"
            }
        }
        print(json.dumps(error_response), flush=True)
        sys.exit(1)
    
    server = SnowflakeMCPServer()
    await server.run()


if __name__ == "__main__":
    asyncio.run(main())