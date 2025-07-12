"""
MCP (Model Context Protocol) client implementation.
Handles communication with MCP servers for LLM, Snowflake, and Orchestrator services.
"""

import asyncio
import json
import logging
import os
from typing import Any, Dict, List, Optional, Union
from pathlib import Path
import subprocess
from dataclasses import asdict

from ..core.models import MCPRequest, MCPResponse, SessionContext
from ..core.config import MCPServerConfig, get_config


logger = logging.getLogger(__name__)


class MCPClientError(Exception):
    """Custom exception for MCP client errors"""
    pass


class MCPClient:
    """
    Generic MCP client for communicating with MCP servers.
    Supports stdio and HTTP transports.
    """
    
    def __init__(self, server_config: MCPServerConfig):
        self.config = server_config
        self.process: Optional[subprocess.Popen] = None
        self.is_connected = False
        
    async def connect(self) -> bool:
        """Connect to the MCP server"""
        try:
            logger.info(f"Connecting to MCP server: {self.config.name}")
            
            # Start the MCP server process
            # For Windows, we need to handle shell commands differently
            import platform
            if platform.system() == "Windows":
                # On Windows, we may need to use shell=True for proper module execution
                command_line = [self.config.command] + self.config.args
                self.process = subprocess.Popen(
                    command_line,
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    env={**os.environ, **self.config.env},
                    shell=False,  # Try without shell first
                    creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if hasattr(subprocess, 'CREATE_NEW_PROCESS_GROUP') else 0
                )
            else:
                self.process = subprocess.Popen(
                    [self.config.command] + self.config.args,
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    env={**os.environ, **self.config.env}
                )
            
            # Give the process a moment to start
            await asyncio.sleep(0.5)
            
            # Check if process started successfully
            if self.process.poll() is not None:
                # Process has already terminated
                stderr_output = self.process.stderr.read() if self.process.stderr else "No error output"
                raise Exception(f"MCP server process terminated immediately. Error: {stderr_output}")
            
            # Send initialization request
            init_request = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {
                        "roots": {"listChanged": True},
                        "sampling": {}
                    },
                    "clientInfo": {
                        "name": "nl2sql-v2-client",
                        "version": "1.0.0"
                    }
                }
            }
            
            response = await self._send_request(init_request)
            self.is_connected = True
            logger.info(f"Successfully connected to {self.config.name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to {self.config.name}: {e}")
            self.is_connected = False
            return False
    
    async def disconnect(self):
        """Disconnect from the MCP server"""
        if self.process:
            try:
                self.process.terminate()
                # Use asyncio.create_task to await process.wait()
                import concurrent.futures
                loop = asyncio.get_event_loop()
                await asyncio.wait_for(loop.run_in_executor(None, self.process.wait), timeout=5.0)
            except asyncio.TimeoutError:
                self.process.kill()
            finally:
                self.process = None
                self.is_connected = False
                logger.info(f"Disconnected from {self.config.name}")
    
    async def call_tool(self, tool_name: str, arguments: Dict[str, Any]) -> MCPResponse:
        """Call a tool on the MCP server"""
        if not self.is_connected:
            raise MCPClientError(f"Not connected to {self.config.name}")
        
        request = {
            "jsonrpc": "2.0",
            "id": self._generate_id(),
            "method": "tools/call",
            "params": {
                "name": tool_name,
                "arguments": arguments
            }
        }
        
        try:
            response_data = await self._send_request(request)
            # Extract data from MCP response format
            result = response_data.get("result", {})
            if "content" in result and result["content"]:
                # Parse JSON content from our custom server
                content_text = result["content"][0].get("text", "{}")
                try:
                    data = json.loads(content_text)
                except json.JSONDecodeError:
                    data = {"raw_content": content_text}
            else:
                data = result
                
            return MCPResponse(
                request_id=str(request["id"]),
                success=True,
                data=data,
                metadata={"tool_name": tool_name}
            )
        except Exception as e:
            logger.error(f"Tool call failed for {tool_name}: {e}")
            return MCPResponse(
                request_id=str(request["id"]),
                success=False,
                error_message=str(e),
                metadata={"tool_name": tool_name}
            )
    
    async def list_tools(self) -> List[Dict[str, Any]]:
        """List available tools from the MCP server"""
        if not self.is_connected:
            raise MCPClientError(f"Not connected to {self.config.name}")
        
        request = {
            "jsonrpc": "2.0",
            "id": self._generate_id(),
            "method": "tools/list",
            "params": {}
        }
        
        response_data = await self._send_request(request)
        return response_data.get("result", {}).get("tools", [])
    
    async def get_resource(self, uri: str) -> MCPResponse:
        """Get a resource from the MCP server"""
        if not self.is_connected:
            raise MCPClientError(f"Not connected to {self.config.name}")
        
        request = {
            "jsonrpc": "2.0",
            "id": self._generate_id(),
            "method": "resources/read",
            "params": {
                "uri": uri
            }
        }
        
        try:
            response_data = await self._send_request(request)
            return MCPResponse(
                request_id=str(request["id"]),
                success=True,
                data=response_data.get("result"),
                metadata={"uri": uri}
            )
        except Exception as e:
            logger.error(f"Resource read failed for {uri}: {e}")
            return MCPResponse(
                request_id=str(request["id"]),
                success=False,
                error_message=str(e),
                metadata={"uri": uri}
            )
    
    async def _send_request(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Send a request to the MCP server and get response"""
        if not self.process or not self.process.stdin or not self.process.stdout:
            raise MCPClientError("No active process connection")
        
        # Check if process is still running
        if self.process.poll() is not None:
            raise MCPClientError("MCP server process has terminated")
        
        try:
            # Send request
            request_json = json.dumps(request) + "\n"
            self.process.stdin.write(request_json)
            self.process.stdin.flush()
            
            # Read response with timeout
            import select
            import platform
            
            if platform.system() == "Windows":
                # On Windows, we can't use select with pipes, so use a different approach
                # Try to read with a simple blocking read and hope the server responds quickly
                try:
                    # Use asyncio to run the blocking readline in an executor with timeout
                    loop = asyncio.get_event_loop()
                    response_line = await asyncio.wait_for(
                        loop.run_in_executor(None, self.process.stdout.readline),
                        timeout=5.0
                    )
                except asyncio.TimeoutError:
                    raise MCPClientError("Timeout waiting for server response")
            else:
                # Unix/Linux: use select for timeout
                ready, _, _ = select.select([self.process.stdout], [], [], 5.0)
                if not ready:
                    raise MCPClientError("Timeout waiting for server response")
                response_line = self.process.stdout.readline()
            
            if not response_line:
                raise MCPClientError("No response from server")
            
            try:
                response_data = json.loads(response_line.strip())
                if "error" in response_data:
                    raise MCPClientError(f"Server error: {response_data['error']}")
                return response_data
            except json.JSONDecodeError as e:
                raise MCPClientError(f"Invalid JSON response: {e}")
                
        except Exception as e:
            # Check if process crashed
            if self.process.poll() is not None:
                stderr_output = ""
                if self.process.stderr:
                    try:
                        stderr_output = self.process.stderr.read()
                    except:
                        pass
                raise MCPClientError(f"MCP server process crashed. Error: {stderr_output}")
            raise
    
    def _generate_id(self) -> int:
        """Generate a unique request ID"""
        import time
        return int(time.time() * 1000000) % 1000000


class SnowflakeMCPClient(MCPClient):
    """Specialized MCP client for Snowflake operations"""
    
    def __init__(self):
        config = get_config()
        super().__init__(config.snowflake_mcp)
    
    async def execute_query(self, sql: str, connection_config: Dict[str, Any]) -> MCPResponse:
        """Execute a SQL query"""
        return await self.call_tool("execute_query", {
            "sql": sql,
            "connection": connection_config
        })
    
    async def get_databases(self, connection_config: Dict[str, Any]) -> MCPResponse:
        """Get list of databases"""
        return await self.call_tool("list_databases", {
            "connection": connection_config
        })
    
    async def get_schemas(self, database: str, connection_config: Dict[str, Any]) -> MCPResponse:
        """Get list of schemas in a database"""
        return await self.call_tool("list_schemas", {
            "database": database,
            "connection": connection_config
        })
    
    async def get_tables(self, database: str, schema: str, connection_config: Dict[str, Any]) -> MCPResponse:
        """Get list of tables in a schema"""
        return await self.call_tool("list_tables", {
            "database": database,
            "schema": schema,
            "connection": connection_config
        })
    
    async def describe_table(self, table: str, schema: str, database: str, connection_config: Dict[str, Any]) -> MCPResponse:
        """Get table structure and metadata"""
        return await self.call_tool("describe_table", {
            "table": table,
            "schema": schema,
            "database": database,
            "connection": connection_config
        })


class LLMDirectClient:
    """Direct OpenAI client (no MCP server needed - using OpenAI SDK directly)"""
    
    def __init__(self):
        self.config = get_config()
        import openai
        self.client = openai.OpenAI(api_key=self.config.llm.api_key)
        self.is_connected = True
    
    async def generate_sql(self, natural_language: str, schema_context: Dict[str, Any]) -> MCPResponse:
        """Generate SQL from natural language"""
        try:
            # Build schema context string
            schema_info = ""
            if schema_context.get("tables"):
                schema_info = "Available tables:\n"
                for table in schema_context["tables"]:
                    schema_info += f"- {table['name']}: {table.get('description', 'No description')}\n"
                    if table.get('columns'):
                        for col in table['columns'][:5]:  # First 5 columns
                            schema_info += f"  - {col.get('name', 'unknown')}: {col.get('type', 'unknown')}\n"
            
            prompt = f"""Convert this natural language query to SQL:
Query: {natural_language}

{schema_info}

Database: {schema_context.get('database', 'unknown')}
Schema: {schema_context.get('schema', 'unknown')}

Return only the SQL query, no explanation."""

            response = self.client.chat.completions.create(
                model=self.config.llm.model,
                messages=[{"role": "user", "content": prompt}],
                temperature=self.config.llm.temperature,
                max_tokens=1000
            )
            
            sql = response.choices[0].message.content.strip()
            # Clean up SQL (remove markdown formatting if present)
            if sql.startswith("```sql"):
                sql = sql[6:]
            if sql.startswith("```"):
                sql = sql[3:]
            if sql.endswith("```"):
                sql = sql[:-3]
            
            return MCPResponse(
                request_id="direct_sql",
                success=True,
                data={"sql": sql.strip()}
            )
            
        except Exception as e:
            return MCPResponse(
                request_id="direct_sql",
                success=False,
                error_message=str(e)
            )
    
    async def classify_intent(self, text: str, context: Dict[str, Any]) -> MCPResponse:
        """Classify user intent"""
        try:
            prompt = f"""Classify the user's intent from this text: "{text}"

Context:
- Connected to database: {context.get('session_connected', False)}
- Has selected tables: {context.get('has_selected_tables', False)}
- Has YAML content: {context.get('has_yaml_content', False)}

Possible intents:
- connection: User wants to connect/disconnect from database
- query: User wants to run a data query
- exploration: User wants to explore database structure
- dictionary: User wants to work with data dictionaries
- help: User needs help or information

Return only the intent name (one word)."""

            response = self.client.chat.completions.create(
                model=self.config.llm.model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
                max_tokens=50
            )
            
            intent = response.choices[0].message.content.strip().lower()
            
            return MCPResponse(
                request_id="classify_intent",
                success=True,
                data={"intent": intent}
            )
            
        except Exception as e:
            return MCPResponse(
                request_id="classify_intent", 
                success=False,
                error_message=str(e)
            )
    
    async def generate_response(self, prompt: str, context: Dict[str, Any]) -> MCPResponse:
        """Generate a conversational response"""
        try:
            response = self.client.chat.completions.create(
                model=self.config.llm.model,
                messages=[{"role": "user", "content": prompt}],
                temperature=self.config.llm.temperature,
                max_tokens=self.config.llm.max_tokens
            )
            
            return MCPResponse(
                request_id="generate_response",
                success=True,
                data={"response": response.choices[0].message.content}
            )
            
        except Exception as e:
            return MCPResponse(
                request_id="generate_response",
                success=False,
                error_message=str(e)
            )
    
    async def summarize_results(self, query: str, sql: str, results: List[Dict[str, Any]]) -> MCPResponse:
        """Generate summary of query results"""
        try:
            prompt = f"""Summarize these query results in a conversational way:

Original Question: {query}
SQL Query: {sql}
Results: {str(results[:3])}... ({len(results)} total rows)

Provide a brief, helpful summary of what the data shows."""

            response = self.client.chat.completions.create(
                model=self.config.llm.model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3,
                max_tokens=300
            )
            
            return MCPResponse(
                request_id="summarize_results",
                success=True,
                data={"summary": response.choices[0].message.content}
            )
            
        except Exception as e:
            return MCPResponse(
                request_id="summarize_results",
                success=False,
                error_message=str(e)
            )


# Orchestration is handled directly in our application layer
# No separate MCP server needed


class MCPClientManager:
    """Manages MCP clients - simplified to use existing servers"""
    
    def __init__(self):
        self.clients: Dict[str, Any] = {}
    
    async def initialize(self):
        """Initialize MCP clients"""
        logger.info("Initializing MCP clients...")
        
        try:
            # Initialize Snowflake MCP client (existing server)
            try:
                self.clients["snowflake"] = SnowflakeMCPClient()
                success = await self.clients["snowflake"].connect()
                if not success:
                    logger.warning("Failed to connect to Snowflake MCP client - database operations will be limited")
                    # Remove failed client
                    del self.clients["snowflake"]
            except Exception as e:
                logger.error(f"Failed to initialize Snowflake MCP client: {e}")
                logger.warning("Database operations will be limited without Snowflake connector")
            
            # Initialize direct OpenAI client (no MCP server needed)
            self.clients["llm"] = LLMDirectClient()
            logger.info("Direct OpenAI client initialized")
            
            logger.info("MCP clients initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize MCP clients: {e}")
            raise
    
    async def shutdown(self):
        """Shutdown all MCP clients"""
        logger.info("Shutting down MCP clients...")
        
        try:
            if "snowflake" in self.clients:
                await self.clients["snowflake"].disconnect()
                logger.info("Disconnected Snowflake MCP client")
            
            # OpenAI direct client doesn't need explicit shutdown
            
            self.clients.clear()
            logger.info("All MCP clients shut down")
            
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")
    
    def get_client(self, client_name: str) -> Optional[Any]:
        """Get a specific client"""
        return self.clients.get(client_name)
    
    def get_snowflake_client(self) -> Optional[SnowflakeMCPClient]:
        """Get Snowflake MCP client"""
        return self.clients.get("snowflake")
    
    def get_llm_client(self) -> Optional[LLMDirectClient]:
        """Get LLM direct client"""
        return self.clients.get("llm")