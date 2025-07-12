"""
Service implementations using MCP clients.
These services implement the abstract repository interfaces using MCP communication.
"""

import logging
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta
import asyncio

from ..core.models import (
    DatabaseConnection, TableMetadata, QueryResult, SessionContext,
    ConnectionStatus, AgentIntent, ConnectionRepository, QueryRepository,
    MetadataRepository, SessionRepository, IntentClassificationService, LLMService
)
from ..mcp.client import MCPClientManager
from ..core.config import get_config


logger = logging.getLogger(__name__)


class MCPConnectionRepository(ConnectionRepository):
    """Connection repository implementation using MCP"""
    
    def __init__(self, mcp_manager: MCPClientManager):
        self.mcp_manager = mcp_manager
        self._connections: Dict[str, DatabaseConnection] = {}
    
    async def create_connection(self, config: DatabaseConnection) -> DatabaseConnection:
        """Create and test a new database connection"""
        try:
            snowflake_client = self.mcp_manager.get_snowflake_client()
            if not snowflake_client:
                raise Exception("Snowflake MCP client not available")
            
            # Test the connection
            connection_config = {
                "account": config.account,
                "user": config.user,
                "password": config.password,
                "warehouse": config.warehouse,
                "database": config.database,
                "schema": config.schema,
                "role": config.role
            }
            
            # Try a simple query to test connection
            test_response = await snowflake_client.execute_query(
                "SELECT CURRENT_VERSION()", 
                connection_config
            )
            
            if test_response.success:
                config.status = ConnectionStatus.CONNECTED
                config.last_used = datetime.now()
                config.error_message = None
                logger.info(f"Successfully connected to Snowflake: {config.account}")
            else:
                config.status = ConnectionStatus.ERROR
                config.error_message = test_response.error_message
                logger.error(f"Failed to connect to Snowflake: {test_response.error_message}")
            
            self._connections[config.id] = config
            return config
            
        except Exception as e:
            config.status = ConnectionStatus.ERROR
            config.error_message = str(e)
            logger.error(f"Connection creation failed: {e}")
            self._connections[config.id] = config
            return config
    
    async def get_connection(self, connection_id: str) -> Optional[DatabaseConnection]:
        """Get a connection by ID"""
        return self._connections.get(connection_id)
    
    async def test_connection(self, config: DatabaseConnection) -> bool:
        """Test if a connection is working"""
        try:
            snowflake_client = self.mcp_manager.get_snowflake_client()
            if not snowflake_client:
                return False
            
            connection_config = {
                "account": config.account,
                "user": config.user,
                "password": config.password,
                "warehouse": config.warehouse,
                "database": config.database,
                "schema": config.schema,
                "role": config.role
            }
            
            response = await snowflake_client.execute_query(
                "SELECT 1", 
                connection_config
            )
            
            return response.success
            
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False
    
    async def close_connection(self, connection_id: str) -> bool:
        """Close a connection"""
        if connection_id in self._connections:
            self._connections[connection_id].status = ConnectionStatus.DISCONNECTED
            return True
        return False


class MCPQueryRepository(QueryRepository):
    """Query repository implementation using MCP"""
    
    def __init__(self, mcp_manager: MCPClientManager):
        self.mcp_manager = mcp_manager
    
    async def execute_query(self, sql: str, connection: DatabaseConnection) -> QueryResult:
        """Execute a SQL query"""
        start_time = datetime.now()
        result = QueryResult()
        result.sql_query = sql
        
        try:
            snowflake_client = self.mcp_manager.get_snowflake_client()
            if not snowflake_client:
                raise Exception("Snowflake MCP client not available")
            
            connection_config = {
                "account": connection.account,
                "user": connection.user,
                "password": connection.password,
                "warehouse": connection.warehouse,
                "database": connection.database,
                "schema": connection.schema,
                "role": connection.role
            }
            
            response = await snowflake_client.execute_query(sql, connection_config)
            
            if response.success and response.data:
                result.success = True
                result.data = response.data.get("rows", [])
                result.column_names = response.data.get("columns", [])
                result.row_count = len(result.data)
            else:
                result.success = False
                result.error_message = response.error_message or "Unknown error"
            
        except Exception as e:
            result.success = False
            result.error_message = str(e)
            logger.error(f"Query execution failed: {e}")
        
        finally:
            end_time = datetime.now()
            result.execution_time_ms = (end_time - start_time).total_seconds() * 1000
        
        return result
    
    async def generate_sql(self, natural_language: str, context: SessionContext) -> str:
        """Generate SQL from natural language"""
        try:
            llm_client = self.mcp_manager.get_llm_client()
            if not llm_client:
                raise Exception("LLM MCP client not available")
            
            # Build schema context from session
            schema_context = {
                "tables": [table.to_dict() for table in context.selected_tables],
                "yaml_content": context.yaml_content,
                "database": context.connection.database if context.connection else None,
                "schema": context.connection.schema if context.connection else None
            }
            
            response = await llm_client.generate_sql(natural_language, schema_context)
            
            if response.success and response.data:
                return response.data.get("sql", "")
            else:
                raise Exception(f"SQL generation failed: {response.error_message}")
                
        except Exception as e:
            logger.error(f"SQL generation failed: {e}")
            raise
    
    async def validate_sql(self, sql: str) -> bool:
        """Validate SQL syntax (basic check)"""
        try:
            # Basic SQL validation - could be enhanced with proper parser
            sql = sql.strip().upper()
            if not sql:
                return False
            
            # Check for basic SQL keywords
            valid_starts = ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'WITH', 'CREATE', 'ALTER', 'DROP']
            return any(sql.startswith(keyword) for keyword in valid_starts)
            
        except Exception:
            return False


class MCPMetadataRepository(MetadataRepository):
    """Metadata repository implementation using MCP"""
    
    def __init__(self, mcp_manager: MCPClientManager):
        self.mcp_manager = mcp_manager
    
    async def get_databases(self, connection: DatabaseConnection) -> List[str]:
        """Get list of databases"""
        try:
            snowflake_client = self.mcp_manager.get_snowflake_client()
            if not snowflake_client:
                raise Exception("Snowflake MCP client not available")
            
            connection_config = {
                "account": connection.account,
                "user": connection.user,
                "password": connection.password,
                "warehouse": connection.warehouse,
                "database": connection.database,
                "schema": connection.schema,
                "role": connection.role
            }
            
            response = await snowflake_client.get_databases(connection_config)
            
            if response.success and response.data:
                return response.data.get("databases", [])
            else:
                logger.error(f"Failed to get databases: {response.error_message}")
                return []
                
        except Exception as e:
            logger.error(f"Get databases failed: {e}")
            return []
    
    async def get_schemas(self, connection: DatabaseConnection, database: str) -> List[str]:
        """Get list of schemas in a database"""
        try:
            snowflake_client = self.mcp_manager.get_snowflake_client()
            if not snowflake_client:
                raise Exception("Snowflake MCP client not available")
            
            connection_config = {
                "account": connection.account,
                "user": connection.user,
                "password": connection.password,
                "warehouse": connection.warehouse,
                "database": connection.database,
                "schema": connection.schema,
                "role": connection.role
            }
            
            response = await snowflake_client.get_schemas(database, connection_config)
            
            if response.success and response.data:
                return response.data.get("schemas", [])
            else:
                logger.error(f"Failed to get schemas: {response.error_message}")
                return []
                
        except Exception as e:
            logger.error(f"Get schemas failed: {e}")
            return []
    
    async def get_tables(self, connection: DatabaseConnection, database: str, schema: str) -> List[TableMetadata]:
        """Get list of tables in a schema"""
        try:
            snowflake_client = self.mcp_manager.get_snowflake_client()
            if not snowflake_client:
                raise Exception("Snowflake MCP client not available")
            
            connection_config = {
                "account": connection.account,
                "user": connection.user,
                "password": connection.password,
                "warehouse": connection.warehouse,
                "database": connection.database,
                "schema": connection.schema,
                "role": connection.role
            }
            
            response = await snowflake_client.get_tables(database, schema, connection_config)
            
            if response.success and response.data:
                tables = []
                for table_info in response.data.get("tables", []):
                    table = TableMetadata(
                        name=table_info.get("name", ""),
                        schema=schema,
                        database=database,
                        columns=table_info.get("columns", []),
                        row_count=table_info.get("row_count"),
                        description=table_info.get("description")
                    )
                    tables.append(table)
                return tables
            else:
                logger.error(f"Failed to get tables: {response.error_message}")
                return []
                
        except Exception as e:
            logger.error(f"Get tables failed: {e}")
            return []
    
    async def get_table_metadata(self, connection: DatabaseConnection, table_name: str, 
                               schema: str, database: str) -> TableMetadata:
        """Get detailed metadata for a specific table"""
        try:
            snowflake_client = self.mcp_manager.get_snowflake_client()
            if not snowflake_client:
                raise Exception("Snowflake MCP client not available")
            
            connection_config = {
                "account": connection.account,
                "user": connection.user,
                "password": connection.password,
                "warehouse": connection.warehouse,
                "database": connection.database,
                "schema": connection.schema,
                "role": connection.role
            }
            
            response = await snowflake_client.describe_table(table_name, schema, database, connection_config)
            
            if response.success and response.data:
                table_info = response.data
                return TableMetadata(
                    name=table_name,
                    schema=schema,
                    database=database,
                    columns=table_info.get("columns", []),
                    row_count=table_info.get("row_count"),
                    description=table_info.get("description"),
                    tags=table_info.get("tags", [])
                )
            else:
                logger.error(f"Failed to get table metadata: {response.error_message}")
                return TableMetadata(name=table_name, schema=schema, database=database)
                
        except Exception as e:
            logger.error(f"Get table metadata failed: {e}")
            return TableMetadata(name=table_name, schema=schema, database=database)


class InMemorySessionRepository(SessionRepository):
    """In-memory session repository implementation"""
    
    def __init__(self):
        self._sessions: Dict[str, SessionContext] = {}
        self.config = get_config()
    
    async def create_session(self) -> SessionContext:
        """Create a new session"""
        session = SessionContext()
        self._sessions[session.session_id] = session
        logger.info(f"Created new session: {session.session_id}")
        return session
    
    async def get_session(self, session_id: str) -> Optional[SessionContext]:
        """Get session by ID"""
        session = self._sessions.get(session_id)
        if session:
            # Check if session has expired
            timeout_delta = timedelta(minutes=self.config.session_timeout_minutes)
            if datetime.now() - session.last_activity > timeout_delta:
                await self.delete_session(session_id)
                return None
        return session
    
    async def update_session(self, session: SessionContext) -> bool:
        """Update session"""
        if session.session_id in self._sessions:
            session.update_activity()
            self._sessions[session.session_id] = session
            return True
        return False
    
    async def delete_session(self, session_id: str) -> bool:
        """Delete session"""
        if session_id in self._sessions:
            del self._sessions[session_id]
            logger.info(f"Deleted session: {session_id}")
            return True
        return False


class MCPIntentClassificationService(IntentClassificationService):
    """Intent classification service using MCP"""
    
    def __init__(self, mcp_manager: MCPClientManager):
        self.mcp_manager = mcp_manager
    
    async def classify_intent(self, text: str, context: SessionContext) -> AgentIntent:
        """Classify user intent using LLM"""
        try:
            llm_client = self.mcp_manager.get_llm_client()
            if not llm_client:
                raise Exception("LLM MCP client not available")
            
            context_data = {
                "session_connected": context.is_connected(),
                "has_selected_tables": len(context.selected_tables) > 0,
                "has_yaml_content": context.yaml_content is not None,
                "recent_queries": [q.sql_query for q in context.query_history[-3:]]  # Last 3 queries
            }
            
            response = await llm_client.classify_intent(text, context_data)
            
            if response.success and response.data:
                intent_str = response.data.get("intent", "unknown")
                try:
                    return AgentIntent(intent_str)
                except ValueError:
                    return AgentIntent.UNKNOWN
            else:
                logger.error(f"Intent classification failed: {response.error_message}")
                return AgentIntent.UNKNOWN
                
        except Exception as e:
            logger.error(f"Intent classification error: {e}")
            return AgentIntent.UNKNOWN


class MCPLLMService(LLMService):
    """LLM service implementation using direct OpenAI client"""
    
    def __init__(self, mcp_manager: MCPClientManager):
        self.mcp_manager = mcp_manager
    
    async def generate_response(self, prompt: str, context: Dict[str, Any]) -> str:
        """Generate a conversational response"""
        try:
            llm_client = self.mcp_manager.get_llm_client()
            if not llm_client:
                raise Exception("LLM MCP client not available")
            
            response = await llm_client.generate_response(prompt, context)
            
            if response.success and response.data:
                return response.data.get("response", "")
            else:
                logger.error(f"Response generation failed: {response.error_message}")
                return "I'm sorry, I encountered an error while generating a response."
                
        except Exception as e:
            logger.error(f"Response generation error: {e}")
            return "I'm sorry, I encountered an error while processing your request."
    
    async def generate_sql(self, natural_language: str, schema_context: Dict[str, Any]) -> str:
        """Generate SQL from natural language"""
        try:
            llm_client = self.mcp_manager.get_llm_client()
            if not llm_client:
                raise Exception("LLM MCP client not available")
            
            response = await llm_client.generate_sql(natural_language, schema_context)
            
            if response.success and response.data:
                return response.data.get("sql", "")
            else:
                logger.error(f"SQL generation failed: {response.error_message}")
                raise Exception(f"SQL generation failed: {response.error_message}")
                
        except Exception as e:
            logger.error(f"SQL generation error: {e}")
            raise