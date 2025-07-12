"""
Connection Agent - Handles database connection and authentication.
Specialized agent following the Tool Use pattern.
"""

import logging
from typing import Dict, Any
import re

from ..core.models import (
    QueryRequest, SessionContext, DatabaseConnection, ConnectionStatus
)
from ..core.config import get_config
from ..services.implementations import MCPConnectionRepository, MCPMetadataRepository, MCPLLMService
from ..mcp.client import MCPClientManager
from .base import BaseAgent, AgentResponse


logger = logging.getLogger(__name__)


class ConnectionAgent(BaseAgent):
    """Specialized agent for handling database connections"""
    
    def __init__(self, connection_repo: MCPConnectionRepository, 
                 metadata_repo: MCPMetadataRepository, llm_service: MCPLLMService,
                 mcp_manager: MCPClientManager):
        super().__init__("connection")
        self.connection_repo = connection_repo
        self.metadata_repo = metadata_repo
        self.llm_service = llm_service
        self.mcp_manager = mcp_manager
        self.config = get_config()
    
    async def execute(self, request: QueryRequest, session: SessionContext) -> AgentResponse:
        """Execute connection-related tasks"""
        user_input = request.natural_language.lower()
        
        try:
            # Determine specific connection action
            if any(word in user_input for word in ['connect', 'login', 'authenticate']):
                return await self._handle_connection_request(request, session)
            
            elif any(word in user_input for word in ['disconnect', 'logout', 'close']):
                return await self._handle_disconnection_request(session)
            
            elif any(word in user_input for word in ['status', 'check', 'test']):
                return await self._handle_connection_status(session)
            
            elif any(word in user_input for word in ['config', 'setup', 'configure']):
                return await self._handle_configuration_help(request, session)
            
            else:
                # Default to connection attempt if no connection exists
                if not session.is_connected():
                    return await self._handle_connection_request(request, session)
                else:
                    return await self._handle_connection_status(session)
        
        except Exception as e:
            logger.error(f"Connection agent error: {e}")
            return self._create_error_response(str(e))
    
    async def _handle_connection_request(self, request: QueryRequest, session: SessionContext) -> AgentResponse:
        """Handle database connection requests"""
        try:
            # Try to extract connection details from user input
            connection_details = self._extract_connection_details(request.natural_language)
            
            # Use existing connection or create new one
            if session.connection and session.connection.status == ConnectionStatus.CONNECTED:
                return self._create_response(
                    success=True,
                    message="You're already connected to the database!",
                    data={"connection": session.connection.to_dict()}
                )
            
            # Create connection config (prefer extracted details, fallback to env config)
            config_details = self.config.snowflake
            connection_config = DatabaseConnection(
                account=connection_details.get('account') or config_details.account,
                user=connection_details.get('user') or config_details.user,
                password=connection_details.get('password') or config_details.password,
                warehouse=connection_details.get('warehouse') or config_details.warehouse,
                database=connection_details.get('database') or config_details.database,
                schema=connection_details.get('schema') or config_details.schema,
                role=connection_details.get('role') or config_details.role
            )
            
            # Validate required fields
            if not all([connection_config.account, connection_config.user, connection_config.password]):
                return await self._handle_missing_credentials(request, session)
            
            # Check if Snowflake MCP client is available
            snowflake_client = self.mcp_manager.get_snowflake_client()
            if not snowflake_client:
                return self._create_error_response(
                    "❌ Snowflake database connector is not available.\n\n"
                    "📝 To fix this issue:\n"
                    "1. Install the Snowflake connector: pip install snowflake-connector-python\n"
                    "2. Restart the CLI after installation\n\n"
                    "💡 You can also run: python install_windows.py"
                )
            
            # Attempt connection
            logger.info(f"Attempting connection to Snowflake account: {connection_config.account}")
            connection = await self.connection_repo.create_connection(connection_config)
            
            if connection.status == ConnectionStatus.CONNECTED:
                session.connection = connection
                
                # Get basic database info
                databases = await self.metadata_repo.get_databases(connection)
                
                response_message = f"✅ Successfully connected to Snowflake!\n"
                response_message += f"Account: {connection.account}\n"
                response_message += f"User: {connection.user}\n"
                response_message += f"Warehouse: {connection.warehouse}\n"
                if databases:
                    response_message += f"Available databases: {', '.join(databases[:5])}"
                    if len(databases) > 5:
                        response_message += f" and {len(databases) - 5} more"
                
                return self._create_response(
                    success=True,
                    message=response_message,
                    data={
                        "connection": connection.to_dict(),
                        "databases": databases,
                        "next_steps": [
                            "Explore available databases and schemas",
                            "Load a data dictionary",
                            "Start querying your data"
                        ]
                    }
                )
            else:
                error_msg = connection.error_message or "Unknown connection error"
                return self._create_error_response(
                    f"Failed to connect to Snowflake: {error_msg}",
                    {"connection_details": connection.to_dict()}
                )
        
        except Exception as e:
            logger.error(f"Connection request failed: {e}")
            return self._create_error_response(f"Connection failed: {str(e)}")
    
    async def _handle_disconnection_request(self, session: SessionContext) -> AgentResponse:
        """Handle disconnection requests"""
        if not session.connection:
            return self._create_response(
                success=True,
                message="You're not currently connected to any database.",
                data={}
            )
        
        try:
            await self.connection_repo.close_connection(session.connection.id)
            session.connection = None
            
            return self._create_response(
                success=True,
                message="✅ Successfully disconnected from the database.",
                data={"status": "disconnected"}
            )
        
        except Exception as e:
            logger.error(f"Disconnection failed: {e}")
            return self._create_error_response(f"Disconnection failed: {str(e)}")
    
    async def _handle_connection_status(self, session: SessionContext) -> AgentResponse:
        """Check and report connection status"""
        if not session.connection:
            return self._create_response(
                success=True,
                message="❌ Not connected to any database. Use 'connect' to establish a connection.",
                data={"status": "disconnected"}
            )
        
        try:
            # Test the connection
            is_active = await self.connection_repo.test_connection(session.connection)
            
            if is_active:
                conn = session.connection
                status_message = f"✅ Connected to Snowflake\n"
                status_message += f"Account: {conn.account}\n"
                status_message += f"Database: {conn.database}\n"
                status_message += f"Schema: {conn.schema}\n"
                status_message += f"Warehouse: {conn.warehouse}"
                
                return self._create_response(
                    success=True,
                    message=status_message,
                    data={"connection": conn.to_dict(), "status": "connected"}
                )
            else:
                session.connection.status = ConnectionStatus.ERROR
                return self._create_response(
                    success=False,
                    message="❌ Connection appears to be inactive. Try reconnecting.",
                    data={"status": "error", "connection": session.connection.to_dict()}
                )
        
        except Exception as e:
            logger.error(f"Connection status check failed: {e}")
            return self._create_error_response(f"Status check failed: {str(e)}")
    
    async def _handle_configuration_help(self, request: QueryRequest, session: SessionContext) -> AgentResponse:
        """Provide help with connection configuration"""
        try:
            help_context = {
                "user_request": request.natural_language,
                "has_connection": session.connection is not None
            }
            
            prompt = f"""
            The user is asking for help with database connection configuration.
            Request: {request.natural_language}
            
            Provide helpful guidance about:
            1. Required Snowflake connection parameters
            2. How to set up environment variables
            3. Common connection issues and solutions
            4. Next steps after connecting
            
            Be concise and practical.
            """
            
            help_response = await self.llm_service.generate_response(prompt, help_context)
            
            return self._create_response(
                success=True,
                message=help_response,
                data={
                    "required_params": [
                        "SNOWFLAKE_ACCOUNT",
                        "SNOWFLAKE_USER", 
                        "SNOWFLAKE_PASSWORD",
                        "SNOWFLAKE_WAREHOUSE",
                        "SNOWFLAKE_DATABASE",
                        "SNOWFLAKE_SCHEMA",
                        "SNOWFLAKE_ROLE"
                    ]
                }
            )
        
        except Exception as e:
            logger.error(f"Configuration help failed: {e}")
            return self._create_error_response(f"Help generation failed: {str(e)}")
    
    async def _handle_missing_credentials(self, request: QueryRequest, session: SessionContext) -> AgentResponse:
        """Handle cases where connection credentials are missing"""
        try:
            prompt = f"""
            The user wants to connect to Snowflake but some credentials are missing.
            User request: {request.natural_language}
            
            Provide helpful guidance on:
            1. What connection parameters are required
            2. How to provide them (environment variables, command line)
            3. Security best practices
            
            Be encouraging and helpful.
            """
            
            help_response = await self.llm_service.generate_response(prompt, {})
            
            return self._create_response(
                success=False,
                message=f"Missing required connection credentials.\n\n{help_response}",
                data={
                    "missing_credentials": True,
                    "required_env_vars": [
                        "SNOWFLAKE_ACCOUNT",
                        "SNOWFLAKE_USER", 
                        "SNOWFLAKE_PASSWORD"
                    ]
                }
            )
        
        except Exception as e:
            logger.error(f"Missing credentials help failed: {e}")
            return self._create_error_response(
                "Missing required connection credentials. Please set SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, and SNOWFLAKE_PASSWORD environment variables."
            )
    
    def _extract_connection_details(self, user_input: str) -> Dict[str, str]:
        """Extract connection details from user input using regex patterns"""
        details = {}
        
        # Extract account
        account_match = re.search(r'account[:\s]+([a-zA-Z0-9\-_\.]+)', user_input, re.IGNORECASE)
        if account_match:
            details['account'] = account_match.group(1)
        
        # Extract user
        user_match = re.search(r'user[:\s]+([a-zA-Z0-9\-_\.@]+)', user_input, re.IGNORECASE)
        if user_match:
            details['user'] = user_match.group(1)
        
        # Extract database
        db_match = re.search(r'database[:\s]+([a-zA-Z0-9\-_\.]+)', user_input, re.IGNORECASE)
        if db_match:
            details['database'] = db_match.group(1)
        
        # Extract warehouse  
        wh_match = re.search(r'warehouse[:\s]+([a-zA-Z0-9\-_\.]+)', user_input, re.IGNORECASE)
        if wh_match:
            details['warehouse'] = wh_match.group(1)
        
        # Extract schema
        schema_match = re.search(r'schema[:\s]+([a-zA-Z0-9\-_\.]+)', user_input, re.IGNORECASE)
        if schema_match:
            details['schema'] = schema_match.group(1)
        
        return details