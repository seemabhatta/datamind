"""
Exploration Agent - Handles database exploration and schema discovery.
Helps users navigate databases, schemas, and tables.
"""

import logging
from typing import Dict, Any, List

from ..core.models import QueryRequest, SessionContext, TableMetadata
from ..services.implementations import MCPMetadataRepository, MCPLLMService
from .base import BaseAgent, AgentResponse


logger = logging.getLogger(__name__)


class ExplorationAgent(BaseAgent):
    """Specialized agent for database exploration and discovery"""
    
    def __init__(self, metadata_repo: MCPMetadataRepository, llm_service: MCPLLMService):
        super().__init__("exploration")
        self.metadata_repo = metadata_repo
        self.llm_service = llm_service
    
    async def execute(self, request: QueryRequest, session: SessionContext) -> AgentResponse:
        """Execute exploration-related tasks"""
        user_input = request.natural_language.lower()
        
        try:
            # Check connection first
            if not session.is_connected():
                return self._create_error_response(
                    "No database connection available. Please connect first.",
                    {"suggestion": "Use 'connect' to establish a database connection"}
                )
            
            # Determine exploration action
            if any(word in user_input for word in ['database', 'databases', 'list db']):
                return await self._handle_database_exploration(request, session)
            
            elif any(word in user_input for word in ['schema', 'schemas']):
                return await self._handle_schema_exploration(request, session)
            
            elif any(word in user_input for word in ['table', 'tables', 'list tables']):
                return await self._handle_table_exploration(request, session)
            
            elif any(word in user_input for word in ['select', 'choose', 'pick']):
                return await self._handle_selection(request, session)
            
            elif any(word in user_input for word in ['describe', 'detail', 'info', 'about']):
                return await self._handle_describe_request(request, session)
            
            else:
                # Default exploration based on current context
                return await self._handle_contextual_exploration(request, session)
        
        except Exception as e:
            logger.error(f"Exploration agent error: {e}")
            return self._create_error_response(str(e))
    
    async def _handle_database_exploration(self, request: QueryRequest, session: SessionContext) -> AgentResponse:
        """Handle database listing and exploration"""
        try:
            databases = await self.metadata_repo.get_databases(session.connection)
            
            if not databases:
                return self._create_response(
                    success=True,
                    message="No databases found or insufficient permissions to list databases.",
                    data={"databases": []}
                )
            
            # Generate friendly response
            response_message = f"📊 Found {len(databases)} database(s):\n\n"
            for i, db in enumerate(databases, 1):
                response_message += f"{i}. {db}\n"
            
            response_message += f"\nCurrent database: {session.connection.database}"
            
            return self._create_response(
                success=True,
                message=response_message,
                data={
                    "databases": databases,
                    "current_database": session.connection.database,
                    "suggestions": [
                        f"Explore schemas in {session.connection.database}",
                        "Switch to a different database",
                        "List tables in current schema"
                    ]
                }
            )
        
        except Exception as e:
            logger.error(f"Database exploration failed: {e}")
            return self._create_error_response(f"Failed to explore databases: {str(e)}")
    
    async def _handle_schema_exploration(self, request: QueryRequest, session: SessionContext) -> AgentResponse:
        """Handle schema listing and exploration"""
        try:
            # Determine which database to explore
            database = session.connection.database
            user_input = request.natural_language
            
            # Check if user specified a different database
            for word in user_input.split():
                if word.upper() != database.upper() and len(word) > 3:
                    # Might be a database name
                    databases = await self.metadata_repo.get_databases(session.connection)
                    if word.upper() in [db.upper() for db in databases]:
                        database = word.upper()
                        break
            
            schemas = await self.metadata_repo.get_schemas(session.connection, database)
            
            if not schemas:
                return self._create_response(
                    success=True,
                    message=f"No schemas found in database '{database}' or insufficient permissions.",
                    data={"schemas": [], "database": database}
                )
            
            response_message = f"📁 Found {len(schemas)} schema(s) in database '{database}':\n\n"
            for i, schema in enumerate(schemas, 1):
                response_message += f"{i}. {schema}\n"
            
            response_message += f"\nCurrent schema: {session.connection.schema}"
            
            return self._create_response(
                success=True,
                message=response_message,
                data={
                    "schemas": schemas,
                    "database": database,
                    "current_schema": session.connection.schema,
                    "suggestions": [
                        f"Explore tables in {session.connection.schema}",
                        "Switch to a different schema",
                        "Load data dictionary"
                    ]
                }
            )
        
        except Exception as e:
            logger.error(f"Schema exploration failed: {e}")
            return self._create_error_response(f"Failed to explore schemas: {str(e)}")
    
    async def _handle_table_exploration(self, request: QueryRequest, session: SessionContext) -> AgentResponse:
        """Handle table listing and exploration"""
        try:
            database = session.connection.database
            schema = session.connection.schema
            
            tables = await self.metadata_repo.get_tables(session.connection, database, schema)
            
            if not tables:
                return self._create_response(
                    success=True,
                    message=f"No tables found in {database}.{schema} or insufficient permissions.",
                    data={"tables": [], "database": database, "schema": schema}
                )
            
            # Format table information
            response_message = f"📋 Found {len(tables)} table(s) in {database}.{schema}:\n\n"
            
            for i, table in enumerate(tables, 1):
                response_message += f"{i}. {table.name}"
                if table.row_count is not None:
                    response_message += f" ({table.row_count:,} rows)"
                if table.description:
                    response_message += f" - {table.description[:50]}..."
                response_message += "\n"
            
            # Add selection status
            if session.selected_tables:
                selected_names = [t.name for t in session.selected_tables]
                response_message += f"\n✅ Currently selected: {', '.join(selected_names)}"
            
            return self._create_response(
                success=True,
                message=response_message,
                data={
                    "tables": [table.to_dict() for table in tables],
                    "database": database,
                    "schema": schema,
                    "selected_tables": [t.name for t in session.selected_tables],
                    "suggestions": [
                        "Select tables for querying",
                        "Describe a specific table",
                        "Start querying the data"
                    ]
                }
            )
        
        except Exception as e:
            logger.error(f"Table exploration failed: {e}")
            return self._create_error_response(f"Failed to explore tables: {str(e)}")
    
    async def _handle_selection(self, request: QueryRequest, session: SessionContext) -> AgentResponse:
        """Handle table/schema selection requests"""
        try:
            user_input = request.natural_language.lower()
            
            # Extract selection targets from input
            if 'table' in user_input:
                return await self._handle_table_selection(request, session)
            elif 'schema' in user_input:
                return await self._handle_schema_selection(request, session)
            else:
                # Try to infer what they want to select
                return await self._handle_smart_selection(request, session)
        
        except Exception as e:
            logger.error(f"Selection handling failed: {e}")
            return self._create_error_response(f"Selection failed: {str(e)}")
    
    async def _handle_table_selection(self, request: QueryRequest, session: SessionContext) -> AgentResponse:
        """Handle table selection"""
        try:
            # Get available tables
            tables = await self.metadata_repo.get_tables(
                session.connection, 
                session.connection.database, 
                session.connection.schema
            )
            
            if not tables:
                return self._create_error_response("No tables available for selection")
            
            # Extract table names from user input
            user_input = request.natural_language.lower()
            selected_tables = []
            
            # Check for "all" keyword
            if 'all' in user_input:
                selected_tables = tables
            else:
                # Look for specific table names
                for table in tables:
                    if table.name.lower() in user_input:
                        selected_tables.append(table)
                
                # If no specific names found, try to parse numbers (1, 2, 3, etc.)
                if not selected_tables:
                    import re
                    numbers = re.findall(r'\b(\d+)\b', user_input)
                    for num_str in numbers:
                        try:
                            index = int(num_str) - 1  # Convert to 0-based index
                            if 0 <= index < len(tables):
                                selected_tables.append(tables[index])
                        except ValueError:
                            continue
            
            if not selected_tables:
                return self._create_response(
                    success=False,
                    message="I couldn't identify which tables to select. Please specify table names or numbers.",
                    data={
                        "available_tables": [t.name for t in tables],
                        "example": "Try: 'select table 1' or 'select CUSTOMERS table'"
                    }
                )
            
            # Update session
            session.selected_tables = selected_tables
            
            response_message = f"✅ Selected {len(selected_tables)} table(s):\n"
            for table in selected_tables:
                response_message += f"• {table.name}\n"
            
            response_message += "\nYou can now start querying this data!"
            
            return self._create_response(
                success=True,
                message=response_message,
                data={
                    "selected_tables": [t.to_dict() for t in selected_tables],
                    "ready_for_queries": True,
                    "suggestions": [
                        "Ask questions about your data",
                        "Get sample data from tables",
                        "Describe table structures"
                    ]
                }
            )
        
        except Exception as e:
            logger.error(f"Table selection failed: {e}")
            return self._create_error_response(f"Table selection failed: {str(e)}")
    
    async def _handle_describe_request(self, request: QueryRequest, session: SessionContext) -> AgentResponse:
        """Handle requests to describe tables or database objects"""
        try:
            user_input = request.natural_language.lower()
            
            # Try to identify what to describe
            tables = await self.metadata_repo.get_tables(
                session.connection,
                session.connection.database, 
                session.connection.schema
            )
            
            table_to_describe = None
            
            # Look for table names in the request
            for table in tables:
                if table.name.lower() in user_input:
                    table_to_describe = table
                    break
            
            if not table_to_describe:
                # If no specific table mentioned, ask for clarification
                table_list = ", ".join([t.name for t in tables[:5]])
                return self._create_response(
                    success=False,
                    message=f"Which table would you like me to describe? Available tables: {table_list}",
                    data={"available_tables": [t.name for t in tables]}
                )
            
            # Get detailed metadata
            detailed_table = await self.metadata_repo.get_table_metadata(
                session.connection,
                table_to_describe.name,
                session.connection.schema,
                session.connection.database
            )
            
            # Format description
            response_message = f"📋 Table: {detailed_table.name}\n"
            response_message += f"Database: {detailed_table.database}\n"
            response_message += f"Schema: {detailed_table.schema}\n"
            
            if detailed_table.row_count is not None:
                response_message += f"Rows: {detailed_table.row_count:,}\n"
            
            if detailed_table.description:
                response_message += f"Description: {detailed_table.description}\n"
            
            response_message += f"\nColumns ({len(detailed_table.columns)}):\n"
            for col in detailed_table.columns:
                col_info = f"• {col.get('name', 'Unknown')}"
                if col.get('type'):
                    col_info += f" ({col['type']})"
                if col.get('nullable') is False:
                    col_info += " NOT NULL"
                if col.get('description'):
                    col_info += f" - {col['description']}"
                response_message += col_info + "\n"
            
            return self._create_response(
                success=True,
                message=response_message,
                data={
                    "table_metadata": detailed_table.to_dict(),
                    "suggestions": [
                        f"Query data from {detailed_table.name}",
                        f"Select {detailed_table.name} for analysis",
                        "Explore other tables"
                    ]
                }
            )
        
        except Exception as e:
            logger.error(f"Describe request failed: {e}")
            return self._create_error_response(f"Description failed: {str(e)}")
    
    async def _handle_contextual_exploration(self, request: QueryRequest, session: SessionContext) -> AgentResponse:
        """Handle general exploration requests based on current context"""
        try:
            # Generate contextual exploration suggestions
            if not session.selected_tables:
                # No tables selected - suggest table exploration
                return await self._handle_table_exploration(request, session)
            else:
                # Tables selected - provide summary and next steps
                response_message = "🔍 Current Exploration Status:\n\n"
                response_message += f"Connected to: {session.connection.database}.{session.connection.schema}\n"
                response_message += f"Selected tables: {', '.join([t.name for t in session.selected_tables])}\n"
                response_message += "\nYou're ready to start querying! What would you like to know about your data?"
                
                return self._create_response(
                    success=True,
                    message=response_message,
                    data={
                        "context": {
                            "database": session.connection.database,
                            "schema": session.connection.schema,
                            "selected_tables": [t.name for t in session.selected_tables]
                        },
                        "suggestions": [
                            "Ask questions about your data",
                            "Get sample data",
                            "Explore table relationships",
                            "Load data dictionary"
                        ]
                    }
                )
        
        except Exception as e:
            logger.error(f"Contextual exploration failed: {e}")
            return self._create_error_response(f"Exploration failed: {str(e)}")
    
    async def _handle_smart_selection(self, request: QueryRequest, session: SessionContext) -> AgentResponse:
        """Intelligently handle selection when type is ambiguous"""
        try:
            # Use LLM to understand what the user wants to select
            context = {
                "user_request": request.natural_language,
                "current_database": session.connection.database,
                "current_schema": session.connection.schema,
                "has_selected_tables": len(session.selected_tables) > 0
            }
            
            prompt = f"""
            User request: {request.natural_language}
            Current context: {context}
            
            The user wants to select something but it's not clear what. 
            Based on the context, what do they most likely want to select?
            Respond with either 'tables', 'schema', or 'database'.
            """
            
            suggestion = await self.llm_service.generate_response(prompt, context)
            
            if 'table' in suggestion.lower():
                return await self._handle_table_selection(request, session)
            elif 'schema' in suggestion.lower():
                return await self._handle_schema_selection(request, session)
            else:
                # Default to table selection as it's most common
                return await self._handle_table_selection(request, session)
        
        except Exception as e:
            logger.error(f"Smart selection failed: {e}")
            return self._create_error_response(f"Selection failed: {str(e)}")
    
    async def _handle_schema_selection(self, request: QueryRequest, session: SessionContext) -> AgentResponse:
        """Handle schema selection (placeholder for future implementation)"""
        return self._create_response(
            success=False,
            message="Schema selection is not yet implemented. You can explore schemas but connection schema switching will be added in a future version.",
            data={"feature": "schema_selection", "status": "not_implemented"}
        )