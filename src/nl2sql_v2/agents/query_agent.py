"""
Query Agent - Handles natural language to SQL conversion and execution.
Implements advanced NL2SQL patterns with reflection and validation.
"""

import logging
from typing import Dict, Any, List
import json

from ..core.models import QueryRequest, SessionContext, QueryResult, QueryType
from ..services.implementations import MCPQueryRepository, MCPMetadataRepository, MCPLLMService
from .base import BaseAgent, AgentResponse


logger = logging.getLogger(__name__)


class QueryAgent(BaseAgent):
    """Specialized agent for handling natural language queries"""
    
    def __init__(self, query_repo: MCPQueryRepository, 
                 metadata_repo: MCPMetadataRepository, llm_service: MCPLLMService):
        super().__init__("query")
        self.query_repo = query_repo
        self.metadata_repo = metadata_repo
        self.llm_service = llm_service
    
    async def can_handle(self, request: QueryRequest, session: SessionContext) -> bool:
        """Check if this agent can handle the query request"""
        return session.is_connected() and len(session.selected_tables) > 0
    
    async def execute(self, request: QueryRequest, session: SessionContext) -> AgentResponse:
        """Execute natural language query processing"""
        try:
            # Validate prerequisites
            if not session.is_connected():
                return self._create_error_response(
                    "No database connection available. Please connect first.",
                    {"suggestion": "Use 'connect' to establish a database connection"}
                )
            
            if len(session.selected_tables) == 0:
                return self._create_error_response(
                    "No tables selected for querying. Please explore and select tables first.",
                    {"suggestion": "Use 'explore' to see available tables"}
                )
            
            # Determine query type and approach
            query_type = self._classify_query_type(request.natural_language)
            
            if query_type in [QueryType.INSERT, QueryType.UPDATE, QueryType.DELETE]:
                return await self._handle_write_operation(request, session, query_type)
            else:
                return await self._handle_read_operation(request, session)
        
        except Exception as e:
            logger.error(f"Query agent error: {e}")
            return self._create_error_response(str(e))
    
    async def _handle_read_operation(self, request: QueryRequest, session: SessionContext) -> AgentResponse:
        """Handle SELECT and read-only operations"""
        try:
            # Step 1: Generate SQL using LLM with schema context
            logger.info(f"Generating SQL for: {request.natural_language}")
            
            schema_context = self._build_schema_context(session)
            sql_query = await self.query_repo.generate_sql(request.natural_language, session)
            
            if not sql_query:
                return self._create_error_response("Failed to generate SQL query from your request")
            
            logger.info(f"Generated SQL: {sql_query}")
            
            # Step 2: Validate SQL
            if not await self.query_repo.validate_sql(sql_query):
                return await self._handle_invalid_sql(sql_query, request, session)
            
            # Step 3: Execute query
            query_result = await self.query_repo.execute_query(sql_query, session.connection)
            
            # Step 4: Process results
            if query_result.success:
                # Add to session history
                session.query_history.append(query_result)
                
                # Generate response message
                response_message = await self._generate_query_response_message(
                    request.natural_language, query_result, session
                )
                
                return self._create_response(
                    success=True,
                    message=response_message,
                    data={
                        "query_result": {
                            "sql": query_result.sql_query,
                            "data": query_result.data,
                            "columns": query_result.column_names,
                            "row_count": query_result.row_count,
                            "execution_time_ms": query_result.execution_time_ms
                        },
                        "query_type": "read"
                    }
                )
            else:
                # Handle query execution error
                return await self._handle_query_error(query_result, request, session)
        
        except Exception as e:
            logger.error(f"Read operation failed: {e}")
            return self._create_error_response(f"Query execution failed: {str(e)}")
    
    async def _handle_write_operation(self, request: QueryRequest, session: SessionContext, 
                                    query_type: QueryType) -> AgentResponse:
        """Handle INSERT, UPDATE, DELETE operations with extra safety"""
        try:
            # Safety check for write operations
            user_input = request.natural_language.lower()
            if not any(confirm_word in user_input for confirm_word in ['confirm', 'execute', 'run', 'yes']):
                return self._create_response(
                    success=False,
                    message=f"This appears to be a {query_type.value.upper()} operation. "
                           f"Please confirm by adding 'confirm' to your request for safety.",
                    data={
                        "query_type": query_type.value,
                        "requires_confirmation": True,
                        "safety_warning": "Write operations can modify your data"
                    }
                )
            
            # Generate and validate SQL
            sql_query = await self.query_repo.generate_sql(request.natural_language, session)
            
            if not sql_query:
                return self._create_error_response("Failed to generate SQL for write operation")
            
            # Extra validation for write operations
            if not self._validate_write_query(sql_query, query_type):
                return self._create_error_response(
                    "Generated SQL doesn't match the expected operation type",
                    {"generated_sql": sql_query}
                )
            
            # Execute with result
            query_result = await self.query_repo.execute_query(sql_query, session.connection)
            
            if query_result.success:
                session.query_history.append(query_result)
                
                response_message = f"✅ {query_type.value.upper()} operation completed successfully.\n"
                response_message += f"Rows affected: {query_result.row_count}\n"
                response_message += f"Execution time: {query_result.execution_time_ms:.2f}ms"
                
                return self._create_response(
                    success=True,
                    message=response_message,
                    data={
                        "query_result": {
                            "sql": query_result.sql_query,
                            "rows_affected": query_result.row_count,
                            "execution_time_ms": query_result.execution_time_ms
                        },
                        "query_type": "write"
                    }
                )
            else:
                return self._create_error_response(
                    f"Write operation failed: {query_result.error_message}",
                    {"sql": sql_query}
                )
        
        except Exception as e:
            logger.error(f"Write operation failed: {e}")
            return self._create_error_response(f"Write operation failed: {str(e)}")
    
    async def _handle_invalid_sql(self, sql_query: str, request: QueryRequest, 
                                session: SessionContext) -> AgentResponse:
        """Handle invalid SQL with suggestions for improvement"""
        try:
            # Use LLM to suggest fixes
            prompt = f"""
            The following SQL query appears to be invalid:
            {sql_query}
            
            Original request: {request.natural_language}
            
            Please provide suggestions for fixing this query.
            """
            
            suggestion = await self.llm_service.generate_response(prompt, {})
            
            return self._create_response(
                success=False,
                message=f"Generated SQL appears to be invalid.\n\nSQL: {sql_query}\n\nSuggestion: {suggestion}",
                data={
                    "invalid_sql": sql_query,
                    "suggestion": suggestion,
                    "can_retry": True
                }
            )
        
        except Exception as e:
            logger.error(f"Invalid SQL handling failed: {e}")
            return self._create_error_response(
                f"Generated invalid SQL: {sql_query}",
                {"invalid_sql": sql_query}
            )
    
    async def _handle_query_error(self, query_result: QueryResult, request: QueryRequest, 
                                session: SessionContext) -> AgentResponse:
        """Handle query execution errors with intelligent suggestions"""
        try:
            # Analyze the error and provide suggestions
            error_analysis_prompt = f"""
            SQL Query failed with error:
            SQL: {query_result.sql_query}
            Error: {query_result.error_message}
            Original request: {request.natural_language}
            
            Provide a helpful explanation and suggestion for fixing this error.
            """
            
            error_help = await self.llm_service.generate_response(error_analysis_prompt, {})
            
            return self._create_response(
                success=False,
                message=f"Query execution failed.\n\n{error_help}",
                data={
                    "sql": query_result.sql_query,
                    "error": query_result.error_message,
                    "execution_time_ms": query_result.execution_time_ms,
                    "suggestion": error_help
                }
            )
        
        except Exception as e:
            logger.error(f"Query error handling failed: {e}")
            return self._create_error_response(
                f"Query failed: {query_result.error_message}",
                {"sql": query_result.sql_query}
            )
    
    async def _generate_query_response_message(self, original_query: str, 
                                             query_result: QueryResult, session: SessionContext) -> str:
        """Generate a conversational response message for successful queries"""
        try:
            # Use LLM to generate a natural response
            llm_client = session._get_llm_client() if hasattr(session, '_get_llm_client') else None
            
            if query_result.row_count == 0:
                return f"Your query executed successfully but returned no results.\n\nSQL: {query_result.sql_query}"
            
            elif query_result.row_count <= 10:
                # Show all results for small result sets
                response = f"Found {query_result.row_count} result(s):\n\n"
                response += self._format_table_results(query_result)
                response += f"\n\nSQL: {query_result.sql_query}"
                return response
            
            else:
                # Show summary for large result sets
                response = f"Found {query_result.row_count} results. Here are the first 10:\n\n"
                limited_result = QueryResult(
                    sql_query=query_result.sql_query,
                    data=query_result.data[:10],
                    column_names=query_result.column_names,
                    row_count=10
                )
                response += self._format_table_results(limited_result)
                response += f"\n\nShowing 10 of {query_result.row_count} total results."
                response += f"\nSQL: {query_result.sql_query}"
                return response
        
        except Exception as e:
            logger.error(f"Response message generation failed: {e}")
            return f"Query executed successfully. Found {query_result.row_count} results.\n\nSQL: {query_result.sql_query}"
    
    def _format_table_results(self, query_result: QueryResult) -> str:
        """Format query results as a readable table"""
        if not query_result.data or not query_result.column_names:
            return "No data to display"
        
        # Calculate column widths
        col_widths = {}
        for col in query_result.column_names:
            col_widths[col] = len(str(col))
        
        for row in query_result.data:
            for col in query_result.column_names:
                value = str(row.get(col, ''))
                col_widths[col] = max(col_widths[col], len(value))
        
        # Build table
        result = ""
        
        # Header
        header_row = " | ".join(col.ljust(col_widths[col]) for col in query_result.column_names)
        result += header_row + "\n"
        result += "-" * len(header_row) + "\n"
        
        # Data rows
        for row in query_result.data:
            data_row = " | ".join(str(row.get(col, '')).ljust(col_widths[col]) for col in query_result.column_names)
            result += data_row + "\n"
        
        return result
    
    def _classify_query_type(self, natural_language: str) -> QueryType:
        """Classify the type of query from natural language"""
        text = natural_language.lower()
        
        if any(word in text for word in ['insert', 'add', 'create new', 'put in']):
            return QueryType.INSERT
        elif any(word in text for word in ['update', 'modify', 'change', 'edit']):
            return QueryType.UPDATE
        elif any(word in text for word in ['delete', 'remove', 'drop']):
            return QueryType.DELETE
        elif any(word in text for word in ['describe', 'explain', 'show structure']):
            return QueryType.DESCRIBE
        else:
            return QueryType.SELECT
    
    def _validate_write_query(self, sql_query: str, expected_type: QueryType) -> bool:
        """Validate that generated SQL matches expected write operation type"""
        sql_upper = sql_query.upper().strip()
        
        if expected_type == QueryType.INSERT:
            return sql_upper.startswith('INSERT')
        elif expected_type == QueryType.UPDATE:
            return sql_upper.startswith('UPDATE')
        elif expected_type == QueryType.DELETE:
            return sql_upper.startswith('DELETE')
        
        return True
    
    def _build_schema_context(self, session: SessionContext) -> Dict[str, Any]:
        """Build schema context for SQL generation"""
        return {
            "tables": [table.to_dict() for table in session.selected_tables],
            "yaml_content": session.yaml_content,
            "database": session.connection.database if session.connection else None,
            "schema": session.connection.schema if session.connection else None,
            "recent_queries": [q.sql_query for q in session.query_history[-3:]]  # Last 3 queries for context
        }