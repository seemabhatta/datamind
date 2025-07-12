"""
Agent Orchestrator - Coordinates between specialized agents based on user intent.
Implements the Multi-Agent Collaboration pattern.
"""

import logging
from typing import Dict, Any, Optional, List
from dataclasses import asdict
import asyncio

from ..core.models import (
    SessionContext, QueryRequest, AgentIntent, QueryResult,
    DatabaseConnection, TableMetadata
)
from ..services.implementations import (
    MCPConnectionRepository, MCPQueryRepository, MCPMetadataRepository,
    InMemorySessionRepository, MCPIntentClassificationService, MCPLLMService
)
from ..mcp.client import MCPClientManager
from .base import BaseAgent, AgentResponse


logger = logging.getLogger(__name__)


class AgentOrchestrator:
    """
    Main orchestrator that routes requests to specialized agents.
    Follows the Multi-Agent Collaboration pattern from 2024 best practices.
    """
    
    def __init__(self, mcp_manager: MCPClientManager):
        self.mcp_manager = mcp_manager
        
        # Initialize repositories
        self.connection_repo = MCPConnectionRepository(mcp_manager)
        self.query_repo = MCPQueryRepository(mcp_manager)
        self.metadata_repo = MCPMetadataRepository(mcp_manager)
        self.session_repo = InMemorySessionRepository()
        
        # Initialize services
        self.intent_service = MCPIntentClassificationService(mcp_manager)
        self.llm_service = MCPLLMService(mcp_manager)
        
        # Initialize specialized agents
        self.agents: Dict[AgentIntent, BaseAgent] = {}
        self._initialize_agents()
    
    def _initialize_agents(self):
        """Initialize all specialized agents"""
        from .connection_agent import ConnectionAgent
        from .query_agent import QueryAgent
        from .exploration_agent import ExplorationAgent
        from .dictionary_agent import DictionaryAgent
        
        self.agents[AgentIntent.CONNECTION] = ConnectionAgent(
            self.connection_repo, 
            self.metadata_repo, 
            self.llm_service,
            self.mcp_manager
        )
        
        self.agents[AgentIntent.QUERY] = QueryAgent(
            self.query_repo, 
            self.metadata_repo, 
            self.llm_service
        )
        
        self.agents[AgentIntent.EXPLORATION] = ExplorationAgent(
            self.metadata_repo, 
            self.llm_service
        )
        
        self.agents[AgentIntent.DICTIONARY] = DictionaryAgent(
            self.metadata_repo, 
            self.llm_service
        )
    
    async def process_request(self, user_input: str, session_id: Optional[str] = None) -> AgentResponse:
        """
        Main entry point for processing user requests.
        Implements intent classification and agent routing.
        """
        try:
            # Get or create session
            if session_id:
                session = await self.session_repo.get_session(session_id)
                if not session:
                    session = await self.session_repo.create_session()
            else:
                session = await self.session_repo.create_session()
            
            logger.info(f"Processing request in session {session.session_id}: {user_input[:100]}...")
            
            # Create query request
            request = QueryRequest(
                natural_language=user_input,
                context={"session_id": session.session_id}
            )
            
            # Classify intent
            intent = await self.intent_service.classify_intent(user_input, session)
            request.intent = intent
            
            logger.info(f"Classified intent: {intent.value}")
            
            # Route to appropriate agent
            response = await self._route_to_agent(request, session)
            
            # Update session
            session.update_activity()
            await self.session_repo.update_session(session)
            
            return response
            
        except Exception as e:
            logger.error(f"Error processing request: {e}")
            return AgentResponse(
                success=False,
                message=f"I encountered an error while processing your request: {str(e)}",
                data={"error": str(e)}
            )
    
    async def _route_to_agent(self, request: QueryRequest, session: SessionContext) -> AgentResponse:
        """Route request to appropriate specialized agent"""
        intent = request.intent
        
        # Handle unknown intent with a fallback strategy
        if intent == AgentIntent.UNKNOWN:
            intent = await self._infer_intent_from_context(request, session)
        
        # Get the appropriate agent
        agent = self.agents.get(intent)
        if not agent:
            return await self._handle_unknown_intent(request, session)
        
        # Execute agent
        try:
            response = await agent.execute(request, session)
            
            # Apply reflection pattern - validate and improve response if needed
            if response.success and intent == AgentIntent.QUERY:
                response = await self._apply_reflection_pattern(response, request, session)
            
            return response
            
        except Exception as e:
            logger.error(f"Agent execution failed for {intent.value}: {e}")
            return AgentResponse(
                success=False,
                message=f"I encountered an error while handling your {intent.value} request: {str(e)}",
                data={"error": str(e), "intent": intent.value}
            )
    
    async def _infer_intent_from_context(self, request: QueryRequest, session: SessionContext) -> AgentIntent:
        """
        Infer intent from session context when classification is uncertain.
        Implements contextual reasoning pattern.
        """
        user_input = request.natural_language.lower()
        
        # Connection-related keywords
        if any(word in user_input for word in ['connect', 'login', 'snowflake', 'database']):
            return AgentIntent.CONNECTION
        
        # Query-related keywords
        if any(word in user_input for word in ['query', 'select', 'find', 'show', 'get', 'list']):
            if session.is_connected():
                return AgentIntent.QUERY
            else:
                return AgentIntent.CONNECTION  # Need connection first
        
        # Dictionary-related keywords
        if any(word in user_input for word in ['dictionary', 'schema', 'metadata', 'describe', 'yaml']):
            return AgentIntent.DICTIONARY
        
        # Exploration-related keywords
        if any(word in user_input for word in ['explore', 'browse', 'tables', 'schemas', 'databases']):
            return AgentIntent.EXPLORATION
        
        # Default based on session state
        if not session.is_connected():
            return AgentIntent.CONNECTION
        elif len(session.selected_tables) == 0:
            return AgentIntent.EXPLORATION
        else:
            return AgentIntent.QUERY
    
    async def _handle_unknown_intent(self, request: QueryRequest, session: SessionContext) -> AgentResponse:
        """Handle requests with unknown intent using LLM"""
        try:
            context = {
                "session_connected": session.is_connected(),
                "has_tables": len(session.selected_tables) > 0,
                "has_yaml": session.yaml_content is not None
            }
            
            prompt = f"""
            User request: {request.natural_language}
            
            Current context:
            - Connected to database: {context['session_connected']}
            - Has selected tables: {context['has_tables']}
            - Has YAML schema: {context['has_yaml']}
            
            Please provide a helpful response explaining what you can help with.
            """
            
            response_text = await self.llm_service.generate_response(prompt, context)
            
            return AgentResponse(
                success=True,
                message=response_text,
                data={"intent": "unknown", "suggestions": self._get_suggestions(session)}
            )
            
        except Exception as e:
            logger.error(f"Error handling unknown intent: {e}")
            return AgentResponse(
                success=False,
                message="I'm not sure how to help with that. Could you please rephrase your request?",
                data={"error": str(e)}
            )
    
    def _get_suggestions(self, session: SessionContext) -> List[str]:
        """Get contextual suggestions for the user"""
        suggestions = []
        
        if not session.is_connected():
            suggestions.append("Connect to your Snowflake database")
            suggestions.append("Set up database connection")
        else:
            if len(session.selected_tables) == 0:
                suggestions.append("Explore available tables")
                suggestions.append("Load a data dictionary")
            else:
                suggestions.append("Ask questions about your data")
                suggestions.append("Generate queries from natural language")
        
        suggestions.extend([
            "Get help with available commands",
            "Ask about specific database operations"
        ])
        
        return suggestions
    
    async def _apply_reflection_pattern(self, response: AgentResponse, request: QueryRequest, 
                                      session: SessionContext) -> AgentResponse:
        """
        Apply reflection pattern to validate and improve query responses.
        Following 2024 best practices for agentic systems.
        """
        if not response.success or not response.data.get("query_result"):
            return response
        
        try:
            query_result = response.data["query_result"]
            
            # Check if results look reasonable
            if isinstance(query_result, QueryResult):
                # Validate result quality
                if query_result.success and query_result.row_count > 0:
                    # Generate summary if results are good
                    llm_client = self.mcp_manager.get_llm_client()
                    if llm_client:
                        summary_response = await llm_client.summarize_results(
                            request.natural_language,
                            query_result.sql_query,
                            query_result.data[:10]  # First 10 rows for summary
                        )
                        
                        if summary_response.success:
                            response.data["summary"] = summary_response.data.get("summary", "")
                
                elif query_result.success and query_result.row_count == 0:
                    # Suggest improvements for empty results
                    response.message += "\n\nNote: This query returned no results. You might want to check your filters or try a broader search."
                
                elif not query_result.success:
                    # Suggest query improvements for errors
                    response.message += f"\n\nQuery Error: {query_result.error_message}. Let me try to help you fix this."
            
            return response
            
        except Exception as e:
            logger.error(f"Reflection pattern failed: {e}")
            return response  # Return original response if reflection fails
    
    async def get_session_context(self, session_id: str) -> Optional[SessionContext]:
        """Get session context for external access"""
        return await self.session_repo.get_session(session_id)
    
    async def create_new_session(self) -> str:
        """Create a new session and return its ID"""
        session = await self.session_repo.create_session()
        return session.session_id
    
    async def delete_session(self, session_id: str) -> bool:
        """Delete a session"""
        return await self.session_repo.delete_session(session_id)
    
    async def shutdown(self):
        """Shutdown the orchestrator and clean up resources"""
        logger.info("Shutting down Agent Orchestrator...")
        # The MCP client manager will be shut down by the main application