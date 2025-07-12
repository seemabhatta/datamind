"""
Base agent class and common response structures.
Provides the foundation for all specialized agents.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, Optional
from datetime import datetime

from ..core.models import QueryRequest, SessionContext


@dataclass
class AgentResponse:
    """Standardized response from agents"""
    success: bool
    message: str
    data: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)
    agent_type: Optional[str] = None
    execution_time_ms: Optional[float] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            "success": self.success,
            "message": self.message,
            "data": self.data,
            "timestamp": self.timestamp.isoformat(),
            "agent_type": self.agent_type,
            "execution_time_ms": self.execution_time_ms
        }


class BaseAgent(ABC):
    """
    Abstract base class for all specialized agents.
    Implements common functionality and enforces interface.
    """
    
    def __init__(self, agent_type: str):
        self.agent_type = agent_type
    
    @abstractmethod
    async def execute(self, request: QueryRequest, session: SessionContext) -> AgentResponse:
        """Execute the agent's main logic"""
        pass
    
    async def can_handle(self, request: QueryRequest, session: SessionContext) -> bool:
        """Check if this agent can handle the request"""
        return True  # Override in subclasses for specific logic
    
    def _create_response(self, success: bool, message: str, 
                        data: Optional[Dict[str, Any]] = None) -> AgentResponse:
        """Helper to create standardized responses"""
        return AgentResponse(
            success=success,
            message=message,
            data=data or {},
            agent_type=self.agent_type
        )
    
    def _create_error_response(self, error_message: str, 
                              error_data: Optional[Dict[str, Any]] = None) -> AgentResponse:
        """Helper to create error responses"""
        return AgentResponse(
            success=False,
            message=f"Error: {error_message}",
            data={"error": error_message, **(error_data or {})},
            agent_type=self.agent_type
        )