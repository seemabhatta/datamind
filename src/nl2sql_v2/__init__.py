"""
NL2SQL v2 - Modern Multi-Agent Natural Language to SQL System

A complete rewrite implementing 2024/2025 best practices:
- Multi-Agent Collaboration Pattern  
- Existing MCP Server Integration (Snowflake)
- Direct OpenAI API Integration
- Domain-Driven Design with Repository Pattern
- Reflection and Planning Patterns
- Enterprise-grade NL2SQL Architecture
"""

__version__ = "2.0.0"
__author__ = "NL2SQL Development Team"
__description__ = "Modern Multi-Agent Natural Language to SQL System"

# Core exports
from .core.models import *
from .core.config import get_config, reload_config
from .mcp.client import MCPClientManager, SnowflakeMCPClient, LLMDirectClient
from .agents.orchestrator import AgentOrchestrator

__all__ = [
    "get_config",
    "reload_config", 
    "MCPClientManager",
    "SnowflakeMCPClient", 
    "LLMDirectClient",
    "AgentOrchestrator"
]