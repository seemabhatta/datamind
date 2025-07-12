"""
Core domain models for the NL2SQL system.
Following Domain-Driven Design patterns with clean interfaces.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Union
from datetime import datetime
import uuid


class ConnectionStatus(Enum):
    """Connection status enumeration"""
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    ERROR = "error"


class QueryType(Enum):
    """Types of queries the system can handle"""
    SELECT = "select"
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"
    DDL = "ddl"
    DESCRIBE = "describe"


class AgentIntent(Enum):
    """Types of intents the system can classify"""
    CONNECTION = "connection"
    QUERY = "query"
    EXPLORATION = "exploration"
    DICTIONARY = "dictionary"
    HELP = "help"
    UNKNOWN = "unknown"


@dataclass
class DatabaseConnection:
    """Represents a database connection configuration"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    account: str = ""
    user: str = ""
    password: str = ""
    warehouse: str = ""
    database: str = ""
    schema: str = ""
    role: str = ""
    status: ConnectionStatus = ConnectionStatus.DISCONNECTED
    created_at: datetime = field(default_factory=datetime.now)
    last_used: Optional[datetime] = None
    error_message: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            "id": self.id,
            "account": self.account,
            "user": self.user,
            "warehouse": self.warehouse,
            "database": self.database,
            "schema": self.schema,
            "role": self.role,
            "status": self.status.value,
            "created_at": self.created_at.isoformat(),
            "last_used": self.last_used.isoformat() if self.last_used else None,
            "error_message": self.error_message
        }


@dataclass
class TableMetadata:
    """Represents table metadata information"""
    name: str
    schema: str
    database: str
    columns: List[Dict[str, Any]] = field(default_factory=list)
    row_count: Optional[int] = None
    description: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "schema": self.schema,
            "database": self.database,
            "columns": self.columns,
            "row_count": self.row_count,
            "description": self.description,
            "tags": self.tags
        }


@dataclass
class QueryRequest:
    """Represents a natural language query request"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    natural_language: str = ""
    intent: AgentIntent = AgentIntent.UNKNOWN
    context: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.now)
    user_id: Optional[str] = None


@dataclass
class QueryResult:
    """Represents the result of a query execution"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    request_id: str = ""
    sql_query: str = ""
    data: List[Dict[str, Any]] = field(default_factory=list)
    column_names: List[str] = field(default_factory=list)
    row_count: int = 0
    execution_time_ms: float = 0.0
    success: bool = False
    error_message: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.now)


@dataclass
class SessionContext:
    """Maintains session state across interactions"""
    session_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    connection: Optional[DatabaseConnection] = None
    selected_tables: List[TableMetadata] = field(default_factory=list)
    query_history: List[QueryResult] = field(default_factory=list)
    yaml_content: Optional[Dict[str, Any]] = None
    user_preferences: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.now)
    last_activity: datetime = field(default_factory=datetime.now)

    def update_activity(self):
        """Update last activity timestamp"""
        self.last_activity = datetime.now()

    def is_connected(self) -> bool:
        """Check if there's an active database connection"""
        return (self.connection is not None and 
                self.connection.status == ConnectionStatus.CONNECTED)


@dataclass
class MCPRequest:
    """Generic MCP request wrapper"""
    method: str
    params: Dict[str, Any] = field(default_factory=dict)
    request_id: str = field(default_factory=lambda: str(uuid.uuid4()))


@dataclass
class MCPResponse:
    """Generic MCP response wrapper"""
    request_id: str
    success: bool
    data: Any = None
    error_message: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


# Abstract interfaces following Repository pattern

class ConnectionRepository(ABC):
    """Abstract interface for connection management"""
    
    @abstractmethod
    async def create_connection(self, config: DatabaseConnection) -> DatabaseConnection:
        pass
    
    @abstractmethod
    async def get_connection(self, connection_id: str) -> Optional[DatabaseConnection]:
        pass
    
    @abstractmethod
    async def test_connection(self, config: DatabaseConnection) -> bool:
        pass
    
    @abstractmethod
    async def close_connection(self, connection_id: str) -> bool:
        pass


class QueryRepository(ABC):
    """Abstract interface for query operations"""
    
    @abstractmethod
    async def execute_query(self, sql: str, connection: DatabaseConnection) -> QueryResult:
        pass
    
    @abstractmethod
    async def generate_sql(self, natural_language: str, context: SessionContext) -> str:
        pass
    
    @abstractmethod
    async def validate_sql(self, sql: str) -> bool:
        pass


class MetadataRepository(ABC):
    """Abstract interface for metadata operations"""
    
    @abstractmethod
    async def get_databases(self, connection: DatabaseConnection) -> List[str]:
        pass
    
    @abstractmethod
    async def get_schemas(self, connection: DatabaseConnection, database: str) -> List[str]:
        pass
    
    @abstractmethod
    async def get_tables(self, connection: DatabaseConnection, database: str, schema: str) -> List[TableMetadata]:
        pass
    
    @abstractmethod
    async def get_table_metadata(self, connection: DatabaseConnection, table_name: str, 
                                schema: str, database: str) -> TableMetadata:
        pass


class SessionRepository(ABC):
    """Abstract interface for session management"""
    
    @abstractmethod
    async def create_session(self) -> SessionContext:
        pass
    
    @abstractmethod
    async def get_session(self, session_id: str) -> Optional[SessionContext]:
        pass
    
    @abstractmethod
    async def update_session(self, session: SessionContext) -> bool:
        pass
    
    @abstractmethod
    async def delete_session(self, session_id: str) -> bool:
        pass


# Service interfaces

class IntentClassificationService(ABC):
    """Abstract interface for intent classification"""
    
    @abstractmethod
    async def classify_intent(self, text: str, context: SessionContext) -> AgentIntent:
        pass


class LLMService(ABC):
    """Abstract interface for LLM operations"""
    
    @abstractmethod
    async def generate_response(self, prompt: str, context: Dict[str, Any]) -> str:
        pass
    
    @abstractmethod
    async def generate_sql(self, natural_language: str, schema_context: Dict[str, Any]) -> str:
        pass