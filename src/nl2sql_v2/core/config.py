"""
Configuration management for the NL2SQL v2 system.
Supports environment variables and configuration files.
"""

import os
import sys
import platform
from dataclasses import dataclass, field
from typing import Dict, Any, Optional, List
from pathlib import Path
import yaml
import json
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


@dataclass
class MCPServerConfig:
    """Configuration for MCP servers"""
    name: str
    command: str
    args: List[str] = field(default_factory=list)
    env: Dict[str, str] = field(default_factory=dict)
    timeout: int = 30
    max_retries: int = 3


@dataclass
class SnowflakeConfig:
    """Snowflake connection configuration"""
    account: str = ""
    user: str = ""
    password: str = ""
    warehouse: str = ""
    database: str = ""
    schema: str = ""
    role: str = ""
    
    @classmethod
    def from_env(cls) -> 'SnowflakeConfig':
        """Create configuration from environment variables"""
        return cls(
            account=os.getenv('SNOWFLAKE_ACCOUNT', ''),
            user=os.getenv('SNOWFLAKE_USER', ''),
            password=os.getenv('SNOWFLAKE_PASSWORD', ''),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE', ''),
            database=os.getenv('SNOWFLAKE_DATABASE', ''),
            schema=os.getenv('SNOWFLAKE_SCHEMA', ''),
            role=os.getenv('SNOWFLAKE_ROLE', '')
        )


@dataclass
class LLMConfig:
    """LLM service configuration"""
    provider: str = "openai"  # openai, anthropic, azure
    model: str = "gpt-4"
    api_key: str = ""
    base_url: Optional[str] = None
    max_tokens: int = 4000
    temperature: float = 0.1
    
    @classmethod
    def from_env(cls) -> 'LLMConfig':
        """Create configuration from environment variables"""
        return cls(
            provider=os.getenv('LLM_PROVIDER', 'openai'),
            model=os.getenv('LLM_MODEL', 'gpt-4'),
            api_key=os.getenv('OPENAI_API_KEY', ''),
            base_url=os.getenv('LLM_BASE_URL'),
            max_tokens=int(os.getenv('LLM_MAX_TOKENS', '4000')),
            temperature=float(os.getenv('LLM_TEMPERATURE', '0.1'))
        )


@dataclass
class ApplicationConfig:
    """Main application configuration"""
    # MCP Server configurations
    snowflake_mcp: MCPServerConfig = field(default_factory=lambda: MCPServerConfig(
        name="snowflake_server",
        command=sys.executable,  # Use the same Python interpreter that's running this code
        args=["-m", "src.nl2sql_v2.mcp.snowflake_server"],  # Use our custom MCP server
        env={
            "SNOWFLAKE_ACCOUNT": os.getenv('SNOWFLAKE_ACCOUNT', ''),
            "SNOWFLAKE_USER": os.getenv('SNOWFLAKE_USER', ''),
            "SNOWFLAKE_PASSWORD": os.getenv('SNOWFLAKE_PASSWORD', ''),
            "SNOWFLAKE_WAREHOUSE": os.getenv('SNOWFLAKE_WAREHOUSE', ''),
            "SNOWFLAKE_DATABASE": os.getenv('SNOWFLAKE_DATABASE', ''),
            "SNOWFLAKE_SCHEMA": os.getenv('SNOWFLAKE_SCHEMA', ''),
            "SNOWFLAKE_ROLE": os.getenv('SNOWFLAKE_ROLE', '')
        },
        timeout=60
    ))
    
    # We'll use OpenAI directly via their API instead of MCP server
    # since OpenAI Agents SDK has built-in MCP support
    llm_direct: bool = True  # Use OpenAI API directly
    
    # For orchestration, we'll handle it in our application layer
    # instead of separate MCP server
    orchestrator_direct: bool = True
    
    # Database and LLM configs
    snowflake: SnowflakeConfig = field(default_factory=SnowflakeConfig.from_env)
    llm: LLMConfig = field(default_factory=LLMConfig.from_env)
    
    # Application settings
    debug: bool = False
    log_level: str = "INFO"
    session_timeout_minutes: int = 60
    max_query_results: int = 1000
    enable_query_caching: bool = True
    
    # File paths
    prompts_dir: Path = field(default_factory=lambda: Path("utils/system-prompts"))
    cache_dir: Path = field(default_factory=lambda: Path(".cache"))
    logs_dir: Path = field(default_factory=lambda: Path("logs"))
    
    def __post_init__(self):
        """Ensure directories exist"""
        self.cache_dir.mkdir(exist_ok=True)
        self.logs_dir.mkdir(exist_ok=True)
        
        # Load from environment
        self.debug = os.getenv('DEBUG', 'false').lower() == 'true'
        self.log_level = os.getenv('LOG_LEVEL', 'INFO')
        self.session_timeout_minutes = int(os.getenv('SESSION_TIMEOUT_MINUTES', '60'))
        self.max_query_results = int(os.getenv('MAX_QUERY_RESULTS', '1000'))
        
    @classmethod
    def load_from_file(cls, config_path: Path) -> 'ApplicationConfig':
        """Load configuration from YAML or JSON file"""
        if not config_path.exists():
            return cls()
            
        with open(config_path, 'r') as f:
            if config_path.suffix.lower() == '.yaml' or config_path.suffix.lower() == '.yml':
                data = yaml.safe_load(f)
            else:
                data = json.load(f)
        
        return cls(**data)
    
    def save_to_file(self, config_path: Path):
        """Save configuration to file"""
        config_data = {
            'snowflake_mcp': {
                'name': self.snowflake_mcp.name,
                'command': self.snowflake_mcp.command,
                'args': self.snowflake_mcp.args,
                'timeout': self.snowflake_mcp.timeout
            },
            'debug': self.debug,
            'log_level': self.log_level,
            'session_timeout_minutes': self.session_timeout_minutes,
            'max_query_results': self.max_query_results,
            'enable_query_caching': self.enable_query_caching
        }
        
        with open(config_path, 'w') as f:
            if config_path.suffix.lower() == '.yaml' or config_path.suffix.lower() == '.yml':
                yaml.dump(config_data, f, default_flow_style=False)
            else:
                json.dump(config_data, f, indent=2)


# Global configuration instance
config = ApplicationConfig()


def get_config() -> ApplicationConfig:
    """Get the global configuration instance"""
    return config


def reload_config(config_path: Optional[Path] = None):
    """Reload configuration from file or environment"""
    global config
    if config_path:
        config = ApplicationConfig.load_from_file(config_path)
    else:
        config = ApplicationConfig()