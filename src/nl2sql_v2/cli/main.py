"""
Modern NL2SQL CLI v2 - Built with MCP and Multi-Agent Architecture
A complete rewrite implementing 2024/2025 best practices for agentic AI systems.
"""

import asyncio
import logging
import sys
import signal
from pathlib import Path
from typing import Optional
import click
from datetime import datetime

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from src.nl2sql_v2.core.config import get_config, reload_config
from src.nl2sql_v2.mcp.client import MCPClientManager
from src.nl2sql_v2.agents.orchestrator import AgentOrchestrator


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('nl2sql_v2.log')
    ]
)
logger = logging.getLogger(__name__)


class NL2SQLCLIv2:
    """
    Modern NL2SQL CLI with MCP integration and multi-agent architecture.
    Implements the latest patterns for conversational AI systems.
    """
    
    def __init__(self):
        self.config = get_config()
        self.mcp_manager: Optional[MCPClientManager] = None
        self.orchestrator: Optional[AgentOrchestrator] = None
        self.current_session_id: Optional[str] = None
        self.running = False
        
    async def initialize(self):
        """Initialize the CLI system"""
        try:
            logger.info("Initializing NL2SQL CLI v2...")
            
            # Initialize MCP client manager
            self.mcp_manager = MCPClientManager()
            await self.mcp_manager.initialize()
            
            # Initialize agent orchestrator
            self.orchestrator = AgentOrchestrator(self.mcp_manager)
            
            # Create initial session
            self.current_session_id = await self.orchestrator.create_new_session()
            
            logger.info("NL2SQL CLI v2 initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Initialization failed: {e}")
            click.echo(f"❌ Failed to initialize: {e}")
            return False
    
    async def shutdown(self):
        """Gracefully shutdown the CLI system"""
        logger.info("Shutting down NL2SQL CLI v2...")
        
        if self.orchestrator:
            await self.orchestrator.shutdown()
        
        if self.mcp_manager:
            await self.mcp_manager.shutdown()
        
        logger.info("Shutdown complete")
    
    async def process_input(self, user_input: str) -> str:
        """Process user input through the orchestrator"""
        try:
            if not self.orchestrator:
                return "❌ System not initialized. Please restart the application."
            
            # Process through orchestrator
            response = await self.orchestrator.process_request(
                user_input, 
                self.current_session_id
            )
            
            return response.message
            
        except Exception as e:
            logger.error(f"Input processing failed: {e}")
            return f"❌ Error processing your request: {str(e)}"
    
    async def run_interactive(self):
        """Run interactive CLI session"""
        self.running = True
        
        # Display welcome message
        self._display_welcome()
        
        # Auto-initialize connection if possible
        await self._auto_initialize()
        
        # Main interaction loop
        while self.running:
            try:
                # Get user input
                user_input = await self._get_user_input()
                
                if not user_input:
                    continue
                
                # Handle special commands
                if user_input.lower() in ['quit', 'exit', 'q', 'bye']:
                    click.echo("👋 Thanks for using NL2SQL CLI v2!")
                    break
                
                elif user_input.lower() in ['help', '?']:
                    self._display_help()
                    continue
                
                elif user_input.lower().startswith('config'):
                    await self._handle_config_command(user_input)
                    continue
                
                elif user_input.lower() == 'status':
                    await self._display_status()
                    continue
                
                elif user_input.lower() == 'clear':
                    click.clear()
                    continue
                
                # Process through orchestrator
                click.echo("🤖 Processing your request...")
                response = await self.process_input(user_input)
                click.echo(f"\n{response}\n")
                
            except KeyboardInterrupt:
                click.echo("\n\n👋 Thanks for using NL2SQL CLI v2!")
                break
            except Exception as e:
                logger.error(f"Interactive loop error: {e}")
                click.echo(f"❌ An error occurred: {e}")
                
                if not click.confirm("Continue session?", default=True):
                    break
        
        self.running = False
    
    def _display_welcome(self):
        """Display welcome message"""
        click.echo("=" * 70)
        click.echo("🚀 NL2SQL CLI v2 - Modern Multi-Agent Architecture")
        click.echo("=" * 70)
        click.echo("💡 Ask questions about your data in natural language")
        click.echo("🔍 Explore databases, generate queries, create dictionaries")
        click.echo("🤖 Powered by MCP and specialized AI agents")
        click.echo("=" * 70)
        click.echo()
        click.echo("📚 Commands:")
        click.echo("  • Type naturally: 'connect to snowflake', 'show me tables', 'find customers'")
        click.echo("  • Special: 'help', 'status', 'quit', 'clear'")
        click.echo("  • Config: 'config reload', 'config show'")
        click.echo()
    
    def _display_help(self):
        """Display help information"""
        click.echo("\n📖 NL2SQL CLI v2 Help")
        click.echo("=" * 50)
        click.echo()
        click.echo("🔧 System Commands:")
        click.echo("  help, ?          - Show this help")
        click.echo("  status           - Show system status")
        click.echo("  config show      - Show current configuration")
        click.echo("  config reload    - Reload configuration")
        click.echo("  clear            - Clear screen")
        click.echo("  quit, exit, q    - Exit application")
        click.echo()
        click.echo("💬 Natural Language Examples:")
        click.echo("  Connection:")
        click.echo("    • 'connect to snowflake'")
        click.echo("    • 'show connection status'")
        click.echo("    • 'disconnect'")
        click.echo()
        click.echo("  Exploration:")
        click.echo("    • 'show me the databases'")
        click.echo("    • 'list tables in current schema'")
        click.echo("    • 'describe the customers table'")
        click.echo("    • 'select tables 1, 2, and 3'")
        click.echo()
        click.echo("  Querying:")
        click.echo("    • 'how many customers do we have?'")
        click.echo("    • 'show me top 10 sales by amount'")
        click.echo("    • 'find orders from last month'")
        click.echo()
        click.echo("  Dictionary:")
        click.echo("    • 'generate data dictionary'")
        click.echo("    • 'load dictionary from file.yaml'")
        click.echo("    • 'save dictionary as schema.yaml'")
        click.echo()
    
    async def _auto_initialize(self):
        """Attempt auto-initialization if credentials are available"""
        try:
            if self.config.snowflake.account and self.config.snowflake.user:
                click.echo("🔄 Auto-connecting with configured credentials...")
                response = await self.process_input("connect to snowflake")
                if "successfully connected" in response.lower():
                    click.echo("✅ Auto-connection successful!")
                else:
                    click.echo("ℹ️  Auto-connection failed. You can connect manually.")
            else:
                click.echo("ℹ️  No credentials configured. Use 'connect' command or set environment variables.")
        except Exception as e:
            logger.warning(f"Auto-initialization failed: {e}")
    
    async def _get_user_input(self) -> str:
        """Get user input with proper async handling"""
        try:
            # In a real async environment, you'd use aioconsole or similar
            # For now, using synchronous input
            return click.prompt("\n🔍 You", type=str, prompt_suffix=" ").strip()
        except (EOFError, KeyboardInterrupt):
            raise KeyboardInterrupt()
    
    async def _handle_config_command(self, command: str):
        """Handle configuration commands"""
        parts = command.lower().split()
        
        if len(parts) < 2:
            click.echo("❌ Usage: config [show|reload]")
            return
        
        action = parts[1]
        
        if action == 'show':
            click.echo("\n⚙️  Current Configuration:")
            click.echo(f"Debug Mode: {self.config.debug}")
            click.echo(f"Log Level: {self.config.log_level}")
            click.echo(f"Session Timeout: {self.config.session_timeout_minutes} minutes")
            click.echo(f"Max Query Results: {self.config.max_query_results}")
            click.echo(f"Snowflake Account: {self.config.snowflake.account or 'Not configured'}")
            click.echo(f"Snowflake User: {self.config.snowflake.user or 'Not configured'}")
        
        elif action == 'reload':
            try:
                reload_config()
                click.echo("✅ Configuration reloaded")
            except Exception as e:
                click.echo(f"❌ Failed to reload config: {e}")
        
        else:
            click.echo("❌ Unknown config action. Use 'show' or 'reload'")
    
    async def _display_status(self):
        """Display system status"""
        click.echo("\n📊 System Status")
        click.echo("=" * 40)
        
        # MCP Client status
        if self.mcp_manager:
            click.echo("🔌 MCP Clients:")
            for name, client in self.mcp_manager.clients.items():
                status = "✅ Connected" if client.is_connected else "❌ Disconnected"
                click.echo(f"  {name}: {status}")
        else:
            click.echo("🔌 MCP Clients: ❌ Not initialized")
        
        # Session status
        if self.current_session_id and self.orchestrator:
            session = await self.orchestrator.get_session_context(self.current_session_id)
            if session:
                click.echo(f"\n🎯 Current Session: {self.current_session_id[:8]}...")
                click.echo(f"Database Connected: {'✅ Yes' if session.is_connected() else '❌ No'}")
                if session.connection:
                    click.echo(f"Database: {session.connection.database}")
                    click.echo(f"Schema: {session.connection.schema}")
                click.echo(f"Selected Tables: {len(session.selected_tables)}")
                click.echo(f"Query History: {len(session.query_history)}")
                click.echo(f"Has Dictionary: {'✅ Yes' if session.yaml_content else '❌ No'}")
        
        click.echo(f"\n⏰ Uptime: Started at {datetime.now().strftime('%H:%M:%S')}")


# CLI Interface
@click.group()
@click.option('--config-file', type=click.Path(exists=True), help='Configuration file path')
@click.option('--debug', is_flag=True, help='Enable debug mode')
def cli(config_file, debug):
    """NL2SQL CLI v2 - Modern Multi-Agent Natural Language to SQL System"""
    if config_file:
        reload_config(Path(config_file))
    
    if debug:
        logging.getLogger().setLevel(logging.DEBUG)


@cli.command()
@click.option('--query', '-q', help='Single query to execute')
@click.option('--session-timeout', type=int, help='Session timeout in minutes')
def chat(query, session_timeout):
    """Start interactive chat session"""
    async def run_chat():
        app = NL2SQLCLIv2()
        
        # Setup signal handlers for graceful shutdown
        def signal_handler(signum, frame):
            app.running = False
            asyncio.create_task(app.shutdown())
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        try:
            # Initialize
            if not await app.initialize():
                return 1
            
            # Handle single query
            if query:
                click.echo(f"🔍 Query: {query}")
                response = await app.process_input(query)
                click.echo(f"🤖 Response: {response}")
                return 0
            
            # Run interactive session
            await app.run_interactive()
            return 0
            
        except Exception as e:
            logger.error(f"Chat session failed: {e}")
            click.echo(f"❌ Fatal error: {e}")
            return 1
        finally:
            await app.shutdown()
    
    # Run the async application
    try:
        return asyncio.run(run_chat())
    except KeyboardInterrupt:
        click.echo("\n👋 Goodbye!")
        return 0


@cli.command()
def version():
    """Show version information"""
    click.echo("NL2SQL CLI v2.0.0")
    click.echo("Modern Multi-Agent Architecture with MCP Integration")
    click.echo("Built with 2024/2025 AI Agent Best Practices")


@cli.command()
def health():
    """Check system health"""
    async def run_health_check():
        app = NL2SQLCLIv2()
        try:
            if await app.initialize():
                click.echo("✅ System healthy")
                await app.shutdown()
                return 0
            else:
                click.echo("❌ System unhealthy")
                return 1
        except Exception as e:
            click.echo(f"❌ Health check failed: {e}")
            return 1
    
    return asyncio.run(run_health_check())


if __name__ == '__main__':
    cli()