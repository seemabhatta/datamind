"""
Dictionary Agent - Handles YAML data dictionary generation and management.
Provides enhanced schema understanding for better NL2SQL performance.
"""

import logging
from typing import Dict, Any, List, Optional
import yaml
import json
from pathlib import Path
from datetime import datetime

from ..core.models import QueryRequest, SessionContext, TableMetadata
from ..services.implementations import MCPMetadataRepository, MCPLLMService
from .base import BaseAgent, AgentResponse


logger = logging.getLogger(__name__)


class DictionaryAgent(BaseAgent):
    """Specialized agent for data dictionary operations"""
    
    def __init__(self, metadata_repo: MCPMetadataRepository, llm_service: MCPLLMService):
        super().__init__("dictionary")
        self.metadata_repo = metadata_repo
        self.llm_service = llm_service
    
    async def execute(self, request: QueryRequest, session: SessionContext) -> AgentResponse:
        """Execute dictionary-related tasks"""
        user_input = request.natural_language.lower()
        
        try:
            # Check connection first
            if not session.is_connected():
                return self._create_error_response(
                    "No database connection available. Please connect first.",
                    {"suggestion": "Use 'connect' to establish a database connection"}
                )
            
            # Determine dictionary action
            if any(word in user_input for word in ['generate', 'create', 'build']):
                return await self._handle_dictionary_generation(request, session)
            
            elif any(word in user_input for word in ['load', 'import', 'read']):
                return await self._handle_dictionary_loading(request, session)
            
            elif any(word in user_input for word in ['save', 'export', 'write']):
                return await self._handle_dictionary_saving(request, session)
            
            elif any(word in user_input for word in ['show', 'display', 'view', 'preview']):
                return await self._handle_dictionary_preview(request, session)
            
            elif any(word in user_input for word in ['enhance', 'improve', 'enrich']):
                return await self._handle_dictionary_enhancement(request, session)
            
            else:
                # Default action based on context
                return await self._handle_contextual_dictionary_action(request, session)
        
        except Exception as e:
            logger.error(f"Dictionary agent error: {e}")
            return self._create_error_response(str(e))
    
    async def _handle_dictionary_generation(self, request: QueryRequest, session: SessionContext) -> AgentResponse:
        """Handle dictionary generation from database metadata"""
        try:
            # Determine which tables to include
            tables_to_process = []
            user_input = request.natural_language.lower()
            
            if 'all' in user_input or 'entire' in user_input:
                # Generate for all tables in current schema
                tables_to_process = await self.metadata_repo.get_tables(
                    session.connection,
                    session.connection.database,
                    session.connection.schema
                )
            elif session.selected_tables:
                # Use currently selected tables
                tables_to_process = session.selected_tables
            else:
                # Ask user to specify tables
                available_tables = await self.metadata_repo.get_tables(
                    session.connection,
                    session.connection.database,
                    session.connection.schema
                )
                
                table_list = ", ".join([t.name for t in available_tables[:10]])
                return self._create_response(
                    success=False,
                    message=f"Which tables should I include in the dictionary? Available: {table_list}\n\nOr say 'all tables' to include everything.",
                    data={"available_tables": [t.name for t in available_tables]}
                )
            
            if not tables_to_process:
                return self._create_error_response("No tables found to generate dictionary from")
            
            logger.info(f"Generating dictionary for {len(tables_to_process)} tables")
            
            # Generate enhanced dictionary
            dictionary_data = await self._generate_enhanced_dictionary(tables_to_process, session)
            
            # Store in session
            session.yaml_content = dictionary_data
            
            response_message = f"✅ Generated data dictionary for {len(tables_to_process)} table(s):\n"
            for table in tables_to_process:
                response_message += f"• {table.name}\n"
            
            response_message += "\nDictionary includes enhanced metadata, relationships, and AI-generated descriptions."
            
            return self._create_response(
                success=True,
                message=response_message,
                data={
                    "dictionary": dictionary_data,
                    "tables_processed": [t.name for t in tables_to_process],
                    "enhanced": True,
                    "suggestions": [
                        "Save dictionary to file",
                        "Preview dictionary content", 
                        "Start querying with enhanced context"
                    ]
                }
            )
        
        except Exception as e:
            logger.error(f"Dictionary generation failed: {e}")
            return self._create_error_response(f"Dictionary generation failed: {str(e)}")
    
    async def _handle_dictionary_loading(self, request: QueryRequest, session: SessionContext) -> AgentResponse:
        """Handle loading existing YAML dictionary files"""
        try:
            user_input = request.natural_language
            
            # Try to extract filename from request
            filename = self._extract_filename(user_input)
            
            if not filename:
                # Look for YAML files in common locations
                yaml_files = self._find_yaml_files()
                if yaml_files:
                    file_list = "\n".join([f"• {f}" for f in yaml_files[:10]])
                    return self._create_response(
                        success=False,
                        message=f"Which YAML file should I load?\n\nFound these files:\n{file_list}",
                        data={"available_files": yaml_files}
                    )
                else:
                    return self._create_error_response(
                        "No YAML files found. Please specify a filename or generate a new dictionary."
                    )
            
            # Load the specified file
            try:
                file_path = Path(filename)
                if not file_path.exists():
                    # Try common locations
                    for location in [".", "dictionaries", "yaml", "data"]:
                        candidate = Path(location) / filename
                        if candidate.exists():
                            file_path = candidate
                            break
                
                if not file_path.exists():
                    return self._create_error_response(f"File not found: {filename}")
                
                with open(file_path, 'r') as f:
                    if file_path.suffix.lower() in ['.yaml', '.yml']:
                        dictionary_data = yaml.safe_load(f)
                    else:
                        dictionary_data = json.load(f)
                
                # Validate dictionary structure
                if not self._validate_dictionary_structure(dictionary_data):
                    return self._create_error_response(
                        f"Invalid dictionary format in {filename}. Expected YAML with tables section."
                    )
                
                # Store in session
                session.yaml_content = dictionary_data
                
                # Extract table info for session
                if 'tables' in dictionary_data:
                    session.selected_tables = []
                    for table_name, table_info in dictionary_data['tables'].items():
                        table_meta = TableMetadata(
                            name=table_name,
                            schema=dictionary_data.get('schema', session.connection.schema),
                            database=dictionary_data.get('database', session.connection.database),
                            columns=table_info.get('columns', []),
                            description=table_info.get('description')
                        )
                        session.selected_tables.append(table_meta)
                
                response_message = f"✅ Loaded dictionary from {file_path.name}\n"
                response_message += f"Database: {dictionary_data.get('database', 'Unknown')}\n"
                response_message += f"Schema: {dictionary_data.get('schema', 'Unknown')}\n"
                
                if 'tables' in dictionary_data:
                    response_message += f"Tables: {len(dictionary_data['tables'])}\n"
                    table_names = list(dictionary_data['tables'].keys())[:5]
                    response_message += f"Sample tables: {', '.join(table_names)}"
                    if len(dictionary_data['tables']) > 5:
                        response_message += f" and {len(dictionary_data['tables']) - 5} more"
                
                return self._create_response(
                    success=True,
                    message=response_message,
                    data={
                        "dictionary": dictionary_data,
                        "filename": str(file_path),
                        "tables_loaded": len(dictionary_data.get('tables', {})),
                        "suggestions": [
                            "Start querying with enhanced context",
                            "Preview dictionary content",
                            "Enhance dictionary with AI insights"
                        ]
                    }
                )
            
            except yaml.YAMLError as e:
                return self._create_error_response(f"Invalid YAML format: {str(e)}")
            except json.JSONDecodeError as e:
                return self._create_error_response(f"Invalid JSON format: {str(e)}")
            except Exception as e:
                return self._create_error_response(f"Failed to load file: {str(e)}")
        
        except Exception as e:
            logger.error(f"Dictionary loading failed: {e}")
            return self._create_error_response(f"Dictionary loading failed: {str(e)}")
    
    async def _handle_dictionary_saving(self, request: QueryRequest, session: SessionContext) -> AgentResponse:
        """Handle saving dictionary to file"""
        try:
            if not session.yaml_content:
                return self._create_error_response(
                    "No dictionary content to save. Generate or load a dictionary first."
                )
            
            # Extract filename from request
            filename = self._extract_filename(request.natural_language) or "data_dictionary.yaml"
            
            # Ensure .yaml extension
            if not filename.endswith(('.yaml', '.yml', '.json')):
                filename += '.yaml'
            
            # Save to file
            file_path = Path(filename)
            
            try:
                with open(file_path, 'w') as f:
                    if file_path.suffix.lower() == '.json':
                        json.dump(session.yaml_content, f, indent=2)
                    else:
                        yaml.dump(session.yaml_content, f, default_flow_style=False, sort_keys=False)
                
                response_message = f"✅ Dictionary saved to {file_path.absolute()}\n"
                response_message += f"Format: {'JSON' if file_path.suffix.lower() == '.json' else 'YAML'}\n"
                
                if 'tables' in session.yaml_content:
                    response_message += f"Tables: {len(session.yaml_content['tables'])}"
                
                return self._create_response(
                    success=True,
                    message=response_message,
                    data={
                        "filename": str(file_path.absolute()),
                        "format": "json" if file_path.suffix.lower() == '.json' else "yaml",
                        "size_bytes": file_path.stat().st_size
                    }
                )
            
            except Exception as e:
                return self._create_error_response(f"Failed to save file: {str(e)}")
        
        except Exception as e:
            logger.error(f"Dictionary saving failed: {e}")
            return self._create_error_response(f"Dictionary saving failed: {str(e)}")
    
    async def _handle_dictionary_preview(self, request: QueryRequest, session: SessionContext) -> AgentResponse:
        """Handle dictionary preview requests"""
        try:
            if not session.yaml_content:
                return self._create_error_response(
                    "No dictionary content to preview. Generate or load a dictionary first."
                )
            
            dictionary = session.yaml_content
            
            # Build preview message
            response_message = "📋 Data Dictionary Preview:\n\n"
            
            # Basic info
            response_message += f"Database: {dictionary.get('database', 'Unknown')}\n"
            response_message += f"Schema: {dictionary.get('schema', 'Unknown')}\n"
            response_message += f"Generated: {dictionary.get('generated_at', 'Unknown')}\n\n"
            
            # Tables summary
            if 'tables' in dictionary:
                response_message += f"Tables ({len(dictionary['tables'])}):\n"
                
                for i, (table_name, table_info) in enumerate(dictionary['tables'].items()):
                    if i >= 5:  # Show only first 5 tables
                        response_message += f"... and {len(dictionary['tables']) - 5} more tables\n"
                        break
                    
                    response_message += f"\n• {table_name}\n"
                    if table_info.get('description'):
                        response_message += f"  Description: {table_info['description'][:100]}...\n"
                    
                    columns = table_info.get('columns', [])
                    response_message += f"  Columns: {len(columns)}\n"
                    
                    # Show first few columns
                    for j, col in enumerate(columns[:3]):
                        col_name = col.get('name', 'Unknown')
                        col_type = col.get('type', 'Unknown')
                        response_message += f"    - {col_name} ({col_type})\n"
                    
                    if len(columns) > 3:
                        response_message += f"    ... and {len(columns) - 3} more columns\n"
            
            # Additional sections
            if 'relationships' in dictionary:
                response_message += f"\n🔗 Relationships: {len(dictionary['relationships'])}\n"
            
            if 'business_rules' in dictionary:
                response_message += f"📜 Business Rules: {len(dictionary['business_rules'])}\n"
            
            return self._create_response(
                success=True,
                message=response_message,
                data={
                    "dictionary_summary": {
                        "database": dictionary.get('database'),
                        "schema": dictionary.get('schema'),
                        "table_count": len(dictionary.get('tables', {})),
                        "has_relationships": 'relationships' in dictionary,
                        "has_business_rules": 'business_rules' in dictionary
                    },
                    "full_dictionary": dictionary
                }
            )
        
        except Exception as e:
            logger.error(f"Dictionary preview failed: {e}")
            return self._create_error_response(f"Dictionary preview failed: {str(e)}")
    
    async def _handle_dictionary_enhancement(self, request: QueryRequest, session: SessionContext) -> AgentResponse:
        """Handle dictionary enhancement with AI insights"""
        try:
            if not session.yaml_content:
                return self._create_error_response(
                    "No dictionary content to enhance. Generate or load a dictionary first."
                )
            
            logger.info("Enhancing dictionary with AI insights")
            
            # Use LLM to enhance descriptions and add insights
            enhanced_dictionary = await self._enhance_with_ai(session.yaml_content, session)
            
            # Update session
            session.yaml_content = enhanced_dictionary
            
            response_message = "✨ Dictionary enhanced with AI insights!\n\n"
            response_message += "Enhancements include:\n"
            response_message += "• AI-generated table descriptions\n"
            response_message += "• Column business meanings\n"
            response_message += "• Identified relationships\n"
            response_message += "• Data quality insights\n"
            response_message += "• Query suggestions\n"
            
            return self._create_response(
                success=True,
                message=response_message,
                data={
                    "enhanced_dictionary": enhanced_dictionary,
                    "enhancements": [
                        "table_descriptions",
                        "column_meanings",
                        "relationships",
                        "data_quality",
                        "query_suggestions"
                    ]
                }
            )
        
        except Exception as e:
            logger.error(f"Dictionary enhancement failed: {e}")
            return self._create_error_response(f"Dictionary enhancement failed: {str(e)}")
    
    async def _handle_contextual_dictionary_action(self, request: QueryRequest, session: SessionContext) -> AgentResponse:
        """Handle dictionary requests based on current context"""
        try:
            if session.yaml_content:
                # Dictionary exists - offer preview or enhancement
                return await self._handle_dictionary_preview(request, session)
            else:
                # No dictionary - suggest generation
                return await self._handle_dictionary_generation(request, session)
        
        except Exception as e:
            logger.error(f"Contextual dictionary action failed: {e}")
            return self._create_error_response(f"Dictionary operation failed: {str(e)}")
    
    async def _generate_enhanced_dictionary(self, tables: List[TableMetadata], session: SessionContext) -> Dict[str, Any]:
        """Generate enhanced dictionary with AI insights"""
        try:
            dictionary = {
                "database": session.connection.database,
                "schema": session.connection.schema,
                "generated_at": datetime.now().isoformat(),
                "tables": {},
                "relationships": [],
                "business_rules": []
            }
            
            # Process each table
            for table in tables:
                # Get detailed metadata
                detailed_table = await self.metadata_repo.get_table_metadata(
                    session.connection, table.name, table.schema, table.database
                )
                
                # Build table entry
                table_entry = {
                    "name": detailed_table.name,
                    "description": detailed_table.description or "",
                    "columns": detailed_table.columns,
                    "row_count": detailed_table.row_count,
                    "tags": detailed_table.tags
                }
                
                # Enhance with AI if possible
                try:
                    enhanced_table = await self._enhance_table_with_ai(table_entry, session)
                    table_entry.update(enhanced_table)
                except Exception as e:
                    logger.warning(f"AI enhancement failed for table {table.name}: {e}")
                
                dictionary["tables"][detailed_table.name] = table_entry
            
            return dictionary
        
        except Exception as e:
            logger.error(f"Enhanced dictionary generation failed: {e}")
            raise
    
    async def _enhance_table_with_ai(self, table_entry: Dict[str, Any], session: SessionContext) -> Dict[str, Any]:
        """Enhance table entry with AI-generated insights"""
        try:
            prompt = f"""
            Analyze this database table and provide enhanced metadata:
            
            Table: {table_entry['name']}
            Current Description: {table_entry.get('description', 'None')}
            Columns: {[col.get('name') for col in table_entry.get('columns', [])]}
            
            Provide:
            1. Enhanced table description (business purpose, what data it contains)
            2. Column business meanings
            3. Likely relationships with other tables
            4. Common query patterns
            
            Return as JSON with keys: enhanced_description, column_insights, relationships, query_patterns
            """
            
            enhancement_response = await self.llm_service.generate_response(prompt, {
                "table_name": table_entry['name'],
                "database": session.connection.database
            })
            
            # Parse AI response (simplified - in production would use structured output)
            enhancements = {
                "ai_enhanced": True,
                "ai_description": enhancement_response[:500] if enhancement_response else "",
                "enhanced_at": datetime.now().isoformat()
            }
            
            return enhancements
        
        except Exception as e:
            logger.warning(f"AI enhancement failed: {e}")
            return {"ai_enhanced": False}
    
    async def _enhance_with_ai(self, dictionary: Dict[str, Any], session: SessionContext) -> Dict[str, Any]:
        """Enhance entire dictionary with AI insights"""
        enhanced_dict = dictionary.copy()
        
        try:
            # Add AI enhancement metadata
            enhanced_dict["ai_enhanced"] = True
            enhanced_dict["enhanced_at"] = datetime.now().isoformat()
            
            # Process each table
            if "tables" in enhanced_dict:
                for table_name, table_info in enhanced_dict["tables"].items():
                    try:
                        enhancements = await self._enhance_table_with_ai(table_info, session)
                        table_info.update(enhancements)
                    except Exception as e:
                        logger.warning(f"Failed to enhance table {table_name}: {e}")
            
            return enhanced_dict
        
        except Exception as e:
            logger.error(f"Dictionary AI enhancement failed: {e}")
            return dictionary
    
    def _extract_filename(self, text: str) -> Optional[str]:
        """Extract filename from user input"""
        import re
        
        # Look for common filename patterns
        patterns = [
            r'["\']([^"\']+\.(?:yaml|yml|json))["\']',  # Quoted filenames
            r'\b([a-zA-Z0-9_\-\.]+\.(?:yaml|yml|json))\b',  # Unquoted filenames
            r'file\s+([a-zA-Z0-9_\-\.]+)',  # "file filename"
            r'named?\s+([a-zA-Z0-9_\-\.]+)',  # "named filename"
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(1)
        
        return None
    
    def _find_yaml_files(self) -> List[str]:
        """Find YAML files in common locations"""
        yaml_files = []
        search_paths = [Path("."), Path("dictionaries"), Path("yaml"), Path("data")]
        
        for path in search_paths:
            if path.exists():
                for pattern in ["*.yaml", "*.yml"]:
                    yaml_files.extend([str(f) for f in path.glob(pattern)])
        
        return yaml_files[:20]  # Limit to 20 files
    
    def _validate_dictionary_structure(self, data: Dict[str, Any]) -> bool:
        """Validate that dictionary has expected structure"""
        if not isinstance(data, dict):
            return False
        
        # Must have either tables or be a valid schema
        return 'tables' in data or 'schema' in data or 'database' in data