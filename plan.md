# Plan: Connect UI to CLI Agents

## Current State Analysis

### Existing Components
- **CLI Agents**: Both `agentic_generate_yaml_cli.py` and `agentic_query_cli.py` are fully functional
- **UI Structure**: React-based UI with TypeScript, has existing agent service framework
- **Current Gap**: The UI's `agent-service.ts` attempts to spawn Python processes but needs path and integration fixes

### Key Files Identified
- `src/cli/agentic_generate_yaml_cli.py` - YAML dictionary generation agent
- `src/cli/agentic_query_cli.py` - Natural language query processing agent  
- `ui/server/services/agent-service.ts` - Existing service that needs updates
- `low-level-design.md` - Contains detailed architecture specifications

## Implementation Plan

### Phase 1: Fix Agent Service Integration
**Target**: Update existing `agent-service.ts` to properly connect to CLI agents

1. **Fix Path References**
   - Update hardcoded path in `agent-service.ts` line 93 (currently points to `../datamind/src/cli/`)
   - Change to correct path: `../../src/cli/`

2. **Update Process Execution**
   - Fix `runPythonAgent()` method to use correct script paths
   - Ensure proper argument passing to CLI agents
   - Handle session ID management properly

### Phase 2: Enhance Agent Communication
**Target**: Improve input/output handling between UI and CLI agents

3. **Input/Output Parsing**
   - Update `parseAgentOutput()` to handle CLI agent response formats
   - Ensure proper JSON extraction from agent responses
   - Handle both text and structured responses

4. **Session Management**
   - Verify session IDs are properly passed to CLI processes
   - Maintain conversation context across interactions
   - Handle agent state persistence

### Phase 3: Environment and Configuration
**Target**: Ensure proper environment setup

5. **Environment Variables**
   - Verify all required environment variables are accessible
   - Ensure Snowflake credentials are properly passed
   - Configure OpenAI API key access

6. **Error Handling**
   - Improve error messages and logging
   - Handle CLI agent failures gracefully
   - Provide meaningful feedback to UI

## Implementation Approach

### Minimal Changes Strategy
- **No new FastAPI wrapper needed** - UI already has agent service infrastructure
- **Update existing code** rather than creating new components
- **Preserve existing UI functionality** while fixing backend integration

### Key Integration Points
1. **Generate Agent**: UI routes "generate" keywords to `agentic_generate_yaml_cli.py`
2. **Query Agent**: UI routes "@query" keywords to `agentic_query_cli.py`  
3. **WebSocket Support**: Maintain real-time communication through existing channels

## Expected Outcome
After implementation:
- UI chat interface will successfully route messages to appropriate CLI agents
- Users can generate YAML dictionaries through the UI
- Users can perform natural language queries through the UI
- Session context is maintained across interactions
- All existing CLI functionality accessible through web interface