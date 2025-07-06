from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# Import all active routers
from .routers.connection_router import router as connection_router
from .routers.metadata_router import router as metadata_router
from .routers.query_router import router as query_router
from .routers.dictionary_router import router as dictionary_router
from .routers.stage_router import router as stage_router

# Unused router is available but not included by default
# To activate: uncomment the next two lines
# from .routers.unused_router import router as unused_router
# app.include_router(unused_router, tags=["unused"])

# Import connection utilities for health check
from .utils.connection_utils import get_active_connections_count

app = FastAPI(title="Snowflake Cortex Analyst API", description="Modular API for Snowflake-native NL2SQL")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include all active routers
app.include_router(connection_router, tags=["connection"])
app.include_router(metadata_router, tags=["metadata"]) 
app.include_router(query_router, tags=["query"])
app.include_router(dictionary_router, tags=["dictionary"])
app.include_router(stage_router, tags=["stage"])

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "active_connections": get_active_connections_count()}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("snowflake_api_modular:app", host="0.0.0.0", port=8001, reload=True)