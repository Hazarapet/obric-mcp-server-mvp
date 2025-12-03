"""Shared MCP server and Neo4j wiring for Obric MCP tools.

All MCP tools and the server entrypoint must import and use this module
so that there is exactly one FastMCP and one Neo4j client/CoreDB/PathDB
per process.
"""

from mcp.server.fastmcp import FastMCP

from .config import Config
from .neo4j import CoreDB, Neo4jClient, PathDB

# Single shared MCP server instance
mcp = FastMCP("obric-mcp-server-mvp")

# Shared Neo4j wiring for all tools
config = Config()
neo4j_client = Neo4jClient(config=config)
coredb = CoreDB(neo4j_client)
pathdb = PathDB(neo4j_client)

# Convenience alias for defining tools bound to this server
tool = mcp.tool

__all__ = ["mcp", "tool", "config", "neo4j_client", "coredb", "pathdb"]

