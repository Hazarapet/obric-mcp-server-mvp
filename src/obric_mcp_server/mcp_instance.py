"""Shared MCP server and Neo4j wiring for Obric MCP tools.

All MCP tools and the server entrypoint must import and use this module
so that there is exactly one FastMCP and one Neo4j client/EntityDB/PathDB
per process.
"""

from mcp.server.fastmcp import FastMCP

from .config import Config
from .llm import EmbeddingClient
from .neo4j import EntityDB, NeighbourhoodDB, Neo4jClient, PathDB

# Single shared MCP server instance
mcp = FastMCP(
    "obric-mcp-server-mvp",
    host='0.0.0.0',
    streamable_http_path="/",
    port=8000
    # description='''An MCP server to deal with complex relationship data between different type of entities.
    # Entities can be companies, commettes, regulators, NGOs etc. Each 2 Entities may have multiple relationships between them.
    # Each Relationship contains details about the specific connections between the 2 Entities.
    # Those connections can be ownership, supply-chain, partnership, regulation, investment, services, etc.

    # Entities contain metadata about the entity like name, ticker, short name, legal name, entity type, etc.
    # Relationship Details contain metadata about the relationship like description, relationship type, source url, created at, etc.

    # The direction of relationship matters. Entity1 -> Entity2 is different from Entity2 -> Entity1.
    # This info is often provided in the tools results.

    # The path between 2 Entities is an ordered sequence of Entities (and may include Relationship Details).
    # Each level of depth in the path is called a tier.
    # Tier-N entities are the entities that are N steps away from the starting entity 
    # in the given direction: either inbound or outbound.''',
    )

# Shared Neo4j wiring for all tools
config = Config()
neo4j_client = Neo4jClient(config=config)
entitydb = EntityDB(neo4j_client)
neighbourhooddb = NeighbourhoodDB(neo4j_client)
pathdb = PathDB(neo4j_client)
embedding_client = EmbeddingClient(config=config)

# Convenience alias for defining tools bound to this server
tool = mcp.tool

__all__ = ["mcp", "tool", "config", "neo4j_client", "entitydb", "neighbourhooddb", "pathdb", "embedding_client"]

