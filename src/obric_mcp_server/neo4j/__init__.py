"""
Neo4j database client, connection management, and low-level queries.

This package should contain ONLY Neo4j-specific logic:
- Connection/client setup
- Raw Cypher query functions

Higher-level domain logic and compositions of these queries
belong in the `tools` package.
"""

from .client import Neo4jClient
from .entity import EntityDB
from .neighbourhood import NeighbourhoodDB
from .path import PathDB
from .relationship_details import RelationshipDetailsDB
from .person import PersonDB

__all__ = [
    "Neo4jClient",
    "EntityDB",
    "NeighbourhoodDB",
    "PathDB",
    "RelationshipDetailsDB",
    "PersonDB",
]
