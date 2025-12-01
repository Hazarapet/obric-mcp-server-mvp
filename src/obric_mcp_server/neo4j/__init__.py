"""
Neo4j database client, connection management, and low-level queries.

This package should contain ONLY Neo4j-specific logic:
- Connection/client setup
- Raw Cypher query functions

Higher-level domain logic and compositions of these queries
belong in the `tools` package.
"""

from .client import Neo4jClient
from .core import CoreDB

__all__ = [
    "Neo4jClient",
    "CoreDB",
]
