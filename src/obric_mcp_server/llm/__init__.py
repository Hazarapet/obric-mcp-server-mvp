"""LLM and embedding utilities for the MCP server.

This module provides integration with langchain for generating embeddings
and querying entities by relationship embeddings.
"""

from .embeddings import EmbeddingClient

__all__ = [
    "EmbeddingClient",
]

