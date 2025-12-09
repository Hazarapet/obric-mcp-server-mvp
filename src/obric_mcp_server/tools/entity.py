"""MCP tools for entity-level Neo4j operations.

This module exposes `EntityDB` entity methods as MCP tools.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from ..mcp_instance import embedding_client, entitydb, tool


@tool()
def find_entity(
    id: Optional[str] = None,
    ticker: Optional[str] = None,
    short_name: Optional[str] = None,
    legal_name: Optional[str] = None
) -> Dict[str, Any]:
    """Find an entity using a prioritized lookup strategy.

    Use this tool when:
        - You need to get the entity metadata for a given ticker or entity name (including the id).
        - You need to find an entity by its id, ticker, short name or legal name.

    Priority:
    1. Internal entity id (exact match)
    2. Ticker (case-insensitive exact match)
    3. Short name / legal name (fuzzy CONTAINS search)

    Use short name or legal name if finding by ticker gives no results.

    Args:
        id: Internal entity id. Highest priority if provided.
        ticker: Ticker symbol. Used if id is not provided.
        short_name: Short name text (possibly noisy).
        legal_name: Legal name text (possibly noisy).

    Returns:
        A JSON-serializable dict:

            {
              "count": <int>,
              "results": [ { dict(), ... }, ... ]
            }

    Example:
        find_entity(legal_name="nvidia", limit=1)
        {
            "count": 1,
            "results": [
                {
                    "id": "1234567890",
                    "ticker": "NVDA",
                    "short_name": "NVIDIA",
                    "entity_type": "company",
                    "legal_name": "NVIDIA Corporation",
                }
            ]
        }

        because we use "like" operation to find similar entities (with case-insensitive matching) 
        which short name or legal name contain "nvidia".
    """
    records = entitydb.find_entity(
        id=id,
        ticker=ticker,
        short_name=short_name,
        legal_name=legal_name
    )

    return {
        "count": len(records),
        "results": records,
    }


@tool()
def query_entities(
    query: str,
    limit: int = 250,
) -> Dict[str, Any]:
    """Query entities by searching across multiple fields.

    This tool searches for entities where any field contains the query string.
    It searches across:
    - ticker
    - entity_type
    - short_name
    - legal_name

    Use this tool when:
        - You want to find entities by searching across all their metadata fields.
        - You have a partial search term and want to find matching entities.

    Args:
        query: Search string to match against entity fields (case-insensitive).
        limit: Maximum number of records to return.

    Returns:
        A JSON-serializable dict:

            {
              "count": <int>,
              "results": [ { dict(), ... }, ... ]
            }

    Example:
        query_entities(query="energy", limit=10)
        {
            "count": 5,
            "results": [
                {
                    "id": "1234567890",
                    "ticker": "LEU",
                    "short_name": "Centrus",
                    "entity_type": "company",
                    "legal_name": "Centrus Energy Corporation",
                }, ...
            ]
        }

        This will find entities where "energy" appears in any of the searchable fields.
    """
    records = entitydb.query_entity(
        query=query,
        limit=limit,
    )

    return {
        "count": len(records),
        "results": records,
    }


@tool()
def find_entities_by_business_activity(
    query: str,
    direction: Optional[str] = None,
    threshold: float = 0.6,
    limit: int = 250
) -> Dict[str, Any]:
    """Find entities involved in business activities matching a semantic query.

    This tool generates an embedding from the query text and finds
    RelationshipDetails whose descriptions or types are semantically similar
    (using cosine similarity). It then returns the entities connected to
    those relationships.

    Use this tool when:
    - You want entities involved in a specific business activity.
    - You need semantic search (not keyword match).
    - You want to filter by relationship direction ("inbound", "outbound").

    Args:
        query: Text describing the business activity (e.g. "commission payments").
        direction: "inbound", "outbound", or None for both.
        threshold: Min similarity score (0-1). Default: 0.7.
        limit: Max number of records to return.

    Example query:
    "commission_payment" or "Centrus pays UBS a 1.5% commission on gross sales under the agreement."

    Returns:
        A JSON-serializable dict:

            {
              "count": <int>,
              "results": [
                {
                  <Entity node properties>
                },
                ...
              ]
            }

    Example:
        find_entities_by_business_activity(query="Centrus pays UBS a 1.5% commission on gross sales under the agreement.", direction="outbound", threshold=0.8)
        {
            "count": 40,
            "results": [
                {
                "created_at": "2025-12-03T13:23:43.753100",
                "entity_type": "company",
                "id": "abf45d5a-4584-48aa-b9d6-8b3264839e38",
                "legal_name": "evercore group l.l.c.",
                "short_name": "evercore group l.l.c."
                }, ...
            ]
        }
    """
    # Generate embedding from query text
    embedding = embedding_client.embed_text(query)

    # Find entities using embedding similarity
    records = entitydb.find_entity_by_relationship_embedding(
        embedding=embedding,
        threshold=threshold,
        direction=direction,
        limit=limit,
    )

    return {
        "count": len(records),
        "results": records,
    }

