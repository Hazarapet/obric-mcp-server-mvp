"""MCP tools for entity-level Neo4j operations.

This module exposes `CoreDB` entity methods as MCP tools.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from ..mcp_instance import coredb, tool


@tool()
def find_entity(
    id: Optional[str] = None,
    ticker: Optional[str] = None,
    short_name: Optional[str] = None,
    legal_name: Optional[str] = None,
    limit: int = 250,
) -> Dict[str, Any]:
    """Find entities (such as organizations or companies) using a prioritized lookup strategy.

    Use this tool when:
        - You need to get the company metadata for a given ticker or company name (including the id).

    Priority:
    1. Internal entity id (exact match)
    3. Ticker (case-insensitive exact match)
    3. Short name / legal name (fuzzy CONTAINS search)

    Args:
        id: Internal entity id. Highest priority if provided.
        ticker: Ticker symbol. Used if id is not provided.
        short_name: Short name text (possibly noisy).
        legal_name: Legal name text (possibly noisy).
        limit: Maximum number of records to return (for non-id lookups).

    Returns:
        A JSON-serializable dict with the raw records returned by CoreDB:

            {
              "count": <int>,
              "results": [ { "node": dict(), ... }, ... ]
            }

    Example:
        find_entity(legal_name="nvidia", limit=1)
        {
            "count": 1,
            "results": [
                {
                    "node": {
                        "id": "1234567890",
                        "ticker": "NVDA",
                        "short_name": "NVIDIA",
                        "entity_type": "company",
                        "legal_name": "NVIDIA Corporation",
                    }
                }
            ]
        }

        because we use "like" operation to find similar entities (with case-insensitive matching) 
        which short name or legal name contain "nvidia".
    """
    records = coredb.find_entity(
        id=id,
        ticker=ticker,
        short_name=short_name,
        legal_name=legal_name,
        limit=limit,
    )

    return {
        "count": len(records),
        "results": records,
    }


@tool()
def find_relationship_details(
    id1: Optional[str] = None,
    ticker1: Optional[str] = None,
    short_name1: Optional[str] = None,
    legal_name1: Optional[str] = None,
    id2: Optional[str] = None,
    ticker2: Optional[str] = None,
    short_name2: Optional[str] = None,
    legal_name2: Optional[str] = None,
    limit: int = 250,
) -> Dict[str, Any]:
    """This provides information about organizations and relationships between them.
    Each organization is an Entity and 2 Entities are related by multiple Relationship Details,
    cause they may have different business relationships with each other. 
    Each relationship has a direction (EntityA -> RelationshipDetail -> EntityB) or (EntityA <- RelationshipDetail <- EntityB).
    
    Find all Relationship Details between two entities which have direct relationships.

    Use this tool when you need to understand *how* two entities are
    related (e.g., ownership, supply-chain, partnership), not just that
    they are related.

    Both entities are resolved using the same priority as ``find_entity``:
    1. Internal entity id (exact match)
    2. Ticker (case-insensitive exact match)
    3. Short name / legal name (fuzzy CONTAINS search)

    Args:
        id1, ticker1, short_name1, legal_name1: Identification for the
            first entity.
        id2, ticker2, short_name2, legal_name2: Identification for the
            second entity.
        limit: Maximum number of RelationshipDetail records to return.

    Returns:
        A JSON-serializable dict:

            {
              "count": <int>,
              "results": [
                {
                  "id": ...,
                  "description": ...,
                  "relationship_type": ...,
                  "source_url": ...,
                  "created_at": ...,
                  "relationship_direction": "EntityA -> EntityB"
                },
                ...
              ]
            }

    Example:
        find_relationship_details(ticker1="OKLO", short_name2="goldman sachs")
        {
            "count": 3,
            "results": [
                {
                    created_at": "2025-09-27T09:21:35.340055",
                    "description": "Oklo entered into an Equity Distribution Agreement dated June 2, 2025 with Goldman Sachs & Co. LLC pursuant to which Goldman Sachs is acting as a sales agent to offer and sell Oklo's Class A common stock.",
                    "id": "7d407d1d-7467-4848-a693-8adf6fcf4b35",
                    "relationship_direction": "oklo inc. -> goldman sachs & co. llc",
                    "relationship_type": "sales_agent",
                    "source_url": "[url to the SEC filing]"
                }, ...
            ]
        }
    """
    records = coredb.find_relationship_details(
        id1=id1,
        ticker1=ticker1,
        short_name1=short_name1,
        legal_name1=legal_name1,
        id2=id2,
        ticker2=ticker2,
        short_name2=short_name2,
        legal_name2=legal_name2,
        limit=limit,
    )

    return {
        "count": len(records),
        "results": records,
    }

