"""MCP tools for relationship-level Neo4j operations.

This module exposes `RelationshipDetailsDB` methods as MCP tools.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from ..mcp_instance import relationship_detailsdb, tool


@tool()
def find_government_awards(
    id: Optional[str] = None,
    ticker: Optional[str] = None,
    short_name: Optional[str] = None,
    legal_name: Optional[str] = None,
    limit: int = 250
) -> Dict[str, Any]:
    """Find government awards for an entity and its affiliates.

    This tool finds all governmental awards given to the entity and its affiliates.
    for the given entity and all its affiliate entities (subsidiaries, parent companies,    
    ownership relationships, etc.). It returns the awards information along
    with the government agency name that awarded it.

    Use this tool when:
        - You need to find government awards, contracts, or grants given to a company.
        - You want to see awards given to a company's affiliates as well.
        - You need to identify which government agency awarded the contract or grant.

    Priority for entity identification:
    1. Internal entity id (exact match)
    2. Ticker (case-insensitive exact match)
    3. Short name / legal name (fuzzy CONTAINS search)

    Args:
        id: Internal entity id. Highest priority if provided.
        ticker: Ticker symbol. Used if id is not provided.
        short_name: Short name text (possibly noisy).
        legal_name: Legal name text (possibly noisy).
        limit: Maximum number of awards records to return. Default: 250.

    Returns:
        A JSON-serializable dict:

            {
              "count": <int>,
              "results": [
                {
                  "id": <award id>,
                  "description": <award description>,
                  "source_url": <source URL>,
                  "awarded_from": <government agency name>,
                  "affiliate_entity": <affiliate entity name>
                },
                ...
              ]
            }

    Example:
        find_government_awards(ticker="AAPL", limit=50)
        {
            "count": 5,
            "results": [
                {
                    "id": "abc123",
                    "description": "Award for technology development",
                    "source_url": "https://example.com/award",
                    "awarded_from": "Department of Energy",
                    "affiliate_entity": "Apple Inc."
                },
                ...
            ]
        }

        This will find all government awards given to Apple and its affiliate companies.
    """
    records = relationship_detailsdb.find_government_awards(
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

