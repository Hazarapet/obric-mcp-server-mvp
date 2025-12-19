"""MCP tools for neighbourhood-oriented Neo4j operations.

These tools expose `NeighbourhoodDB` methods via the shared MCP server instance.
They are focused on finding connected entities within tier ranges.
"""

from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

from ..mcp_instance import neighbourhooddb, tool
from .utils import log_mcp_tool


@tool()
def find_related_entities(
    id: Optional[str] = None,
    ticker: Optional[str] = None,
    short_name: Optional[str] = None,
    legal_name: Optional[str] = None,
    min_tier: int = 1,
    max_tier: int = 1,
    direction: Optional[str] = None,
    limit: int = 250,
) -> Dict[str, Any]:
    """Find related entities of a given entity within a tier range.

    This tool finds all entities related to the starting entity within
    the specified tier range. For example, min_tier=1, max_tier=2 means
    Tier 1 and Tier 2 entities.

    Use this tool when:
      - You want to see all entities related to a given entity within
        a range of tiers (e.g., Tier 1 and Tier 2 neighbours).
      - You need to explore the neighbourhood of an entity.

    Args:
        id: Internal entity id (highest priority).
        ticker: Ticker symbol of the starting entity.
        short_name: Short name text of the starting entity.
        legal_name: Legal name text of the starting entity.
        min_tier: Minimum tier to include (1 = Tier 1, 2 = Tier 2, etc.).
        max_tier: Maximum tier to include.
        direction: Connection direction - "inbound", "outbound", or None for both.
        limit: Maximum number of entities to return.

    Returns:
        A JSON-serializable dict:

            {
              "count": <int>,
              "min_tier": <int>,
              "max_tier": <int>,
              "direction": <str | None>,
              "results": [
                {
                  "id": <str>,
                  "ticker": <str>,
                  "short_name": <str>,
                  "legal_name": <str>,
                  "entity_type": <str>,
                  "tier": <int>,
                },
                ...
              ]
            }

    Example:
        find_related_entities(ticker="OKLO", min_tier=1, max_tier=2, direction="outbound", limit=50)
        {
          "count": 15,
          "min_tier": 0,
          "max_tier": 2,
          "direction": "outbound",
          "results": [
            {
              "id": "7d746dec-a46d-4d0a-b59e-c27e06cd31b7",
              "ticker": "NNE",
              "short_name": "nano",
              "legal_name": "nano nuclear energy inc.",
              "entity_type": "company",
              "tier": 1,
            }, ...
          ]
        }
    """
    start_time = time.time()
    log_mcp_tool("find_related_entities", "called", {
        "id": id,
        "ticker": ticker,
        "short_name": short_name,
        "legal_name": legal_name,
        "min_tier": min_tier,
        "max_tier": max_tier,
        "direction": direction,
        "limit": limit,
    })

    records: List[Dict[str, Any]] = neighbourhooddb.find_connected_entities(
        id=id,
        ticker=ticker,
        short_name=short_name,
        legal_name=legal_name,
        min_tier=min_tier,
        max_tier=max_tier,
        direction=direction,
        limit=limit,
    )

    duration = time.time() - start_time
    log_mcp_tool("find_related_entities", "completed", {
        "id": id,
        "ticker": ticker,
        "short_name": short_name,
        "legal_name": legal_name,
        "min_tier": min_tier,
        "max_tier": max_tier,
        "direction": direction,
        "limit": limit,
        "result_count": len(records),
    }, duration=duration)

    return {
        "count": len(records),
        "min_tier": min_tier,
        "max_tier": max_tier,
        "direction": direction,
        "results": records,
    }

