"""MCP tools for path-oriented Neo4j operations.

These tools expose `PathDB` methods via the shared MCP server instance.
They are focused on tier-based neighborhood queries and path analysis.
"""

from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

from ..mcp_instance import pathdb, tool
from .utils import log_mcp_tool


@tool()
def find_paths_between_entities(
    id1: Optional[str] = None,
    ticker1: Optional[str] = None,
    short_name1: Optional[str] = None,
    legal_name1: Optional[str] = None,
    id2: Optional[str] = None,
    ticker2: Optional[str] = None,
    short_name2: Optional[str] = None,
    legal_name2: Optional[str] = None,
    direction: Optional[str] = "outbound",
    max_tier: int = 10,
    max_paths: int = 100,
) -> Dict[str, Any]:
    """Find all paths between two entities including relationship details.

    Returns a list of paths, where each path is an ordered sequence of
    segments connecting Entity1 to Entity2. Each segment describes one hop:

        {
          "from": <Entity object>,
          "relationship_detail": {
            "id": <str>,
            "description": <str>,
            "relationship_type": <str>,
            "source_url": <str>,
            "created_at": <str>,
          },
          "to": <Entity object>,
        }

    The result is ordered according to the direction.
    A directed path existance between Entity1 and Entity2 does not guarantee 
    that there is a directed path between Entity2 and Entity1.

    Direction behavior:
      - "outbound": Entity1 -> ... -> Entity2 (directed path)
      - "inbound":  Entity2 <- ... <- Entity1 (directed path)
      - None: direction doesn't matter (bidirectional paths allowed)

    Use this tool when:
      - You want to see all possible connection paths between two entities, including the detailed
        relationship context at each hop.
      - You need to understand both who is connected and how they are connected.

    Args:
        id1, ticker1, short_name1, legal_name1: Identification for the
            first entity.
        id2, ticker2, short_name2, legal_name2: Identification for the
            second entity.
        direction: "outbound", "inbound", or None for bidirectional.
        max_tier: Maximum entity tier distance to consider for each path.
        max_paths: Maximum number of paths to return.

    Returns:
        A JSON-serializable dict:
            {
              "count": <int>,
              "direction": <"inbound" | "outbound" | None>,
              "tier": <int>,
              "paths": [
                [
                  {
                    "from": <Entity>,
                    "relationship_detail": {
                      "id": <str>,
                      "description": <str>,
                      "relationship_type": <str>,
                      "source_url": <str>,
                      "created_at": <str>,
                    },
                    "to": <Entity>,
                  },
                  ...
                ],
                ...
              ]
            }

      Example:
        find_paths_between_entities(short_name1="iren limited", ticker2="MSFT", direction="outbound", max_tier=2, max_paths=10)
        {
          "count": 1,
          "direction": "outbound",
          "tier": 2,
          "paths": [
            [
              {
                "from": {
                  "created_at": "2025-11-27T08:27:05.031176",
                  "entity_type": "company",
                  "id": "5115c557-e99b-4096-b676-39e50a3e0a72",
                  "legal_name": "iren limited",
                  "short_name": "iren limited"
                },
                "relationship_detail": {
                  "created_at": "2025-11-27T08:38:20.132098",
                  "description": "IREN Limited is the parent company and IE US Hardware 3 Inc. is a wholly owned subsidiary of IREN Limited.",
                  "id": "0641f26c-0c09-462c-9750-ba6deb1002ec",
                  "relationship_type": "ownership",
                  "source_url": "[SEC filing url]"
                },
                "to": {
                  "created_at": "2025-11-27T08:38:19.542916",
                  "entity_type": "company",
                  "id": "b30f845c-ccaa-4352-8b92-244ce3eee031",
                  "legal_name": "ie us hardware 3 inc.",
                  "short_name": "ie us hardware 3"
                }
              }, ...
            ]
          ]
        }
    """
    start_time = time.time()
    log_mcp_tool("find_paths_between_entities", "called", {
        "id1": id1,
        "ticker1": ticker1,
        "short_name1": short_name1,
        "legal_name1": legal_name1,
        "id2": id2,
        "ticker2": ticker2,
        "short_name2": short_name2,
        "legal_name2": legal_name2,
        "direction": direction,
        "max_tier": max_tier,
        "max_paths": max_paths,
    })

    paths = pathdb.find_paths_between_entities(
        id1=id1,
        ticker1=ticker1,
        short_name1=short_name1,
        legal_name1=legal_name1,
        id2=id2,
        ticker2=ticker2,
        short_name2=short_name2,
        legal_name2=legal_name2,
        direction=direction,
        max_tier=max_tier,
        max_paths=max_paths,
    )

    duration = time.time() - start_time
    log_mcp_tool("find_paths_between_entities", "completed", {
        "id1": id1,
        "ticker1": ticker1,
        "short_name1": short_name1,
        "legal_name1": legal_name1,
        "id2": id2,
        "ticker2": ticker2,
        "short_name2": short_name2,
        "legal_name2": legal_name2,
        "direction": direction,
        "max_tier": max_tier,
        "max_paths": max_paths,
        "result_count": len(paths),
    }, duration=duration)

    return {
        "count": len(paths),
        "direction": direction,
        "tier": max_tier,
        "paths": paths,
    }


