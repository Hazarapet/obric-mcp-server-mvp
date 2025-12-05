"""MCP tools for path-oriented Neo4j operations.

These tools expose `PathDB` methods via the shared MCP server instance.
They are focused on tier-based neighborhood queries and path analysis.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from ..mcp_instance import pathdb, tool


@tool()
def find_inbound_tier(
    id: Optional[str] = None,
    ticker: Optional[str] = None,
    short_name: Optional[str] = None,
    legal_name: Optional[str] = None,
    tier: int = 1,
    limit: int = 250,
) -> Dict[str, Any]:
    """Find inbound Tier-N entities for a given starting entity.

    This tool returns all entities that are within a given inbound
    "tier" distance from the starting entity.
    Once the tier is provided, the return entities have distance exactly the given tier from the starting entity.
    The direction here matters. It considers a fixed direction along
    the entire path Entity0 <- Entity1 <- ... <- EntityN
    (no cross-direction is allowed).

    This tools considers inbound directions only.

    Use this tool when:
      - You want to see which inbound entities (suppliers, investors,
        parents, etc.) are connected to a given company within N tiers.

    Args:
        id: Internal entity id (highest priority).
        ticker: Ticker symbol of the starting entity.
        short_name: Short name text of the starting entity.
        legal_name: Legal name text of the starting entity.
        tier: Maximum entity tier distance (1 = direct neighbors).
        limit: Maximum number of entities to return.

    Returns:
        A JSON-serializable dict with the raw records returned by PathDB:

            {
              "count": <int>,
              "results": [
                {
                  "id": <str>,
                  "ticker": <str>,
                  "short_name": <str>,
                  "legal_name": <str>,
                  "entity_type": <str>,
                },
                ...
              ]
            }

      Example:
        find_inray_tier(ticker="OKLO", tier=2, limit=100)
        {
          "count": 22,
          "results": [
            {
              "created_at": "2025-11-27T09:45:49.221936",
              "entity_type": "company",
              "id": "7d746dec-a46d-4d0a-b59e-c27e06cd31b7",
              "legal_name": "nano nuclear energy inc.",
              "short_name": "nano"
              "ticker": "NNE"
            }, ...
          ]
        }
    """
    records: List[Dict[str, Any]] = pathdb.find_inray_tier(
        id=id,
        ticker=ticker,
        short_name=short_name,
        legal_name=legal_name,
        tier=tier,
        limit=limit,
    )

    return {
        "count": len(records),
        "results": records,
    }


@tool()
def find_outbound_tier(
    id: Optional[str] = None,
    ticker: Optional[str] = None,
    short_name: Optional[str] = None,
    legal_name: Optional[str] = None,
    tier: int = 1,
    limit: int = 250,
) -> Dict[str, Any]:
    """Find outbound Tier-N entities for a given starting entity.

    This tool returns all entities that are within a given outbound
    "tier" distance from the starting entity.
    Once the tier is provided, the returned entities have distance
    exactly the given tier from the starting entity.
    The direction here matters. It considers a fixed direction along
    the entire path Entity0 -> Entity1 -> ... -> EntityN
    (no cross-direction is allowed).

    This tool considers outbound directions only.

    Use this tool when:
      - You want to see which outbound entities (customers, portfolio
        companies, downstream partners, etc.) are connected to a given
        company within N tiers.

    Args:
        id: Internal entity id (highest priority).
        ticker: Ticker symbol of the starting entity.
        short_name: Short name text of the starting entity.
        legal_name: Legal name text of the starting entity.
        tier: Maximum entity tier distance (1 = direct neighbors).
        limit: Maximum number of entities to return.

    Returns:
        A JSON-serializable dict with the raw records returned by PathDB:

            {
              "count": <int>,
              "results": [
                {
                  "id": <str>,
                  "ticker": <str>,
                  "short_name": <str>,
                  "legal_name": <str>,
                  "entity_type": <str>,
                },
                ...
              ]
            }

      Example:
        find_outray_tier(ticker="TTAN", tier=2, limit=100)
        {
          "count": 4,
          "results": [
            {
              "created_at": "2025-11-27T10:04:36.544175",
              "entity_type": "company",
              "id": "79482336-47a3-4dea-94e1-26aa64d5036d",
              "legal_name": "nuscale power corporation",
              "short_name": "nuscale power",
              "ticker": "SMR"
            }, ...
          ]
        }
    """
    records: List[Dict[str, Any]] = pathdb.find_outray_tier(
        id=id,
        ticker=ticker,
        short_name=short_name,
        legal_name=legal_name,
        tier=tier,
        limit=limit,
    )

    return {
        "count": len(records),
        "results": records,
    }


@tool()
def has_directed_path(
    id1: Optional[str] = None,
    ticker1: Optional[str] = None,
    short_name1: Optional[str] = None,
    legal_name1: Optional[str] = None,
    id2: Optional[str] = None,
    ticker2: Optional[str] = None,
    short_name2: Optional[str] = None,
    legal_name2: Optional[str] = None,
    direction: str = "outbound",
    max_tier: int = 10,
) -> Dict[str, Any]:
    """Detect whether there is a directed path between two entities.

    Direction is defined relative to the first entity:
      - "outbound": Entity1 -> ... -> Entity2
      - "inbound":  Entity1 <- ... <- Entity2

    The entire path must follow a single direction (like a flow); no cross-direction is allowed.
    Path existance between Entity1 and Entity2 does not guarantee that there is a path between Entity2 and Entity1.

    Args:
        id1, ticker1, short_name1, legal_name1: Identification for the
            first entity.
        id2, ticker2, short_name2, legal_name2: Identification for the
            second entity.
        direction: "outbound" or "inbound".
        max_tier: Maximum entity tier distance to consider for the path.

    Returns:
        A JSON-serializable dict:
            {
              "has_path": <bool>,
              "direction": <"inbound" | "outbound">,
              "tier": <int>,
            }
    """
    has_path = pathdb.has_directed_path(
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
    )

    return {
        "has_path": has_path,
        "direction": direction,
        "tier": max_tier,
    }


@tool()
def find_directed_paths(
    id1: Optional[str] = None,
    ticker1: Optional[str] = None,
    short_name1: Optional[str] = None,
    legal_name1: Optional[str] = None,
    id2: Optional[str] = None,
    ticker2: Optional[str] = None,
    short_name2: Optional[str] = None,
    legal_name2: Optional[str] = None,
    direction: str = "outbound",
    max_tier: int = 10,
) -> Dict[str, Any]:
    """Find all directed paths between two entities.

    Returns a list of paths, where each path is an ordered sequence of
    entities connecting Entity1 to Entity2.
    The result is ordered according to the direction.
    Path existance between Entity1 and Entity2 does not guarantee that there is a path between Entity2 and Entity1.

    Direction is defined relative to the first entity:
      - "outbound": Entity1 -> ... -> Entity2
      - "inbound":  Entity1 <- ... <- Entity2

    The entire path must follow a single direction (like a flow); no cross-direction is allowed.
    Each path contains only Entity objects (no relationship information is included).

    Use this tool when:
      - You want to see all possible connection paths between two entities.
      - You need to understand the different ways entities are connected.

    Args:
        id1, ticker1, short_name1, legal_name1: Identification for the
            first entity.
        id2, ticker2, short_name2, legal_name2: Identification for the
            second entity.
        direction: "outbound" or "inbound".
        max_tier: Maximum entity tier distance to consider for each path.

    Returns:
        A JSON-serializable dict:
            {
              "count": <int>,
              "direction": <"inbound" | "outbound">,
              "tier": <int>,
              "paths": [
                [<Entity1>, <Entity2>, ..., <EntityN>],
                ...
              ]
            }

      Example:
        find_directed_paths(short_name1="iren limited", ticker2="MSFT", direction="outbound", max_tier=2)
        {
          "count": 1,
          "direction": "outbound",
          "tier": 2,
          "paths": [
            [{
              "created_at": "2025-11-27T08:27:05.031176",
              "entity_type": "company",
              "id": "5115c557-e99b-4096-b676-39e50a3e0a72",
              "legal_name": "iren limited",
              "short_name": "iren limited"
            },
            {
              "created_at": "2025-11-27T08:38:19.542916",
              "entity_type": "company",
              "id": "b30f845c-ccaa-4352-8b92-244ce3eee031",
              "legal_name": "ie us hardware 3 inc.",
              "short_name": "ie us hardware 3",
            },
            {
              "created_at": "2025-11-27T08:03:35.341657",
              "entity_type": "Company",
              "id": "cfcdb1c3-5ecc-4488-a15b-0eb4015158a3",
              "legal_name": "microsoft corp",
              "short_name": "microsoft corp",
              "ticker": "MSFT"
            }]
          ]
        }
    """
    paths = pathdb.find_directed_paths(
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
    )

    return {
        "count": len(paths),
        "direction": direction,
        "tier": max_tier,
        "paths": paths,
    }


@tool()
def find_directed_paths_with_relationship_details(
    id1: Optional[str] = None,
    ticker1: Optional[str] = None,
    short_name1: Optional[str] = None,
    legal_name1: Optional[str] = None,
    id2: Optional[str] = None,
    ticker2: Optional[str] = None,
    short_name2: Optional[str] = None,
    legal_name2: Optional[str] = None,
    direction: str = "outbound",
    max_tier: int = 10,
) -> Dict[str, Any]:
    """Find all directed paths between two entities including relationship details.

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
    Path existance between Entity1 and Entity2 does not guarantee that there is a path between Entity2 and Entity1.

    Direction is defined relative to the first entity:
      - "outbound": Entity1 -> ... -> Entity2
      - "inbound":  Entity1 <- ... <- Entity2

    The entire path must follow a single direction (like a flow); no cross-direction is allowed.

    Use this tool when:
      - You want to see all possible connection paths between two entities, including the detailed
        relationship context at each hop.
      - You need to understand both who is connected and how they are connected.

    Args:
        id1, ticker1, short_name1, legal_name1: Identification for the
            first entity.
        id2, ticker2, short_name2, legal_name2: Identification for the
            second entity.
        direction: "outbound" or "inbound".
        max_tier: Maximum entity tier distance to consider for each path.

    Returns:
        A JSON-serializable dict:
            {
              "count": <int>,
              "direction": <"inbound" | "outbound">,
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
        find_directed_paths(short_name1="iren limited", ticker2="MSFT", direction="outbound", max_tier=2)
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
    paths = pathdb.find_directed_paths_with_relationship_details(
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
    )

    return {
        "count": len(paths),
        "direction": direction,
        "tier": max_tier,
        "paths": paths,
    }


