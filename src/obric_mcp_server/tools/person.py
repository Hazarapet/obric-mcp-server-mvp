"""MCP tools for person-level Neo4j operations.

This module exposes `PersonDB` methods as MCP tools.
"""

from __future__ import annotations

import time
from typing import Any, Dict, Optional

from ..mcp_instance import persondb, tool
from .utils import log_mcp_tool


@tool()
def query_person(
    id: Optional[str] = None,
    name: Optional[str] = None,
    limit: int = 250,
) -> Dict[str, Any]:
    """Query persons by searching across their identifiers.

    This tool searches for persons using either:
    - internal id (exact match), or
    - name (case-insensitive CONTAINS match on the `name` property)

    Use this tool when:
        - You need to look up a person by their internal id.
        - You want to find persons whose name contains a given substring.

    Args:
        id: Internal person id. Highest priority if provided.
        name: Person name text (possibly noisy). Used if id is not provided.
        limit: Maximum number of records to return for name-based queries.

    Returns:
        A JSON-serializable dict:

            {
              "count": <int>,
              "results": [ { dict(), ... }, ... ]
            }
    """
    start_time = time.time()
    log_mcp_tool("query_person", "called", {
        "id": id,
        "name": name,
        "limit": limit,
    })

    records = persondb.query_person(id=id, name=name, limit=limit)

    duration = time.time() - start_time
    log_mcp_tool("query_person", "completed", {
        "id": id,
        "name": name,
        "limit": limit,
        "result_count": len(records),
    }, duration=duration)

    return {
        "count": len(records),
        "results": records,
    }


@tool()
def find_people_by_entity(
    id: Optional[str] = None,
    ticker: Optional[str] = None,
    short_name: Optional[str] = None,
    legal_name: Optional[str] = None,
    limit: int = 250,
) -> Dict[str, Any]:
    """Find people connected to an entity via relationship details.

    This tool resolves a starting `Entity` using the standard entity
    identification strategy (id, ticker, short_name, legal_name), and then
    finds all `Person` items connected to that entity through any
    `RelationshipDetail`:

    Use this tool when:
        - You want to discover people (insiders, officers, related persons)
          associated with a company or other entity.
        - You need to inspect which people are connected to an entity through
          any type of relationship detail, not just insiders.

    Args:
        id: Internal entity id. Highest priority if provided.
        ticker: Entity ticker symbol. Used if id is not provided.
        short_name: Entity short name text (possibly noisy).
        legal_name: Entity legal name text (possibly noisy).
        limit: Maximum number of distinct people to return.

    Returns:
        A JSON-serializable dict:

            {
              "count": <int>,
              "results": [
                {
                  <Person properties>
                },
                ...
              ]
            }
    """
    start_time = time.time()
    log_mcp_tool("find_people_by_entity", "called", {
        "id": id,
        "ticker": ticker,
        "short_name": short_name,
        "legal_name": legal_name,
        "limit": limit,
    })

    records = persondb.find_people_by_entity(
        id=id,
        ticker=ticker,
        short_name=short_name,
        legal_name=legal_name,
        limit=limit,
    )

    duration = time.time() - start_time
    log_mcp_tool("find_people_by_entity", "completed", {
        "id": id,
        "ticker": ticker,
        "short_name": short_name,
        "legal_name": legal_name,
        "limit": limit,
        "result_count": len(records),
    }, duration=duration)

    return {
        "count": len(records),
        "results": records,
    }


