"""MCP tools for relationship-level Neo4j operations.

This module exposes `RelationshipDetailsDB` methods as MCP tools.
"""

from __future__ import annotations

import time
from typing import Any, Dict, Optional

from ..mcp_instance import relationship_detailsdb, tool
from .utils import log_mcp_tool


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
    start_time = time.time()
    log_mcp_tool("find_government_awards", "called", {
        "id": id,
        "ticker": ticker,
        "short_name": short_name,
        "legal_name": legal_name,
        "limit": limit,
    })

    records = relationship_detailsdb.find_government_awards(
        id=id,
        ticker=ticker,
        short_name=short_name,
        legal_name=legal_name,
        limit=limit,
    )

    duration = time.time() - start_time
    log_mcp_tool("find_government_awards", "completed", {
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


@tool()
def find_recent_insider_activities(
    id: Optional[str] = None,
    ticker: Optional[str] = None,
    short_name: Optional[str] = None,
    legal_name: Optional[str] = None,
    start_date: Optional[str] = None,
    limit: int = 250,
) -> Dict[str, Any]:
    """Find recent insider activities for an entity.

    This tool finds all recent insider activities for a given entity.

    Use this tool when:
        - You need to inspect insider transactions or activities for a
          specific company or entity.
        - You want to filter insider activities by a starting date.

    Priority for entity identification:
        1. Internal entity id (exact match)
        2. Ticker (case-insensitive exact match)
        3. Short name / legal name (fuzzy CONTAINS search)

    Args:
        id: Internal entity id. Highest priority if provided.
        ticker: Ticker symbol. Used if id is not provided.
        short_name: Short name text (possibly noisy).
        legal_name: Legal name text (possibly noisy).
        start_date: Optional lower bound (exclusive) for `event_date` (with "YYYY-MM-DD" format),
            e.g. "2024-01-01". If None, all insider activities are returned.
        limit: Maximum number of insider activities to return. Default: 250.

    Returns:
        A JSON-serializable dict:

            {
              "count": <int>,
              "results": [
                {
                  "id": <insider relationship id>,
                  "description": <insider description>,
                  "event_type": <insider event type>,
                  "event_date": <insider event date>,
                  "relationship_type": <insider relationship type>,
                  "source_url": <insider source URL>,
                  "created_at": <insider creation timestamp>
                },
                ...
              ]
            }
    """
    start_time = time.time()
    log_mcp_tool("find_recent_insider_activities", "called", {
        "id": id,
        "ticker": ticker,
        "short_name": short_name,
        "legal_name": legal_name,
        "start_date": start_date,
        "limit": limit,
    })

    records = relationship_detailsdb.find_recent_insider_activites(
        id=id,
        ticker=ticker,
        short_name=short_name,
        legal_name=legal_name,
        start_date=start_date,
        limit=limit,
    )

    duration = time.time() - start_time
    log_mcp_tool("find_recent_insider_activities", "completed", {
        "id": id,
        "ticker": ticker,
        "short_name": short_name,
        "legal_name": legal_name,
        "start_date": start_date,
        "limit": limit,
        "result_count": len(records),
    }, duration=duration)

    return {
        "count": len(records),
        "results": records,
    }


@tool()
def find_person_entity_relationships(
    id: Optional[str] = None,
    ticker: Optional[str] = None,
    short_name: Optional[str] = None,
    legal_name: Optional[str] = None,
    person_id: Optional[str] = None,
    person_name: Optional[str] = None,
    person_sec_cik: Optional[str] = None,
    start_date: Optional[str] = None,
    limit: int = 250,
) -> Dict[str, Any]:
    """Find relationship details between an entity and a specific person.

    This tool finds all relationship details between a given
    entity and person. It first resolves the entity using the standard
    identification strategy.

    Use this tool when:
        - You want to inspect how a specific person is related to a company
          or other entity (e.g. insider, officer, advisor).
        - You need to see all relationship details between one entity and
          one person, optionally filtered by a starting date.

    Priority for entity identification:
        1. Internal entity id (exact match)
        2. Ticker (case-insensitive exact match)
        3. Short name / legal name (fuzzy CONTAINS search)

    Person identification:
        - At least one of person_id, person_name, or person_sec_cik must be
          provided. These fields are combined to match the target person.

    Args:
        id: Internal entity id. Highest priority if provided.
        ticker: Ticker symbol. Used if id is not provided.
        short_name: Short name text (possibly noisy).
        legal_name: Legal name text (possibly noisy).
        person_id: Internal person id.
        person_name: Person name text.
        person_sec_cik: Person SEC CIK identifier.
        start_date: Optional lower bound (exclusive) for `event_date`
            (with "YYYY-MM-DD" format), e.g. "2024-01-01". If None, all
            dates are included.
        limit: Maximum number of relationship records to return. Default: 250.

    Returns:
        A JSON-serializable dict:

            {
              "count": <int>,
              "results": [
                {
                  "id": <relationship_detail id>,
                  "description": <relationship description>,
                  "relationship_type": <relationship type>,
                  "event_type": <relationship event type>,
                  "source_url": <relationship source URL>,
                  "event_date": <relationship event date>,
                  "created_at": <relationship creation timestamp>
                },
                ...
              ]
            }
    """
    start_time = time.time()
    log_mcp_tool("find_person_entity_relationships", "called", {
        "id": id,
        "ticker": ticker,
        "short_name": short_name,
        "legal_name": legal_name,
        "person_id": person_id,
        "person_name": person_name,
        "person_sec_cik": person_sec_cik,
        "start_date": start_date,
        "limit": limit,
    })

    records = relationship_detailsdb.find_person_entity_relationships(
        id=id,
        ticker=ticker,
        short_name=short_name,
        legal_name=legal_name,
        person_id=person_id,
        person_name=person_name,
        person_sec_cik=person_sec_cik,
        start_date=start_date,
        limit=limit,
    )

    duration = time.time() - start_time
    log_mcp_tool("find_person_entity_relationships", "completed", {
        "id": id,
        "ticker": ticker,
        "short_name": short_name,
        "legal_name": legal_name,
        "person_id": person_id,
        "person_name": person_name,
        "person_sec_cik": person_sec_cik,
        "start_date": start_date,
        "limit": limit,
        "result_count": len(records),
    }, duration=duration)

    return {
        "count": len(records),
        "results": records,
    }

