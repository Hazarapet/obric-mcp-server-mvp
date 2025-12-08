"""Neighbourhood-oriented Neo4j helpers.

This module provides the `NeighbourhoodDB` class for querying entity neighbourhoods
and related graph structures.
"""

from typing import Any, Dict, List, Optional

from neo4j import Result

from .client import Neo4jClient

# TODO: we may need to get all Tier 1 enitites and relationship details for better exposure.
class NeighbourhoodDB:
    """Low-level Neo4j neighbourhood query helpers backed by a Neo4jClient."""

    def __init__(self, client: Neo4jClient) -> None:
        self.client = client

    @staticmethod
    def _norm(value: Optional[str]) -> Optional[str]:
        """Normalize string inputs: strip whitespace, treat empty as None."""
        if value is None:
            return None
        v = value.strip()
        return v if v else None

    def _build_entity_match(
        self,
        *,
        entity_var: str = "n",
        id: Optional[str] = None,
        ticker: Optional[str] = None,
        short_name: Optional[str] = None,
        legal_name: Optional[str] = None,
    ) -> tuple[str, Dict[str, Any]]:
        """Build MATCH/WHERE clause for entity identification.

        Priority:
        1. Internal Neo4j node id (exact match)
        2. Ticker (case-insensitive exact match)
        3. Short name / legal name (fuzzy CONTAINS search)
        """
        # Normalize inputs
        ticker = self._norm(ticker)
        short_name = self._norm(short_name)
        legal_name = self._norm(legal_name)

        params: Dict[str, Any] = {}

        # 1) Highest priority: internal Neo4j id
        if id is not None:
            match_clause = f"MATCH ({entity_var}) WHERE {entity_var}.id = $id"
            params["id"] = id

        # 2) Second priority: ticker (exact, case-insensitive match)
        elif ticker is not None:
            match_clause = (
                f"MATCH ({entity_var}:Entity) "
                f"WHERE toLower({entity_var}.ticker) = toLower($ticker)"
            )
            params["ticker"] = ticker

        # 3) Fallback: short_name / legal_name fuzzy search
        else:
            if short_name is None and legal_name is None:
                raise ValueError(
                    "At least one of short_name or legal_name must be provided "
                    "when id and ticker are not given."
                )

            where_clauses: List[str] = []

            if short_name is not None:
                params["short_name"] = short_name
                where_clauses.append(
                    f"""
                    (toLower({entity_var}.short_name) CONTAINS toLower($short_name)
                     OR toLower({entity_var}.legal_name) CONTAINS toLower($short_name))
                    """
                )

            if legal_name is not None:
                params["legal_name"] = legal_name
                where_clauses.append(
                    f"""
                    (toLower({entity_var}.short_name) CONTAINS toLower($legal_name)
                     OR toLower({entity_var}.legal_name) CONTAINS toLower($legal_name))
                    """
                )

            where_combined = " OR ".join(f"({wc.strip()})" for wc in where_clauses)

            match_clause = f"""
            MATCH ({entity_var}:Entity)
            WHERE {where_combined}
            """

        return match_clause.strip(), params

    def find_connected_entities(
        self,
        *,
        id: Optional[str] = None,
        ticker: Optional[str] = None,
        short_name: Optional[str] = None,
        legal_name: Optional[str] = None,
        min_tier: int = 1,
        max_tier: int = 1,
        direction: Optional[str] = None,
        limit: int = 250,
    ) -> List[Dict[str, Any]]:
        """Find connected entities within a tier range.

        Finds all entities connected to the starting entity within the specified
        tier range. For example, min_tier=0, max_tier=2 means Tier 1 and Tier 2 entities.

        Args:
            id: Internal Neo4j node id for the starting entity. Highest priority if provided.
            ticker: Ticker symbol for the starting entity.
            short_name: Short name text for the starting entity.
            legal_name: Legal name text for the starting entity.
            min_tier: Minimum tier to include (0 = Tier 1, 1 = Tier 2, etc.).
            max_tier: Maximum tier to include.
            direction: Connection direction - "inbound", "outbound", or None for both.
            limit: Maximum number of entities to return.

        Returns:
            List of entity records as dictionaries.

        Raises:
            ValueError: If tier values are invalid or direction is invalid.
        """
        if min_tier < 0:
            raise ValueError("min_tier must be >= 0")
        if max_tier < min_tier:
            raise ValueError("max_tier must be >= min_tier")
        if direction is not None and direction not in {"inbound", "outbound"}:
            raise ValueError('direction must be None, "inbound", or "outbound"')

        # Build starting entity match
        start_match, params = self._build_entity_match(
            entity_var="start",
            id=id,
            ticker=ticker,
            short_name=short_name,
            legal_name=legal_name,
        )

        # Add tier parameters (min_tier and max_tier are already the actual tier values)
        params["minTier"] = min_tier
        params["maxTier"] = max_tier
        params["limit"] = limit

        # Determine relationship pattern based on direction
        if direction == "outbound":
            rel_pattern = f"(start)-[r*1..{2*max_tier}]->(e:Entity)"
        elif direction == "inbound":
            rel_pattern = f"(start)<-[r*1..{2*max_tier}]-(e:Entity)"
        else:  # direction is None - both directions
            rel_pattern = f"(start)-[r*1..{2*max_tier}]-(e:Entity)"

        cypher = f"""
        {start_match}
        WITH DISTINCT start
        MATCH path = {rel_pattern}
        WHERE
          // no node visited twice (simple path => no cycles)
          ALL(n IN nodes(path) WHERE SINGLE(x IN nodes(path) WHERE x = n))

          // enforce alternation: Entity, RelationshipDetail, Entity, ...
          AND ALL(i IN range(0, size(nodes(path)) - 1) WHERE
                (i % 2 = 0 AND 'Entity' IN labels(nodes(path)[i])) OR
                (i % 2 = 1 AND 'RelationshipDetail' IN labels(nodes(path)[i]))
          )
        WITH e, path,
             size([n IN nodes(path) WHERE 'Entity' IN labels(n)]) - 1 AS tier
        WHERE tier >= $minTier AND tier <= $maxTier
        RETURN DISTINCT e AS entity, tier
        ORDER BY tier
        LIMIT $limit
        """

        with self.client.session() as session:
            result: Result = session.run(cypher, params)
            records = result.data()

        return [{**record["entity"], "tier": record["tier"]} for record in records]

