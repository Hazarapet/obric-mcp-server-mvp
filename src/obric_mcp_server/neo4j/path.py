"""Path-oriented Neo4j helpers.

This module is intentionally minimal for now and only provides the
`PathDB` class shell. We'll add concrete path-based query helpers
here as needed.
"""

from typing import Any, Dict, List, Optional

from neo4j import Result

from .client import Neo4jClient

class PathDB:
    def __init__(self, client: Optional[Neo4jClient] = None) -> None:
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
        3. Short name / legal name (fuzzy "LIKE" search using CONTAINS)
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

            # Combine all provided name-based conditions with OR
            where_combined = " OR ".join(f"({wc.strip()})" for wc in where_clauses)

            match_clause = f"""
            MATCH ({entity_var}:Entity)
            WHERE {where_combined}
            """

        return match_clause.strip(), params

    def find_inray_tier(
        self,
        *,
        id: Optional[str] = None,
        ticker: Optional[str] = None,
        short_name: Optional[str] = None,
        legal_name: Optional[str] = None,
        tier: int = 1,
        limit: int = 250,
    ) -> List[Dict[str, Any]]:
        """Find tier-N inbound related entities starting from a resolved entity."""

        if tier < 1:
            raise ValueError("tier must be >= 1")

        start_match, params = self._build_entity_match(
            entity_var="start",
            id=id,
            ticker=ticker,
            short_name=short_name,
            legal_name=legal_name,
        )
        params["limit"] = limit

        # Inbound traversal:
        # First hop: Entity <- RelationshipDetail
        # Remaining hops (up to tier entities): arbitrary, but tier is
        # derived from relationship count assuming Entity-RelationshipDetail-Entity pattern.
        rel_pattern = "(start)"
        for i in range(tier):
            if i == tier - 1:
                rel_pattern += "<-[]-(:RelationshipDetail)<-[]-(n:Entity)"
            else:
                rel_pattern += "<-[]-(:RelationshipDetail)<-[]-(:Entity)"

        cypher = f"""
        {start_match}
        WITH DISTINCT start
        MATCH path = {rel_pattern}
        WHERE ALL(n IN nodes(path) WHERE SINGLE(x IN nodes(path) WHERE x = n))
        WITH n, min(floor(length(path) / 2)) AS tier
        RETURN n AS node, tier
        LIMIT $limit
        """

        with self.client.session() as session:
            result: Result = session.run(cypher, params)
            records = result.data()
            
        return [record["node"] for record in records]

    def find_outray_tier(
        self,
        *,
        id: Optional[str] = None,
        ticker: Optional[str] = None,
        short_name: Optional[str] = None,
        legal_name: Optional[str] = None,
        tier: int = 1,
        limit: int = 25,
    ) -> List[Dict[str, Any]]:
        """Find tier-N outbound related entities starting from a resolved entity."""

        if tier < 1:
            raise ValueError("tier must be >= 1")

        start_match, params = self._build_entity_match(
            entity_var="start",
            id=id,
            ticker=ticker,
            short_name=short_name,
            legal_name=legal_name,
        )
        params["limit"] = limit

        # Outbound traversal:
        # First hop: Entity - RelationshipDetail
        # Remaining hops (up to tier entities): arbitrary, but tier is
        # derived from relationship count assuming Entity-RelationshipDetail-Entity pattern.
        rel_pattern = "(start)"
        for i in range(tier):
            if i == tier - 1:
                rel_pattern += "-[]->(:RelationshipDetail)-[]->(n:Entity)"
            else:
                rel_pattern += "-[]->(:RelationshipDetail)-[]->(:Entity)"

        cypher = f"""
        {start_match}
        WITH DISTINCT start
        MATCH path = {rel_pattern}
        WHERE ALL(n IN nodes(path) WHERE SINGLE(x IN nodes(path) WHERE x = n))
        WITH n, min(floor(length(path) / 2)) AS tier
        RETURN n AS node, tier
        LIMIT $limit
        """

        with self.client.session() as session:
            result: Result = session.run(cypher, params)
            records = result.data()
        
        return [record["node"] for record in records]

    def has_directed_path(
        self,
        *,
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
    ) -> bool:
        """Detect whether there is a directed path between two entities.

        Direction is defined relative to the first entity:
        - \"outbound\": e1 -> ... -> e2
        - \"inbound\":  e1 <- ... <- e2

        The entire path must follow a single direction (like a flow); we
        only consider paths where every relationship arrow is aligned
        with the chosen direction.
        """
        if direction not in {"outbound", "inbound"}:
            raise ValueError('direction must be either "outbound" or "inbound"')
        if max_tier < 1:
            raise ValueError("max_tier must be >= 1")

        # Build entity1 match clause
        e1_match, params1 = self._build_entity_match(
            entity_var="e1",
            id=id1,
            ticker=ticker1,
            short_name=short_name1,
            legal_name=legal_name1,
        )

        # Build entity2 match clause
        e2_match, params2 = self._build_entity_match(
            entity_var="e2",
            id=id2,
            ticker=ticker2,
            short_name=short_name2,
            legal_name=legal_name2,
        )

        # Merge params with simple prefixes to avoid conflicts
        params: Dict[str, Any] = {}
        for key, value in params1.items():
            params[f"1_{key}"] = value
            e1_match = e1_match.replace(f"${key}", f"$1_{key}")
        for key, value in params2.items():
            params[f"2_{key}"] = value
            e2_match = e2_match.replace(f"${key}", f"$2_{key}")

        # Directional path pattern
        if direction == "outbound":
            rel_pattern = f"-[*1..{max_tier*2}]->"
        else:  # inbound
            rel_pattern = f"<-[*1..{max_tier*2}]-"

        cypher = f"""
        {e1_match}
        WITH DISTINCT e1
        {e2_match}
        WITH DISTINCT e1, e2
        MATCH path = (e1){rel_pattern}(e2)
        WHERE ALL(i IN range(0, size(nodes(path)) - 1) WHERE
        (i % 2 = 0 AND 'Entity' IN labels(nodes(path)[i])) OR
        (i % 2 = 1 AND 'RelationshipDetail' IN labels(nodes(path)[i])))
        RETURN count(path) > 0 AS has_path
        """

        with self.client.session() as session:
            result: Result = session.run(cypher, params)
            records = result.data()

        if not records:
            return False
        return bool(records[0].get("has_path", False))


    def find_directed_paths(
        self,
        *,
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
    ) -> List[List[Any]]:
        """Return all directed paths between two entities as lists of Entity nodes.

        Direction is defined relative to the first entity:
        - \"outbound\": e1 -> ... -> e2
        - \"inbound\":  e1 <- ... <- e2

        The entire path must follow a single direction, and paths are
        constrained to alternate Entity and RelationshipDetail nodes:
            Entity0 -> RelationshipDetail -> Entity1 -> ... -> EntityN
        Only the Entity nodes are returned in each path.
        """
        if direction not in {"outbound", "inbound"}:
            raise ValueError('direction must be either "outbound" or "inbound"')
        if max_tier < 1:
            raise ValueError("max_tier must be >= 1")

        # Build entity1 match clause
        e1_match, params1 = self._build_entity_match(
            entity_var="e1",
            id=id1,
            ticker=ticker1,
            short_name=short_name1,
            legal_name=legal_name1,
        )

        # Build entity2 match clause
        e2_match, params2 = self._build_entity_match(
            entity_var="e2",
            id=id2,
            ticker=ticker2,
            short_name=short_name2,
            legal_name=legal_name2,
        )

        # Merge params with simple prefixes to avoid conflicts
        params: Dict[str, Any] = {}
        for key, value in params1.items():
            params[f"1_{key}"] = value
            e1_match = e1_match.replace(f"${key}", f"$1_{key}")
        for key, value in params2.items():
            params[f"2_{key}"] = value
            e2_match = e2_match.replace(f"${key}", f"$2_{key}")

        # Directional path pattern (max_tier entity hops ≈ max_tier*2 rel hops)
        if direction == "outbound":
            rel_pattern = f"-[*1..{max_tier*2}]->"
        else:  # inbound
            rel_pattern = f"<-[*1..{max_tier*2}]-"

        cypher = f"""
        {e1_match}
        WITH DISTINCT e1
        {e2_match}
        WITH DISTINCT e1, e2
        MATCH path = (e1){rel_pattern}(e2)
        WHERE ALL(i IN range(0, size(nodes(path)) - 1) WHERE
          (i % 2 = 0 AND 'Entity' IN labels(nodes(path)[i])) OR
          (i % 2 = 1 AND 'RelationshipDetail' IN labels(nodes(path)[i])))
        WITH [n IN nodes(path) WHERE 'Entity' IN labels(n)] AS entity_path
        RETURN entity_path AS path
        """

        with self.client.session() as session:
            result: Result = session.run(cypher, params)
            records = result.data()

        # Each record["path"] is a list of Entity nodes along one directed path
        if direction == "outbound":
            return [record["path"] for record in records]
        else:
            return [record["path"][::-1] for record in records]

    def find_directed_paths_with_relationship_details(
        self,
        *,
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
    ) -> List[List[Dict[str, Any]]]:
        """Return all directed paths between two entities including RelationshipDetail nodes.

        Each path is returned as an ordered list of segments. Each segment is a dict:

            {
              "from": <Entity node>,
              "relationship_detail": <RelationshipDetail node>,
              "to": <Entity node>,
            }

        Direction is defined relative to the first entity:
        - "outbound": entity1 -> ... -> entity2
        - "inbound":  entity1 <- ... <- entity2

        The entire path must follow a single direction, and paths are
        constrained to alternate Entity and RelationshipDetail nodes:

            Entity0 -> RelationshipDetail -> Entity1 -> ... -> EntityN
        """
        if direction not in {"outbound", "inbound"}:
            raise ValueError('direction must be either "outbound" or "inbound"')
        if max_tier < 1:
            raise ValueError("max_tier must be >= 1")

        # Build entity1 match clause
        e1_match, params1 = self._build_entity_match(
            entity_var="e1",
            id=id1,
            ticker=ticker1,
            short_name=short_name1,
            legal_name=legal_name1,
        )

        # Build entity2 match clause
        e2_match, params2 = self._build_entity_match(
            entity_var="e2",
            id=id2,
            ticker=ticker2,
            short_name=short_name2,
            legal_name=legal_name2,
        )

        # Merge params with simple prefixes to avoid conflicts
        params: Dict[str, Any] = {}
        for key, value in params1.items():
            params[f"1_{key}"] = value
            e1_match = e1_match.replace(f"${key}", f"$1_{key}")
        for key, value in params2.items():
            params[f"2_{key}"] = value
            e2_match = e2_match.replace(f"${key}", f"$2_{key}")

        # Directional path pattern (max_tier entity hops ≈ max_tier*2 rel hops)
        if direction == "outbound":
            rel_pattern = f"-[*1..{max_tier*2}]->"
            direction_key1 = "from"
            direction_key2 = "to"
        else:  # inbound
            rel_pattern = f"<-[*1..{max_tier*2}]-"
            direction_key1 = "to"
            direction_key2 = "from"

        cypher = f"""
        {e1_match}
        WITH DISTINCT e1
        {e2_match}
        WITH DISTINCT e1, e2
        MATCH path = (e1){rel_pattern}(e2)
        WHERE ALL(i IN range(0, size(nodes(path)) - 1) WHERE
          (i % 2 = 0 AND 'Entity' IN labels(nodes(path)[i])) OR
          (i % 2 = 1 AND 'RelationshipDetail' IN labels(nodes(path)[i])))
        WITH nodes(path) AS ns
        WITH [i IN range(0, size(ns) - 3, 2) |
              {{
                {direction_key1}: ns[i],
                relationship_detail: {{id: ns[i + 1].id, description: ns[i + 1].description, relationship_type: ns[i + 1].relationship_type, source_url: ns[i + 1].source_url, created_at: ns[i + 1].created_at}},
                {direction_key2}: ns[i + 2]
              }}] AS segments
        RETURN segments AS path
        """

        with self.client.session() as session:
            result: Result = session.run(cypher, params)
            records = result.data()

        # Each record["path"] is a list of segments (from, relationship_detail, to)
        if direction == "outbound":
            return [record["path"] for record in records]
        else:
            return [record["path"][::-1] for record in records]

