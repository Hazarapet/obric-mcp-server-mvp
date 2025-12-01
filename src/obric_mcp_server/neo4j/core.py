"""Core Neo4j query for basic entity lookup.

This module exposes a single generic function that tries to
reliably find an entity using a prioritized set of identifiers:

Priority:
1. Neo4j internal id (node id)
2. Ticker
3. Short name / legal name (fuzzy "LIKE" search using CONTAINS)

Important: this module assumes the Neo4j client/connection is
managed by the caller. It does NOT create or manage connections,
only uses the provided client to open sessions.
"""

from typing import Any, Dict, List, Optional

from neo4j import Result

from .client import Neo4jClient


class CoreDB:
    """Low-level Neo4j core query helpers backed by a Neo4jClient."""

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
        1. Internal Neo4j node id
        2. Ticker
        3. Short name / legal name (fuzzy "LIKE" search using CONTAINS)

        Args:
            entity_var: Variable name for the entity node in Cypher (e.g., "n", "start", "e1").
            id: Internal Neo4j node id. Highest priority if provided.
            ticker: Ticker symbol. Used if id is not provided.
            short_name: Short name text (possibly noisy).
            legal_name: Legal name text (possibly noisy).

        Returns:
            Tuple of (match_clause, params_dict) where:
            - match_clause: Cypher MATCH/WHERE clause (without RETURN/LIMIT)
            - params_dict: Dictionary of parameters for the query

        Raises:
            ValueError: If no valid identification parameters are provided.
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

        # 2) Second priority: ticker
        elif ticker is not None:
            match_clause = f"MATCH ({entity_var}:Entity) WHERE toLower({entity_var}.ticker) CONTAINS toLower($ticker)"
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

    def find_entity(
        self,
        *,
        id: Optional[str] = None,
        ticker: Optional[str] = None,
        short_name: Optional[str] = None,
        legal_name: Optional[str] = None,
        limit: int = 250,
    ) -> List[Dict[str, Any]]:
        """Find entities using a prioritized, generic lookup strategy.

        Args:
            id: Internal Neo4j node id. Highest priority if provided.
            ticker: Ticker symbol. Used if id is not provided.
            short_name: Short name text (possibly noisy).
            legal_name: Legal name text (possibly noisy).
            limit: Maximum number of records to return (for non-id lookups).

        Behavior:
            - If id is provided, query ONLY by id.
            - Else if ticker is provided, query by ticker (case-insensitive, CONTAINS).
            - Else, use short_name and/or legal_name to perform a fuzzy
              search over n.short_name and n.legal_name using CONTAINS.

        Returns:
            List of records as dictionaries, each containing:
                {"node": <Neo4j node>}
        """

        match_clause, params = self._build_entity_match(
            entity_var="n",
            id=id,
            ticker=ticker,
            short_name=short_name,
            legal_name=legal_name,
        )

        # For id-based queries, use LIMIT 1; for others use $limit
        if id is not None:
            cypher = f"""
            {match_clause}
            RETURN n AS node
            LIMIT 1
            """
        else:
            params["limit"] = limit
            cypher = f"""
            {match_clause}
            RETURN n AS node
            LIMIT $limit
            """

        with self.client.session() as session:
            result: Result = session.run(cypher, params)
            records = result.data()
            return records

    def find_inray_tier(
        self,
        *,
        id: Optional[str] = None,
        ticker: Optional[str] = None,
        short_name: Optional[str] = None,
        legal_name: Optional[str] = None,
        tier: int = 3,
        limit: int = 250,
    ) -> List[Dict[str, Any]]:
        """Find tier-N inbound related entities starting from a resolved entity.

        The starting entity is resolved using the same priority as ``find_entity``:
        1. Internal node id
        2. Ticker
        3. short_name / legal_name fuzzy search

        Args:
            id: Internal Neo4j node id. Highest priority if provided.
            ticker: Ticker symbol. Used if id is not provided.
            short_name: Short name text (possibly noisy).
            legal_name: Legal name text (possibly noisy).
            tier: Maximum hop distance (1..N) to traverse inbound.
            limit: Maximum number of related entities to return.

        Returns:
            List of records, each with:
                {"node": <Neo4j node>, "tier": <hop distance from start>}
        """

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
            return records

    def find_outray_tier(
        self,
        *,
        id: Optional[str] = None,
        ticker: Optional[str] = None,
        short_name: Optional[str] = None,
        legal_name: Optional[str] = None,
        tier: int = 3,
        limit: int = 25,
    ) -> List[Dict[str, Any]]:
        """Find tier-N outbound related entities starting from a resolved entity.

        Direction is the opposite of ``find_inray_tier``:
        relationships go outwards from the starting node up to ``tier`` hops.
        Resolution priority for the starting node matches ``find_entity``.
        """

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
        return records

    def find_relationship_details(
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
        limit: int = 250,
    ) -> List[Dict[str, Any]]:
        """Find all RelationshipDetail nodes between two entities.

        Both entities are resolved using the same priority as ``find_entity``:
        1. Internal node id
        2. Ticker
        3. short_name / legal_name fuzzy search

        Args:
            id1: Internal Neo4j node id for first entity. Highest priority if provided.
            ticker1: Ticker symbol for first entity.
            short_name1: Short name text for first entity (possibly noisy).
            legal_name1: Legal name text for first entity (possibly noisy).
            id2: Internal Neo4j node id for second entity. Highest priority if provided.
            ticker2: Ticker symbol for second entity.
            short_name2: Short name text for second entity (possibly noisy).
            legal_name2: Legal name text for second entity (possibly noisy).
            limit: Maximum number of RelationshipDetail records to return.

        Returns:
            List of records, each with:
                {"relationship_detail": <RelationshipDetail node>, "direction": "outbound" or "inbound"}
                where "outbound" means entity1 -> RelationshipDetail -> entity2,
                and "inbound" means entity1 <- RelationshipDetail <- entity2
        """

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

        # Merge params, prefixing with numbers to avoid conflicts
        params: Dict[str, Any] = {}
        for key, value in params1.items():
            params[f"1_{key}"] = value
            e1_match = e1_match.replace(f"${key}", f"$1_{key}")
        for key, value in params2.items():
            params[f"2_{key}"] = value
            e2_match = e2_match.replace(f"${key}", f"$2_{key}")
        params["limit"] = limit

        # Query for RelationshipDetails in both directions
        cypher = f"""
        {e1_match}
        WITH DISTINCT e1
        {e2_match}
        WITH DISTINCT e1, e2
        OPTIONAL MATCH (e1)-[]->(rd_out:RelationshipDetail)-[]->(e2)
        OPTIONAL MATCH (e1)<-[]-(rd_in:RelationshipDetail)<-[]-(e2)
        WITH e1, e2, 
             collect(DISTINCT {{rd: rd_out, dir: e1.short_name + " -> " + e2.short_name}}) AS outbound_rels,
             collect(DISTINCT {{rd: rd_in, dir: e2.short_name + " -> " + e1.short_name}}) AS inbound_rels
        UNWIND (outbound_rels + inbound_rels) AS rel
        UNWIND rel.rd AS rd
        RETURN rd.id as id, rd.description as description, rd.relationship_type as relationship_type, 
        rd.source_url as source_url, rd.created_at as created_at, rel.dir as relationship_direction
        ORDER BY rd.created_at DESC
        LIMIT $limit
        """

        with self.client.session() as session:
            result: Result = session.run(cypher, params)
            records = result.data()
        return records

