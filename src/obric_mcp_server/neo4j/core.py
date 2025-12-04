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

# TODO: we may need to get all Tier 1 enitites and relationship details for better exposure.
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
        """Build MATCH/WHERE clause for entity identification (CoreDB version).

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
                {<Neo4j node>}
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
            return [record["node"] for record in records]

    def find_entities_by_entity_type(
        self,
        *,
        entity_type: str,
        limit: int = 250,
    ) -> List[Dict[str, Any]]:
        """Find entities by their ``entity_type`` property.

        Args:
            entity_type: Value of the ``entity_type`` property to match.
            limit: Maximum number of records to return.

        Returns:
            List of records as dictionaries, each containing:
                {<Neo4j node>}
        """
        etype = self._norm(entity_type)
        if not etype:
            raise ValueError("entity_type must be a non-empty string")

        cypher = """
        MATCH (n:Entity)
        WHERE toLower(n.entity_type) = toLower($entity_type)
        RETURN n AS node
        LIMIT $limit
        """
        params: Dict[str, Any] = {"entity_type": etype, "limit": limit}

        with self.client.session() as session:
            result: Result = session.run(cypher, params)
            records = result.data()
            return [record["node"] for record in records]

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

