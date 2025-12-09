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


class EntityDB:
    """Low-level Neo4j entity query helpers backed by a Neo4jClient."""

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
        """Build MATCH/WHERE clause for entity identification (EntityDB version).

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
        limit: int = 1,
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

    def query_entity(
        self,
        *,
        query: str,
        limit: int = 250,
    ) -> List[Dict[str, Any]]:
        """Find entities where any field contains the query string.

        Searches across multiple entity fields:
        - ticker
        - entity_type
        - short_name
        - legal_name

        Args:
            query: Search string to match against entity fields (case-insensitive).
            limit: Maximum number of records to return.

        Returns:
            List of records as dictionaries, each containing:
                {<Neo4j node>}
        """
        query_str = self._norm(query)
        if not query_str:
            raise ValueError("query must be a non-empty string")

        cypher = """
        MATCH (n:Entity)
        WHERE toLower(n.ticker) CONTAINS toLower($query)
           OR toLower(n.entity_type) CONTAINS toLower($query)
           OR toLower(n.short_name) CONTAINS toLower($query)
           OR toLower(n.legal_name) CONTAINS toLower($query)
        RETURN n AS node
        LIMIT $limit
        """
        params: Dict[str, Any] = {"query": query_str, "limit": limit}

        with self.client.session() as session:
            result: Result = session.run(cypher, params)
            records = result.data()
            return [record["node"] for record in records]

    def find_entity_by_relationship_query(
        self,
        *,
        query: str,
        direction: Optional[str] = None,
        limit: int = 250,
    ) -> List[Dict[str, Any]]:
        """Find entities connected to RelationshipDetails matching a query.

        Searches RelationshipDetail nodes where relationship_type or description
        contains the query string, then returns all entities connected to those
        relationship details. Each RelationshipDetail has 2 Entities connected.

        Args:
            query: Search string to match against relationship_type and description
                fields (case-insensitive).
            direction: Filter by relationship direction - "inbound", "outbound", or None for both.
            limit: Maximum number of entity records to return.

        Returns:
            List of entity records as dictionaries, each containing:
                {
                    <Entity node properties>,
                    "relationship_direction": "[EntityFrom] -> [EntityTo]"
                }
        """
        query_str = self._norm(query)
        if not query_str:
            raise ValueError("query must be a non-empty string")
        if direction is not None and direction not in {"inbound", "outbound"}:
            raise ValueError('direction must be None, "inbound", or "outbound"')

        params: Dict[str, Any] = {"query": query_str, "limit": limit, "direction": direction}

        
        # Query for RelationshipDetails matching the query, then get connected entities
        # Match RelationshipDetails once, collect entities from both directions per RD, then unwind
        cypher = """
        MATCH (rd:RelationshipDetail)
        WHERE (rd.description IS NOT NULL AND toLower(rd.description) CONTAINS toLower($query))
        OR (rd.relationship_type IS NOT NULL AND toLower(rd.relationship_type) CONTAINS toLower($query))

        // 2) Match both orientations around rd
        OPTIONAL MATCH (src1:Entity)-[]->(rd)-[]->(dst1:Entity)
        OPTIONAL MATCH (dst2:Entity)<-[]-(rd)<-[]-(src2:Entity)
        """

        if direction is None:
            cypher += """
            WITH [src1, dst1, src2, dst2] as entities
            """
        elif direction == "outbound":
            cypher += """
            WITH [src1, src2] as entities
            """

        elif direction == "inbound":
            cypher += """
            WITH [dst1, dst2] as entities
            """

        cypher += """
        UNWIND entities AS entity
        RETURN DISTINCT entity
        LIMIT $limit;
        """

        with self.client.session() as session:
            result: Result = session.run(cypher, params)
            records = result.data()
            
        # Merge entity properties with relationship_direction
        return [
            {**record["entity"]} for record in records
        ]

    def find_entity_by_relationship_embedding(
        self,
        *,
        embedding: List[float],
        threshold: float = 0.7,
        direction: Optional[str] = None,
        limit: int = 250,
    ) -> List[Dict[str, Any]]:
        """Find entities connected to RelationshipDetails matching an embedding similarity.

        Searches RelationshipDetail nodes where the embedding similarity with the given
        embedding is greater than the threshold, then returns all entities connected to
        those relationship details. Each RelationshipDetail has 2 Entities connected.

        Args:
            embedding: Vector embedding (list of floats) to compare against RelationshipDetail embeddings.
            threshold: Minimum similarity score threshold (typically 0.0 to 1.0 for cosine similarity).
            direction: Filter by relationship direction - "inbound", "outbound", or None for both.
            limit: Maximum number of entity records to return.

        Returns:
            List of entity records as dictionaries, each containing:
                {
                    <Entity node properties>
                }

        Raises:
            ValueError: If embedding is empty or direction is invalid.
        """
        if not embedding:
            raise ValueError("embedding must be a non-empty list")
        if not isinstance(embedding, list) or not all(isinstance(x, (int, float)) for x in embedding):
            raise ValueError("embedding must be a list of numbers")
        if not isinstance(threshold, (int, float)):
            raise ValueError("threshold must be a number")
        if direction is not None and direction not in {"inbound", "outbound"}:
            raise ValueError('direction must be None, "inbound", or "outbound"')

        params: Dict[str, Any] = {
            "embedding": embedding,
            "threshold": threshold,
            "limit": limit,
            "direction": direction,
        }

        # Query for RelationshipDetails matching the embedding similarity, then get connected entities
        # Match RelationshipDetails once, collect entities from both directions per RD, then unwind
        cypher = """
        MATCH (rd:RelationshipDetail)
        WHERE rd.embedding IS NOT NULL
        WITH rd, gds.similarity.cosine(rd.embedding, $embedding) AS similarity
        WHERE similarity >= $threshold

        // 2) Match both orientations around rd
        OPTIONAL MATCH (src1:Entity)-[]->(rd)-[]->(dst1:Entity)
        OPTIONAL MATCH (dst2:Entity)<-[]-(rd)<-[]-(src2:Entity)
        """

        if direction is None:
            cypher += """
            WITH [src1, dst1, src2, dst2] as entities
            """
        elif direction == "outbound":
            cypher += """
            WITH [src1, src2] as entities
            """
        elif direction == "inbound":
            cypher += """
            WITH [dst1, dst2] as entities
            """

        cypher += """
        UNWIND entities AS entity
        RETURN DISTINCT entity
        LIMIT $limit;
        """

        with self.client.session() as session:
            result: Result = session.run(cypher, params)
            records = result.data()

        # Merge entity properties
        return [
            {**record["entity"]} for record in records
        ]

