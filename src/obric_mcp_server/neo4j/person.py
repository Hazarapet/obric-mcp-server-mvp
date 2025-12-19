"""
Neo4j helpers for person-level queries.

This module mirrors the structure of `entity.py` but is focused on
`Person`-type nodes. It is responsible only for low-level Cypher queries
backed by a shared `Neo4jClient` instance.

Higher-level logic that composes these queries should live in the
`tools` package.
"""

from typing import Any, Dict, List, Optional

from neo4j import Result

from .client import Neo4jClient
from .entity import EntityDB


class PersonDB:
    """Low-level Neo4j person query helpers backed by a Neo4jClient."""

    def __init__(self, client: Neo4jClient) -> None:
        """Initialize the PersonDB with a shared Neo4j client.

        The client is responsible for connection management; this class
        only opens sessions and executes Cypher queries.
        """
        self.client = client
        # Reuse EntityDB for consistent entity-identification semantics
        self.entitydb = EntityDB(client)

    @staticmethod
    def _norm(value: Optional[str]) -> Optional[str]:
        """Normalize string inputs: strip whitespace, treat empty as None."""
        if value is None:
            return None
        v = value.strip()
        return v if v else None

    def _build_person_match(
        self,
        *,
        person_var: str = "p",
        id: Optional[str] = None,
        name: Optional[str] = None,
        address: Optional[str] = None,
        sec_cik: Optional[str] = None,
    ) -> tuple[str, Dict[str, Any]]:
        """Build MATCH/WHERE clause for identifying a person node.

        Priority:
        1. Internal id property (exact match)
        2. Name (fuzzy CONTAINS search on `name` property)

        `address` and `sec_cik` are optional filters that will be applied
        in addition to the primary identifier when provided.
        """
        name = self._norm(name)
        address = self._norm(address)
        sec_cik = self._norm(sec_cik)

        params: Dict[str, Any] = {}

        # 1) Highest priority: internal id
        if id is not None:
            match_clause = f"MATCH ({person_var}:Person) WHERE {person_var}.id = $id"
            params["id"] = id
            # Optional filters if provided
            conditions = []
            if address is not None:
                conditions.append(
                    f"toLower({person_var}.address) CONTAINS toLower($address)"
                )
                params["address"] = address
            if sec_cik is not None:
                # sec_cik is usually an exact identifier; keep it exact but case-insensitive
                conditions.append(
                    f"toLower({person_var}.sec_cik) = toLower($sec_cik)"
                )
                params["sec_cik"] = sec_cik
            if conditions:
                match_clause += " OR " + " OR ".join(conditions)

        # 2) Fallback: name is required when id is not provided
        else:
            if name is None:
                raise ValueError(
                    "At least one of id or name must be provided when building a person match."
                )

            conditions = [
                f"toLower({person_var}.full_name) CONTAINS toLower($name)",
            ]
            params["name"] = name

            if address is not None:
                conditions.append(
                    f"toLower({person_var}.address) CONTAINS toLower($address)"
                )
                params["address"] = address
            if sec_cik is not None:
                conditions.append(
                    f"toLower({person_var}.sec_cik) = toLower($sec_cik)"
                )
                params["sec_cik"] = sec_cik

            where_clause = " OR ".join(conditions)
            match_clause = f"MATCH ({person_var}:Person) WHERE {where_clause}"

        return match_clause, params

    def query_person(
        self,
        *,
        id: Optional[str] = None,
        name: Optional[str] = None,
        limit: int = 250,
    ) -> List[Dict[str, Any]]:
        """Query persons by id or name.

        This is a lightweight query helper (not using `_build_person_match`)
        intended for exploratory lookup.

        Args:
            id: Internal person id (exact match). Highest priority if provided.
            name: Person name text. Used if id is not provided; matched with
                case-insensitive CONTAINS on `p.name`.
            limit: Maximum number of records to return for name-based queries.

        Returns:
            List of Neo4j person nodes (as dictionaries).
        """
        # Require at least one of id or name
        if id is None and name is None:
            raise ValueError("At least one of id or name must be provided.")

        # 1) Exact id match
        if id is not None:
            cypher = """
            MATCH (p:Person)
            WHERE p.id = $id
            RETURN p AS node
            LIMIT 1
            """
            params: Dict[str, Any] = {"id": id}

        # 2) Fuzzy name search
        else:
            name_str = self._norm(name)
            if not name_str:
                raise ValueError("name must be a non-empty string when id is not provided.")

            cypher = """
            MATCH (p:Person)
            WHERE toLower(p.full_name) CONTAINS toLower($name)
            RETURN p AS node
            LIMIT $limit
            """
            params = {"name": name_str, "limit": limit}

        with self.client.session() as session:
            result: Result = session.run(cypher, params)
            records = result.data()

        return [record["node"] for record in records]

    def find_people_by_entity(
        self,
        *,
        id: Optional[str] = None,
        ticker: Optional[str] = None,
        short_name: Optional[str] = None,
        legal_name: Optional[str] = None,
        limit: int = 250,
    ) -> List[Dict[str, Any]]:
        """Find people connected to a given entity.

        This method:
        1. Identifies an `Entity` node using the standard EntityDB lookup
           semantics (id, ticker, short_name, legal_name).
        2. Finds all `Person` nodes connected to that entity via any
           `RelationshipDetail` node:

               (entity)-[]-(rd:RelationshipDetail)-[]-(p:Person)

        Args:
            id: Internal entity id. Highest priority if provided.
            ticker: Entity ticker symbol. Used if id is not provided.
            short_name: Entity short name text (possibly noisy).
            legal_name: Entity legal name text (possibly noisy).
            limit: Maximum number of distinct people to return.

        Returns:
            List of distinct person nodes (as dictionaries).
        """
        # Reuse EntityDB's match-building logic to keep semantics consistent
        match_clause, params = self.entitydb._build_entity_match(
            entity_var="e",
            id=id,
            ticker=ticker,
            short_name=short_name,
            legal_name=legal_name,
        )

        params["limit"] = limit

        cypher = f"""
        {match_clause}
        MATCH (e)-[]-(rd:RelationshipDetail)-[]-(p:Person)
        RETURN DISTINCT p AS person
        LIMIT $limit
        """

        with self.client.session() as session:
            result: Result = session.run(cypher, params)
            records = result.data()

        return [record["person"] for record in records]


