"""RelationshipDetails-oriented Neo4j helpers.

This module provides the `RelationshipDetailsDB` class for querying
relationship details between entities.
"""

from typing import Any, Dict, List, Optional

from neo4j import Result

from .client import Neo4jClient
from .entity import EntityDB
from .person import PersonDB


class RelationshipDetailsDB:
    """Low-level Neo4j relationship details query helpers backed by a Neo4jClient."""

    def __init__(self, client: Neo4jClient) -> None:
        self.client = client
        # Reuse EntityDB for consistent entity-identification semantics
        self.entitydb = EntityDB(client)
        # Reuse PersonDB for consistent person-identification semantics
        self.persondb = PersonDB(client)

    @staticmethod
    def _norm(value: Optional[str]) -> Optional[str]:
        """Normalize string inputs: strip whitespace, treat empty as None."""
        if value is None:
            return None
        v = value.strip()
        return v if v else None


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

        Both entities are resolved using the same priority as entity identification:
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
        e1_match, params1 = self.entitydb._build_entity_match(
            entity_var="e1",
            id=id1,
            ticker=ticker1,
            short_name=short_name1,
            legal_name=legal_name1,
        )

        # Build entity2 match clause
        e2_match, params2 = self.entitydb._build_entity_match(
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

    def find_government_awards(
        self,
        *,
        id: Optional[str] = None,
        ticker: Optional[str] = None,
        short_name: Optional[str] = None,
        legal_name: Optional[str] = None,
        limit: int = 250,
    ) -> List[Dict[str, Any]]:
        """Find all RelationshipDetails where the entity or its affiliates were awarded to (government awards).

        First finds all affiliate entities connected to the given entity, then finds
        RelationshipDetail nodes where relationship_type is "awarded_to" for any of
        these entities (including the original entity). Returns the RelationshipDetail
        with the government agency name as "awarded_from".

        Args:
            id: Internal Neo4j node id. Highest priority if provided.
            ticker: Ticker symbol. Used if id is not provided.
            short_name: Short name text (possibly noisy).
            legal_name: Legal name text (possibly noisy).
            limit: Maximum number of RelationshipDetail records to return.

        Returns:
            List of records, each containing:
                {
                    <RelationshipDetail node properties>,
                    "awarded_from": <government agency name (short_name or legal_name)>
                }
        """
        # Build match clause for the entity
        match_clause, match_params = self.entitydb._build_entity_match(
            entity_var="start",
            id=id,
            ticker=ticker,
            short_name=short_name,
            legal_name=legal_name,
        )

        # Relationship types for affiliate entities
        relationship_types = [
            'subsidiary', 'parent_company', 'equity_acquisition', 'ownership', 'division_of',
            'asset_acquisition', 'acquisition_of_equity', 'asset_purchase', 'acquisition', 'affiliate',
            'affiliate_of', 'owner', 'ownership_interest', 'equity_holder', 'parent', 'sold_assets_to', 
            'acquirer'
        ]

        params: Dict[str, Any] = {**match_params, "limit": limit, "relationship_types": relationship_types}

        # First find all affiliate entities (including the starting entity)
        # Then find government awards for any of these entities
        cypher = f"""
        {match_clause}
        // Find all affiliate entities connected to the starting entity
        OPTIONAL MATCH (start)-[]-(affiliate_rd:RelationshipDetail)-[]-(affiliate:Entity)
        WHERE affiliate_rd.relationship_type IN $relationship_types
          AND affiliate.entity_type = "company"
          AND start.entity_type = "company"
        
        // Collect all entities (starting entity + affiliates)
        WITH start, collect(DISTINCT affiliate) AS affiliates
        WITH [start] + [a IN affiliates WHERE a IS NOT NULL] AS all_entities
        
        // Unwind to get individual entities
        UNWIND all_entities AS entity
        
        // Find government awards for any of these entities
        MATCH (government_agency:Entity)-[]->(rd:RelationshipDetail)-[]->(entity)
        WHERE rd.relationship_type = "awarded_to"
        
        WITH DISTINCT rd, 
             COALESCE(government_agency.legal_name, government_agency.short_name, "") AS awarded_from,
             COALESCE(entity.legal_name, entity.short_name, "") AS affiliate
        RETURN rd.id as id, rd.source_url as source_url,
        rd.description as description, awarded_from, affiliate as affiliate_entity
        ORDER BY rd.created_at DESC
        LIMIT $limit
        """

        with self.client.session() as session:
            result: Result = session.run(cypher, params)
            records = result.data()

        # Return records with awarded_from and affiliate fields
        return records

    def find_recent_insider_activites(
        self,
        *,
        id: Optional[str] = None,
        ticker: Optional[str] = None,
        short_name: Optional[str] = None,
        legal_name: Optional[str] = None,
        start_date: Optional[str] = None,
        limit: int = 250,
    ) -> List[Dict[str, Any]]:
        """Find recent insider RelationshipDetails for a given entity.

        This method:
        1. Resolves an entity using id, ticker, short_name, or legal_name.
        2. Finds all `RelationshipDetail` nodes with the `Insider` sublabel
           that are connected to that entity and have `event_date` greater
           than the given `start_date`.
        3. Returns these relationship details without the `embedding` property.

        Args:
            id: Internal Neo4j node id. Highest priority if provided.
            ticker: Ticker symbol. Used if id is not provided.
            short_name: Short name text (possibly noisy).
            legal_name: Legal name text (possibly noisy).
            start_date: Optional lower bound (exclusive) for `event_date`. If
                provided, it should be in a comparable format (e.g., ISO date
                string "YYYY-MM-DD"). If None, all insider activities are
                returned regardless of date.
            limit: Maximum number of records to return. Default: 250.

        Returns:
            List of records, each containing selected RelationshipDetail fields
            (excluding any `embedding` property).
        """
        # Build match clause for the entity
        match_clause, match_params = self.entitydb._build_entity_match(
            entity_var="e",
            id=id,
            ticker=ticker,
            short_name=short_name,
            legal_name=legal_name,
        )

        # Normalize start_date if provided
        start_date_norm: Optional[str]
        if start_date is not None:
            start_date_norm = self._norm(start_date)
            if not start_date_norm:
                raise ValueError("start_date must be a non-empty string when provided.")
        else:
            start_date_norm = None

        params: Dict[str, Any] = {
            **match_params,
            "start_date": start_date_norm,
            "limit": limit,
        }

        cypher = f"""
        {match_clause}
        MATCH (e)-[]-(rd:RelationshipDetail:Insider)
        // Compare by DATE portion to handle Date/DateTime uniformly
        WHERE $start_date IS NULL OR date(rd.event_date) > date($start_date)
        RETURN rd.id as id,
               rd.description as description,
               rd.relationship_type as relationship_type,
               rd.source_url as source_url,
               toString(rd.event_date) as event_date,
               rd.created_at as created_at
        ORDER BY rd.event_date DESC, rd.created_at DESC
        LIMIT $limit
        """

        with self.client.session() as session:
            result: Result = session.run(cypher, params)
            records = result.data()

        return records

    def find_person_entity_relationships(
        self,
        *,
        id: Optional[str] = None,
        ticker: Optional[str] = None,
        short_name: Optional[str] = None,
        legal_name: Optional[str] = None,
        person_id: Optional[str] = None,
        person_name: Optional[str] = None,
        person_sec_cik: Optional[str] = None,
        start_date: Optional[str] = None,
        limit: int = 250,
    ) -> List[Dict[str, Any]]:
        """Find RelationshipDetails between an entity and a specific person.

        This method:
        1. Resolves an `Entity` node using id, ticker, short_name, or legal_name.
        2. Resolves a `Person` node using person_id, person_name, or person_sec_cik.
        3. Finds all `RelationshipDetail` nodes connecting that entity and person:

               (e:Entity)-[]-(rd:RelationshipDetail)-[]-(p:Person)

           across all relationship types (not only Insider).
        4. Optionally filters by `event_date` > start_date when provided.
        5. Returns these relationship details without the `embedding` property.

        Args:
            id: Internal entity id. Highest priority if provided.
            ticker: Entity ticker symbol.
            short_name: Entity short name text (possibly noisy).
            legal_name: Entity legal name text (possibly noisy).
            person_id: Internal person id (exact match).
            person_name: Person name text (fuzzy CONTAINS, case-insensitive).
            person_sec_cik: Person SEC CIK identifier (exact, case-insensitive).
            start_date: Optional lower bound (exclusive) for `event_date`. If
                provided, it should be in a comparable format (e.g., ISO date
                string "YYYY-MM-DD"). If None, no date filter is applied.
            limit: Maximum number of records to return. Default: 250.

        Returns:
            List of records, each containing selected RelationshipDetail fields
            (excluding any `embedding` property).
        """
        # Build match clause for the entity
        entity_match, entity_params = self.entitydb._build_entity_match(
            entity_var="e",
            id=id,
            ticker=ticker,
            short_name=short_name,
            legal_name=legal_name,
        )

        # Build person match using PersonDB semantics
        person_match, person_params = self.persondb._build_person_match(
            person_var="p",
            id=person_id,
            name=person_name,
            sec_cik=person_sec_cik,
        )

        # Normalize start_date if provided
        if start_date is not None:
            start_date_norm = self._norm(start_date)
            if not start_date_norm:
                raise ValueError(
                    "start_date must be a non-empty string when provided."
                )
        else:
            start_date_norm = None

        params: Dict[str, Any] = {
            **entity_params,
            **person_params,
            "start_date": start_date_norm,
            "limit": limit,
        }

        cypher = f"""
        {entity_match}
        {person_match}
        MATCH (e)-[]-(rd:RelationshipDetail)-[]-(p)
        // Compare by DATE portion to handle Date/DateTime uniformly
        WHERE ($start_date IS NULL OR date(rd.event_date) > date($start_date))
        RETURN rd.id as id,
               rd.description as description,
               rd.relationship_type as relationship_type,
               rd.source_url as source_url,
               toString(rd.event_date) as event_date,
               rd.created_at as created_at
        ORDER BY rd.event_date DESC, rd.created_at DESC
        LIMIT $limit
        """

        with self.client.session() as session:
            result: Result = session.run(cypher, params)
            records = result.data()

        return records

