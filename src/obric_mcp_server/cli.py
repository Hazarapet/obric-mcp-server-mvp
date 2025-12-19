"""Simple CLI for testing Obric MCP server components offline.

Usage examples (from project root):

    # Ensure src is on PYTHONPATH, then:
    PYTHONPATH=src python -m obric_mcp_server.cli find-entity --ticker AAPL

    PYTHONPATH=src python -m obric_mcp_server.cli find-entity \
        --short-name "Apple" --legal-name "Apple Inc"

The CLI uses:
- .env configuration (NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
- Neo4jClient for connection
- neo4j.entity.find_entity for lookup logic
"""

import argparse
import json
from typing import Any, Dict, List, Optional

from .config import Config
from .llm import EmbeddingClient
from .neo4j import (
    EntityDB,
    NeighbourhoodDB,
    Neo4jClient,
    PathDB,
    PersonDB,
    RelationshipDetailsDB,
)


def _cmd_find_entity(args: argparse.Namespace) -> None:
    """Run the generic entity lookup against Neo4j and print results."""

    config = Config()

    client = Neo4jClient(config=config)

    with client:
        entitydb = EntityDB(client)
        records = entitydb.find_entity(
            id=args.id,
            ticker=args.ticker,
            short_name=args.short_name,
            legal_name=args.legal_name,
            limit=args.limit,
        )

    serializable: Dict[str, Any] = {
        "count": len(records),
        "results": records,
    }

    print(json.dumps(serializable, indent=2, sort_keys=True))


def _cmd_find_government_awards(args: argparse.Namespace) -> None:
    """Find government awards for an entity."""

    config = Config()
    client = Neo4jClient(config=config)

    with client:
        relationship_details_db = RelationshipDetailsDB(client)
        records = relationship_details_db.find_government_awards(
            id=args.id,
            ticker=args.ticker,
            short_name=args.short_name,
            legal_name=args.legal_name,
            limit=args.limit,
        )

    serializable: Dict[str, Any] = {
        "count": len(records),
        "results": records,
    }

    print(json.dumps(serializable, indent=2, sort_keys=True, default=str))


def _cmd_find_affiliate_entities(args: argparse.Namespace) -> None:
    """Find affiliate entities connected through relationship types."""

    config = Config()
    client = Neo4jClient(config=config)

    with client:
        entitydb = EntityDB(client)
        records = entitydb.find_affiliate_entities(
            id=args.id,
            ticker=args.ticker,
            short_name=args.short_name,
            legal_name=args.legal_name,
            limit=args.limit,
        )

    serializable: Dict[str, Any] = {
        "count": len(records),
        "results": records,
    }

    print(json.dumps(serializable, indent=2, sort_keys=True))


def _cmd_find_recent_insider_activites(args: argparse.Namespace) -> None:
    """Find recent insider RelationshipDetails for an entity."""

    config = Config()
    client = Neo4jClient(config=config)

    with client:
        relationship_details_db = RelationshipDetailsDB(client)
        records = relationship_details_db.find_recent_insider_activites(
            id=args.id,
            ticker=args.ticker,
            short_name=args.short_name,
            legal_name=args.legal_name,
            start_date=args.start_date,
            limit=args.limit,
        )

    serializable: Dict[str, Any] = {
        "count": len(records),
        "results": records,
    }

    # Use default=str to serialize Neo4j temporal types (e.g. DateTime) as strings.
    print(json.dumps(serializable, indent=2, sort_keys=True, default=str))


def _cmd_find_person_entity_relationships(args: argparse.Namespace) -> None:
    """Find RelationshipDetails between an entity and a specific person."""

    config = Config()
    client = Neo4jClient(config=config)

    with client:
        relationship_details_db = RelationshipDetailsDB(client)
        records = relationship_details_db.find_person_entity_relationships(
            id=args.id,
            ticker=args.ticker,
            short_name=args.short_name,
            legal_name=args.legal_name,
            person_id=args.person_id,
            person_name=args.person_name,
            person_sec_cik=args.person_sec_cik,
            start_date=args.start_date,
            limit=args.limit,
        )

    serializable: Dict[str, Any] = {
        "count": len(records),
        "results": records,
    }

    # Neo4j may return temporal types (e.g. DateTime) that aren't JSON-serializable
    # by default; use default=str to render them as ISO-like strings.
    print(json.dumps(serializable, indent=2, sort_keys=True, default=str))


def _cmd_find_people_by_entity(args: argparse.Namespace) -> None:
    """Find people connected to an entity via RelationshipDetails."""

    config = Config()
    client = Neo4jClient(config=config)

    with client:
        person_db = PersonDB(client)
        records = person_db.find_people_by_entity(
            id=args.id,
            ticker=args.ticker,
            short_name=args.short_name,
            legal_name=args.legal_name,
            limit=args.limit,
        )

    serializable: Dict[str, Any] = {
        "count": len(records),
        "results": records,
    }

    print(json.dumps(serializable, indent=2, sort_keys=True, default=str))


def _cmd_find_entity_by_relationship_embedding(args: argparse.Namespace) -> None:
    """Find entities connected to RelationshipDetails matching an embedding similarity."""

    config = Config()
    client = Neo4jClient(config=config)
    embedding_client = EmbeddingClient(config=config)

    # Generate embedding from query text
    embedding = embedding_client.embed_text(args.query)

    with client:
        entitydb = EntityDB(client)
        records = entitydb.find_entity_by_relationship_embedding(
            embedding=embedding,
            threshold=args.threshold,
            direction=args.direction,
            limit=args.limit,
        )

    serializable: Dict[str, Any] = {
        "count": len(records),
        "results": records,
    }

    print(json.dumps(serializable, indent=2, sort_keys=True))


def _cmd_find_paths_between_entities(
    args: argparse.Namespace,
) -> None:
    """Find all paths (entities + RelationshipDetails) between two entities."""

    config = Config()
    client = Neo4jClient(config=config)

    with client:
        path_db = PathDB(client)
        paths = path_db.find_paths_between_entities(
            id1=args.id1,
            ticker1=args.ticker1,
            short_name1=args.short_name1,
            legal_name1=args.legal_name1,
            id2=args.id2,
            ticker2=args.ticker2,
            short_name2=args.short_name2,
            legal_name2=args.legal_name2,
            direction=args.direction,
            max_tier=args.max_tier,
            max_paths=args.max_paths,
        )

    result: Dict[str, Any] = {
        "count": len(paths),
        "direction": args.direction,
        "tier": args.max_tier,
        "paths": paths,
    }
    print(json.dumps(result, indent=2, sort_keys=True))


def _cmd_find_connected_entities(args: argparse.Namespace) -> None:
    """Find connected entities within a tier range."""

    config = Config()
    client = Neo4jClient(config=config)

    with client:
        neighbourhood_db = NeighbourhoodDB(client)
        records = neighbourhood_db.find_connected_entities(
            id=args.id,
            ticker=args.ticker,
            short_name=args.short_name,
            legal_name=args.legal_name,
            min_tier=args.min_tier,
            max_tier=args.max_tier,
            direction=args.direction,
            limit=args.limit,
        )

    result: Dict[str, Any] = {
        "count": len(records),
        "min_tier": args.min_tier,
        "max_tier": args.max_tier,
        "direction": args.direction,
        "results": records
    }
    print(json.dumps(result, indent=2, sort_keys=True))


def _parse_direction(value: Optional[str]) -> Optional[str]:
    """Parse direction argument, allowing None for bidirectional."""
    if value is None or value.lower() == "none":
        return None
    if value.lower() not in ["inbound", "outbound"]:
        raise argparse.ArgumentTypeError(f'direction must be "inbound", "outbound", or None, got "{value}"')
    return value.lower()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="CLI for testing Obric MCP Neo4j functions",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # find-entity command (all arguments optional)
    p_find = subparsers.add_parser(
        "find-entity",
        help="Generic entity lookup using id/ticker/short_name/legal_name",
    )
    p_find.add_argument("--id", type=str, help="Neo4j internal node id", dest="id")
    p_find.add_argument("--ticker", type=str, help="Ticker symbol")
    p_find.add_argument("--short-name", type=str, help="Short name text")
    p_find.add_argument("--legal-name", type=str, help="Legal name text")
    p_find.add_argument(
        "--limit",
        type=int,
        default=25,
        help="Maximum number of records to return (non-id lookups)",
    )
    p_find.set_defaults(func=_cmd_find_entity)

    # find-affiliate-entities command
    p_find_affiliate = subparsers.add_parser(
        "find-affiliate-entities",
        help="Find affiliate entities connected through relationship types",
    )
    p_find_affiliate.add_argument("--id", type=str, help="Neo4j internal node id", dest="id")
    p_find_affiliate.add_argument("--ticker", type=str, help="Ticker symbol")
    p_find_affiliate.add_argument("--short-name", type=str, help="Short name text")
    p_find_affiliate.add_argument("--legal-name", type=str, help="Legal name text")
    p_find_affiliate.add_argument(
        "--limit",
        type=int,
        default=250,
        help="Maximum number of entity records to return",
    )
    p_find_affiliate.set_defaults(func=_cmd_find_affiliate_entities)

    # find-government-awards command
    p_find_awards = subparsers.add_parser(
        "find-government-awards",
        help="Find government awards for an entity (RelationshipDetails with 'awarded_to' relationship type)",
    )
    p_find_awards.add_argument("--id", type=str, help="Neo4j internal node id", dest="id")
    p_find_awards.add_argument("--ticker", type=str, help="Ticker symbol")
    p_find_awards.add_argument("--short-name", type=str, help="Short name text")
    p_find_awards.add_argument("--legal-name", type=str, help="Legal name text")
    p_find_awards.add_argument(
        "--limit",
        type=int,
        default=250,
        help="Maximum number of RelationshipDetail records to return",
    )
    p_find_awards.set_defaults(func=_cmd_find_government_awards)

    # find-recent-insider-activites command
    p_find_insider = subparsers.add_parser(
        "find-recent-insider-activites",
        help=(
            "Find recent insider RelationshipDetails for an entity. "
            "Uses the 'Insider' sublabel and optional start_date filter."
        ),
    )
    p_find_insider.add_argument(
        "--id", type=str, help="Neo4j internal node id", dest="id"
    )
    p_find_insider.add_argument(
        "--ticker", type=str, help="Ticker symbol for the entity"
    )
    p_find_insider.add_argument(
        "--short-name", type=str, help="Short name text for the entity"
    )
    p_find_insider.add_argument(
        "--legal-name", type=str, help="Legal name text for the entity"
    )
    p_find_insider.add_argument(
        "--start-date",
        type=str,
        default=None,
        help=(
            "Optional lower bound (exclusive) for event_date, e.g. '2024-01-01'. "
            "If omitted, all insider activities are returned."
        ),
        dest="start_date",
    )
    p_find_insider.add_argument(
        "--limit",
        type=int,
        default=250,
        help="Maximum number of RelationshipDetail records to return",
    )
    p_find_insider.set_defaults(func=_cmd_find_recent_insider_activites)

    # find-person-entity-relationships command
    p_find_person_entity_rels = subparsers.add_parser(
        "find-person-entity-relationships",
        help=(
            "Find RelationshipDetails between an entity and a specific person "
            "(all relationship types, optional start_date filter)"
        ),
    )
    # Entity arguments
    p_find_person_entity_rels.add_argument(
        "--id", type=str, help="Neo4j internal node id for the entity", dest="id"
    )
    p_find_person_entity_rels.add_argument(
        "--ticker", type=str, help="Ticker symbol for the entity"
    )
    p_find_person_entity_rels.add_argument(
        "--short-name", type=str, help="Short name text for the entity"
    )
    p_find_person_entity_rels.add_argument(
        "--legal-name", type=str, help="Legal name text for the entity"
    )
    # Person arguments
    p_find_person_entity_rels.add_argument(
        "--person-id", type=str, help="Internal person id", dest="person_id"
    )
    p_find_person_entity_rels.add_argument(
        "--person-name",
        type=str,
        help="Person name (case-insensitive exact match in DB)",
        dest="person_name",
    )
    p_find_person_entity_rels.add_argument(
        "--person-sec-cik",
        type=str,
        help="Person SEC CIK identifier (case-insensitive exact match)",
        dest="person_sec_cik",
    )
    # Optional start_date
    p_find_person_entity_rels.add_argument(
        "--start-date",
        type=str,
        default=None,
        help=(
            "Optional lower bound (exclusive) for event_date, e.g. '2024-01-01'. "
            "If omitted, all dates are included."
        ),
        dest="start_date",
    )
    p_find_person_entity_rels.add_argument(
        "--limit",
        type=int,
        default=250,
        help="Maximum number of RelationshipDetail records to return",
    )
    p_find_person_entity_rels.set_defaults(func=_cmd_find_person_entity_relationships)

    # find-people-by-entity command
    p_find_people_by_entity = subparsers.add_parser(
        "find-people-by-entity",
        help=(
            "Find Person nodes connected to an entity via RelationshipDetail "
            "segments (Entity - RelationshipDetail - Person)."
        ),
    )
    p_find_people_by_entity.add_argument(
        "--id", type=str, help="Neo4j internal node id for the entity", dest="id"
    )
    p_find_people_by_entity.add_argument(
        "--ticker", type=str, help="Ticker symbol for the entity"
    )
    p_find_people_by_entity.add_argument(
        "--short-name", type=str, help="Short name text for the entity"
    )
    p_find_people_by_entity.add_argument(
        "--legal-name", type=str, help="Legal name text for the entity"
    )
    p_find_people_by_entity.add_argument(
        "--limit",
        type=int,
        default=250,
        help="Maximum number of Person records to return",
    )
    p_find_people_by_entity.set_defaults(func=_cmd_find_people_by_entity)

    # find-entity-by-relationship-embedding command
    p_find_by_emb = subparsers.add_parser(
        "find-entity-by-relationship-embedding",
        help="Find entities connected to RelationshipDetails matching an embedding similarity",
    )
    p_find_by_emb.add_argument(
        "--query",
        type=str,
        required=True,
        help="Text query to generate embedding from and search for semantically similar relationships",
    )
    p_find_by_emb.add_argument(
        "--threshold",
        type=float,
        default=0.8,
        help="Minimum similarity score threshold (typically 0.0 to 1.0 for cosine similarity)",
    )
    p_find_by_emb.add_argument(
        "--direction",
        type=_parse_direction,
        default=None,
        help='Filter by relationship direction: "outbound", "inbound", or "none" for both',
    )
    p_find_by_emb.add_argument(
        "--limit",
        type=int,
        default=250,
        help="Maximum number of entity records to return",
    )
    p_find_by_emb.set_defaults(func=_cmd_find_entity_by_relationship_embedding)

    # find-paths-between-entities command
    p_find_paths = subparsers.add_parser(
        "find-paths-between-entities",
        help=(
            "Find all paths between two entities, including "
            "RelationshipDetail segments"
        ),
    )
    # Entity 1 arguments
    p_find_paths.add_argument(
        "--id1", type=str, help="Neo4j internal node id for first entity", dest="id1"
    )
    p_find_paths.add_argument(
        "--ticker1", type=str, help="Ticker symbol for first entity"
    )
    p_find_paths.add_argument(
        "--short-name1", type=str, help="Short name text for first entity"
    )
    p_find_paths.add_argument(
        "--legal-name1", type=str, help="Legal name text for first entity"
    )
    # Entity 2 arguments
    p_find_paths.add_argument(
        "--id2", type=str, help="Neo4j internal node id for second entity", dest="id2"
    )
    p_find_paths.add_argument(
        "--ticker2", type=str, help="Ticker symbol for second entity"
    )
    p_find_paths.add_argument(
        "--short-name2", type=str, help="Short name text for second entity"
    )
    p_find_paths.add_argument(
        "--legal-name2", type=str, help="Legal name text for second entity"
    )
    p_find_paths.add_argument(
        "--direction",
        type=_parse_direction,
        default="outbound",
        help='Direction of flow: "outbound" (entity1 -> entity2), "inbound" (entity1 <- entity2), or "none" for bidirectional',
    )
    p_find_paths.add_argument(
        "--max-tier",
        type=int,
        default=3,
        help="Maximum entity tier distance to consider for each path",
    )
    p_find_paths.add_argument(
        "--max-paths",
        type=int,
        default=100,
        help="Maximum number of paths to return",
    )
    p_find_paths.set_defaults(
        func=_cmd_find_paths_between_entities
    )

    # find-connected-entities command
    p_find_connected = subparsers.add_parser(
        "find-connected-entities",
        help="Find connected entities within a tier range (neighborhood query)",
    )
    p_find_connected.add_argument(
        "--id", type=str, help="Neo4j internal node id for the starting entity", dest="id"
    )
    p_find_connected.add_argument(
        "--ticker", type=str, help="Ticker symbol for the starting entity"
    )
    p_find_connected.add_argument(
        "--short-name", type=str, help="Short name text for the starting entity"
    )
    p_find_connected.add_argument(
        "--legal-name", type=str, help="Legal name text for the starting entity"
    )
    p_find_connected.add_argument(
        "--min-tier",
        type=int,
        default=0,
        help="Minimum tier to include (0 = Tier 1, 1 = Tier 2, etc.)",
    )
    p_find_connected.add_argument(
        "--max-tier",
        type=int,
        default=1,
        help="Maximum tier to include",
    )
    p_find_connected.add_argument(
        "--direction",
        type=str,
        default=None,
        help='Connection direction: "inbound", "outbound", or omit for both',
    )
    p_find_connected.add_argument(
        "--limit",
        type=int,
        default=250,
        help="Maximum number of entities to return",
    )
    p_find_connected.set_defaults(func=_cmd_find_connected_entities)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if not hasattr(args, "func"):
        parser.print_help()
        return

    args.func(args)


if __name__ == "__main__":
    main()

