"""Simple CLI for testing Obric MCP server components offline.

Usage examples (from project root):

    # Ensure src is on PYTHONPATH, then:
    PYTHONPATH=src python -m obric_mcp_server.cli find-entity --ticker AAPL

    PYTHONPATH=src python -m obric_mcp_server.cli find-entity \
        --short-name "Apple" --legal-name "Apple Inc"

The CLI uses:
- .env configuration (NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
- Neo4jClient for connection
- neo4j.core.find_entity for lookup logic
"""

import argparse
import json
from typing import Any, Dict

from neo4j.graph import Node

from .config import Config
from .neo4j import CoreDB, Neo4jClient, PathDB


def _to_serializable(obj: Any) -> Any:
    """Convert Neo4j Node objects into simple dicts for JSON output."""
    if isinstance(obj, Node):
        return {
            "id": obj.id,
            "labels": list(obj.labels),
            "properties": dict(obj),
        }
    return obj


def _cmd_find_entity(args: argparse.Namespace) -> None:
    """Run the generic entity lookup against Neo4j and print results."""

    config = Config()

    client = Neo4jClient(config=config)

    with client:
        core = CoreDB(client)
        records = core.find_entity(
            id=args.id,
            ticker=args.ticker,
            short_name=args.short_name,
            legal_name=args.legal_name,
            limit=args.limit,
        )

    serializable: Dict[str, Any] = {
        "count": len(records),
        "results": [
            {k: _to_serializable(v) for k, v in record.items()} for record in records
        ],
    }

    print(json.dumps(serializable, indent=2, sort_keys=True))


def _cmd_find_related(args: argparse.Namespace, direction: str) -> None:
    """Run tier-N related-entity lookup (inbound or outbound)."""

    config = Config()
    client = Neo4jClient(config=config)

    with client:
        path_db = PathDB(client)
        if direction == "inbound":
            records = path_db.find_inray_tier(
                id=args.id,
                ticker=args.ticker,
                short_name=args.short_name,
                legal_name=args.legal_name,
                tier=args.tier,
                limit=args.limit,
            )
        else:
            records = path_db.find_outray_tier(
                id=args.id,
                ticker=args.ticker,
                short_name=args.short_name,
                legal_name=args.legal_name,
                tier=args.tier,
                limit=args.limit,
            )

    serializable: Dict[str, Any] = {
        "count": len(records),
        "results": [
            {k: _to_serializable(v) for k, v in record.items()} for record in records
        ]
    }

    print(json.dumps(serializable, indent=2, sort_keys=True))


def _cmd_find_relationship_details(args: argparse.Namespace) -> None:
    """Find all RelationshipDetail nodes between two entities."""

    config = Config()
    client = Neo4jClient(config=config)

    with client:
        core = CoreDB(client)
        records = core.find_relationship_details(
            id1=args.id1,
            ticker1=args.ticker1,
            short_name1=args.short_name1,
            legal_name1=args.legal_name1,
            id2=args.id2,
            ticker2=args.ticker2,
            short_name2=args.short_name2,
            legal_name2=args.legal_name2,
            limit=args.limit,
        )

    serializable: Dict[str, Any] = {
        "count": len(records),
        "results": [
            {k: _to_serializable(v) for k, v in record.items()} for record in records
        ],
    }

    print(json.dumps(serializable, indent=2, sort_keys=True))


def _cmd_has_directed_path(args: argparse.Namespace) -> None:
    """Check if there is a directed path between two entities."""

    config = Config()
    client = Neo4jClient(config=config)

    with client:
        path_db = PathDB(client)
        has_path = path_db.has_directed_path(
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
        )

    result: Dict[str, Any] = {
        "has_path": has_path,
        "direction": args.direction,
        "tier": args.max_tier,
    }
    print(json.dumps(result, indent=2, sort_keys=True))


def _cmd_find_directed_paths(args: argparse.Namespace) -> None:
    """Find all directed paths between two entities (as sequences of entities)."""

    config = Config()
    client = Neo4jClient(config=config)

    with client:
        path_db = PathDB(client)
        paths = path_db.find_directed_paths(
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
        )

    # Serialize each path as a list of simple entity dicts
    serializable_paths = [
        [_to_serializable(entity) for entity in path] for path in paths
    ]

    result: Dict[str, Any] = {
        "count": len(serializable_paths),
        "direction": args.direction,
        "tier": args.max_tier,
        "paths": serializable_paths,
    }
    print(json.dumps(result, indent=2, sort_keys=True))

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

    # inbound related-entities command
    p_inbound = subparsers.add_parser(
        "find-inray-tier",
        help="Find tier-N inbound related entities starting from an entity",
    )
    p_inbound.add_argument("--id", type=str, help="Neo4j internal node id", dest="id")
    p_inbound.add_argument("--ticker", type=str, help="Ticker symbol")
    p_inbound.add_argument("--short-name", type=str, help="Short name text")
    p_inbound.add_argument("--legal-name", type=str, help="Legal name text")
    p_inbound.add_argument(
        "--tier",
        type=int,
        default=1,
        help="Maximum hop distance to traverse inbound",
    )
    p_inbound.add_argument(
        "--limit",
        type=int,
        default=250,
        help="Maximum number of records to return",
    )
    p_inbound.set_defaults(func=lambda args: _cmd_find_related(args, "inbound"))

    # outbound related-entities command
    p_outbound = subparsers.add_parser(
        "find-outray-tier",
        help="Find tier-N outbound related entities starting from an entity",
    )
    p_outbound.add_argument("--id", type=str, help="Neo4j internal node id", dest="id")
    p_outbound.add_argument("--ticker", type=str, help="Ticker symbol")
    p_outbound.add_argument("--short-name", type=str, help="Short name text")
    p_outbound.add_argument("--legal-name", type=str, help="Legal name text")
    p_outbound.add_argument(
        "--tier",
        type=int,
        default=1,
        help="Maximum hop distance to traverse outbound",
    )
    p_outbound.add_argument(
        "--limit",
        type=int,
        default=250,
        help="Maximum number of records to return",
    )
    p_outbound.set_defaults(func=lambda args: _cmd_find_related(args, "outbound"))

    # find-relationship-details command
    p_rel_details = subparsers.add_parser(
        "find-relationship-details",
        help="Find all RelationshipDetail nodes between two entities",
    )
    # Entity 1 arguments
    p_rel_details.add_argument("--id1", type=str, help="Neo4j internal node id for first entity", dest="id1")
    p_rel_details.add_argument("--ticker1", type=str, help="Ticker symbol for first entity")
    p_rel_details.add_argument("--short-name1", type=str, help="Short name text for first entity")
    p_rel_details.add_argument("--legal-name1", type=str, help="Legal name text for first entity")
    # Entity 2 arguments
    p_rel_details.add_argument("--id2", type=str, help="Neo4j internal node id for second entity", dest="id2")
    p_rel_details.add_argument("--ticker2", type=str, help="Ticker symbol for second entity")
    p_rel_details.add_argument("--short-name2", type=str, help="Short name text for second entity")
    p_rel_details.add_argument("--legal-name2", type=str, help="Legal name text for second entity")
    p_rel_details.add_argument(
        "--limit",
        type=int,
        default=250,
        help="Maximum number of RelationshipDetail records to return",
    )
    p_rel_details.set_defaults(func=_cmd_find_relationship_details)

    # has-directed-path command
    p_has_path = subparsers.add_parser(
        "has-directed-path",
        help="Check if there is a directed path between two entities",
    )
    # Entity 1 arguments
    p_has_path.add_argument("--id1", type=str, help="Neo4j internal node id for first entity", dest="id1")
    p_has_path.add_argument("--ticker1", type=str, help="Ticker symbol for first entity")
    p_has_path.add_argument("--short-name1", type=str, help="Short name text for first entity")
    p_has_path.add_argument("--legal-name1", type=str, help="Legal name text for first entity")
    # Entity 2 arguments
    p_has_path.add_argument("--id2", type=str, help="Neo4j internal node id for second entity", dest="id2")
    p_has_path.add_argument("--ticker2", type=str, help="Ticker symbol for second entity")
    p_has_path.add_argument("--short-name2", type=str, help="Short name text for second entity")
    p_has_path.add_argument("--legal-name2", type=str, help="Legal name text for second entity")
    p_has_path.add_argument(
        "--direction",
        type=str,
        choices=["inbound", "outbound"],
        default="outbound",
        help='Direction of flow: "outbound" (entity1 -> entity2) or "inbound" (entity1 <- entity2)',
    )
    p_has_path.add_argument(
        "--max-tier",
        type=int,
        default=3,
        help="Maximum entity tier distance to consider for the path",
    )
    p_has_path.set_defaults(func=_cmd_has_directed_path)

    # find-paths command
    p_find_paths = subparsers.add_parser(
        "find-directed-paths",
        help="Find all directed paths between two entities (as sequences of entities)",
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
        type=str,
        choices=["inbound", "outbound"],
        default="outbound",
        help='Direction of flow: "outbound" (entity1 -> entity2) or "inbound" (entity1 <- entity2)',
    )
    p_find_paths.add_argument(
        "--max-tier",
        type=int,
        default=3,
        help="Maximum entity tier distance to consider for each path",
    )
    p_find_paths.set_defaults(func=_cmd_find_directed_paths)

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

