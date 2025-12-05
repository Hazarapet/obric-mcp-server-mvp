"""
MCP Server implementation for Obric

This module is the entrypoint used when running the MCP server process.
It imports the shared `mcp` instance and all MCP tools so they are
registered on the same FastMCP server.
"""

from __future__ import annotations

import logging

from .mcp_instance import mcp  # shared FastMCP instance

# Import tools so their @tool decorators run and register them on `mcp`.
from .tools import entity as entity_tools  # noqa: F401
from .tools import path as path_tools  # noqa: F401


logger = logging.getLogger(__name__)


def main() -> None:
    """Main entry point for the MCP server."""
    logging.basicConfig(level=logging.INFO)
    logger.info("Starting Obric MCP server 'obric-mcp-server-mvp'...")
    # Run the shared FastMCP instance; this will block the current process.
    mcp.run(transport="streamable-http")


if __name__ == "__main__":
    main()
