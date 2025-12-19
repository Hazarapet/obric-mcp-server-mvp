"""
MCP Server implementation for Obric

This module is the entrypoint used when running the MCP server process.
It imports the shared `mcp` instance and all MCP tools so they are
registered on the same FastMCP server.
"""

from __future__ import annotations

import logging

logging.basicConfig(
    level=logging.INFO,
    handlers=[logging.FileHandler("/tmp/obric_mcp_server.log")]
)

# Import tools so their @tool decorators run and register them on `mcp`.
from .tools import path as path_tools  # noqa: F401
from .tools import entity as entity_tools  # noqa: F401
from .tools import neighbourhood as neighbourhood_tools  # noqa: F401
from .tools import person as person_tools  # noqa: F401
from .tools import relationships as relationships_tools  # noqa: F401
from .mcp_instance import mcp  # shared FastMCP instance


logger = logging.getLogger(__name__)


def main() -> None:
    """Main entry point for the MCP server."""
    # Ensure both loggers respect the DEBUG level
    logger.info("Starting Obric MCP server 'obric-mcp-server-mvp'...")
    # Run the shared FastMCP instance; this will block the current process.
    mcp.run(transport="streamable-http")


if __name__ == "__main__":
    main()
