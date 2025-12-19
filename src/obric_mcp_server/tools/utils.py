"""Utility functions for MCP tools."""
from __future__ import annotations

import logging
from typing import Any, Dict, Optional

mcp_tools_logger = logging.getLogger('obric.mcp.tools')


def log_mcp_tool(function_name: str, phase: str, extra: Dict[str, Any], duration: Optional[float] = None) -> None:
    """Helper function to log MCP tool calls and completions.
    
    Args:
        function_name: Name of the MCP tool function.
        phase: Either "called" or "completed".
        extra: Dictionary of additional data to log.
        duration: Optional duration in seconds (for "completed" phase).
    """
    if duration is not None:
        extra["duration_seconds"] = duration
    mcp_tools_logger.info(
        f"{function_name} {phase}",
        extra=extra
    )

