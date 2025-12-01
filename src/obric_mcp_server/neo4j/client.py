"""Neo4j connection and session management."""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Optional

from neo4j import Driver, GraphDatabase, Session

from ..config import Config

logger = logging.getLogger(__name__)


class Neo4jClient:
    """Neo4j database client with connection pooling."""

    def __init__(self, config: Optional[Config] = None) -> None:
        """Initialize Neo4j client with configuration."""
        self.config = config or Config()
        self._driver: Optional[Driver] = None

    def connect(self) -> None:
        """Establish connection to Neo4j database."""
        if self._driver is None:
            if not self.config.neo4j_password:
                logger.error(
                    "NEO4J_PASSWORD not set in environment variables or .env file"
                )
                raise ValueError(
                    "NEO4J_PASSWORD must be set in environment variables or .env file"
                )

            self._driver = GraphDatabase.driver(
                str(self.config.neo4j_uri),
                auth=(self.config.neo4j_username, self.config.neo4j_password),
                max_connection_lifetime=self.config.neo4j_max_connection_lifetime,
                max_connection_pool_size=self.config.neo4j_max_connection_pool_size,
            )

            # Verify connectivity immediately - driver creation is lazy and doesn't
            # actually connect.
            try:
                self._driver.verify_connectivity()
            except Exception as e:
                logger.error(f"Failed to connect to Neo4j: {e}")
                self._driver.close()
                self._driver = None
                raise ConnectionError(
                    f"Cannot connect to Neo4j database at {self.config.neo4j_uri}. "
                    "Please ensure Neo4j is running and accessible."
                ) from e

    def close(self) -> None:
        """Close the Neo4j driver connection."""
        if self._driver is not None:
            self._driver.close()
            self._driver = None

    @contextmanager
    def session(self, **kwargs) -> Session:
        """Context manager for Neo4j session."""
        if self._driver is None:
            self.connect()

        assert self._driver is not None  # for type checkers
        session = self._driver.session(database=self.config.neo4j_database, **kwargs)
        try:
            yield session
        finally:
            session.close()

    def verify_connectivity(self) -> bool:
        """Verify connection to Neo4j database."""
        try:
            if self._driver is None:
                self.connect()
            assert self._driver is not None
            self._driver.verify_connectivity()
            return True
        except Exception as e:
            logger.error("Neo4j connectivity check failed: %s", e, exc_info=True)
            return False

    def __enter__(self) -> "Neo4jClient":
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.close()

