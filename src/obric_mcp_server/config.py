"""Configuration management for MCP server and Neo4j connection.

Loads configuration from environment variables and an optional .env file
in the project root.

Example .env:

    NEO4J_URI=bolt://localhost:7687
    NEO4J_USERNAME=neo4j
    NEO4J_PASSWORD=your_password_here
    NEO4J_DATABASE=neo4j
    LOG_LEVEL=INFO
"""

from pydantic import AnyUrl, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Config(BaseSettings):
    """Application configuration loaded from environment / .env file.

    We use explicit aliases so the mapping to env vars is obvious and
    easy to consume from scripts.
    """

    # Neo4j configuration
    neo4j_uri: AnyUrl = Field(
        ...,
        alias="NEO4J_URI",
        description="Neo4j connection URI, e.g. bolt://localhost:7687",
    )
    neo4j_username: str = Field(
        ...,
        alias="NEO4J_USERNAME",
        description="Neo4j username",
    )
    neo4j_password: str = Field(
        ...,
        alias="NEO4J_PASSWORD",
        description="Neo4j password",
    )
    neo4j_database: str = Field(
        ...,
        alias="NEO4J_DATABASE",
        description="Neo4j database name (dataset) to connect to",
    )
    neo4j_max_connection_lifetime: int = Field(
        3600,
        alias="NEO4J_MAX_CONNECTION_LIFETIME",
        description="Maximum lifetime of a Neo4j connection in seconds",
    )
    neo4j_max_connection_pool_size: int = Field(
        100,
        alias="NEO4J_MAX_CONNECTION_POOL_SIZE",
        description="Maximum number of connections in the Neo4j pool",
    )

    # Server configuration
    log_level: str = Field(
        "INFO",
        alias="LOG_LEVEL",
        description="Log level for server / CLI (e.g. DEBUG, INFO, WARN, ERROR)",
    )

    # Pydantic v2 settings for env loading
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,  # we use explicit aliases, so keep env lookup strict
        extra="ignore",
        populate_by_name=False,
    )

