"""
Configuration management for MCP server and Neo4j connection
"""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables and .env file"""
    
    # Neo4j Configuration
    neo4j_uri: str
    neo4j_user: str
    neo4j_password: str
    
    # Server Configuration
    log_level: str = "INFO"
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )

