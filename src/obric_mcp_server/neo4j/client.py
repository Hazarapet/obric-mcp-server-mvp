"""
Neo4j database connection client
"""

from typing import Optional
from neo4j import AsyncGraphDatabase, AsyncDriver


class Neo4jClient:
    """Neo4j database connection client - handles connection management only"""
    
    def __init__(self, uri: str, user: str, password: str):
        """
        Initialize Neo4j client with connection credentials
        
        Args:
            uri: Neo4j database URI (e.g., 'bolt://localhost:7687' or 'neo4j://localhost:7687')
            user: Neo4j username
            password: Neo4j password
        """
        self.uri = uri
        self.user = user
        self.password = password
        self._driver: Optional[AsyncDriver] = None
    
    async def connect(self):
        """Establish connection to Neo4j database and verify connectivity"""
        if self._driver is None:
            self._driver = AsyncGraphDatabase.driver(
                self.uri,
                auth=(self.user, self.password)
            )
            # Verify connectivity
            await self._driver.verify_connectivity()
    
    @property
    def driver(self) -> AsyncDriver:
        """
        Get the Neo4j driver instance
        
        Returns:
            AsyncDriver instance
            
        Raises:
            RuntimeError: If driver is not connected
        """
        if self._driver is None:
            raise RuntimeError("Driver not connected. Call connect() first or use async context manager.")
        return self._driver
    
    async def close(self):
        """Close the database connection"""
        if self._driver is not None:
            await self._driver.close()
            self._driver = None
    
    async def __aenter__(self):
        """Async context manager entry"""
        await self.connect()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.close()

