"""
Database utilities for connecting to Heimdall and Odin databases
"""

import asyncio
import logging
from typing import Optional, Dict, Any, List
import asyncpg
import psycopg2
from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
import pandas as pd

from .config import HEIMDALL_DB_CONFIG, ODIN_DB_CONFIG

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Manages database connections for both Heimdall and Odin"""
    
    def __init__(self):
        self.heimdall_engine = None
        self.odin_engine = None
        self.heimdall_async_engine = None
        self.odin_async_engine = None
    
    def get_connection_string(self, config: Dict[str, Any], async_driver: bool = False) -> str:
        """Generate connection string from config"""
        driver = "postgresql+asyncpg" if async_driver else "postgresql+psycopg2"
        return (
            f"{driver}://{config['user']}:{config['password']}"
            f"@{config['host']}:{config['port']}/{config['database']}"
        )
    
    def connect_heimdall_sync(self):
        """Create synchronous connection to Heimdall database"""
        if not self.heimdall_engine:
            conn_str = self.get_connection_string(HEIMDALL_DB_CONFIG)
            self.heimdall_engine = create_engine(conn_str)
            logger.info("Connected to Heimdall database (sync)")
        return self.heimdall_engine
    
    def connect_odin_sync(self):
        """Create synchronous connection to Odin database"""
        if not self.odin_engine:
            conn_str = self.get_connection_string(ODIN_DB_CONFIG)
            self.odin_engine = create_engine(conn_str)
            logger.info("Connected to Odin database (sync)")
        return self.odin_engine
    
    async def connect_heimdall_async(self):
        """Create asynchronous connection to Heimdall database"""
        if not self.heimdall_async_engine:
            conn_str = self.get_connection_string(HEIMDALL_DB_CONFIG, async_driver=True)
            self.heimdall_async_engine = create_async_engine(conn_str)
            logger.info("Connected to Heimdall database (async)")
        return self.heimdall_async_engine
    
    async def connect_odin_async(self):
        """Create asynchronous connection to Odin database"""
        if not self.odin_async_engine:
            conn_str = self.get_connection_string(ODIN_DB_CONFIG, async_driver=True)
            self.odin_async_engine = create_async_engine(conn_str)
            logger.info("Connected to Odin database (async)")
        return self.odin_async_engine
    
    async def execute_query(self, engine, query: str, params: Optional[Dict] = None) -> List[Dict]:
        """Execute a query and return results"""
        async with engine.begin() as conn:
            result = await conn.execute(text(query), params or {})
            return [dict(row._mapping) for row in result.fetchall()]
    
    async def execute_many(self, engine, query: str, data: List[Dict]):
        """Execute a query with multiple parameter sets"""
        async with engine.begin() as conn:
            await conn.execute(text(query), data)
    
    def read_table_to_dataframe(self, table_name: str, engine, limit: Optional[int] = None) -> pd.DataFrame:
        """Read a table into a pandas DataFrame"""
        query = f"SELECT * FROM {table_name}"
        if limit:
            query += f" LIMIT {limit}"
        
        return pd.read_sql(query, engine)
    
    async def get_table_schema(self, engine, table_name: str) -> List[Dict]:
        """Get table schema information"""
        query = """
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_name = :table_name
        ORDER BY ordinal_position
        """
        return await self.execute_query(engine, query, {"table_name": table_name})
    
    async def get_table_list(self, engine) -> List[str]:
        """Get list of all tables in the database"""
        query = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        ORDER BY table_name
        """
        result = await self.execute_query(engine, query)
        return [row["table_name"] for row in result]
    
    def close_connections(self):
        """Close all database connections"""
        if self.heimdall_engine:
            self.heimdall_engine.dispose()
        if self.odin_engine:
            self.odin_engine.dispose()
        if self.heimdall_async_engine:
            asyncio.create_task(self.heimdall_async_engine.dispose())
        if self.odin_async_engine:
            asyncio.create_task(self.odin_async_engine.dispose())


# Global database manager instance
db_manager = DatabaseManager()
