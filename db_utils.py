"""
Database utilities for connecting to V1 and V2 databases
"""

import asyncio
import logging
from typing import List, Dict, Any, Optional
import pandas as pd
from sqlalchemy import create_engine, text, MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker

from .config import V1_DB_CONFIG, V2_DB_CONFIG

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Manages connections to both V1 and V2 databases"""
    
    def __init__(self):
        self.v1_engine = None
        self.v2_engine = None
        self.v1_async_engine = None
        self.v2_async_engine = None
        self.v1_session = None
        self.v2_session = None
        
    def get_connection_string(self, config: Dict[str, Any], async_driver: bool = False) -> str:
        """Build database connection string"""
        driver = "postgresql+asyncpg" if async_driver else "postgresql+psycopg2"
        return f"{driver}://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}"
    
    def connect_v1_sync(self):
        """Connect to V1 database synchronously"""
        if not self.v1_engine:
            connection_string = self.get_connection_string(V1_DB_CONFIG)
            self.v1_engine = create_engine(connection_string, pool_pre_ping=True)
            logger.info("Connected to V1 database (sync)")
        return self.v1_engine
    
    def connect_v2_sync(self):
        """Connect to V2 database synchronously"""
        if not self.v2_engine:
            connection_string = self.get_connection_string(V2_DB_CONFIG)
            self.v2_engine = create_engine(connection_string, pool_pre_ping=True)
            logger.info("Connected to V2 database (sync)")
        return self.v2_engine
    
    async def connect_v1_async(self):
        """Connect to V1 database asynchronously"""
        if not self.v1_async_engine:
            connection_string = self.get_connection_string(V1_DB_CONFIG, async_driver=True)
            self.v1_async_engine = create_async_engine(connection_string, pool_pre_ping=True)
            logger.info("Connected to V1 database (async)")
        return self.v1_async_engine
    
    async def connect_v2_async(self):
        """Connect to V2 database asynchronously"""
        if not self.v2_async_engine:
            connection_string = self.get_connection_string(V2_DB_CONFIG, async_driver=True)
            self.v2_async_engine = create_async_engine(connection_string, pool_pre_ping=True)
            logger.info("Connected to V2 database (async)")
        return self.v2_async_engine
    
    async def get_v1_session(self) -> AsyncSession:
        """Get async session for V1 database"""
        if not self.v1_async_engine:
            await self.connect_v1_async()
        
        if not self.v1_session:
            async_session = async_sessionmaker(self.v1_async_engine, class_=AsyncSession)
            self.v1_session = async_session()
        return self.v1_session
    
    async def get_v2_session(self) -> AsyncSession:
        """Get async session for V2 database"""
        if not self.v2_async_engine:
            await self.connect_v2_async()
        
        if not self.v2_session:
            async_session = async_sessionmaker(self.v2_async_engine, class_=AsyncSession)
            self.v2_session = async_session()
        return self.v2_session
    
    async def get_table_list(self, engine) -> List[str]:
        """Get list of tables from database"""
        async with engine.begin() as conn:
            result = await conn.execute(text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                ORDER BY table_name
            """))
            tables = [row[0] for row in result.fetchall()]
            return tables
    
    def read_table_to_dataframe(self, table_name: str, engine, limit: Optional[int] = None, where_clause: str = "") -> pd.DataFrame:
        """Read table data into pandas DataFrame"""
        query = f"SELECT * FROM {table_name}"
        
        if where_clause:
            query += f" WHERE {where_clause}"
            
        if limit:
            query += f" LIMIT {limit}"
            
        try:
            df = pd.read_sql(query, engine)
            logger.info(f"Read {len(df)} records from {table_name}")
            return df
        except Exception as e:
            logger.error(f"Error reading table {table_name}: {e}")
            return pd.DataFrame()
    
    async def execute_query(self, query: str, params: Dict = None, engine_version: str = "v2") -> List[Dict]:
        """Execute a query and return results"""
        engine = await self.connect_v2_async() if engine_version == "v2" else await self.connect_v1_async()
        
        async with engine.begin() as conn:
            if params:
                result = await conn.execute(text(query), params)
            else:
                result = await conn.execute(text(query))
            
            columns = result.keys()
            rows = result.fetchall()
            
            return [dict(zip(columns, row)) for row in rows]
    
    async def insert_record(self, table_name: str, data: Dict[str, Any], engine_version: str = "v2") -> Optional[int]:
        """Insert a single record and return the ID if available"""
        engine = await self.connect_v2_async() if engine_version == "v2" else await self.connect_v1_async()
        
        columns = ", ".join(data.keys())
        placeholders = ", ".join(f":{key}" for key in data.keys())
        
        query = f"""
            INSERT INTO {table_name} ({columns}) 
            VALUES ({placeholders}) 
            RETURNING id
        """
        
        try:
            async with engine.begin() as conn:
                result = await conn.execute(text(query), data)
                record_id = result.fetchone()
                if record_id:
                    return record_id[0]
                return None
        except Exception as e:
            logger.error(f"Error inserting into {table_name}: {e}")
            logger.error(f"Data: {data}")
            raise
    
    async def bulk_insert(self, table_name: str, records: List[Dict[str, Any]], engine_version: str = "v2"):
        """Insert multiple records in bulk"""
        if not records:
            return
            
        engine = await self.connect_v2_async() if engine_version == "v2" else await self.connect_v1_async()
        
        # Convert to DataFrame and use pandas to_sql for bulk insert
        df = pd.DataFrame(records)
        
        # Use the sync engine for pandas operations
        sync_engine = self.connect_v2_sync() if engine_version == "v2" else self.connect_v1_sync()
        
        try:
            df.to_sql(table_name, sync_engine, if_exists='append', index=False, method='multi')
            logger.info(f"Bulk inserted {len(records)} records into {table_name}")
        except Exception as e:
            logger.error(f"Error bulk inserting into {table_name}: {e}")
            raise
    
    def close_connections(self):
        """Close all database connections"""
        if self.v1_engine:
            self.v1_engine.dispose()
            
        if self.v2_engine:
            self.v2_engine.dispose()
            
        if self.v1_async_engine:
            asyncio.create_task(self.v1_async_engine.dispose())
            
        if self.v2_async_engine:
            asyncio.create_task(self.v2_async_engine.dispose())
            
        logger.info("All database connections closed")


# Global database manager instance
db_manager = DatabaseManager()

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
