"""
Integration tests for database connectivity
"""

import pytest
from unittest.mock import patch, AsyncMock

# Test marker
pytestmark = pytest.mark.integration


class TestDatabaseConnectivity:
    """Test database connection and operations"""

    @pytest.mark.asyncio
    async def test_database_manager_initialization(self):
        """Test that DatabaseManager initializes correctly"""
        from migrator.db_utils import DatabaseManager
        
        db_manager = DatabaseManager()
        assert db_manager.heimdall_engine is None
        assert db_manager.odin_engine is None

    @pytest.mark.asyncio
    @patch('migrator.db_utils.create_async_engine')
    async def test_connect_heimdall_async(self, mock_create_engine):
        """Test async connection to Heimdall database"""
        from migrator.db_utils import DatabaseManager
        
        mock_engine = AsyncMock()
        mock_create_engine.return_value = mock_engine
        
        db_manager = DatabaseManager()
        engine = await db_manager.connect_heimdall_async()
        
        assert engine is mock_engine
        mock_create_engine.assert_called_once()

    @pytest.mark.asyncio
    @patch('migrator.db_utils.create_async_engine')
    async def test_connect_odin_async(self, mock_create_engine):
        """Test async connection to Odin database"""
        from migrator.db_utils import DatabaseManager
        
        mock_engine = AsyncMock()
        mock_create_engine.return_value = mock_engine
        
        db_manager = DatabaseManager()
        engine = await db_manager.connect_odin_async()
        
        assert engine is mock_engine
        mock_create_engine.assert_called_once()

    def test_connection_string_generation(self):
        """Test database connection string generation"""
        from migrator.db_utils import DatabaseManager
        
        db_manager = DatabaseManager()
        config = {
            "host": "localhost",
            "port": 5432,
            "database": "test_db",
            "user": "test_user",
            "password": "test_password"
        }
        
        # Test sync connection string
        conn_str = db_manager.get_connection_string(config, async_driver=False)
        expected = "postgresql+psycopg2://test_user:test_password@localhost:5432/test_db"
        assert conn_str == expected
        
        # Test async connection string  
        conn_str = db_manager.get_connection_string(config, async_driver=True)
        expected = "postgresql+asyncpg://test_user:test_password@localhost:5432/test_db"
        assert conn_str == expected

    @pytest.mark.asyncio
    async def test_execute_query_mock(self, mock_heimdall_engine, sample_table_data):
        """Test query execution with mock engine"""
        from migrator.db_utils import DatabaseManager
        
        # Mock the connection and result
        mock_conn = AsyncMock()
        mock_result = AsyncMock()
        mock_result.fetchall.return_value = [
            type('Row', (), {'_mapping': data})() for data in sample_table_data
        ]
        mock_conn.execute.return_value = mock_result
        mock_heimdall_engine.begin.return_value.__aenter__.return_value = mock_conn
        
        db_manager = DatabaseManager()
        result = await db_manager.execute_query(
            mock_heimdall_engine, 
            "SELECT * FROM users"
        )
        
        assert len(result) == 3
        assert result[0]['name'] == 'User 1'
