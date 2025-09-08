"""
Test configuration and fixtures for migration tests
"""

import os
import pytest
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

# Set test environment
os.environ.setdefault("ENV_FILE", ".env.test")
os.environ.setdefault("DRY_RUN", "True")
os.environ.setdefault("LOG_LEVEL", "DEBUG")

@pytest.fixture
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def mock_db_config():
    """Mock database configuration"""
    return {
        "host": "localhost",
        "port": 5432,
        "database": "test_db",
        "user": "test_user", 
        "password": "test_password"
    }


@pytest.fixture
def mock_heimdall_engine():
    """Mock Heimdall database engine"""
    engine = AsyncMock()
    engine.begin = MagicMock()
    return engine


@pytest.fixture
def mock_odin_engine():
    """Mock Odin database engine"""
    engine = AsyncMock()
    engine.begin = MagicMock()
    return engine


@pytest.fixture
def sample_table_data():
    """Sample table data for testing"""
    return [
        {"id": 1, "name": "User 1", "email": "user1@example.com"},
        {"id": 2, "name": "User 2", "email": "user2@example.com"},
        {"id": 3, "name": "User 3", "email": "user3@example.com"}
    ]


@pytest.fixture
def sample_schema():
    """Sample database schema for testing"""
    return {
        "database": "test_db",
        "tables": [
            {
                "name": "users",
                "columns": [
                    {"column_name": "id", "data_type": "integer", "is_nullable": "NO"},
                    {"column_name": "name", "data_type": "varchar", "is_nullable": "NO"},
                    {"column_name": "email", "data_type": "varchar", "is_nullable": "YES"}
                ],
                "row_count": 100
            }
        ]
    }


@pytest.fixture
def temp_migration_dir(tmp_path):
    """Create temporary directory for migration testing"""
    migration_dir = tmp_path / "migration_test"
    migration_dir.mkdir()
    
    # Create subdirectories
    (migration_dir / "logs").mkdir()
    (migration_dir / "backups").mkdir()
    
    return migration_dir
