"""
Unit tests for configuration module
"""

import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path

# Test marker
pytestmark = pytest.mark.unit


class TestConfig:
    """Test configuration loading and validation"""

    def test_default_config_values(self):
        """Test that default configuration values are set correctly"""
        from migrator.config import BATCH_SIZE, DRY_RUN, LOG_LEVEL
        
        assert BATCH_SIZE > 0
        assert isinstance(DRY_RUN, bool)
        assert LOG_LEVEL in ["DEBUG", "INFO", "WARNING", "ERROR"]

    def test_path_configuration(self):
        """Test that paths are configured correctly"""
        from migrator.config import BASE_DIR, HEIMDALL_DIR, ODIN_DIR
        
        assert isinstance(BASE_DIR, Path)
        assert isinstance(HEIMDALL_DIR, Path)
        assert isinstance(ODIN_DIR, Path)

    @patch.dict('os.environ', {
        'HEIMDALL_DB_HOST': 'test_host',
        'HEIMDALL_DB_PORT': '5433',
        'BATCH_SIZE': '500'
    })
    def test_environment_override(self):
        """Test that environment variables override default config"""
        # Need to reload the config module to pick up env changes
        import importlib
        from migrator import config
        importlib.reload(config)
        
        assert config.HEIMDALL_DB_CONFIG['host'] == 'test_host'
        assert config.HEIMDALL_DB_CONFIG['port'] == 5433
        assert config.BATCH_SIZE == 500

    def test_database_config_structure(self):
        """Test that database configuration has required fields"""
        from migrator.config import HEIMDALL_DB_CONFIG, ODIN_DB_CONFIG
        
        required_fields = ['host', 'port', 'database', 'user', 'password']
        
        for field in required_fields:
            assert field in HEIMDALL_DB_CONFIG
            assert field in ODIN_DB_CONFIG
