"""
Configuration settings for data migration from Heimdall to Odin
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Base paths
BASE_DIR = Path(__file__).parent.parent
HEIMDALL_DIR = BASE_DIR / "Heimdall"
ODIN_DIR = BASE_DIR / "odin"

# Database configurations
HEIMDALL_DB_CONFIG = {
    "host": os.getenv("HEIMDALL_DB_HOST", "localhost"),
    "port": int(os.getenv("HEIMDALL_DB_PORT", "5432")),
    "database": os.getenv("HEIMDALL_DB_NAME", "heimdall"),
    "user": os.getenv("HEIMDALL_DB_USER", "postgres"),
    "password": os.getenv("HEIMDALL_DB_PASSWORD", ""),
}

ODIN_DB_CONFIG = {
    "host": os.getenv("ODIN_DB_HOST", "localhost"),
    "port": int(os.getenv("ODIN_DB_PORT", "5432")),
    "database": os.getenv("ODIN_DB_NAME", "odin"),
    "user": os.getenv("ODIN_DB_USER", "postgres"),
    "password": os.getenv("ODIN_DB_PASSWORD", ""),
}

# Migration settings
BATCH_SIZE = int(os.getenv("MIGRATION_BATCH_SIZE", "1000"))
DRY_RUN = os.getenv("DRY_RUN", "False").lower() == "true"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# File paths for data migration
JSON_DATA_PATH = HEIMDALL_DIR / "public" / "json-data"
SAMPLE_DATA_PATH = HEIMDALL_DIR / "public" / "samples"
