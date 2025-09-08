"""
Configuration settings for data migration from V1 schema to V2 schema
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Base paths
BASE_DIR = Path(__file__).parent
PRISMA_V1_DIR = BASE_DIR / "prisma-files" / "v1"
PRISMA_V2_DIR = BASE_DIR / "prisma-files" / "v2"

# Database configurations - V1 (Old Database)
V1_DB_CONFIG = {
    "host": os.getenv("V1_DB_HOST", "localhost"),
    "port": int(os.getenv("V1_DB_PORT", "5432")),
    "database": os.getenv("V1_DB_NAME", "old_recess"),
    "user": os.getenv("V1_DB_USER", "postgres"),
    "password": os.getenv("V1_DB_PASSWORD", ""),
}

# Database configurations - V2 (New Database)
V2_DB_CONFIG = {
    "host": os.getenv("V2_DB_HOST", "localhost"),
    "port": int(os.getenv("V2_DB_PORT", "5432")),
    "database": os.getenv("V2_DB_NAME", "new_recess"),
    "user": os.getenv("V2_DB_USER", "postgres"),
    "password": os.getenv("V2_DB_PASSWORD", ""),
}

# Migration settings
BATCH_SIZE = int(os.getenv("MIGRATION_BATCH_SIZE", "1000"))
DRY_RUN = os.getenv("DRY_RUN", "False").lower() == "true"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# Migration order - defines the sequence of migration
MIGRATION_ORDER = [
    "schools",
    "teachers", 
    "parents",
    "students",
    "streams",
    "classes",
    "subjects",
    "other_entities"
]

# Default values for missing data
DEFAULT_VALUES = {
    "country": "Kenya",
    "county": "Nairobi",
    "gender": "MALE",  # when not specified
    "user_type_mapping": {
        "teacher": "TEACHER",
        "parent": "PARENT", 
        "student": "STUDENT",
        "school": "SCHOOL_ADMIN"
    }
}

# Curriculum mappings - these need to be set up in V2 database first
DEFAULT_CURRICULUM_ID = 1  # Assumes a default curriculum exists
DEFAULT_GRADE_SYSTEM_ID = 1  # Assumes a default grade system exists
DEFAULT_CLASS_LEVEL_ID = 1  # Assumes a default class level exists
