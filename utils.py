"""
Utility functions for the migration process
"""

import logging
import json
import shutil
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional
import pandas as pd

from .config.config import ODIN_DIR, JSON_DATA_PATH, SAMPLE_DATA_PATH


def setup_logging(level: str = "INFO"):
    """Setup logging configuration"""
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(f'migration_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
        ]
    )


def create_backup() -> Path:
    """Create a backup of the Odin database before migration"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_dir = Path("./backups") / f"odin_backup_{timestamp}"
    backup_dir.mkdir(parents=True, exist_ok=True)
    
    # This would typically dump the database
    # For now, we'll just create a marker file
    with open(backup_dir / "backup_info.json", "w") as f:
        json.dump({
            "timestamp": timestamp,
            "type": "pre_migration_backup",
            "database": "odin"
        }, f, indent=2)
    
    return backup_dir


async def validate_migration(migration_log: List[Dict]) -> Dict[str, Any]:
    """Validate the migration results"""
    validation_result = {
        "status": "success",
        "total_tables": len(migration_log),
        "successful_tables": 0,
        "failed_tables": 0,
        "total_records": 0,
        "issues": []
    }
    
    for log_entry in migration_log:
        if log_entry["status"] == "success":
            validation_result["successful_tables"] += 1
            validation_result["total_records"] += log_entry["records"]
        else:
            validation_result["failed_tables"] += 1
            validation_result["issues"].append({
                "table": log_entry["table"],
                "error": log_entry.get("error", "Unknown error")
            })
    
    if validation_result["failed_tables"] > 0:
        validation_result["status"] = "partial_failure"
    
    if validation_result["successful_tables"] == 0:
        validation_result["status"] = "failure"
    
    return validation_result


def load_json_data(file_path: Path) -> List[Dict]:
    """Load JSON data from a file"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return data if isinstance(data, list) else [data]
    except Exception as e:
        logging.error(f"Error loading JSON file {file_path}: {e}")
        return []


def load_csv_data(file_path: Path) -> pd.DataFrame:
    """Load CSV data from a file"""
    try:
        return pd.read_csv(file_path)
    except Exception as e:
        logging.error(f"Error loading CSV file {file_path}: {e}")
        return pd.DataFrame()


def migrate_json_files():
    """Migrate JSON files from Heimdall to Odin"""
    logging.info("Migrating JSON files...")
    
    if not JSON_DATA_PATH.exists():
        logging.warning(f"JSON data path does not exist: {JSON_DATA_PATH}")
        return
    
    # Create target directory in Odin
    target_dir = ODIN_DIR / "data" / "json"
    target_dir.mkdir(parents=True, exist_ok=True)
    
    # Copy all JSON files
    json_files = list(JSON_DATA_PATH.glob("*.json"))
    for json_file in json_files:
        target_file = target_dir / json_file.name
        shutil.copy2(json_file, target_file)
        logging.info(f"Copied {json_file.name} to {target_file}")
    
    logging.info(f"Migrated {len(json_files)} JSON files")


def migrate_sample_files():
    """Migrate sample CSV files from Heimdall to Odin"""
    logging.info("Migrating sample files...")
    
    if not SAMPLE_DATA_PATH.exists():
        logging.warning(f"Sample data path does not exist: {SAMPLE_DATA_PATH}")
        return
    
    # Create target directory in Odin
    target_dir = ODIN_DIR / "data" / "samples"
    target_dir.mkdir(parents=True, exist_ok=True)
    
    # Copy all CSV files
    csv_files = list(SAMPLE_DATA_PATH.glob("*.csv"))
    for csv_file in csv_files:
        target_file = target_dir / csv_file.name
        shutil.copy2(csv_file, target_file)
        logging.info(f"Copied {csv_file.name} to {target_file}")
    
    logging.info(f"Migrated {len(csv_files)} sample files")


def transform_user_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform user data from Heimdall format to Odin format
    Customize this based on your schema differences
    """
    # Example transformations:
    
    # Rename columns if needed
    column_mapping = {
        # "old_column": "new_column",
    }
    df = df.rename(columns=column_mapping)
    
    # Add default values for new columns
    if 'created_at' not in df.columns:
        df['created_at'] = datetime.now()
    
    if 'updated_at' not in df.columns:
        df['updated_at'] = datetime.now()
    
    # Data type conversions
    # df['some_date_column'] = pd.to_datetime(df['some_date_column'])
    
    return df


def transform_school_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform school data from Heimdall format to Odin format
    """
    # Add custom transformations for school data
    return df


def transform_assessment_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform assessment data from Heimdall format to Odin format
    """
    # Add custom transformations for assessment data
    return df


def get_data_transformer(table_name: str):
    """Get the appropriate data transformer function for a table"""
    transformers = {
        "users": transform_user_data,
        "schools": transform_school_data,
        "assessments": transform_assessment_data,
        # Add more table-specific transformers as needed
    }
    
    return transformers.get(table_name, lambda df: df)  # Default: no transformation


def generate_migration_report(migration_log: List[Dict], output_path: Optional[Path] = None):
    """Generate a detailed migration report"""
    if output_path is None:
        output_path = Path(f"migration_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
    
    report = {
        "migration_timestamp": datetime.now().isoformat(),
        "summary": {
            "total_tables": len(migration_log),
            "successful_migrations": len([log for log in migration_log if log["status"] == "success"]),
            "failed_migrations": len([log for log in migration_log if log["status"] == "failed"]),
            "total_records_migrated": sum([log["records"] for log in migration_log if log["status"] == "success"])
        },
        "detailed_log": migration_log
    }
    
    with open(output_path, 'w') as f:
        json.dump(report, f, indent=2, default=str)
    
    logging.info(f"Migration report saved to: {output_path}")
    return output_path
