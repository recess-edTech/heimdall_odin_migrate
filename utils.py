"""
Simple utility functions for the migration system
"""

import logging
from datetime import datetime
from typing import Dict, Any


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


def create_backup() -> str:
    """Create a backup identifier"""
    return f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"


def validate_migration(results: Dict[str, Any]) -> bool:
    """Validate migration results"""
    if not results:
        return False
    
    total_migrated = 0
    total_failed = 0
    
    for entity, stats in results.items():
        if isinstance(stats, dict) and 'migrated' in stats and 'failed' in stats:
            total_migrated += stats['migrated']
            total_failed += stats['failed']
    
    return total_failed == 0 and total_migrated > 0