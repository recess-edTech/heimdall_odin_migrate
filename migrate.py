#!/usr/bin/env python3
"""
Main migration script for migrating data from Heimdall to Odin
"""

import asyncio
import logging
import sys
from pathlib import Path
from typing import List, Dict, Any
import argparse
from datetime import datetime

from .config import DRY_RUN, LOG_LEVEL, BATCH_SIZE
from .db_utils import db_manager
from .utils import setup_logging, create_backup, validate_migration

logger = logging.getLogger(__name__)


class MigrationRunner:
    """Main class for running data migration"""
    
    def __init__(self, dry_run: bool = DRY_RUN):
        self.dry_run = dry_run
        self.migration_log = []
        
    async def analyze_schemas(self):
        """Analyze both database schemas to understand the migration requirements"""
        logger.info("Analyzing database schemas...")
        
        # Connect to both databases
        heimdall_engine = await db_manager.connect_heimdall_async()
        odin_engine = await db_manager.connect_odin_async()
        
        # Get table lists
        heimdall_tables = await db_manager.get_table_list(heimdall_engine)
        odin_tables = await db_manager.get_table_list(odin_engine)
        
        logger.info(f"Heimdall tables: {len(heimdall_tables)}")
        logger.info(f"Odin tables: {len(odin_tables)}")
        
        # Analyze schema differences
        schema_analysis = {
            "heimdall_tables": heimdall_tables,
            "odin_tables": odin_tables,
            "common_tables": list(set(heimdall_tables) & set(odin_tables)),
            "heimdall_only": list(set(heimdall_tables) - set(odin_tables)),
            "odin_only": list(set(odin_tables) - set(heimdall_tables))
        }
        
        return schema_analysis
    
    async def get_table_mappings(self, schema_analysis: Dict) -> Dict[str, str]:
        """
        Define how tables from Heimdall map to tables in Odin
        This needs to be customized based on your specific schema
        """
        
        # Default 1:1 mapping for common tables
        mappings = {}
        for table in schema_analysis["common_tables"]:
            mappings[table] = table
            
        # Add custom mappings here based on your schema analysis
        # Example:
        # mappings["heimdall_users"] = "odin_users"
        # mappings["heimdall_posts"] = "odin_content"
        
        return mappings
    
    async def migrate_table(self, source_table: str, target_table: str, transform_func=None):
        """Migrate data from one table to another"""
        logger.info(f"Migrating {source_table} -> {target_table}")
        
        if self.dry_run:
            logger.info(f"DRY RUN: Would migrate {source_table} to {target_table}")
            return
        
        try:
            # Read data from Heimdall
            heimdall_engine = db_manager.connect_heimdall_sync()
            df = db_manager.read_table_to_dataframe(source_table, heimdall_engine)
            
            if df.empty:
                logger.warning(f"No data found in {source_table}")
                return
            
            # Apply transformation if provided
            if transform_func:
                df = transform_func(df)
            
            # Write data to Odin in batches
            odin_engine = db_manager.connect_odin_sync()
            
            for i in range(0, len(df), BATCH_SIZE):
                batch = df.iloc[i:i+BATCH_SIZE]
                batch.to_sql(target_table, odin_engine, if_exists='append', index=False)
                logger.info(f"Migrated batch {i//BATCH_SIZE + 1} ({len(batch)} records)")
            
            self.migration_log.append({
                "table": source_table,
                "target": target_table,
                "records": len(df),
                "status": "success",
                "timestamp": datetime.now()
            })
            
            logger.info(f"Successfully migrated {len(df)} records from {source_table}")
            
        except Exception as e:
            logger.error(f"Error migrating {source_table}: {e}")
            self.migration_log.append({
                "table": source_table,
                "target": target_table,
                "records": 0,
                "status": "failed",
                "error": str(e),
                "timestamp": datetime.now()
            })
    
    async def run_migration(self):
        """Run the complete migration process"""
        logger.info("Starting migration process...")
        
        try:
            # Step 1: Analyze schemas
            schema_analysis = await self.analyze_schemas()
            logger.info("Schema analysis complete")
            
            # Step 2: Get table mappings
            table_mappings = await self.get_table_mappings(schema_analysis)
            logger.info(f"Found {len(table_mappings)} table mappings")
            
            # Step 3: Create backup (if not dry run)
            if not self.dry_run:
                backup_path = create_backup()
                logger.info(f"Backup created at: {backup_path}")
            
            # Step 4: Migrate each table
            for source_table, target_table in table_mappings.items():
                await self.migrate_table(source_table, target_table)
            
            # Step 5: Validate migration
            if not self.dry_run:
                validation_result = await validate_migration(self.migration_log)
                logger.info(f"Migration validation: {validation_result}")
            
            logger.info("Migration completed successfully")
            
        except Exception as e:
            logger.error(f"Migration failed: {e}")
            raise
        finally:
            db_manager.close_connections()
    
    def print_summary(self):
        """Print migration summary"""
        print("\n" + "="*50)
        print("MIGRATION SUMMARY")
        print("="*50)
        
        successful = len([log for log in self.migration_log if log["status"] == "success"])
        failed = len([log for log in self.migration_log if log["status"] == "failed"])
        total_records = sum([log["records"] for log in self.migration_log if log["status"] == "success"])
        
        print(f"Total tables processed: {len(self.migration_log)}")
        print(f"Successful migrations: {successful}")
        print(f"Failed migrations: {failed}")
        print(f"Total records migrated: {total_records}")
        
        if failed > 0:
            print("\nFailed migrations:")
            for log in self.migration_log:
                if log["status"] == "failed":
                    print(f"  - {log['table']}: {log.get('error', 'Unknown error')}")


async def main():
    """Main function"""
    parser = argparse.ArgumentParser(description="Migrate data from Heimdall to Odin")
    parser.add_argument("--dry-run", action="store_true", help="Run in dry-run mode")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    
    args = parser.parse_args()
    
    # Setup logging
    log_level = "DEBUG" if args.verbose else LOG_LEVEL
    setup_logging(log_level)
    
    # Run migration
    runner = MigrationRunner(dry_run=args.dry_run or DRY_RUN)
    
    try:
        await runner.run_migration()
        runner.print_summary()
    except KeyboardInterrupt:
        logger.info("Migration cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
