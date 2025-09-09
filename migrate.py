#!/usr/bin/env python3
"""
Main migration script for migrating data from V1 schema to V2 schema
Focuses on Users (Teachers, Parents, Students) migration with proper sequencing
"""

import asyncio
import logging
import sys
from pathlib import Path
from typing import List, Dict, Any
import argparse
from datetime import datetime

from config import DRY_RUN, LOG_LEVEL, BATCH_SIZE, MIGRATION_ORDER
from db_utils import db_manager
from utils import setup_logging, create_backup, validate_migration
from user_utils import user_manager
from migration_session import create_migration_session, get_migration_session, MigrationPhase
from validation_utils import migration_validator
from migrators.school_migrator import school_migrator
from migrators.teacher_migrator import teacher_migrator
from migrators.parent_migrator import parent_migrator
from migrators.student_migrator import student_migrator

logger = logging.getLogger(__name__)


class MigrationRunner:
    """Main class for running data migration from V1 to V2 schema"""
    
    def __init__(self, dry_run: bool = DRY_RUN):
        self.dry_run = dry_run
        self.migration_log = []
        self.start_time = datetime.now()
        
    async def analyze_schemas(self):
        """Analyze both database schemas to understand the migration requirements"""
        logger.info("Analyzing database schemas...")
        
        # Connect to both databases
        v1_engine = await db_manager.connect_v1_async()
        v2_engine = await db_manager.connect_v2_async()
        
        # Get table lists
        v1_tables = await db_manager.get_table_list(v1_engine)
        v2_tables = await db_manager.get_table_list(v2_engine)
        
        logger.info(f"V1 database tables: {len(v1_tables)}")
        logger.info(f"V2 database tables: {len(v2_tables)}")
        
        # Analyze schema differences
        schema_analysis = {
            "v1_tables": v1_tables,
            "v2_tables": v2_tables,
            "timestamp": datetime.now()
        }
        
        return schema_analysis
    
    async def run_migration(self):
        """Run the complete migration process"""
        logger.info("Starting V1 to V2 migration process...")
        logger.info(f"Dry run mode: {self.dry_run}")
        
        try:
            # Step 0: Create migration session
            session_id = f"migration_{self.start_time.strftime('%Y%m%d_%H%M%S')}"
            session = create_migration_session(session_id)
            logger.info(f"Created migration session: {session.session_id}")
            
            # Step 0.5: CRITICAL - Validate V1 data integrity before migration
            logger.info("=== VALIDATING V1 DATA INTEGRITY ===")
            v1_validation = await migration_validator.validate_v1_data_integrity()
            
            if not v1_validation.is_valid:
                logger.error("V1 data validation failed - cannot proceed with migration")
                for error in v1_validation.errors:
                    logger.error(f"VALIDATION ERROR: {error}")
                logger.info("V1 validation details:")
                for category, details in v1_validation.details.items():
                    logger.info(f"  {category}: {details}")
                
                raise ValueError("V1 data integrity validation failed - fix data issues before migration")
            
            # Log validation warnings
            if v1_validation.warnings:
                logger.warning("V1 data validation warnings:")
                for warning in v1_validation.warnings:
                    logger.warning(f"  {warning}")
            
            logger.info("V1 data integrity validation passed")
            logger.info("V1 validation summary:")
            for category, details in v1_validation.details.items():
                logger.info(f"  {category}: {details}")
            
            # Step 1: Analyze schemas
            schema_analysis = await self.analyze_schemas()
            logger.info("Schema analysis complete")
            
            # Step 2: Create backup (if not dry run)
            if not self.dry_run:
                backup_path = create_backup()
                logger.info(f"Backup created at: {backup_path}")
            
            # Step 3: Run migrations in sequence
            await self._run_sequential_migration()
            
            # Step 4: Validate migration consistency
            logger.info("=== VALIDATING MIGRATION SESSION INTEGRITY ===")
            session_validation = await migration_validator.validate_migration_session_integrity()
            
            if not session_validation.is_valid:
                logger.error("Migration session validation failed")
                for error in session_validation.errors:
                    logger.error(f"SESSION ERROR: {error}")
            else:
                logger.info("Migration session validation passed")
            
            if session_validation.warnings:
                for warning in session_validation.warnings:
                    logger.warning(f"SESSION WARNING: {warning}")
            
            logger.info("Session validation details:")
            for key, value in session_validation.details.items():
                logger.info(f"  {key}: {value}")
            
            validation_result = await session.validate_school_consistency()
            logger.info(f"School consistency validation: {validation_result}")
            
            # Step 5: Print session summary
            summary = session.get_session_summary()
            logger.info(f"Migration session summary: {summary}")
            
            # Step 6: Validate migration (if not dry run)
            if not self.dry_run:
                migration_validation = await self._validate_migration()
                logger.info(f"Migration validation: {migration_validation}")
            
            await session.start_phase(MigrationPhase.COMPLETION)
            logger.info("Migration completed successfully")
            
        except Exception as e:
            logger.error(f"Migration failed: {e}")
            raise
        finally:
            db_manager.close_connections()
    
    async def _run_sequential_migration(self):
        """Run migrations in the defined sequence"""
        logger.info("Starting sequential migration...")
        
        # Step 1: Migrate Schools
        if not self.dry_run:
            logger.info("=== MIGRATING SCHOOLS ===")
            school_result = await school_migrator.migrate_schools()
            self._log_migration_step("schools", school_result)
            
            # Step 2: Migrate Teachers (requires schools)
            logger.info("=== MIGRATING TEACHERS ===")
            teacher_result = await teacher_migrator.migrate_teachers()
            self._log_migration_step("teachers", teacher_result)
            
            # Step 3: Migrate Parents (requires schools)
            logger.info("=== MIGRATING PARENTS ===")
            parent_result = await parent_migrator.migrate_parents()
            self._log_migration_step("parents", parent_result)
            
            # Step 4: Migrate Students (requires schools and parents)
            logger.info("=== MIGRATING STUDENTS ===")
            student_result = await student_migrator.migrate_students()
            self._log_migration_step("students", student_result)
            
            # Additional migration steps can be added here
            # Step 5: Migrate Streams, Classes, etc.
            
        else:
            logger.info("DRY RUN: Would execute migration sequence:")
            for step in MIGRATION_ORDER:
                logger.info(f"  - {step.capitalize()}")
    
    def _log_migration_step(self, step_name: str, result: Dict[str, Any]):
        """Log the result of a migration step"""
        self.migration_log.append({
            "step": step_name,
            "result": result,
            "timestamp": datetime.now()
        })
        
        if result.get("success"):
            logger.info(f"{step_name.capitalize()} migration: {result.get('migrated', 0)} migrated, {result.get('failed', 0)} failed")
        else:
            logger.error(f"{step_name.capitalize()} migration failed: {result.get('error', 'Unknown error')}")
    
    async def _validate_migration(self) -> Dict[str, Any]:
        """Validate the migration results"""
        logger.info("Validating migration results...")
        
        validation_results = {}
        
        # Count records in key tables
        try:
            # V2 record counts
            users_count = await db_manager.execute_query("SELECT COUNT(*) as count FROM users")
            schools_count = await db_manager.execute_query("SELECT COUNT(*) as count FROM schools")
            teachers_count = await db_manager.execute_query("SELECT COUNT(*) as count FROM teachers")
            parents_count = await db_manager.execute_query("SELECT COUNT(*) as count FROM parents")
            students_count = await db_manager.execute_query("SELECT COUNT(*) as count FROM students")
            
            validation_results = {
                "v2_users": users_count[0]['count'] if users_count else 0,
                "v2_schools": schools_count[0]['count'] if schools_count else 0,
                "v2_teachers": teachers_count[0]['count'] if teachers_count else 0,
                "v2_parents": parents_count[0]['count'] if parents_count else 0,
                "v2_students": students_count[0]['count'] if students_count else 0,
                "migration_successful": True
            }
            
            # V1 record counts for comparison
            v1_schools = await db_manager.execute_query("SELECT COUNT(*) as count FROM \"School\" WHERE \"isDeleted\" = false", engine_version="v1")
            v1_teachers = await db_manager.execute_query("SELECT COUNT(*) as count FROM \"Teacher\" WHERE \"isDeleted\" = false", engine_version="v1")
            v1_parents = await db_manager.execute_query("SELECT COUNT(*) as count FROM \"Parent\" WHERE \"isDeleted\" = false", engine_version="v1")
            v1_students = await db_manager.execute_query("SELECT COUNT(*) as count FROM \"Student\" WHERE \"isDeleted\" = false", engine_version="v1")
            
            validation_results.update({
                "v1_schools": v1_schools[0]['count'] if v1_schools else 0,
                "v1_teachers": v1_teachers[0]['count'] if v1_teachers else 0,
                "v1_parents": v1_parents[0]['count'] if v1_parents else 0,
                "v1_students": v1_students[0]['count'] if v1_students else 0,
            })
            
        except Exception as e:
            logger.error(f"Validation failed: {e}")
            validation_results["migration_successful"] = False
            validation_results["error"] = str(e)
        
        return validation_results
    
    def print_summary(self):
        """Print migration summary"""
        print("\n" + "="*70)
        print("MIGRATION SUMMARY")
        print("="*70)
        
        duration = datetime.now() - self.start_time
        print(f"Total duration: {duration}")
        print(f"Dry run mode: {self.dry_run}")
        
        if self.migration_log:
            print("\nMigration Steps:")
            for log_entry in self.migration_log:
                step = log_entry["step"]
                result = log_entry["result"]
                
                if result.get("success"):
                    migrated = result.get("migrated", 0)
                    failed = result.get("failed", 0)
                    total = result.get("total", 0)
                    print(f"  {step.capitalize()}: {migrated}/{total} migrated ({failed} failed)")
                else:
                    print(f"  {step.capitalize()}: FAILED - {result.get('error', 'Unknown error')}")
        
        # Print user manager stats
        user_stats = user_manager.get_mapping_stats()
        print(f"\nUser Creation Stats:")
        print(f"  Total users created: {user_stats['total_users_created']}")
        print(f"  Unique emails generated: {user_stats['unique_emails_generated']}")
        print(f"  Unique phones generated: {user_stats['unique_phones_generated']}")


async def main():
    """Main function"""
    parser = argparse.ArgumentParser(description="Migrate data from V1 schema to V2 schema")
    parser.add_argument("--dry-run", action="store_true", help="Run in dry-run mode")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    parser.add_argument("--step", choices=MIGRATION_ORDER, help="Run only specific migration step")
    
    args = parser.parse_args()
    
    # Setup logging
    log_level = "DEBUG" if args.verbose else LOG_LEVEL
    setup_logging(log_level)
    
    # Run migration
    runner = MigrationRunner(dry_run=args.dry_run or DRY_RUN)
    
    try:
        if args.step:
            logger.info(f"Running single migration step: {args.step}")
            # TODO: Implement single step execution
            logger.warning("Single step execution not implemented yet")
        else:
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
