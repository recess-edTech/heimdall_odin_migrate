#!/usr/bin/env python3
"""
Supabase Database Dump Generator
Generates SQL dump files from V1 and V2 Supabase databases for reference and backup
"""

import argparse
import asyncio
import logging
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from config import V1_DB_CONFIG, V2_DB_CONFIG
from db_utils import db_manager

logger = logging.getLogger(__name__)


class SupabaseDumpGenerator:
    """Generate SQL dumps from Supabase databases"""

    def __init__(self, output_dir: str = "database_dumps"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(
                    self.output_dir / f"dump_generation_{self.timestamp}.log"),
                logging.StreamHandler()
            ]
        )

    async def generate_all_dumps(self, include_data: bool = True, include_schema: bool = True):
        """Generate dumps for both V1 and V2 databases"""
        logger.info("Starting database dump generation...")

        try:
            await self.generate_v1_dumps(include_data, include_schema)
            await self.generate_schema_comparison()

            await self.generate_data_statistics()

            logger.info("Database dump generation completed successfully")

        except Exception as e:
            logger.error(f"Error generating dumps: {e}")
            raise

    async def generate_v1_dumps(self, include_data: bool = True, include_schema: bool = True):
        """Generate dumps for V1 database"""
        logger.info("Generating V1 database dumps...")

        v1_dir = self.output_dir / "v1"
        v1_dir.mkdir(exist_ok=True)

        await self._generate_pg_dump("v1", V1_DB_CONFIG, v1_dir, include_data, include_schema)
        await self._generate_table_dumps("v1", v1_dir, include_data, include_schema)
        await self._generate_v1_analysis(v1_dir)

    async def _generate_pg_dump(self, db_version: str, db_config: Dict[str, Any],
                                output_dir: Path, include_data: bool, include_schema: bool):
        """Generate dump using pg_dump if available"""
        try:
            result = subprocess.run(
                ["which", "pg_dump"], capture_output=True, text=True)
            if result.returncode != 0:
                logger.warning(
                    "pg_dump not found, skipping pg_dump generation")
                return

            timestamp = self.timestamp

            if include_schema and include_data:
                dump_file = output_dir / \
                    f"{db_version}_complete_dump_{timestamp}.sql"
                cmd = [
                    "pg_dump",
                    "-h", db_config["host"],
                    "-p", str(db_config["port"]),
                    "-U", db_config["user"],
                    "-d", db_config["database"],
                    "--no-password",
                    "-f", str(dump_file)
                ]
            elif include_schema:
                dump_file = output_dir / \
                    f"{db_version}_schema_only_{timestamp}.sql"
                cmd = [
                    "pg_dump",
                    "-h", db_config["host"],
                    "-p", str(db_config["port"]),
                    "-U", db_config["user"],
                    "-d", db_config["database"],
                    "--no-password",
                    "--schema-only",
                    "-f", str(dump_file)
                ]
            elif include_data:
                dump_file = output_dir / \
                    f"{db_version}_data_only_{timestamp}.sql"
                cmd = [
                    "pg_dump",
                    "-h", db_config["host"],
                    "-p", str(db_config["port"]),
                    "-U", db_config["user"],
                    "-d", db_config["database"],
                    "--no-password",
                    "--data-only",
                    "-f", str(dump_file)
                ]
            else:
                return

            # Set password environment variable
            env = os.environ.copy()
            env["PGPASSWORD"] = db_config["password"]

            # Run pg_dump
            logger.info(f"Running pg_dump for {db_version} database...")
            result = subprocess.run(
                cmd, env=env, capture_output=True, text=True)

            if result.returncode == 0:
                logger.info(f"Successfully generated pg_dump: {dump_file}")
            else:
                logger.error(
                    f"pg_dump failed for {db_version}: {result.stderr}")

        except Exception as e:
            logger.error(f"Error running pg_dump for {db_version}: {e}")

    async def _generate_table_dumps(self, db_version: str, output_dir: Path,
                                    include_data: bool, include_schema: bool):
        """Generate table-by-table dumps using SQL queries"""
        logger.info(f"Generating table-by-table dumps for {db_version}...")

        try:
            # Get all tables
            tables = await self._get_all_tables(db_version)

            for table in tables:
                table_name = table['table_name']

                if include_schema:
                    await self._dump_table_schema(db_version, table_name, output_dir)

                if include_data:
                    await self._dump_table_data(db_version, table_name, output_dir)

        except Exception as e:
            logger.error(f"Error generating table dumps for {db_version}: {e}")

    async def _get_all_tables(self, db_version: str) -> List[Dict[str, str]]:
        """Get list of all tables in the database"""
        query = """
            SELECT table_name, table_type
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """

        return await db_manager.execute_query(query, engine_version=db_version)

    async def _dump_table_schema(self, db_version: str, table_name: str, output_dir: Path):
        """Dump schema for a specific table"""
        try:
            # Get table structure
            schema_query = f"""
                SELECT column_name, data_type, is_nullable, column_default, character_maximum_length
                FROM information_schema.columns 
                WHERE table_name = '{table_name}' 
                AND table_schema = 'public'
                ORDER BY ordinal_position
            """

            columns = await db_manager.execute_query(schema_query, engine_version=db_version)

            # Get constraints
            constraints_query = f"""
                SELECT tc.constraint_name, tc.constraint_type, kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu 
                    ON tc.constraint_name = kcu.constraint_name
                WHERE tc.table_name = '{table_name}'
                AND tc.table_schema = 'public'
            """

            constraints = await db_manager.execute_query(constraints_query, engine_version=db_version)

            # Generate CREATE TABLE statement
            schema_file = output_dir / \
                f"schema_{table_name}_{self.timestamp}.sql"

            with open(schema_file, 'w') as f:
                f.write(f"-- Schema for table: {table_name}\n")
                f.write(f"-- Generated: {datetime.now()}\n\n")

                f.write(f'CREATE TABLE IF NOT EXISTS "{table_name}" (\n')

                column_definitions = []
                for col in columns:
                    col_def = f'    "{col["column_name"]}" {col["data_type"]}'

                    if col["character_maximum_length"]:
                        col_def += f'({col["character_maximum_length"]})'

                    if col["is_nullable"] == "NO":
                        col_def += " NOT NULL"

                    if col["column_default"]:
                        col_def += f' DEFAULT {col["column_default"]}'

                    column_definitions.append(col_def)

                f.write(',\n'.join(column_definitions))
                f.write('\n);\n\n')

                # Add constraints
                for constraint in constraints:
                    if constraint["constraint_type"] == "PRIMARY KEY":
                        f.write(
                            f'ALTER TABLE "{table_name}" ADD CONSTRAINT {constraint["constraint_name"]} PRIMARY KEY ("{constraint["column_name"]}");\n')
                    elif constraint["constraint_type"] == "UNIQUE":
                        f.write(
                            f'ALTER TABLE "{table_name}" ADD CONSTRAINT {constraint["constraint_name"]} UNIQUE ("{constraint["column_name"]}");\n')

            logger.info(
                f"Generated schema dump for table {table_name}: {schema_file}")

        except Exception as e:
            logger.error(f"Error dumping schema for table {table_name}: {e}")

    async def _dump_table_data(self, db_version: str, table_name: str, output_dir: Path):
        """Dump data for a specific table"""
        try:
            # Get row count first
            count_query = f'SELECT COUNT(*) as count FROM "{table_name}"'
            count_result = await db_manager.execute_query(count_query, engine_version=db_version)
            row_count = count_result[0]['count'] if count_result else 0

            if row_count == 0:
                logger.info(f"Table {table_name} is empty, skipping data dump")
                return

            # Get sample data (limit to 1000 rows for large tables)
            limit = min(1000, row_count)
            data_query = f'SELECT * FROM "{table_name}" LIMIT {limit}'

            data = await db_manager.execute_query(data_query, engine_version=db_version)

            if not data:
                return

            # Generate INSERT statements
            data_file = output_dir / f"data_{table_name}_{self.timestamp}.sql"

            with open(data_file, 'w') as f:
                f.write(f"-- Data for table: {table_name}\n")
                f.write(f"-- Generated: {datetime.now()}\n")
                f.write(f"-- Total rows in table: {row_count}\n")
                f.write(f"-- Rows in this dump: {len(data)}\n\n")

                if data:
                    columns = list(data[0].keys())

                    for row in data:
                        values = []
                        for col in columns:
                            value = row[col]
                            if value is None:
                                values.append("NULL")
                            elif isinstance(value, str):
                                # Escape single quotes
                                escaped_value = value.replace("'", "''")
                                values.append(f"'{escaped_value}'")
                            elif isinstance(value, bool):
                                values.append("TRUE" if value else "FALSE")
                            else:
                                values.append(str(value))

                        column_list = '", "'.join(columns)
                        values_list = ', '.join(values)
                        f.write(
                            f'INSERT INTO "{table_name}" ("{column_list}") VALUES ({values_list});\n')

            logger.info(
                f"Generated data dump for table {table_name}: {data_file} ({len(data)} rows)")

        except Exception as e:
            logger.error(f"Error dumping data for table {table_name}: {e}")

    async def _generate_v1_analysis(self, output_dir: Path):
        """Generate V1-specific analysis"""
        try:
            analysis_file = output_dir / f"v1_analysis_{self.timestamp}.md"

            with open(analysis_file, 'w') as f:
                f.write("# V1 Database Analysis\n\n")
                f.write(f"Generated: {datetime.now()}\n\n")

                # Table counts
                f.write("## Table Row Counts\n\n")
                tables = await self._get_all_tables("v1")

                for table in tables:
                    table_name = table['table_name']
                    try:
                        count_query = f'SELECT COUNT(*) as count FROM "{table_name}"'
                        count_result = await db_manager.execute_query(count_query, engine_version="v1")
                        count = count_result[0]['count'] if count_result else 0
                        f.write(f"- **{table_name}**: {count:,} rows\n")
                    except Exception as e:
                        f.write(
                            f"- **{table_name}**: Error getting count - {e}\n")

                # School analysis
                f.write("\n## School Analysis\n\n")
                try:
                    school_query = '''
                        SELECT 
                            COUNT(*) as total_schools,
                            COUNT(CASE WHEN "isDeleted" = false THEN 1 END) as active_schools,
                            COUNT(CASE WHEN "isDeleted" = true THEN 1 END) as deleted_schools
                        FROM "School"
                    '''
                    school_stats = await db_manager.execute_query(school_query, engine_version="v1")
                    if school_stats:
                        stats = school_stats[0]
                        f.write(f"- Total Schools: {stats['total_schools']}\n")
                        f.write(
                            f"- Active Schools: {stats['active_schools']}\n")
                        f.write(
                            f"- Deleted Schools: {stats['deleted_schools']}\n")
                except Exception as e:
                    f.write(f"Error analyzing schools: {e}\n")

                # User entity analysis
                f.write("\n## User Entity Analysis\n\n")

                entities = ['Teacher', 'Parent', 'Student']
                for entity in entities:
                    try:
                        entity_query = f'''
                            SELECT 
                                COUNT(*) as total_{entity.lower()}s,
                                COUNT(CASE WHEN "isDeleted" = false THEN 1 END) as active_{entity.lower()}s,
                                COUNT(CASE WHEN "isDeleted" = true THEN 1 END) as deleted_{entity.lower()}s
                            FROM "{entity}"
                        '''
                        entity_stats = await db_manager.execute_query(entity_query, engine_version="v1")
                        if entity_stats:
                            stats = entity_stats[0]
                            f.write(f"### {entity}s\n")
                            f.write(
                                f"- Total: {stats[f'total_{entity.lower()}s']}\n")
                            f.write(
                                f"- Active: {stats[f'active_{entity.lower()}s']}\n")
                            f.write(
                                f"- Deleted: {stats[f'deleted_{entity.lower()}s']}\n\n")
                    except Exception as e:
                        f.write(f"Error analyzing {entity}s: {e}\n\n")

            logger.info(f"Generated V1 analysis: {analysis_file}")

        except Exception as e:
            logger.error(f"Error generating V1 analysis: {e}")

    async def _generate_v2_analysis(self, output_dir: Path):
        """Generate V2-specific analysis"""
        try:
            analysis_file = output_dir / f"v2_analysis_{self.timestamp}.md"

            with open(analysis_file, 'w') as f:
                f.write("# V2 Database Analysis\n\n")
                f.write(f"Generated: {datetime.now()}\n\n")

                # Table counts
                f.write("## Table Row Counts\n\n")
                tables = await self._get_all_tables("v2")

                for table in tables:
                    table_name = table['table_name']
                    try:
                        count_query = f'SELECT COUNT(*) as count FROM "{table_name}"'
                        count_result = await db_manager.execute_query(count_query, engine_version="v2")
                        count = count_result[0]['count'] if count_result else 0
                        f.write(f"- **{table_name}**: {count:,} rows\n")
                    except Exception as e:
                        f.write(
                            f"- **{table_name}**: Error getting count - {e}\n")

                # V2 specific analysis
                f.write("\n## V2 Schema Analysis\n\n")

                # User table analysis
                try:
                    user_query = '''
                        SELECT 
                            role,
                            COUNT(*) as count
                        FROM "User"
                        GROUP BY role
                        ORDER BY count DESC
                    '''
                    user_stats = await db_manager.execute_query(user_query, engine_version="v2")
                    if user_stats:
                        f.write("### User Role Distribution\n")
                        for stat in user_stats:
                            f.write(f"- {stat['role']}: {stat['count']}\n")
                        f.write("\n")
                except Exception as e:
                    f.write(f"Error analyzing users: {e}\n\n")

                # School analysis
                try:
                    school_query = '''
                        SELECT 
                            COUNT(*) as total_schools,
                            COUNT(CASE WHEN is_active = true THEN 1 END) as active_schools,
                            COUNT(CASE WHEN is_deleted = true THEN 1 END) as deleted_schools
                        FROM "School"
                    '''
                    school_stats = await db_manager.execute_query(school_query, engine_version="v2")
                    if school_stats:
                        stats = school_stats[0]
                        f.write("### Schools\n")
                        f.write(f"- Total Schools: {stats['total_schools']}\n")
                        f.write(
                            f"- Active Schools: {stats['active_schools']}\n")
                        f.write(
                            f"- Deleted Schools: {stats['deleted_schools']}\n\n")
                except Exception as e:
                    f.write(f"Error analyzing schools: {e}\n\n")

            logger.info(f"Generated V2 analysis: {analysis_file}")

        except Exception as e:
            logger.error(f"Error generating V2 analysis: {e}")

    async def generate_schema_comparison(self):
        """Generate comparison between V1 and V2 schemas"""
        try:
            comparison_file = self.output_dir / \
                f"schema_comparison_{self.timestamp}.md"

            with open(comparison_file, 'w') as f:
                f.write("# V1 vs V2 Schema Comparison\n\n")
                f.write(f"Generated: {datetime.now()}\n\n")

                # Get tables from both databases
                v1_tables = await self._get_all_tables("v1")
                v2_tables = await self._get_all_tables("v2")

                v1_table_names = {table['table_name'] for table in v1_tables}
                v2_table_names = {table['table_name'] for table in v2_tables}

                f.write(f"## Table Comparison\n\n")
                f.write(f"- V1 Tables: {len(v1_table_names)}\n")
                f.write(f"- V2 Tables: {len(v2_table_names)}\n\n")

                # Tables only in V1
                v1_only = v1_table_names - v2_table_names
                if v1_only:
                    f.write("### Tables only in V1:\n")
                    for table in sorted(v1_only):
                        f.write(f"- {table}\n")
                    f.write("\n")

                # Tables only in V2
                v2_only = v2_table_names - v1_table_names
                if v2_only:
                    f.write("### Tables only in V2:\n")
                    for table in sorted(v2_only):
                        f.write(f"- {table}\n")
                    f.write("\n")

                # Common tables
                common_tables = v1_table_names & v2_table_names
                if common_tables:
                    f.write("### Common Tables:\n")
                    for table in sorted(common_tables):
                        f.write(f"- {table}\n")
                    f.write("\n")

                # Migration mapping analysis
                f.write("## Migration Mapping Analysis\n\n")
                f.write("### Key Entity Mappings:\n")
                f.write("- V1 separate auth tables → V2 unified User table\n")
                f.write(
                    "- V1 Teacher/Parent/Student → V2 User + role-specific tables\n")
                f.write("- V1 School → V2 School (enhanced with curriculum)\n")
                f.write("- V1 Class → V2 SchoolClass (restructured)\n")

            logger.info(f"Generated schema comparison: {comparison_file}")

        except Exception as e:
            logger.error(f"Error generating schema comparison: {e}")

    async def generate_data_statistics(self):
        """Generate comprehensive data statistics"""
        try:
            stats_file = self.output_dir / \
                f"data_statistics_{self.timestamp}.md"

            with open(stats_file, 'w') as f:
                f.write("# Migration Data Statistics\n\n")
                f.write(f"Generated: {datetime.now()}\n\n")

                f.write("## Summary\n\n")
                f.write(
                    "This report provides comprehensive statistics about the data to be migrated.\n\n")

                # Migration readiness analysis
                f.write("## Migration Readiness\n\n")

                try:
                    # School readiness
                    school_query = '''
                        SELECT 
                            COUNT(*) as total,
                            COUNT(CASE WHEN "schoolName" IS NOT NULL AND "schoolName" != '' THEN 1 END) as with_name,
                            COUNT(CASE WHEN email IS NOT NULL AND email != '' THEN 1 END) as with_email,
                            COUNT(CASE WHEN "schoolCode" IS NOT NULL AND "schoolCode" != '' THEN 1 END) as with_code
                        FROM "School"
                        WHERE "isDeleted" = false
                    '''
                    school_stats = await db_manager.execute_query(school_query, engine_version="v1")

                    if school_stats:
                        stats = school_stats[0]
                        f.write("### School Data Quality\n")
                        f.write(f"- Total active schools: {stats['total']}\n")
                        f.write(
                            f"- Schools with name: {stats['with_name']} ({stats['with_name']/stats['total']*100:.1f}%)\n")
                        f.write(
                            f"- Schools with email: {stats['with_email']} ({stats['with_email']/stats['total']*100:.1f}%)\n")
                        f.write(
                            f"- Schools with code: {stats['with_code']} ({stats['with_code']/stats['total']*100:.1f}%)\n\n")

                except Exception as e:
                    f.write(f"Error analyzing school data quality: {e}\n\n")

                # Entity-school relationship analysis
                f.write("### Entity-School Relationships\n\n")

                entities = ['Teacher', 'Parent', 'Student']
                for entity in entities:
                    try:
                        relationship_query = f'''
                            SELECT 
                                COUNT(*) as total,
                                COUNT(CASE WHEN "schoolId" IS NOT NULL THEN 1 END) as with_school,
                                COUNT(CASE WHEN s.id IS NOT NULL THEN 1 END) as valid_school
                            FROM "{entity}" e
                            LEFT JOIN "School" s ON e."schoolId" = s.id AND s."isDeleted" = false
                            WHERE e."isDeleted" = false
                        '''
                        stats = await db_manager.execute_query(relationship_query, engine_version="v1")

                        if stats:
                            stat = stats[0]
                            f.write(f"#### {entity}s\n")
                            f.write(f"- Total active: {stat['total']}\n")
                            f.write(
                                f"- With school ID: {stat['with_school']} ({stat['with_school']/stat['total']*100:.1f}%)\n")
                            f.write(
                                f"- Valid school ref: {stat['valid_school']} ({stat['valid_school']/stat['total']*100:.1f}%)\n")

                            if stat['total'] > stat['valid_school']:
                                orphaned = stat['total'] - stat['valid_school']
                                f.write(
                                    f"- **⚠️ Orphaned records: {orphaned}**\n")
                            f.write("\n")

                    except Exception as e:
                        f.write(
                            f"Error analyzing {entity} relationships: {e}\n\n")

                # Data volume estimation
                f.write("## Migration Volume Estimation\n\n")
                f.write(
                    "Based on the analysis above, the following data will be migrated:\n\n")

                try:
                    total_users = 0
                    for entity in entities:
                        count_query = f'SELECT COUNT(*) as count FROM "{entity}" WHERE "isDeleted" = false'
                        result = await db_manager.execute_query(count_query, engine_version="v1")
                        if result:
                            count = result[0]['count']
                            total_users += count
                            f.write(f"- {entity}s: {count:,} records\n")

                    f.write(
                        f"\n**Total User records to create: {total_users:,}**\n\n")

                except Exception as e:
                    f.write(f"Error estimating migration volume: {e}\n\n")

                # Recommendations
                f.write("## Recommendations\n\n")
                f.write("Based on this analysis:\n\n")
                f.write(
                    "1. **Data Quality**: Review any records missing required fields\n")
                f.write(
                    "2. **Orphaned Records**: Fix entity-school relationships before migration\n")
                f.write(
                    "3. **Volume Planning**: Prepare for the estimated data volume\n")
                f.write(
                    "4. **Backup Strategy**: Ensure comprehensive backups before migration\n")

            logger.info(f"Generated data statistics: {stats_file}")

        except Exception as e:
            logger.error(f"Error generating data statistics: {e}")


async def main():
    """Main function to run dump generation"""
    parser = argparse.ArgumentParser(
        description="Generate Supabase database dumps")
    parser.add_argument("--output-dir", default="database_dumps",
                        help="Output directory for dumps")
    parser.add_argument("--no-data", action="store_true",
                        help="Skip data dumps, schema only")
    parser.add_argument("--no-schema", action="store_true",
                        help="Skip schema dumps, data only")
    parser.add_argument("--v1-only", action="store_true",
                        help="Only dump V1 database")
    parser.add_argument("--v2-only", action="store_true",
                        help="Only dump V2 database")

    args = parser.parse_args()

    # Validate arguments
    if args.no_data and args.no_schema:
        print("Error: Cannot skip both data and schema")
        sys.exit(1)

    include_data = not args.no_data
    include_schema = not args.no_schema

    # Create dump generator
    generator = SupabaseDumpGenerator(args.output_dir)

    try:
        if args.v1_only:
            await generator.generate_v1_dumps(include_data, include_schema)
        else:
            await generator.generate_all_dumps(include_data, include_schema)

        print(
            f"\n✅ Database dumps generated successfully in: {generator.output_dir}")
        print(f"📊 Check the analysis files for migration readiness assessment")

    except Exception as e:
        print(f"\n❌ Error generating dumps: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
