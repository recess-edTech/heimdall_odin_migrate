"""
School migration from V1 to V2 schema
Schools are migrated first as they have the fewest dependencies
"""

import logging
from typing import Dict, Any, List, Optional
import pandas as pd
from typing import Union, Literal

from ..db_utils import db_manager
from ..config import DEFAULT_CURRICULUM_ID, CLASS_LEVELS, DEFAULT_GRADE_SYSTEM_ID
from ..user_utils import user_manager, UserType, UserData
from ..migration_session import get_migration_session, MigrationPhase

logger = logging.getLogger(__name__)


class SchoolMigrator:
    """Handles migration of schools from V1 to V2"""

    def __init__(self):
        self.school_mappings = {}  # Maps V1 school IDs to V2 school IDs
        self.migrated_count = 0
        self.failed_count = 0

    async def migrate_schools(self) -> Dict[str, Any]:
        """Main method to migrate all schools"""
        logger.info("Starting school migration...")

        # Get migration session
        session = get_migration_session()
        if not session:
            logger.error("No active migration session - create one first")
            return {"success": False, "error": "No active migration session"}

        # Start schools phase
        await session.start_phase(MigrationPhase.SCHOOLS)

        try:
            # Get all schools from V1
            v1_schools = await self._get_v1_schools()
            logger.info(f"Found {len(v1_schools)} schools in V1 database")

            session.stats["schools"]["total"] = len(v1_schools)

            # Migrate each school
            for school in v1_schools:
                success = await self._migrate_single_school(school)
                if success:
                    self.migrated_count += 1
                else:
                    self.failed_count += 1
                    session.stats["schools"]["failed"] += 1

            # Create school admin users for each school
            await self._create_school_admin_users(v1_schools)

            # Validate all schools have curriculums assigned
            await session.start_phase(MigrationPhase.CURRICULUMS)

            result = {
                "success": True,
                "migrated": self.migrated_count,
                "failed": self.failed_count,
                "total": len(v1_schools),
                "mappings": self.school_mappings
            }

            logger.info(
                f"School migration completed: {self.migrated_count} successful, {self.failed_count} failed")
            return result

        except Exception as e:
            logger.error(f"School migration failed: {e}")
            return {"success": False, "error": str(e)}

    async def _get_v1_schools(self) -> List[Dict[str, Any]]:
        """Get all schools from V1 database"""
        query = """
            SELECT 
                id, schoolName, email, phone, schoolCode, schoolLevel,
                schoolMotto, schoolVision, country, county, logo, 
                schoolAddress, isActive, isVerified, isDeleted,
                CreatedAt, UpdatedAt, type
            FROM "School"
            WHERE isDeleted = false
            ORDER BY "CreatedAt"
        """

        return await db_manager.execute_query(query, engine_version="v1")

    async def _migrate_single_school(self, v1_school: Dict[str, Any]) -> bool:
        """Migrate a single school from V1 to V2"""
        try:
            session = get_migration_session()

            # Map school level to class level (this might need customization)
            level_id = await self.get_school_level_id(self.determine_school_level(v1_school))

            # Prepare school data for V2
            school_data = {
                "name": v1_school['schoolname'],
                "email": v1_school['email'],
                "country": v1_school.get('country', 'Kenya'),
                "county": v1_school.get('county', 'Nairobi'),
                "curriculum_id": DEFAULT_CURRICULUM_ID, # all schools have the same curriculum
                "is_deleted": v1_school.get('isdeleted', False),
                "is_active": v1_school.get('isactive', True),
                "address": v1_school.get('schooladdress', ''),
                "phone_number": v1_school['phone'],
                "type": self._map_school_type(v1_school.get('type', 'PRIVATE')),
                "logo": v1_school.get('logo'),
                "government_code": v1_school['schoolcode'],
                "is_verified": v1_school.get('isverified', False),
                "level_id": level_id,
                "grade_system_id": DEFAULT_GRADE_SYSTEM_ID,
                "vision_statement": v1_school.get('schoolvision'),
                "motto": v1_school.get('schoolmotto'),
                "vision": v1_school.get('schoolvision'),  # Legacy field
                "onboarded": True,
            }

            # Remove None values
            school_data = {k: v for k, v in school_data.items()
                           if v is not None}

            # Insert school into V2
            v2_school_id = await db_manager.insert_record("schools", school_data)

            if v2_school_id:
                # Add to legacy mapping
                self.school_mappings[v1_school['id']] = v2_school_id

                # Add to session mapping with curriculum validation
                if session:
                    session.add_school_mapping(
                        v1_school['id'], v2_school_id, v1_school)
                    await session.validate_and_assign_curriculum(v1_school['id'], v2_school_id, v1_school)

                logger.info(
                    f"Migrated school: {v1_school['schoolname']} (V1: {v1_school['id']} -> V2: {v2_school_id})")
                return True
            else:
                logger.error(
                    f"Failed to create school: {v1_school['schoolname']}")
                return False

        except Exception as e:
            logger.error(
                f"Error migrating school {v1_school.get('schoolname', 'Unknown')}: {e}")
            logger.error(f"School data: {v1_school}")
            return False

    def get_v2_school_id(self, v1_school_id: int) -> Optional[int]:
        """Get V2 school ID from V1 school ID using migration session or fallback to local mapping"""
        session = get_migration_session()
        if session:
            return session.get_v2_school_id(v1_school_id)

        # Fallback to local mapping for backwards compatibility
        return self.school_mappings.get(v1_school_id)

    def _map_school_type(self, v1_type: str) -> str:
        """Map V1 school type to V2 school type"""
        type_mapping = {
            "PRIVATE": "PRIVATE",
            "PUBLIC": "PUBLIC",
            "INTERNATIONAL": "INTERNATIONAL"
        }

        return type_mapping.get(v1_type, "PRIVATE")

    async def _create_school_admin_users(self, v1_schools: List[Dict[str, Any]]):
        """Create school admin users for schools that had direct login credentials"""
        logger.info("Creating school admin users...")

        for v1_school in v1_schools:
            v2_school_id = self.school_mappings.get(v1_school['id'])
            if not v2_school_id:
                continue

            # V1 schools had direct email/password - create admin user
            if v1_school.get('email') and v1_school.get('password'):
                try:
                    # Extract name from school name for admin user
                    admin_name = self._extract_admin_name(
                        v1_school['schoolname'])

                    # Create user data for school admin
                    user_data = UserData(
                        first_name=admin_name,
                        middle_name=None,
                        last_name="Admin",
                        email=v1_school['email'],
                        phone_number=v1_school.get('phone'),
                        password=v1_school.get('password'),
                        user_type=UserType.SCHOOL_ADMIN,
                        school_id=v2_school_id,
                        is_active=v1_school.get('isactive', True),
                        is_verified=v1_school.get('isverified', False),
                        v1_id=f"school_admin_{v1_school['id']}"
                    )

                    # Create user
                    admin_user_id = await user_manager.create_user(user_data)

                    if admin_user_id:
                        # Create SchoolAdmin record
                        admin_data = {
                            "user_id": admin_user_id,
                            "school_id": v2_school_id,
                            "is_main": True,  # Mark as main admin
                            "position": "Principal"  # Default position
                        }

                        await db_manager.insert_record("school_admins", admin_data)
                        logger.info(
                            f"Created school admin for {v1_school['schoolname']}")

                except Exception as e:
                    logger.error(
                        f"Error creating school admin for {v1_school['schoolname']}: {e}")

    def _extract_admin_name(self, school_name: str) -> str:
        """Extract a reasonable admin name from school name"""
        # Simple logic - take first word or use "School"
        words = school_name.split()
        if words:
            return words[0]
        return "School"

    def get_mapping_stats(self) -> Dict[str, int]:
        """Get statistics about school migration"""
        return {
            "total_migrated": self.migrated_count,
            "total_failed": self.failed_count,
            "total_mappings": len(self.school_mappings)
        }

    def determine_school_level(self, v1_school: Dict[str, Any]) -> str:
        """Determine school level based on migrated data"""
        # V1 has a column directly in the database while V2 has a separate table for levels
        if "schoolLevel" not in v1_school or v1_school["schoolLevel"] is None:
            raise ValueError("schoolLevel is missing in V1 school data")
        school_level = v1_school["schoolLevel"].upper()
        return school_level

        "since v1 schoolLevel name is same as v2 we can fetch the id directly from v2"

    async def get_school_level_id(self, school_level: str) -> int:
        """Get V2 class level ID from school level name"""
        print('checking for school level from the default values')
        """
        Given a string, tries to match it against known levels.
        Uses case-insensitive substring matching.
        Raises ValueError if nothing matches.
        """
        value = school_level.lower().strip()
        for key, level_id in CLASS_LEVELS.items():
            if key in value or value in key:
                return level_id
        raise ValueError(f"Unknown school level: {school_level}")

        # Global school migrator instance
school_migrator = SchoolMigrator()
