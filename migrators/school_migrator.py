"""
Enhanced School migration from V1 to V2 schema
Provides robust validation, error handling, and data integrity
"""

import logging
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime

from db_utils import db_manager
from config import DEFAULT_CURRICULUM_ID, DEFAULT_GRADE_SYSTEM_ID
from user_utils import user_manager, UserType, UserData
from migration_session import get_migration_session, MigrationPhase
from validation_utils import enhanced_validator, prerequisite_manager, EnhancedValidationResult

logger = logging.getLogger(__name__)


class SchoolMigrator:
    """Enhanced School migrator with comprehensive validation and error handling"""
    
    def __init__(self):
        self.school_mappings = {}  # Maps V1 school IDs to V2 school IDs
        self.admin_user_mappings = {}  # Maps V1 school IDs to V2 admin user IDs
        self.migrated_count = 0
        self.failed_count = 0
        self.validation_errors = []
        self.validation_warnings = []

    async def migrate_schools(self) -> Dict[str, Any]:
        """Enhanced main method to migrate all schools"""
        logger.info("Starting enhanced school migration...")
        
        # Get migration session
        session = get_migration_session()
        if not session:
            logger.error("No active migration session - create one first")
            return {"success": False, "error": "No active migration session"}
        
        # Start schools phase
        await session.start_phase(MigrationPhase.SCHOOLS)
        
        try:
            # Step 1: Get all schools from V1
            v1_schools = await self._get_v1_schools()
            logger.info(f"Found {len(v1_schools)} schools in V1 database")
            
            if not v1_schools:
                logger.warning("No schools found in V1 database")
                return {"success": True, "migrated": 0, "failed": 0, "total": 0}
            
            session.stats["schools"]["total"] = len(v1_schools)
            
            # Step 2: Set up prerequisites
            school_ids = [school['id'] for school in v1_schools]
            prerequisites = await prerequisite_manager.setup_prerequisites(school_ids)
            logger.info("Prerequisites setup completed")
            
            # Step 3: Validate all schools before migration
            validated_schools = await self._validate_all_schools(v1_schools)
            
            # Step 4: Migrate each validated school
            for school_data in validated_schools:
                success = await self._migrate_single_school_enhanced(
                    school_data, prerequisites)
                if success:
                    self.migrated_count += 1
                else:
                    self.failed_count += 1
                    session.stats["schools"]["failed"] += 1
            
            # Step 5: Create school admin users for successfully migrated schools
            await self._create_school_admin_users_enhanced(validated_schools)
            
            # Step 6: Validate migration integrity
            await self._validate_migration_integrity()
            
            result = {
                "success": True,
                "migrated": self.migrated_count,
                "failed": self.failed_count,
                "total": len(v1_schools),
                "validation_errors": len(self.validation_errors),
                "validation_warnings": len(self.validation_warnings),
                "school_mappings": self.school_mappings,
                "admin_user_mappings": self.admin_user_mappings
            }
            
            logger.info(
                f"Enhanced school migration completed: {self.migrated_count} successful, "
                f"{self.failed_count} failed, {len(self.validation_errors)} validation errors, "
                f"{len(self.validation_warnings)} warnings")
            
            return result
            
        except Exception as e:
            logger.error(f"Enhanced school migration failed: {e}")
            return {"success": False, "error": str(e)}

    async def _get_v1_schools(self) -> List[Dict[str, Any]]:
        """Get all schools from V1 database with comprehensive field selection"""
        query = """
            SELECT 
                id, "schoolName", email, phone, "schoolCode", "schoolLevel",
                "schoolMotto", "schoolVision", country, county, logo, 
                "schoolAddress", "isActive", "isVerified", "isDeleted",
                "CreatedAt", "UpdatedAt", type, password,
                "lastLoggedIn", "deletedAt"
            FROM "School"
            WHERE "isDeleted" = false
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

    async def _validate_all_schools(self, v1_schools: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Validate all schools and collect validation results"""
        logger.info("Validating all schools before migration...")
        
        validated_schools = []
        
        for v1_school in v1_schools:
            validation_result = enhanced_validator.validate_v1_school(v1_school)
            
            # Collect validation issues
            self.validation_errors.extend([
                f"School {v1_school.get('schoolName', 'Unknown')}: {error}" 
                for error in validation_result.errors
            ])
            self.validation_warnings.extend([
                f"School {v1_school.get('schoolName', 'Unknown')}: {warning}" 
                for warning in validation_result.warnings
            ])
            
            if validation_result.is_valid:
                # Use corrected data for migration
                validated_schools.append(validation_result.corrected_data)
                logger.info(f"Validated school: {v1_school.get('schoolName')}")
            else:
                logger.error(
                    f"School {v1_school.get('schoolName')} failed validation: "
                    f"{validation_result.errors}")
        
        logger.info(
            f"Validation completed: {len(validated_schools)} valid schools, "
            f"{len(self.validation_errors)} errors, {len(self.validation_warnings)} warnings")
        
        return validated_schools
    
    async def _migrate_single_school_enhanced(
        self, v1_school: Dict[str, Any], 
        prerequisites) -> bool:
        """Enhanced single school migration with comprehensive error handling"""
        
        school_name = v1_school.get('schoolName', 'Unknown')
        
        try:
            session = get_migration_session()
            
            # Use pre-validated level ID
            level_id = v1_school.get('_mapped_level_id')
            if not level_id:
                logger.error(f"No mapped level ID for school {school_name}")
                return False
            
            # Prepare enhanced school data for V2
            school_data = {
                "name": v1_school['schoolName'],
                "email": v1_school['email'],
                "country": v1_school.get('country', 'Kenya'),
                "county": v1_school.get('county', 'Nairobi'),
                "curriculum_id": prerequisites.curriculum_id,
                "is_deleted": v1_school.get('isDeleted', False),
                "is_active": v1_school.get('isActive', True),
                "address": v1_school.get('schoolAddress', ''),
                "phone_number": v1_school.get('phone', ''),
                "type": v1_school.get('type', 'PRIVATE'),
                "logo": v1_school.get('logo'),
                "government_code": v1_school.get('schoolCode', ''),
                "is_verified": v1_school.get('isVerified', False),
                "level_id": level_id,
                "grade_system_id": prerequisites.grade_system_id,
                "vision_statement": v1_school.get('schoolVision'),
                "motto": v1_school.get('schoolMotto'),
                "onboarded": True,
                "created_at": v1_school.get('CreatedAt', datetime.now()),
                "updated_at": v1_school.get('UpdatedAt', datetime.now())
            }
            
            # Remove None values
            school_data = {k: v for k, v in school_data.items() if v is not None}
            
            # Handle potential duplicate email/government code
            school_data = await self._handle_duplicate_constraints(school_data, v1_school)
            
            # Insert school into V2
            v2_school_id = await db_manager.insert_record("schools", school_data)
            
            if v2_school_id:
                # Add to mappings
                self.school_mappings[v1_school['id']] = v2_school_id
                
                # Add to session mapping
                if session:
                    session.add_school_mapping(v1_school['id'], v2_school_id, v1_school)
                
                logger.info(
                    f"Successfully migrated school: {school_name} "
                    f"(V1: {v1_school['id']} -> V2: {v2_school_id})")
                return True
            else:
                logger.error(f"Failed to create school: {school_name}")
                return False
                
        except Exception as e:
            logger.error(f"Error migrating school {school_name}: {e}")
            logger.error(f"School data: {v1_school}")
            return False
    
    async def _handle_duplicate_constraints(
        self, school_data: Dict[str, Any], 
        v1_school: Dict[str, Any]) -> Dict[str, Any]:
        """Handle potential duplicate constraint violations"""
        
        # Handle duplicate email
        email = school_data.get('email')
        if email:
            existing_email = await db_manager.execute_query(
                "SELECT id FROM schools WHERE email = :email LIMIT 1",
                {"email": email}
            )
            
            if existing_email:
                # Generate unique email
                base_email, domain = email.split('@', 1)
                unique_email = f"{base_email}+school{v1_school['id']}@{domain}"
                school_data['email'] = unique_email
                logger.warning(
                    f"Duplicate email {email} detected, using {unique_email}")
        
        # Handle duplicate government code
        gov_code = school_data.get('government_code')
        if gov_code:
            existing_code = await db_manager.execute_query(
                "SELECT id FROM schools WHERE government_code = :code LIMIT 1",
                {"code": gov_code}
            )
            
            if existing_code:
                # Generate unique code
                unique_code = f"{gov_code}_{v1_school['id']}"
                school_data['government_code'] = unique_code
                logger.warning(
                    f"Duplicate government code {gov_code} detected, using {unique_code}")
        
        return school_data
    
    async def _create_school_admin_users_enhanced(
        self, validated_schools: List[Dict[str, Any]]):
        """Enhanced school admin user creation with validation"""
        
        logger.info("Creating enhanced school admin users...")
        
        for v1_school in validated_schools:
            v2_school_id = self.school_mappings.get(v1_school['id'])
            if not v2_school_id:
                continue
            
            school_name = v1_school.get('schoolName', 'Unknown')
            
            # V1 schools had direct email/password - create admin user
            email = v1_school.get('email')
            password = v1_school.get('password')
            
            if email and password:
                try:
                    admin_user_id = await self._create_single_admin_user(
                        v1_school, v2_school_id)
                    
                    if admin_user_id:
                        # Create SchoolAdmin record
                        await self._create_school_admin_record(admin_user_id, v2_school_id)
                        
                        self.admin_user_mappings[v1_school['id']] = admin_user_id
                        logger.info(f"Created school admin for {school_name}")
                    else:
                        logger.error(f"Failed to create admin user for {school_name}")
                        
                except Exception as e:
                    logger.error(f"Error creating school admin for {school_name}: {e}")
            else:
                logger.warning(f"School {school_name} has no email/password for admin creation")
    
    async def _create_single_admin_user(
        self, v1_school: Dict[str, Any], v2_school_id: int) -> Optional[int]:
        """Create a single admin user with validation"""
        
        school_name = v1_school.get('schoolName', 'Unknown')
        admin_name = self._extract_admin_name(school_name)
        
        # Handle potential duplicate email
        base_email = v1_school['email']
        admin_email = await self._ensure_unique_admin_email(base_email, v1_school['id'])
        
        # Create user data for school admin
        user_data = UserData(
            first_name=admin_name,
            middle_name=None,
            last_name="Admin",
            email=admin_email,
            phone_number=v1_school.get('phone'),
            password=v1_school.get('password'),
            user_type=UserType.SCHOOL_ADMIN,
            school_id=v2_school_id,
            is_active=v1_school.get('isActive', True),
            is_verified=v1_school.get('isVerified', False),
            country=v1_school.get('country', 'Kenya'),
            v1_id=f"school_admin_{v1_school['id']}"
        )
        
        return await user_manager.create_user(user_data)
    
    async def _ensure_unique_admin_email(self, base_email: str, v1_school_id: str) -> str:
        """Ensure admin email is unique"""
        # Check if base email is already taken by a user
        existing = await db_manager.execute_query(
            "SELECT id FROM users WHERE email = :email LIMIT 1",
            {"email": base_email}
        )
        
        if existing:
            # Generate unique admin email
            local_part, domain = base_email.split('@', 1)
            admin_email = f"{local_part}+admin{v1_school_id}@{domain}"
            
            # Double-check uniqueness
            existing_admin = await db_manager.execute_query(
                "SELECT id FROM users WHERE email = :email LIMIT 1",
                {"email": admin_email}
            )
            
            if existing_admin:
                # Add timestamp for absolute uniqueness
                timestamp = datetime.now().strftime('%m%d%H%M')
                admin_email = f"{local_part}+admin{v1_school_id}{timestamp}@{domain}"
            
            logger.warning(f"Email {base_email} taken, using {admin_email} for admin")
            return admin_email
        
        return base_email
    
    async def _create_school_admin_record(self, admin_user_id: int, v2_school_id: int):
        """Create SchoolAdmin record"""
        admin_data = {
            "user_id": admin_user_id,
            "school_id": v2_school_id,
            "is_main": True,  # Mark as main admin
            "position": "Principal"  # Default position
        }
        
        await db_manager.insert_record("school_admins", admin_data)
    
    async def _validate_migration_integrity(self):
        """Validate the integrity of migrated schools"""
        logger.info("Validating migration integrity...")
        
        # Check that all migrated schools have required references
        integrity_issues = []
        
        for v1_id, v2_id in self.school_mappings.items():
            # Verify school exists and has valid references
            school_check = await db_manager.execute_query(
                """
                SELECT s.id, s.name, s.curriculum_id, s.level_id, s.grade_system_id,
                       c.id as curr_exists, cl.id as level_exists, gs.id as grade_exists
                FROM schools s
                LEFT JOIN curriculums c ON s.curriculum_id = c.id
                LEFT JOIN class_levels cl ON s.level_id = cl.id
                LEFT JOIN grade_systems gs ON s.grade_system_id = gs.id
                WHERE s.id = :school_id
                """,
                {"school_id": v2_id}
            )
            
            if not school_check:
                integrity_issues.append(f"School {v2_id} not found after migration")
                continue
            
            school = school_check[0]
            
            if not school['curr_exists']:
                integrity_issues.append(
                    f"School {school['name']} references non-existent curriculum {school['curriculum_id']}")
            
            if not school['level_exists']:
                integrity_issues.append(
                    f"School {school['name']} references non-existent class level {school['level_id']}")
            
            if school['grade_system_id'] and not school['grade_exists']:
                integrity_issues.append(
                    f"School {school['name']} references non-existent grade system {school['grade_system_id']}")
        
        if integrity_issues:
            logger.error(f"Migration integrity issues found: {integrity_issues}")
            self.validation_errors.extend(integrity_issues)
        else:
            logger.info("Migration integrity validation passed")
    
    def get_admin_user_id(self, v1_school_id: str) -> Optional[int]:
        """Get admin user ID for a V1 school"""
        return self.admin_user_mappings.get(v1_school_id)
    
    def get_migration_report(self) -> Dict[str, Any]:
        """Get comprehensive migration report"""
        return {
            "total_schools": self.migrated_count + self.failed_count,
            "migrated_successfully": self.migrated_count,
            "migration_failures": self.failed_count,
            "validation_errors": len(self.validation_errors),
            "validation_warnings": len(self.validation_warnings),
            "admin_users_created": len(self.admin_user_mappings),
            "school_mappings_count": len(self.school_mappings),
            "success_rate": (self.migrated_count / (self.migrated_count + self.failed_count) * 100) if (self.migrated_count + self.failed_count) > 0 else 0,
            "detailed_errors": self.validation_errors[:10],  # First 10 errors
            "detailed_warnings": self.validation_warnings[:10]  # First 10 warnings
        }


# Global enhanced school migrator instance
school_migrator = SchoolMigrator()
