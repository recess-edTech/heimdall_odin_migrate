"""
Teacher migration from V1 to V2 schema
Creates User records and Teacher records for each V1 teacher
"""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime

from ..db_utils import db_manager
from ..user_utils import user_manager
from ..migration_session import get_migration_session, MigrationPhase
from .school_migrator import school_migrator

logger = logging.getLogger(__name__)


class TeacherMigrator:
    """Handles migration of teachers from V1 to V2"""
    
    def __init__(self):
        self.teacher_mappings = {}  # Maps V1 teacher IDs to V2 user IDs
        self.migrated_count = 0
        self.failed_count = 0
        
    async def migrate_teachers(self) -> Dict[str, Any]:
        """Main method to migrate all teachers"""
        logger.info("Starting teacher migration...")
        
        # Get migration session
        session = get_migration_session()
        if not session:
            logger.error("No active migration session - create one first")
            return {"success": False, "error": "No active migration session"}
        
        # Start teachers phase
        await session.start_phase(MigrationPhase.TEACHERS)
        
        try:
            # Get all teachers from V1
            v1_teachers = await self._get_v1_teachers()
            logger.info(f"Found {len(v1_teachers)} teachers in V1 database")
            
            session.stats["teachers"]["total"] = len(v1_teachers)
            
            # Migrate each teacher
            for teacher in v1_teachers:
                success = await self._migrate_single_teacher(teacher)
                if success:
                    self.migrated_count += 1
                else:
                    self.failed_count += 1
                    session.stats["teachers"]["failed"] += 1
            
            result = {
                "success": True,
                "migrated": self.migrated_count,
                "failed": self.failed_count,
                "total": len(v1_teachers),
                "mappings": self.teacher_mappings
            }
            
            logger.info(f"Teacher migration completed: {self.migrated_count} successful, {self.failed_count} failed")
            return result
            
        except Exception as e:
            logger.error(f"Teacher migration failed: {e}")
            return {"success": False, "error": str(e)}
    
    async def _get_v1_teachers(self) -> List[Dict[str, Any]]:
        """Get all teachers from V1 database"""
        query = """
            SELECT 
                id, firstName, middleName, lastName, gender, experience,
                isLoginBarred, phoneNumber, email, profileImage, password,
                isFirstTimePasswordChanged, schoolId, schoolDepartmentId,
                isClassTeacher, isDeleted, subjects, experienceYears,
                lastLoggedIn
            FROM "Teacher"
            WHERE isDeleted = false
            ORDER BY "schoolId", firstName
        """
        
        return await db_manager.execute_query(query, engine_version="v1")
    
    async def _migrate_single_teacher(self, v1_teacher: Dict[str, Any]) -> bool:
        """Migrate a single teacher from V1 to V2 with strict school validation"""
        try:
            session = get_migration_session()
            
            # CRITICAL: Validate V1 school exists in migration session
            v1_school_id = v1_teacher.get('schoolid')
            if not v1_school_id:
                logger.error(f"Teacher {v1_teacher.get('firstname', '')} {v1_teacher.get('lastname', '')} has no school ID")
                return False
            
            # Validate the V1 school was successfully migrated
            if session and not session.validate_v1_school_reference(v1_school_id, "teacher", v1_teacher):
                return False
            
            # Get V2 school ID from session (more reliable than direct migrator call)
            if session:
                v2_school_id = session.get_v2_school_id(v1_school_id)
            else:
                # Fallback for when session isn't available (shouldn't happen)
                v2_school_id = school_migrator.get_v2_school_id(v1_school_id)
            
            if not v2_school_id:
                error_msg = f"No V2 school mapping found for teacher {v1_teacher.get('firstname', '')} {v1_teacher.get('lastname', '')} with V1 school ID: {v1_school_id}"
                logger.error(error_msg)
                if session:
                    session.validation_errors.append(error_msg)
                return False
            
            # Verify school exists in session
            if session and not session.validate_school_exists(v2_school_id):
                error_msg = f"V2 School ID {v2_school_id} does not exist in migration session for teacher {v1_teacher.get('firstname', '')} {v1_teacher.get('lastname', '')}"
                logger.error(error_msg)
                return False
            
            # Log school assignment for audit trail
            school_info = session.get_school_info(v2_school_id) if session else None
            logger.info(f"Migrating teacher {v1_teacher.get('firstname', '')} {v1_teacher.get('lastname', '')} to school: {school_info.get('name', 'Unknown') if school_info else 'Unknown'} (V2 ID: {v2_school_id})")
            
            # Step 1: Create User record
            user_id = await user_manager.create_teacher_user(v1_teacher, v2_school_id)
            if not user_id:
                logger.error(f"Failed to create user for teacher: {v1_teacher['firstname']} {v1_teacher['lastname']}")
                return False
            
            # Step 2: Create Teacher record
            teacher_data = {
                "user_id": user_id,
                "school_id": v2_school_id,
                "qualification": v1_teacher.get('qualification'),
                "subject_specialization": self._format_subjects(v1_teacher.get('subjects')),
                "employment_date": self._get_employment_date(v1_teacher),
                "is_active": not v1_teacher.get('isloginbarred', False) and not v1_teacher.get('isdeleted', False),
                "is_deleted": v1_teacher.get('isdeleted', False),
            }
            
            # Remove None values
            teacher_data = {k: v for k, v in teacher_data.items() if v is not None}
            
            # Insert teacher
            await db_manager.insert_record("teachers", teacher_data)
            
            # Store mapping in local store
            self.teacher_mappings[v1_teacher['id']] = user_id  # Map to user_id, not teacher_id
            
            # Store mapping in session with validation
            if session:
                success = session.add_teacher_mapping(v1_teacher['id'], user_id, v2_school_id, v1_teacher)
                if not success:
                    logger.error(f"Failed to add teacher mapping to session for {v1_teacher.get('firstname', '')} {v1_teacher.get('lastname', '')}")
                    return False
            
            logger.info(f"Successfully migrated teacher: {v1_teacher['firstname']} {v1_teacher['lastname']} (User ID: {user_id}) to school {school_info.get('name', 'Unknown') if school_info else 'Unknown'}")
            return True
            return True
                
        except Exception as e:
            logger.error(f"Error migrating teacher {v1_teacher.get('firstname', '')} {v1_teacher.get('lastname', '')}: {e}")
            logger.error(f"Teacher data: {v1_teacher}")
            return False
    
    def _format_subjects(self, subjects: Any) -> Optional[str]:
        """Format subjects from V1 format to V2 format"""
        if not subjects:
            return None
            
        if isinstance(subjects, str):
            return subjects
        elif isinstance(subjects, list):
            return ", ".join(subjects)
        else:
            return str(subjects)
    
    def _get_employment_date(self, v1_teacher: Dict[str, Any]) -> datetime:
        """Get employment date, fallback to created date or current date"""
        # V1 doesn't have employment date, so we'll use a reasonable fallback
        if 'createdat' in v1_teacher and v1_teacher['createdat']:
            return v1_teacher['createdat']
        else:
            return datetime.now()
    
    def get_v2_user_id(self, v1_teacher_id: str) -> Optional[int]:
        """Get V2 user ID from V1 teacher ID"""
        return self.teacher_mappings.get(v1_teacher_id)
    
    def get_mapping_stats(self) -> Dict[str, int]:
        """Get statistics about teacher migration"""
        return {
            "total_migrated": self.migrated_count,
            "total_failed": self.failed_count,
            "total_mappings": len(self.teacher_mappings)
        }


# Global teacher migrator instance  
teacher_migrator = TeacherMigrator()
