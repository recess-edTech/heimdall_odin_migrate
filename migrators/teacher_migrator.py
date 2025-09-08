"""
Teacher migration from V1 to V2 schema
Creates User records and Teacher records for each V1 teacher
"""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime

from ..db_utils.db_utils import db_manager
from ..user_utils.user_utils import user_manager
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
        
        try:
            # Get all teachers from V1
            v1_teachers = await self._get_v1_teachers()
            logger.info(f"Found {len(v1_teachers)} teachers in V1 database")
            
            # Migrate each teacher
            for teacher in v1_teachers:
                success = await self._migrate_single_teacher(teacher)
                if success:
                    self.migrated_count += 1
                else:
                    self.failed_count += 1
            
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
        """Migrate a single teacher from V1 to V2"""
        try:
            # Get V2 school ID from mapping
            v2_school_id = school_migrator.get_v2_school_id(v1_teacher['schoolid'])
            if not v2_school_id:
                logger.error(f"No V2 school found for V1 school ID: {v1_teacher['schoolid']}")
                return False
            
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
            
            # Store mapping
            self.teacher_mappings[v1_teacher['id']] = user_id  # Map to user_id, not teacher_id
            logger.info(f"Migrated teacher: {v1_teacher['firstname']} {v1_teacher['lastname']} (User ID: {user_id})")
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
