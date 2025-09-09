"""
Student migration from V1 to V2 schema
Creates User records, Student records, and StudentParent relationships
"""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, date

from ..db_utils import db_manager
from ..user_utils import user_manager
from ..migration_session import get_migration_session, MigrationPhase
from .school_migrator import school_migrator
from .parent_migrator import parent_migrator
from ..config import DEFAULT_CLASS_LEVEL_ID

logger = logging.getLogger(__name__)


class StudentMigrator:
    """Handles migration of students from V1 to V2"""
    
    def __init__(self):
        self.student_mappings = {}  # Maps V1 student IDs to V2 user IDs
        self.migrated_count = 0
        self.failed_count = 0
        self.student_parent_relationships = []
        
    async def migrate_students(self) -> Dict[str, Any]:
        """Main method to migrate all students"""
        logger.info("Starting student migration...")
        
        # Get migration session
        session = get_migration_session()
        if not session:
            logger.error("No active migration session - create one first")
            return {"success": False, "error": "No active migration session"}
        
        # Start students phase
        await session.start_phase(MigrationPhase.STUDENTS)
        
        try:
            # Get all students from V1
            v1_students = await self._get_v1_students()
            logger.info(f"Found {len(v1_students)} students in V1 database")
            
            session.stats["students"]["total"] = len(v1_students)
            
            # Migrate each student
            for student in v1_students:
                success = await self._migrate_single_student(student)
                if success:
                    self.migrated_count += 1
                else:
                    self.failed_count += 1
                    session.stats["students"]["failed"] += 1
            
            # Create student-parent relationships
            await self._create_student_parent_relationships()
            
            result = {
                "success": True,
                "migrated": self.migrated_count,
                "failed": self.failed_count,
                "total": len(v1_students),
                "mappings": self.student_mappings,
                "relationships_created": len(self.student_parent_relationships)
            }
            
            logger.info(f"Student migration completed: {self.migrated_count} successful, {self.failed_count} failed")
            return result
            
        except Exception as e:
            logger.error(f"Student migration failed: {e}")
            return {"success": False, "error": str(e)}
    
    async def _get_v1_students(self) -> List[Dict[str, Any]]:
        """Get all students from V1 database"""
        query = """
            SELECT 
                id, firstName, middleName, lastName, studentAdmissionNumber,
                profileImage, gender, schoolId, classId, streamId, parentId,
                isDeleted, lastLoggedIn
            FROM "Student"
            WHERE isDeleted = false
            ORDER BY schoolId, firstName
        """
        
        return await db_manager.execute_query(query, engine_version="v1")
    
    async def _migrate_single_student(self, v1_student: Dict[str, Any]) -> bool:
        """Migrate a single student from V1 to V2 with strict school validation"""
        try:
            session = get_migration_session()
            
            # CRITICAL: Validate V1 school exists in migration session
            v1_school_id = v1_student.get('schoolid')
            if not v1_school_id:
                logger.error(f"Student {v1_student.get('firstname', '')} {v1_student.get('lastname', '')} has no school ID")
                return False
            
            # Validate the V1 school was successfully migrated
            if session and not session.validate_v1_school_reference(v1_school_id, "student", v1_student):
                return False
            
            # Get V2 school ID from session (more reliable than direct migrator call)
            if session:
                v2_school_id = session.get_v2_school_id(v1_school_id)
            else:
                # Fallback for when session isn't available (shouldn't happen)
                v2_school_id = school_migrator.get_v2_school_id(v1_school_id)
            
            if not v2_school_id:
                error_msg = f"No V2 school mapping found for student {v1_student.get('firstname', '')} {v1_student.get('lastname', '')} with V1 school ID: {v1_school_id}"
                logger.error(error_msg)
                if session:
                    session.validation_errors.append(error_msg)
                return False
            
            # Verify school exists in session
            if session and not session.validate_school_exists(v2_school_id):
                error_msg = f"V2 School ID {v2_school_id} does not exist in migration session for student {v1_student.get('firstname', '')} {v1_student.get('lastname', '')}"
                logger.error(error_msg)
                return False
            
            # Log school assignment for audit trail
            school_info = session.get_school_info(v2_school_id) if session else None
            logger.info(f"Migrating student {v1_student.get('firstname', '')} {v1_student.get('lastname', '')} to school: {school_info.get('name', 'Unknown') if school_info else 'Unknown'} (V2 ID: {v2_school_id})")
            
            # Step 1: Create User record
            user_id = await user_manager.create_student_user(v1_student, v2_school_id)
            if not user_id:
                logger.error(f"Failed to create user for student: {v1_student['firstname']} {v1_student['lastname']}")
                return False
            
            # Step 2: Get or create default school class
            school_class_id = await self._get_or_create_default_school_class(v2_school_id, v1_student.get('classid'))
            
            # Step 3: Create Student record
            student_data = {
                "user_id": user_id,
                "gender": self._map_gender(v1_student.get('gender')),
                "date_of_birth": self._estimate_date_of_birth(v1_student),
                "address": None,  # V1 doesn't have student address
                "is_deleted": v1_student.get('isdeleted', False),
                "is_active": not v1_student.get('isdeleted', False),
                "is_enrolled": True,
                "enrollment_date": datetime.now(),  # Use current date as enrollment
                "school_id": v2_school_id,
                "school_class_id": school_class_id,
                "admission_number": v1_student['studentadmissionnumber'],
                "stream_id": None,  # Will be handled later when streams are migrated
            }
            
            # Remove None values
            student_data = {k: v for k, v in student_data.items() if v is not None}
            
            # Insert student
            await db_manager.insert_record("students", student_data)
            
            # Store mapping in local store
            self.student_mappings[v1_student['id']] = user_id
            
            # Store mapping in session with validation
            if session:
                success = session.add_student_mapping(v1_student['id'], user_id, v2_school_id, v1_student)
                if not success:
                    logger.error(f"Failed to add student mapping to session for {v1_student.get('firstname', '')} {v1_student.get('lastname', '')}")
                    return False
            
            if v1_student.get('parentid'):
                self.student_parent_relationships.append({
                    "student_v1_id": v1_student['id'],
                    "parent_v1_id": v1_student['parentid'],
                    "student_user_id": user_id
                })
            
            logger.info(f"Successfully migrated student: {v1_student['firstname']} {v1_student['lastname']} (User ID: {user_id}) to school {school_info.get('name', 'Unknown') if school_info else 'Unknown'}")
            return True
                
        except Exception as e:
            logger.error(f"Error migrating student {v1_student.get('firstname', '')} {v1_student.get('lastname', '')}: {e}")
            logger.error(f"Student data: {v1_student}")
            return False
    
    async def _get_or_create_default_school_class(self, v2_school_id: int, v1_class_id: Optional[str]) -> int:
        """Get or create a default school class for students"""
        # For now, create a default class for the school
        # In a real migration, you'd want to properly map V1 classes to V2 school classes
        
        class_name = f"Migrated Class - {v1_class_id}" if v1_class_id else "Migrated Class - Default"
        
        # Check if default class already exists for this school
        existing_class = await db_manager.execute_query(
            """
            SELECT id FROM school_classes 
            WHERE school_id = :school_id AND name = :name 
            LIMIT 1
            """,
            {"school_id": v2_school_id, "name": class_name}
        )
        
        if existing_class:
            return existing_class[0]['id']
        
        # Create new school class
        # First, get a default academic year for the school
        academic_year_id = await self._get_or_create_default_academic_year(v2_school_id)
        
        class_data = {
            "name": class_name,
            "school_id": v2_school_id,
            "class_id": 1,  # Default global class ID - this should be set up in advance
            "academic_year_id": academic_year_id,
        }
        
        class_id = await db_manager.insert_record("school_classes", class_data)
        return class_id if class_id is not None else 1  # Return default if failed
    
    async def _get_or_create_default_academic_year(self, v2_school_id: int) -> int:
        """Get or create default academic year for the school"""
        # Check if academic year exists for this school
        existing_year = await db_manager.execute_query(
            """
            SELECT id FROM academic_years 
            WHERE school_id = :school_id AND is_active = true
            LIMIT 1
            """,
            {"school_id": v2_school_id}
        )
        
        if existing_year:
            return existing_year[0]['id']
        
        # Create default academic year
        current_year = datetime.now().year
        year_data = {
            "name": f"Migrated Academic Year {current_year}",
            "start_date": datetime(current_year, 1, 1),
            "end_date": datetime(current_year, 12, 31),
            "school_id": v2_school_id,
            "is_active": True,
        }
        
        year_id = await db_manager.insert_record("academic_years", year_data)
        return year_id if year_id is not None else 1  # Return default if failed
    
    def _map_gender(self, v1_gender: Optional[str]) -> str:
        """Map V1 gender to V2 gender enum"""
        if not v1_gender:
            return "MALE"  # Default
            
        gender_lower = v1_gender.lower()
        
        if "female" in gender_lower or "f" == gender_lower:
            return "FEMALE"
        else:
            return "MALE"
    
    def _estimate_date_of_birth(self, v1_student: Dict[str, Any]) -> date:
        """Estimate date of birth for student (V1 doesn't have this)"""
        # Very rough estimation - assume students are around 10-15 years old
        # This is a placeholder - in real migration you might have better logic
        current_year = datetime.now().year
        estimated_birth_year = current_year - 12  # Assume 12 years old
        
        return date(estimated_birth_year, 1, 1)
    
    async def _create_student_parent_relationships(self):
        """Create StudentParent relationships based on stored mappings"""
        logger.info("Creating student-parent relationships...")
        
        created_count = 0
        failed_count = 0
        
        for relationship in self.student_parent_relationships:
            try:
                # Get parent user ID
                parent_user_id = parent_migrator.get_v2_user_id(relationship['parent_v1_id'])
                if not parent_user_id:
                    logger.warning(f"Parent not found for V1 ID: {relationship['parent_v1_id']}")
                    failed_count += 1
                    continue
                
                # Get student ID from user ID
                student_records = await db_manager.execute_query(
                    "SELECT id FROM students WHERE user_id = :user_id LIMIT 1",
                    {"user_id": relationship['student_user_id']}
                )
                
                if not student_records:
                    logger.warning(f"Student record not found for user ID: {relationship['student_user_id']}")
                    failed_count += 1
                    continue
                
                student_id = student_records[0]['id']
                
                # Get parent ID from user ID
                parent_records = await db_manager.execute_query(
                    "SELECT id FROM parents WHERE user_id = :user_id LIMIT 1",
                    {"user_id": parent_user_id}
                )
                
                if not parent_records:
                    logger.warning(f"Parent record not found for user ID: {parent_user_id}")
                    failed_count += 1
                    continue
                
                parent_id = parent_records[0]['id']
                
                # Create relationship
                relationship_data = {
                    "student_id": student_id,
                    "parent_id": parent_id,
                }
                
                await db_manager.insert_record("student_parents", relationship_data)
                created_count += 1
                
            except Exception as e:
                logger.error(f"Error creating student-parent relationship: {e}")
                failed_count += 1
        
        logger.info(f"Student-parent relationships: {created_count} created, {failed_count} failed")
    
    def get_v2_user_id(self, v1_student_id: str) -> Optional[int]:
        """Get V2 user ID from V1 student ID"""
        return self.student_mappings.get(v1_student_id)
    
    def get_mapping_stats(self) -> Dict[str, int]:
        """Get statistics about student migration"""
        return {
            "total_migrated": self.migrated_count,
            "total_failed": self.failed_count,
            "total_mappings": len(self.student_mappings),
            "relationships_created": len(self.student_parent_relationships)
        }


# Global student migrator instance  
student_migrator = StudentMigrator()
