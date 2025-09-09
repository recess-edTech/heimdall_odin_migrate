"""
Migration validation utilities
Ensures data integrity before and during migration process
"""

import logging
from typing import Dict, Any, List, Set, Optional, Tuple
from dataclasses import dataclass

from db_utils import db_manager
from migration_session import get_migration_session

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """Result of a validation check"""
    is_valid: bool
    errors: List[str]
    warnings: List[str]
    details: Dict[str, Any]


class MigrationValidator:
    """Validates migration data integrity"""
    
    def __init__(self):
        self.validation_errors = []
        self.validation_warnings = []
    
    async def validate_v1_data_integrity(self) -> ValidationResult:
        """Validate V1 data integrity before migration"""
        logger.info("Validating V1 data integrity...")
        
        errors = []
        warnings = []
        details = {}
        
        # Check schools have required data
        school_validation = await self._validate_v1_schools()
        errors.extend(school_validation.errors)
        warnings.extend(school_validation.warnings)
        details["schools"] = school_validation.details
        
        # Check teacher-school relationships
        teacher_validation = await self._validate_v1_teacher_school_refs()
        errors.extend(teacher_validation.errors)
        warnings.extend(teacher_validation.warnings)
        details["teachers"] = teacher_validation.details
        
        # Check parent-school relationships
        parent_validation = await self._validate_v1_parent_school_refs()
        errors.extend(parent_validation.errors)
        warnings.extend(parent_validation.warnings)
        details["parents"] = parent_validation.details
        
        # Check student-school relationships
        student_validation = await self._validate_v1_student_school_refs()
        errors.extend(student_validation.errors)
        warnings.extend(student_validation.warnings)
        details["students"] = student_validation.details
        
        # Check student-parent relationships
        student_parent_validation = await self._validate_v1_student_parent_refs()
        errors.extend(student_parent_validation.errors)
        warnings.extend(student_parent_validation.warnings)
        details["student_parent_relationships"] = student_parent_validation.details
        
        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            details=details
        )
    
    async def _validate_v1_schools(self) -> ValidationResult:
        """Validate V1 schools have required data"""
        query = """
            SELECT id, schoolName, email, schoolCode, isDeleted
            FROM "School"
            WHERE isDeleted = false
        """
        
        schools = await db_manager.execute_query(query, engine_version="v1")
        
        errors = []
        warnings = []
        
        schools_without_email = 0
        schools_without_code = 0
        schools_without_name = 0
        
        for school in schools:
            school_id = school['id']
            school_name = school.get('schoolname', 'Unknown')
            
            if not school.get('schoolname'):
                errors.append(f"School ID {school_id} has no name")
                schools_without_name += 1
            
            if not school.get('email'):
                warnings.append(f"School '{school_name}' (ID: {school_id}) has no email")
                schools_without_email += 1
            
            if not school.get('schoolcode'):
                warnings.append(f"School '{school_name}' (ID: {school_id}) has no school code")
                schools_without_code += 1
        
        details = {
            "total_schools": len(schools),
            "schools_without_email": schools_without_email,
            "schools_without_code": schools_without_code,
            "schools_without_name": schools_without_name
        }
        
        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            details=details
        )
    
    async def _validate_v1_teacher_school_refs(self) -> ValidationResult:
        """Validate teachers reference valid schools"""
        query = """
            SELECT t.id, t.firstName, t.lastName, t.schoolId, s.schoolName
            FROM "Teacher" t
            LEFT JOIN "School" s ON t.schoolId = s.id
            WHERE t.isDeleted = false
        """
        
        teachers = await db_manager.execute_query(query, engine_version="v1")
        
        errors = []
        warnings = []
        
        teachers_without_school = 0
        teachers_with_deleted_school = 0
        
        for teacher in teachers:
            teacher_name = f"{teacher.get('firstname', '')} {teacher.get('lastname', '')}"
            teacher_id = teacher['id']
            school_id = teacher.get('schoolid')
            
            if not school_id:
                errors.append(f"Teacher '{teacher_name}' (ID: {teacher_id}) has no school assigned")
                teachers_without_school += 1
                continue
            
            if not teacher.get('schoolname'):
                errors.append(f"Teacher '{teacher_name}' (ID: {teacher_id}) references non-existent or deleted school ID: {school_id}")
                teachers_with_deleted_school += 1
        
        details = {
            "total_teachers": len(teachers),
            "teachers_without_school": teachers_without_school,
            "teachers_with_deleted_school": teachers_with_deleted_school
        }
        
        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            details=details
        )
    
    async def _validate_v1_parent_school_refs(self) -> ValidationResult:
        """Validate parents reference valid schools"""
        query = """
            SELECT p.id, p.firstName, p.lastName, p.schoolId, s.schoolName
            FROM "Parent" p
            LEFT JOIN "School" s ON p.schoolId = s.id
            WHERE p.isDeleted = false
        """
        
        parents = await db_manager.execute_query(query, engine_version="v1")
        
        errors = []
        warnings = []
        
        parents_without_school = 0
        parents_with_deleted_school = 0
        
        for parent in parents:
            parent_name = f"{parent.get('firstname', '')} {parent.get('lastname', '')}"
            parent_id = parent['id']
            school_id = parent.get('schoolid')
            
            if not school_id:
                errors.append(f"Parent '{parent_name}' (ID: {parent_id}) has no school assigned")
                parents_without_school += 1
                continue
            
            if not parent.get('schoolname'):
                errors.append(f"Parent '{parent_name}' (ID: {parent_id}) references non-existent or deleted school ID: {school_id}")
                parents_with_deleted_school += 1
        
        details = {
            "total_parents": len(parents),
            "parents_without_school": parents_without_school,
            "parents_with_deleted_school": parents_with_deleted_school
        }
        
        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            details=details
        )
    
    async def _validate_v1_student_school_refs(self) -> ValidationResult:
        """Validate students reference valid schools"""
        query = """
            SELECT st.id, st.firstName, st.lastName, st.schoolId, s.schoolName
            FROM "Student" st
            LEFT JOIN "School" s ON st.schoolId = s.id
            WHERE st.isDeleted = false
        """
        
        students = await db_manager.execute_query(query, engine_version="v1")
        
        errors = []
        warnings = []
        
        students_without_school = 0
        students_with_deleted_school = 0
        
        for student in students:
            student_name = f"{student.get('firstname', '')} {student.get('lastname', '')}"
            student_id = student['id']
            school_id = student.get('schoolid')
            
            if not school_id:
                errors.append(f"Student '{student_name}' (ID: {student_id}) has no school assigned")
                students_without_school += 1
                continue
            
            if not student.get('schoolname'):
                errors.append(f"Student '{student_name}' (ID: {student_id}) references non-existent or deleted school ID: {school_id}")
                students_with_deleted_school += 1
        
        details = {
            "total_students": len(students),
            "students_without_school": students_without_school,
            "students_with_deleted_school": students_with_deleted_school
        }
        
        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            details=details
        )
    
    async def _validate_v1_student_parent_refs(self) -> ValidationResult:
        """Validate student-parent relationships"""
        query = """
            SELECT st.id, st.firstName, st.lastName, st.parentId, p.firstName as parent_firstname, p.lastName as parent_lastname
            FROM "Student" st
            LEFT JOIN "Parent" p ON st.parentId = p.id AND p.isDeleted = false
            WHERE st.isDeleted = false AND st.parentId IS NOT NULL
        """
        
        student_parent_relationships = await db_manager.execute_query(query, engine_version="v1")
        
        errors = []
        warnings = []
        
        students_with_invalid_parent = 0
        
        for relationship in student_parent_relationships:
            student_name = f"{relationship.get('firstname', '')} {relationship.get('lastname', '')}"
            student_id = relationship['id']
            parent_id = relationship.get('parentid')
            
            if parent_id and not relationship.get('parent_firstname'):
                errors.append(f"Student '{student_name}' (ID: {student_id}) references non-existent or deleted parent ID: {parent_id}")
                students_with_invalid_parent += 1
        
        details = {
            "total_student_parent_relationships": len(student_parent_relationships),
            "students_with_invalid_parent": students_with_invalid_parent
        }
        
        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            details=details
        )
    
    async def validate_migration_session_integrity(self) -> ValidationResult:
        """Validate migration session has proper integrity"""
        session = get_migration_session()
        
        if not session:
            return ValidationResult(
                is_valid=False,
                errors=["No active migration session found"],
                warnings=[],
                details={}
            )
        
        errors = []
        warnings = []
        
        # Check all schools have curriculums
        schools_without_curriculum = 0
        for school_id in session.school_mappings:
            v2_school_id = session.school_mappings[school_id].v2_id
            if v2_school_id not in session.school_curriculums:
                warnings.append(f"School V2 ID {v2_school_id} has no curriculum assigned")
                schools_without_curriculum += 1
        
        # Check for orphaned entities
        total_teachers = len(session.teacher_mappings)
        total_parents = len(session.parent_mappings)
        total_students = len(session.student_mappings)
        
        # Validate all entities have valid school references
        orphaned_teachers = 0
        for teacher_mapping in session.teacher_mappings.values():
            if teacher_mapping.school_id not in [s.v2_id for s in session.school_mappings.values()]:
                orphaned_teachers += 1
                errors.append(f"Teacher V2 User ID {teacher_mapping.v2_id} has invalid school reference: {teacher_mapping.school_id}")
        
        orphaned_parents = 0
        for parent_mapping in session.parent_mappings.values():
            if parent_mapping.school_id not in [s.v2_id for s in session.school_mappings.values()]:
                orphaned_parents += 1
                errors.append(f"Parent V2 User ID {parent_mapping.v2_id} has invalid school reference: {parent_mapping.school_id}")
        
        orphaned_students = 0
        for student_mapping in session.student_mappings.values():
            if student_mapping.school_id not in [s.v2_id for s in session.school_mappings.values()]:
                orphaned_students += 1
                errors.append(f"Student V2 User ID {student_mapping.v2_id} has invalid school reference: {student_mapping.school_id}")
        
        details = {
            "session_id": session.session_id,
            "total_schools": len(session.school_mappings),
            "schools_without_curriculum": schools_without_curriculum,
            "total_teachers": total_teachers,
            "orphaned_teachers": orphaned_teachers,
            "total_parents": total_parents,
            "orphaned_parents": orphaned_parents,
            "total_students": total_students,
            "orphaned_students": orphaned_students,
            "session_errors": len(session.validation_errors),
            "session_warnings": len(session.validation_warnings)
        }
        
        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            details=details
        )


# Global validator instance
migration_validator = MigrationValidator()
