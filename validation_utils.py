"""
Enhanced validation utilities for V1 to V2 migration
Provides comprehensive validation, data cleaning, and error handling
"""

import logging
import re
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, Tuple
from dataclasses import dataclass
from enum import Enum

from db_utils import db_manager
from migration_session import get_migration_session
from config import CLASS_LEVELS, DEFAULT_CURRICULUM_ID, DEFAULT_GRADE_SYSTEM_ID

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


@dataclass
class EnhancedValidationResult:
    """Result of an enhanced validation operation"""
    is_valid: bool
    errors: List[str]
    warnings: List[str]
    corrected_data: Optional[Dict[str, Any]] = None


@dataclass
class MigrationPrerequisites:
    """Prerequisites that must exist before migration"""
    curriculum_id: int
    grade_system_id: int
    class_levels: Dict[str, int]
    academic_years: Dict[int, int]  # school_id -> academic_year_id


class EnhancedValidator:
    """Enhanced validation for migration data"""
    
    def __init__(self):
        self.email_pattern = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
        self.phone_pattern = re.compile(r'^\+?[1-9]\d{1,14}$')  # Basic international format
        
    def validate_v1_school(self, v1_school: Dict[str, Any]) -> EnhancedValidationResult:
        """Comprehensive validation of V1 school data"""
        errors = []
        warnings = []
        corrected_data = v1_school.copy()
        
        # Required fields validation
        required_fields = ['id', 'schoolName', 'email', 'phone']
        for field in required_fields:
            if not v1_school.get(field):
                errors.append(f"Missing required field: {field}")
        
        # Email validation and correction
        email = v1_school.get('email', '').strip()
        if email:
            if not self.email_pattern.match(email):
                warnings.append(f"Invalid email format: {email}")
                # Attempt to correct common issues
                corrected_email = self._attempt_email_correction(email)
                if corrected_email and self.email_pattern.match(corrected_email):
                    corrected_data['email'] = corrected_email
                    warnings.append(f"Corrected email to: {corrected_email}")
                else:
                    errors.append(f"Cannot correct invalid email: {email}")
        
        # Phone validation and correction
        phone = v1_school.get('phone', '').strip()
        if phone:
            cleaned_phone = self._clean_phone_number(phone)
            if not self.phone_pattern.match(cleaned_phone):
                warnings.append(f"Invalid phone format: {phone}")
                # Kenya-specific phone correction
                corrected_phone = self._attempt_kenya_phone_correction(phone)
                if corrected_phone:
                    corrected_data['phone'] = corrected_phone
                    warnings.append(f"Corrected phone to: {corrected_phone}")
                else:
                    errors.append(f"Cannot correct invalid phone: {phone}")
            else:
                corrected_data['phone'] = cleaned_phone
        
        # School level validation
        school_level = v1_school.get('schoolLevel', '').strip()
        if school_level:
            mapped_level_id = self._validate_and_map_school_level(school_level)
            if mapped_level_id is None:
                errors.append(f"Cannot map school level: {school_level}")
            else:
                corrected_data['_mapped_level_id'] = mapped_level_id
        else:
            errors.append("Missing school level")
        
        # School type validation
        school_type = v1_school.get('type', '').strip().upper()
        valid_types = ['PRIVATE', 'PUBLIC', 'INTERNATIONAL']
        if school_type and school_type not in valid_types:
            warnings.append(f"Unknown school type: {school_type}, defaulting to PRIVATE")
            corrected_data['type'] = 'PRIVATE'
        elif not school_type:
            corrected_data['type'] = 'PRIVATE'
            warnings.append("Missing school type, defaulting to PRIVATE")
        
        # Government code validation
        gov_code = v1_school.get('schoolCode', '').strip()
        if not gov_code:
            warnings.append("Missing government code - will generate from school name")
            corrected_data['schoolCode'] = self._generate_government_code(
                v1_school.get('schoolName', 'SCHOOL'))
        
        return EnhancedValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            corrected_data=corrected_data
        )
    
    def validate_v1_user_entity(self, v1_entity: Dict[str, Any], 
                               entity_type: str) -> EnhancedValidationResult:
        """Validate V1 user entity (teacher, parent, student)"""
        errors = []
        warnings = []
        corrected_data = v1_entity.copy()
        
        # Required fields validation
        required_fields = ['id', 'firstName', 'lastName']
        for field in required_fields:
            if not v1_entity.get(field):
                errors.append(f"Missing required field: {field}")
        
        # Name validation and correction
        first_name = v1_entity.get('firstName', '').strip()
        last_name = v1_entity.get('lastName', '').strip()
        
        if not first_name:
            errors.append("Missing first name")
        elif len(first_name) < 2:
            warnings.append(f"Very short first name: {first_name}")
        
        if not last_name:
            errors.append("Missing last name")
        elif len(last_name) < 2:
            warnings.append(f"Very short last name: {last_name}")
        
        # Email validation (if present)
        email = v1_entity.get('email', '').strip()
        if email:
            if not self.email_pattern.match(email):
                corrected_email = self._attempt_email_correction(email)
                if corrected_email:
                    corrected_data['email'] = corrected_email
                    warnings.append(f"Corrected email from {email} to {corrected_email}")
                else:
                    warnings.append(f"Invalid email format: {email}")
                    corrected_data['email'] = None
        
        # Phone validation (if present)
        phone = v1_entity.get('phoneNumber', '').strip()
        if phone:
            cleaned_phone = self._clean_phone_number(phone)
            corrected_phone = self._attempt_kenya_phone_correction(phone)
            if corrected_phone:
                corrected_data['phoneNumber'] = corrected_phone
            else:
                warnings.append(f"Invalid phone number: {phone}")
                corrected_data['phoneNumber'] = None
        
        # School ID validation
        school_id = v1_entity.get('schoolId')
        if not school_id:
            errors.append("Missing school ID")
        
        # Entity-specific validations
        if entity_type == 'student':
            self._validate_student_specific(v1_entity, corrected_data, errors, warnings)
        elif entity_type == 'parent':
            self._validate_parent_specific(v1_entity, corrected_data, errors, warnings)
        elif entity_type == 'teacher':
            self._validate_teacher_specific(v1_entity, corrected_data, errors, warnings)
        
        return EnhancedValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            corrected_data=corrected_data
        )
    
    def _validate_student_specific(self, v1_student: Dict[str, Any], 
                                  corrected_data: Dict[str, Any],
                                  errors: List[str], warnings: List[str]):
        """Student-specific validation"""
        # Admission number validation
        admission_num = v1_student.get('studentAdmissionNumber', '').strip()
        if not admission_num:
            errors.append("Missing student admission number")
        
        # Gender estimation (required in V2)
        gender = v1_student.get('gender', '').strip().upper()
        if gender not in ['MALE', 'FEMALE']:
            # Attempt to infer from name
            estimated_gender = self._estimate_gender_from_name(
                v1_student.get('firstName', ''))
            corrected_data['gender'] = estimated_gender
            warnings.append(f"Estimated gender as {estimated_gender} based on name")
        
        # Date of birth estimation (required in V2)
        if 'dateOfBirth' not in v1_student or not v1_student['dateOfBirth']:
            # Estimate based on class level if available
            estimated_dob = self._estimate_date_of_birth(v1_student)
            corrected_data['dateOfBirth'] = estimated_dob
            warnings.append(f"Estimated date of birth as {estimated_dob}")
        
        # Parent validation
        parent_id = v1_student.get('parentId')
        if not parent_id:
            warnings.append("Student has no parent ID - will need manual parent assignment")
    
    def _validate_parent_specific(self, v1_parent: Dict[str, Any],
                                 corrected_data: Dict[str, Any],
                                 errors: List[str], warnings: List[str]):
        """Parent-specific validation"""
        # Relationship mapping
        relationship = v1_parent.get('relationship', '').strip().lower()
        parent_type = self._map_parent_type(relationship)
        corrected_data['_mapped_parent_type'] = parent_type
        
        if not relationship:
            warnings.append("Missing parent relationship, defaulting to GUARDIAN")
    
    def _validate_teacher_specific(self, v1_teacher: Dict[str, Any],
                                  corrected_data: Dict[str, Any],
                                  errors: List[str], warnings: List[str]):
        """Teacher-specific validation"""
        # Qualification validation
        qualification = v1_teacher.get('qualification', '').strip()
        if not qualification:
            warnings.append("Missing teacher qualification")
        
        # Subjects validation
        subjects = v1_teacher.get('subjects')
        if subjects:
            if isinstance(subjects, list):
                corrected_data['_formatted_subjects'] = ', '.join(subjects)
            elif isinstance(subjects, str):
                corrected_data['_formatted_subjects'] = subjects
            else:
                warnings.append(f"Invalid subjects format: {subjects}")
        
        # Employment date estimation
        if 'employmentDate' not in v1_teacher or not v1_teacher['employmentDate']:
            # Use creation date or current date
            estimated_date = v1_teacher.get('CreatedAt', datetime.now())
            corrected_data['employmentDate'] = estimated_date
            warnings.append(f"Estimated employment date as {estimated_date}")
    
    def _attempt_email_correction(self, email: str) -> Optional[str]:
        """Attempt to correct common email format issues"""
        if not email:
            return None
        
        email = email.strip().lower()
        
        # Fix common issues
        corrections = [
            # Fix missing .com
            (r'@([^.]+)$', r'@\1.com'),
            # Fix double @
            (r'@@+', r'@'),
            # Fix spaces
            (r'\s+', ''),
            # Fix common domain typos
            (r'@gmail\.co$', '@gmail.com'),
            (r'@yahoo\.co$', '@yahoo.com'),
        ]
        
        for pattern, replacement in corrections:
            email = re.sub(pattern, replacement, email)
        
        # Final validation
        if self.email_pattern.match(email):
            return email
        
        return None
    
    def _clean_phone_number(self, phone: str) -> str:
        """Clean phone number by removing non-digits"""
        return re.sub(r'[^\d+]', '', phone.strip())
    
    def _attempt_kenya_phone_correction(self, phone: str) -> Optional[str]:
        """Attempt to correct Kenyan phone numbers"""
        if not phone:
            return None
        
        # Remove all non-digits except +
        cleaned = self._clean_phone_number(phone)
        
        # Handle Kenyan numbers
        if cleaned.startswith('0'):
            # Local format: 0712345678 -> +254712345678
            if len(cleaned) == 10:
                return f"+254{cleaned[1:]}"
        elif cleaned.startswith('254'):
            # Missing +: 254712345678 -> +254712345678
            if len(cleaned) == 12:
                return f"+{cleaned}"
        elif cleaned.startswith('+254'):
            # Already correct format
            if len(cleaned) == 13:
                return cleaned
        
        # For other patterns, validate as international
        if self.phone_pattern.match(cleaned):
            return cleaned
        
        return None
    
    def _validate_and_map_school_level(self, school_level: str) -> Optional[int]:
        """Validate and map school level to class level ID"""
        if not school_level:
            return None
        
        level_lower = school_level.lower().strip()
        
        # Direct mapping from config
        for key, level_id in CLASS_LEVELS.items():
            if key in level_lower or level_lower in key:
                return level_id
        
        # Extended mapping for common variations
        extended_mapping = {
            'pre-primary': CLASS_LEVELS.get('pre primary'),
            'preprimary': CLASS_LEVELS.get('pre primary'),
            'pp': CLASS_LEVELS.get('pre primary'),
            'primary': CLASS_LEVELS.get('primary'),
            'pri': CLASS_LEVELS.get('primary'),
            'junior': CLASS_LEVELS.get('junior secondary'),
            'junior secondary': CLASS_LEVELS.get('junior secondary'),
            'jss': CLASS_LEVELS.get('junior secondary'),
            'secondary': CLASS_LEVELS.get('senior secondary'),
            'senior secondary': CLASS_LEVELS.get('senior secondary'),
            'high': CLASS_LEVELS.get('senior secondary'),
            'high school': CLASS_LEVELS.get('senior secondary'),
        }
        
        for pattern, level_id in extended_mapping.items():
            if pattern in level_lower and level_id:
                return level_id
        
        return None
    
    def _generate_government_code(self, school_name: str) -> str:
        """Generate a government code from school name"""
        # Take first 3 characters of each word, uppercase
        words = school_name.upper().split()
        code_parts = [word[:3] for word in words[:3]]  # Max 3 words
        base_code = ''.join(code_parts)
        
        # Add timestamp to ensure uniqueness
        timestamp = datetime.now().strftime('%m%d')
        return f"{base_code}{timestamp}"
    
    def _estimate_gender_from_name(self, first_name: str) -> str:
        """Estimate gender from first name (basic implementation)"""
        if not first_name:
            return 'MALE'  # Default
        
        name_lower = first_name.lower()
        
        # Simple heuristics for common Kenyan names
        female_indicators = ['mary', 'jane', 'grace', 'faith', 'mercy', 'joy', 'ann', 'lucy']
        male_indicators = ['john', 'peter', 'paul', 'david', 'james', 'michael', 'samuel']
        
        for indicator in female_indicators:
            if indicator in name_lower:
                return 'FEMALE'
        
        for indicator in male_indicators:
            if indicator in name_lower:
                return 'MALE'
        
        # Default to MALE if uncertain
        return 'MALE'
    
    def _estimate_date_of_birth(self, v1_student: Dict[str, Any]) -> datetime:
        """Estimate date of birth based on class level and average age"""
        # Basic age estimation based on class
        class_id = v1_student.get('classId')
        if class_id:
            # This is a simplified estimation - you'd want actual class data
            # For now, estimate based on common class-age mappings
            estimated_age = 10  # Default age
            
            # You could enhance this by looking up actual class information
            # from the V1 database and mapping to typical ages
        else:
            estimated_age = 10
        
        # Calculate birth date
        birth_year = datetime.now().year - estimated_age
        return datetime(birth_year, 1, 1)
    
    def _map_parent_type(self, relationship: str) -> str:
        """Map V1 parent relationship to V2 parent type"""
        if not relationship:
            return 'GUARDIAN'
        
        rel_lower = relationship.lower()
        
        if 'father' in rel_lower or 'dad' in rel_lower:
            return 'FATHER'
        elif 'mother' in rel_lower or 'mom' in rel_lower or 'mum' in rel_lower:
            return 'MOTHER'
        else:
            return 'GUARDIAN'


class PrerequisiteManager:
    """Manages migration prerequisites"""
    
    async def setup_prerequisites(self, school_ids: List[str]) -> MigrationPrerequisites:
        """Set up all prerequisites for migration"""
        logger.info("Setting up migration prerequisites...")
        
        # Ensure curriculum exists
        curriculum_id = await self._ensure_default_curriculum()
        
        # Ensure grade system exists  
        grade_system_id = await self._ensure_default_grade_system()
        
        # Set up class levels
        class_levels = await self._ensure_class_levels(curriculum_id)
        
        # Create academic years for schools
        academic_years = await self._ensure_academic_years(school_ids)
        
        return MigrationPrerequisites(
            curriculum_id=curriculum_id,
            grade_system_id=grade_system_id,
            class_levels=class_levels,
            academic_years=academic_years
        )
    
    async def _ensure_default_curriculum(self) -> int:
        """Ensure default curriculum exists"""
        # Check if default curriculum exists
        existing = await db_manager.execute_query(
            "SELECT id FROM curriculums WHERE id = :id",
            {"id": DEFAULT_CURRICULUM_ID}
        )
        
        if existing:
            return DEFAULT_CURRICULUM_ID
        
        # Create default curriculum
        curriculum_data = {
            "id": DEFAULT_CURRICULUM_ID,
            "name": "kenya_8_4_4_system",
            "alias": "Kenya 8-4-4 System",
            "country": "Kenya",
            "grade_system_id": DEFAULT_GRADE_SYSTEM_ID,
            "is_active": True
        }
        
        await db_manager.insert_record("curriculums", curriculum_data)
        logger.info(f"Created default curriculum with ID: {DEFAULT_CURRICULUM_ID}")
        
        return DEFAULT_CURRICULUM_ID
    
    async def _ensure_default_grade_system(self) -> int:
        """Ensure default grade system exists"""
        # Check if exists
        existing = await db_manager.execute_query(
            "SELECT id FROM grade_systems WHERE id = :id",
            {"id": DEFAULT_GRADE_SYSTEM_ID}
        )
        
        if existing:
            return DEFAULT_GRADE_SYSTEM_ID
        
        # Create default grade system
        grade_system_data = {
            "id": DEFAULT_GRADE_SYSTEM_ID,
            "name": "Kenya 8-4-4 Standard",
            "description": "Standard Kenyan grading system",
            "grade_type": "LETTER",
            "is_default": True,
            "is_predefined": True,
            "country": "Kenya"
        }
        
        await db_manager.insert_record("grade_systems", grade_system_data)
        
        # Create default grades (A, B, C, D, E)
        grades = [
            {"grade_system_id": DEFAULT_GRADE_SYSTEM_ID, "name": "A", "min_score": 80.0, "max_score": 100.0, "remark": "Excellent"},
            {"grade_system_id": DEFAULT_GRADE_SYSTEM_ID, "name": "B", "min_score": 70.0, "max_score": 79.9, "remark": "Good"},
            {"grade_system_id": DEFAULT_GRADE_SYSTEM_ID, "name": "C", "min_score": 60.0, "max_score": 69.9, "remark": "Average"},
            {"grade_system_id": DEFAULT_GRADE_SYSTEM_ID, "name": "D", "min_score": 40.0, "max_score": 59.9, "remark": "Below Average"},
            {"grade_system_id": DEFAULT_GRADE_SYSTEM_ID, "name": "E", "min_score": 0.0, "max_score": 39.9, "remark": "Fail"},
        ]
        
        for grade_data in grades:
            await db_manager.insert_record("grades", grade_data)
        
        logger.info(f"Created default grade system with ID: {DEFAULT_GRADE_SYSTEM_ID}")
        return DEFAULT_GRADE_SYSTEM_ID
    
    async def _ensure_class_levels(self, curriculum_id: int) -> Dict[str, int]:
        """Ensure class levels exist for curriculum"""
        level_mapping = {}
        
        # Define standard class levels for Kenya 8-4-4
        levels = [
            {"name": "NURSERY", "min_age": 3, "max_age": 5},
            {"name": "PRE PRIMARY", "min_age": 5, "max_age": 7},
            {"name": "PRIMARY", "min_age": 6, "max_age": 14},
            {"name": "JUNIOR SECONDARY", "min_age": 14, "max_age": 17},
            {"name": "SENIOR SECONDARY", "min_age": 17, "max_age": 19},
        ]
        
        for level_data in levels:
            # Check if exists
            existing = await db_manager.execute_query(
                "SELECT id FROM class_levels WHERE name = :name AND curriculum_id = :curriculum_id",
                {"name": level_data["name"], "curriculum_id": curriculum_id}
            )
            
            if existing:
                level_id = existing[0]['id']
            else:
                # Create class level
                insert_data = {
                    "name": level_data["name"],
                    "min_age": level_data["min_age"],
                    "max_age": level_data["max_age"],
                    "curriculum_id": curriculum_id
                }
                level_id = await db_manager.insert_record("class_levels", insert_data)
                logger.info(f"Created class level: {level_data['name']}")
            
            level_mapping[level_data["name"].lower()] = level_id
        
        return level_mapping
    
    async def _ensure_academic_years(self, school_ids: List[str]) -> Dict[int, int]:
        """Create academic years for schools"""
        academic_year_mapping = {}
        
        # Current academic year
        current_year = datetime.now().year
        year_name = f"{current_year}/{current_year + 1}"
        start_date = datetime(current_year, 1, 1)
        end_date = datetime(current_year, 12, 31)
        
        for school_id in school_ids:
            # Check if academic year exists for school
            existing = await db_manager.execute_query(
                "SELECT id FROM academic_years WHERE school_id = :school_id AND is_active = true",
                {"school_id": school_id}
            )
            
            if existing:
                academic_year_mapping[school_id] = existing[0]['id']
            else:
                # Create academic year
                year_data = {
                    "name": year_name,
                    "start_date": start_date,
                    "end_date": end_date,
                    "school_id": school_id,
                    "is_active": True
                }
                
                year_id = await db_manager.insert_record("academic_years", year_data)
                academic_year_mapping[school_id] = year_id
                logger.info(f"Created academic year for school {school_id}")
        
        return academic_year_mapping


# Global validator instance
migration_validator = MigrationValidator()

# Global enhanced instances
enhanced_validator = EnhancedValidator()
prerequisite_manager = PrerequisiteManager()
