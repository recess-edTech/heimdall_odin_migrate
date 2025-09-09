"""
Migration Session Management
Provides centralized session management for data migration with consistency guarantees
"""

import logging
from typing import Dict, Any, List, Optional, Set
from datetime import datetime
from dataclasses import dataclass, field
from enum import Enum

from db_utils import db_manager

logger = logging.getLogger(__name__)


class MigrationPhase(Enum):
    """Migration phases in order"""
    INITIALIZATION = "initialization"
    SCHOOLS = "schools"
    CURRICULUMS = "curriculums"
    TEACHERS = "teachers"
    PARENTS = "parents"
    STUDENTS = "students"
    VALIDATION = "validation"
    COMPLETION = "completion"


@dataclass
class MigrationMapping:
    """Stores mapping between V1 and V2 IDs for an entity"""
    v1_id: Any
    v2_id: Any
    entity_type: str
    school_id: Optional[int] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.now)


@dataclass
class SchoolCurriculumMapping:
    """Maps school to its validated curriculum"""
    school_id: int
    curriculum_id: int
    curriculum_name: str
    grade_system_id: int
    is_validated: bool = False
    validation_errors: List[str] = field(default_factory=list)


class MigrationSession:
    """
    Central session manager for migration process
    Ensures data consistency and proper entity-to-school mapping
    """
    
    def __init__(self, session_id: Optional[str] = None):
        self.session_id = session_id or f"migration_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.current_phase = MigrationPhase.INITIALIZATION
        self.start_time = datetime.now()
        
        # Entity mappings (V1 ID -> V2 ID)
        self.school_mappings: Dict[int, MigrationMapping] = {}
        self.teacher_mappings: Dict[int, MigrationMapping] = {}
        self.parent_mappings: Dict[int, MigrationMapping] = {}
        self.student_mappings: Dict[int, MigrationMapping] = {}
        self.user_mappings: Dict[str, MigrationMapping] = {}  # keyed by email for uniqueness
        
        # School-specific mappings
        self.school_curriculums: Dict[int, SchoolCurriculumMapping] = {}
        self.school_teachers: Dict[int, Set[int]] = {}  # school_id -> set of teacher_user_ids
        self.school_parents: Dict[int, Set[int]] = {}   # school_id -> set of parent_user_ids
        self.school_students: Dict[int, Set[int]] = {}  # school_id -> set of student_user_ids
        
        # Statistics
        self.stats = {
            "schools": {"migrated": 0, "failed": 0, "total": 0},
            "teachers": {"migrated": 0, "failed": 0, "total": 0},
            "parents": {"migrated": 0, "failed": 0, "total": 0},
            "students": {"migrated": 0, "failed": 0, "total": 0},
            "users": {"created": 0, "failed": 0, "total": 0}
        }
        
        # Validation results
        self.validation_errors: List[str] = []
        self.validation_warnings: List[str] = []
        
        logger.info(f"Migration session {self.session_id} initialized")
    
    async def start_phase(self, phase: MigrationPhase) -> bool:
        """Start a new migration phase"""
        logger.info(f"Starting migration phase: {phase.value}")
        self.current_phase = phase
        
        # Validate phase prerequisites
        if not await self._validate_phase_prerequisites(phase):
            return False
            
        return True
    
    async def _validate_phase_prerequisites(self, phase: MigrationPhase) -> bool:
        """Validate prerequisites for starting a phase"""
        if phase == MigrationPhase.TEACHERS:
            if not self.school_mappings:
                self.validation_errors.append("Cannot start teacher migration: No schools migrated")
                return False
                
        elif phase == MigrationPhase.PARENTS:
            if not self.school_mappings:
                self.validation_errors.append("Cannot start parent migration: No schools migrated")
                return False
                
        elif phase == MigrationPhase.STUDENTS:
            if not self.school_mappings:
                self.validation_errors.append("Cannot start student migration: No schools migrated")
                return False
            if not self.parent_mappings:
                logger.warning("Starting student migration without parents - students may not have parent relationships")
                
        return True
    
    def add_school_mapping(self, v1_id: int, v2_id: int, school_data: Dict[str, Any]) -> bool:
        """Add school mapping and initialize related structures"""
        mapping = MigrationMapping(
            v1_id=v1_id,
            v2_id=v2_id,
            entity_type="school",
            metadata={"name": school_data.get("schoolName", ""), "code": school_data.get("schoolCode", "")}
        )
        
        self.school_mappings[v1_id] = mapping
        self.school_teachers[v2_id] = set()
        self.school_parents[v2_id] = set()
        self.school_students[v2_id] = set()
        
        self.stats["schools"]["migrated"] += 1
        logger.info(f"School mapping added: V1({v1_id}) -> V2({v2_id}) [{school_data.get('schoolName', '')}]")
        return True
    
    def get_v2_school_id(self, v1_school_id: int) -> Optional[int]:
        """Get V2 school ID from V1 school ID"""
        mapping = self.school_mappings.get(v1_school_id)
        return mapping.v2_id if mapping else None
    
    async def validate_and_assign_curriculum(self, v1_school_id: int, v2_school_id: int, 
                                           school_data: Dict[str, Any]) -> bool:
        """Validate and assign appropriate curriculum to school"""
        try:
            # Get available curriculums from V2
            curriculums = await self._get_available_curriculums()
            if not curriculums:
                self.validation_errors.append(f"No curriculums available for school {school_data.get('schoolName')}")
                return False
            
            # Determine appropriate curriculum based on school data
            curriculum = await self._determine_school_curriculum(school_data, curriculums)
            if not curriculum:
                self.validation_errors.append(f"Could not determine curriculum for school {school_data.get('schoolName')}")
                return False
            
            # Create school-curriculum mapping
            curriculum_mapping = SchoolCurriculumMapping(
                school_id=v2_school_id,
                curriculum_id=curriculum["id"],
                curriculum_name=curriculum["name"],
                grade_system_id=curriculum["grade_system_id"],
                is_validated=True
            )
            
            self.school_curriculums[v2_school_id] = curriculum_mapping
            
            # Update school with curriculum assignment
            await db_manager.execute_query(
                'UPDATE "School" SET curriculum_id = $1 WHERE id = $2',
                {"curriculum_id": curriculum["id"], "school_id": v2_school_id},
                engine_version='v2'
            )
            
            logger.info(f"Curriculum assigned: School({v2_school_id}) -> Curriculum({curriculum['name']})")
            return True
            
        except Exception as e:
            error_msg = f"Failed to validate curriculum for school {school_data.get('schoolName')}: {e}"
            self.validation_errors.append(error_msg)
            logger.error(error_msg)
            return False
    
    async def _get_available_curriculums(self) -> List[Dict[str, Any]]:
        """Get all available curriculums from V2 database"""
        query = '''
            SELECT c.id, c.name, c.description, c.grade_system_id, gs.name as grade_system_name
            FROM "Curriculum" c
            JOIN "GradeSystem" gs ON c.grade_system_id = gs.id
            WHERE c.is_active = true
            ORDER BY c.name
        '''
        return await db_manager.execute_query(query, engine_version='v2')
    
    async def _determine_school_curriculum(self, school_data: Dict[str, Any], 
                                         curriculums: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Determine the most appropriate curriculum for a school"""
        school_level = school_data.get('schoolLevel', '').lower()
        school_name = school_data.get('schoolName', '').lower()
        
        # Priority matching rules
        curriculum_priorities = [
            # Exact matches
            {"keywords": ["kenyan", "8-4-4"], "weight": 100},
            {"keywords": ["cambridge", "igcse"], "weight": 90},
            {"keywords": ["ib", "international baccalaureate"], "weight": 85},
            {"keywords": ["american", "us"], "weight": 80},
            
            # Level-based matches
            {"keywords": ["primary", "elementary"], "weight": 70, "levels": ["primary", "elementary"]},
            {"keywords": ["secondary", "high school"], "weight": 70, "levels": ["secondary", "high"]},
            {"keywords": ["kindergarten", "pre-school"], "weight": 60, "levels": ["pre", "kg"]},
        ]
        
        best_curriculum = None
        best_score = 0
        
        for curriculum in curriculums:
            score = 0
            curriculum_name = curriculum['name'].lower()
            
            # Check against priority rules
            for rule in curriculum_priorities:
                rule_score = 0
                
                # Keyword matching
                for keyword in rule['keywords']:
                    if keyword in curriculum_name or keyword in school_name:
                        rule_score += rule['weight']
                
                # Level matching
                if 'levels' in rule:
                    for level in rule['levels']:
                        if level in school_level:
                            rule_score += rule['weight'] // 2
                
                score += rule_score
            
            if score > best_score:
                best_score = score
                best_curriculum = curriculum
        
        # Default to first curriculum if no good match
        if not best_curriculum and curriculums:
            best_curriculum = curriculums[0]
            logger.warning(f"Using default curriculum for school: {school_data.get('schoolName')}")
        
        return best_curriculum
    
    def validate_school_exists(self, v2_school_id: int) -> bool:
        """Validate that a school exists in the migration session"""
        for mapping in self.school_mappings.values():
            if mapping.v2_id == v2_school_id:
                return True
        return False
    
    def get_school_info(self, v2_school_id: int) -> Optional[Dict[str, Any]]:
        """Get school information by V2 school ID"""
        for mapping in self.school_mappings.values():
            if mapping.v2_id == v2_school_id:
                return {
                    "v1_id": mapping.v1_id,
                    "v2_id": mapping.v2_id,
                    "name": mapping.metadata.get("name", ""),
                    "code": mapping.metadata.get("code", ""),
                    "created_at": mapping.created_at
                }
        return None
    
    def add_teacher_mapping(self, v1_id: int, v2_user_id: int, v2_school_id: int, 
                          teacher_data: Dict[str, Any]) -> bool:
        """Add teacher mapping with strict school validation"""
        # CRITICAL: Validate school exists before adding teacher
        if not self.validate_school_exists(v2_school_id):
            error_msg = f"Cannot add teacher {teacher_data.get('firstname', '')} {teacher_data.get('lastname', '')}: School ID {v2_school_id} does not exist in migration session"
            self.validation_errors.append(error_msg)
            logger.error(error_msg)
            return False
        
        # Validate teacher has required data
        if not teacher_data.get('email'):
            error_msg = f"Cannot add teacher V1({v1_id}): Missing email address"
            self.validation_errors.append(error_msg)
            logger.error(error_msg)
            return False
        
        school_info = self.get_school_info(v2_school_id)
        
        mapping = MigrationMapping(
            v1_id=v1_id,
            v2_id=v2_user_id,
            entity_type="teacher",
            school_id=v2_school_id,
            metadata={
                "name": f"{teacher_data.get('firstname', '')} {teacher_data.get('lastname', '')}",
                "email": teacher_data.get('email', ''),
                "school_name": school_info.get("name", "") if school_info else "",
                "v1_school_id": school_info.get("v1_id") if school_info else None
            }
        )
        
        self.teacher_mappings[v1_id] = mapping
        
        # Associate teacher with school
        if v2_school_id in self.school_teachers:
            self.school_teachers[v2_school_id].add(v2_user_id)
        else:
            # This should not happen if validation passed, but safety check
            error_msg = f"School {v2_school_id} not found in school_teachers mapping"
            self.validation_errors.append(error_msg)
            logger.error(error_msg)
            return False
        
        self.stats["teachers"]["migrated"] += 1
        logger.info(f"Teacher mapping added: V1({v1_id}) -> V2_User({v2_user_id}) @ School({v2_school_id}) [{school_info.get('name', '') if school_info else 'Unknown'}]")
        return True
    
    def add_parent_mapping(self, v1_id: int, v2_user_id: int, v2_school_id: int, 
                         parent_data: Dict[str, Any]) -> bool:
        """Add parent mapping with strict school validation"""
        # CRITICAL: Validate school exists before adding parent
        if not self.validate_school_exists(v2_school_id):
            error_msg = f"Cannot add parent {parent_data.get('firstname', '')} {parent_data.get('lastname', '')}: School ID {v2_school_id} does not exist in migration session"
            self.validation_errors.append(error_msg)
            logger.error(error_msg)
            return False
        
        # Validate parent has required data
        if not parent_data.get('email'):
            error_msg = f"Cannot add parent V1({v1_id}): Missing email address"
            self.validation_errors.append(error_msg)
            logger.error(error_msg)
            return False
        
        school_info = self.get_school_info(v2_school_id)
        
        mapping = MigrationMapping(
            v1_id=v1_id,
            v2_id=v2_user_id,
            entity_type="parent",
            school_id=v2_school_id,
            metadata={
                "name": f"{parent_data.get('firstname', '')} {parent_data.get('lastname', '')}",
                "email": parent_data.get('email', ''),
                "school_name": school_info.get("name", "") if school_info else "",
                "v1_school_id": school_info.get("v1_id") if school_info else None
            }
        )
        
        self.parent_mappings[v1_id] = mapping
        
        # Associate parent with school
        if v2_school_id in self.school_parents:
            self.school_parents[v2_school_id].add(v2_user_id)
        else:
            # This should not happen if validation passed, but safety check
            error_msg = f"School {v2_school_id} not found in school_parents mapping"
            self.validation_errors.append(error_msg)
            logger.error(error_msg)
            return False
        
        self.stats["parents"]["migrated"] += 1
        logger.info(f"Parent mapping added: V1({v1_id}) -> V2_User({v2_user_id}) @ School({v2_school_id}) [{school_info.get('name', '') if school_info else 'Unknown'}]")
        return True
    
    def add_student_mapping(self, v1_id: int, v2_user_id: int, v2_school_id: int, 
                          student_data: Dict[str, Any]) -> bool:
        """Add student mapping with strict school validation"""
        # CRITICAL: Validate school exists before adding student
        if not self.validate_school_exists(v2_school_id):
            error_msg = f"Cannot add student {student_data.get('firstname', '')} {student_data.get('lastname', '')}: School ID {v2_school_id} does not exist in migration session"
            self.validation_errors.append(error_msg)
            logger.error(error_msg)
            return False
        
        # Validate student has required data
        if not student_data.get('email') and not student_data.get('admissionNumber'):
            error_msg = f"Cannot add student V1({v1_id}): Missing both email and admission number"
            self.validation_errors.append(error_msg)
            logger.error(error_msg)
            return False
        
        school_info = self.get_school_info(v2_school_id)
        
        mapping = MigrationMapping(
            v1_id=v1_id,
            v2_id=v2_user_id,
            entity_type="student",
            school_id=v2_school_id,
            metadata={
                "name": f"{student_data.get('firstname', '')} {student_data.get('lastname', '')}",
                "email": student_data.get('email', ''),
                "admission_number": student_data.get('admissionNumber', ''),
                "school_name": school_info.get("name", "") if school_info else "",
                "v1_school_id": school_info.get("v1_id") if school_info else None
            }
        )
        
        self.student_mappings[v1_id] = mapping
        
        # Associate student with school
        if v2_school_id in self.school_students:
            self.school_students[v2_school_id].add(v2_user_id)
        else:
            # This should not happen if validation passed, but safety check
            error_msg = f"School {v2_school_id} not found in school_students mapping"
            self.validation_errors.append(error_msg)
            logger.error(error_msg)
            return False
        
        self.stats["students"]["migrated"] += 1
        logger.info(f"Student mapping added: V1({v1_id}) -> V2_User({v2_user_id}) @ School({v2_school_id}) [{school_info.get('name', '') if school_info else 'Unknown'}]")
        return True
    
    def validate_v1_school_reference(self, v1_school_id: int, entity_type: str, entity_data: Dict[str, Any]) -> bool:
        """Validate that a V1 school reference exists in the migration session"""
        v2_school_id = self.get_v2_school_id(v1_school_id)
        
        if not v2_school_id:
            entity_name = f"{entity_data.get('firstname', '')} {entity_data.get('lastname', '')}"
            error_msg = f"Cannot migrate {entity_type} '{entity_name}' (V1 ID: {entity_data.get('id', 'Unknown')}): V1 School ID {v1_school_id} was not successfully migrated"
            self.validation_errors.append(error_msg)
            logger.error(error_msg)
            return False
        
        return True
    
    async def validate_school_consistency(self) -> Dict[str, Any]:
        """Validate that all entities are correctly associated with their schools"""
        validation_results = {
            "is_valid": True,
            "errors": [],
            "warnings": [],
            "school_summaries": {}
        }
        
        for v2_school_id, curriculum_mapping in self.school_curriculums.items():
            school_mapping = None
            for mapping in self.school_mappings.values():
                if mapping.v2_id == v2_school_id:
                    school_mapping = mapping
                    break
            
            if not school_mapping:
                error = f"School ID {v2_school_id} has curriculum but no school mapping"
                validation_results["errors"].append(error)
                validation_results["is_valid"] = False
                continue
            
            # Validate school has entities
            teacher_count = len(self.school_teachers.get(v2_school_id, set()))
            parent_count = len(self.school_parents.get(v2_school_id, set()))
            student_count = len(self.school_students.get(v2_school_id, set()))
            
            validation_results["school_summaries"][v2_school_id] = {
                "name": school_mapping.metadata.get("name", ""),
                "curriculum": curriculum_mapping.curriculum_name,
                "teachers": teacher_count,
                "parents": parent_count,
                "students": student_count
            }
            
            # Warnings for unusual patterns
            if teacher_count == 0:
                validation_results["warnings"].append(f"School '{school_mapping.metadata.get('name')}' has no teachers")
            
            if student_count == 0:
                validation_results["warnings"].append(f"School '{school_mapping.metadata.get('name')}' has no students")
            
            if student_count > 0 and parent_count == 0:
                validation_results["warnings"].append(f"School '{school_mapping.metadata.get('name')}' has students but no parents")
        
        return validation_results
        """Get detailed migration statistics with validation info"""
        return {
            "session_id": self.session_id,
            "phase": self.current_phase.value,
            "schools": {
                "total_mapped": len(self.school_mappings),
                "with_curriculum": len(self.school_curriculums),
                "with_teachers": len([s for s in self.school_teachers.values() if s]),
                "with_parents": len([s for s in self.school_parents.values() if s]),
                "with_students": len([s for s in self.school_students.values() if s])
            },
            "entities": {
                "teachers": len(self.teacher_mappings),
                "parents": len(self.parent_mappings),
                "students": len(self.student_mappings)
            },
            "validation": {
                "errors": len(self.validation_errors),
                "warnings": len(self.validation_warnings)
            },
            "duration": (datetime.now() - self.start_time).total_seconds()
        }
        """Validate that all entities are correctly associated with their schools"""
        validation_results = {
            "is_valid": True,
            "errors": [],
            "warnings": [],
            "school_summaries": {}
        }
        
        for v2_school_id, curriculum_mapping in self.school_curriculums.items():
            school_mapping = None
            for mapping in self.school_mappings.values():
                if mapping.v2_id == v2_school_id:
                    school_mapping = mapping
                    break
            
            if not school_mapping:
                error = f"School ID {v2_school_id} has curriculum but no school mapping"
                validation_results["errors"].append(error)
                validation_results["is_valid"] = False
                continue
            
            # Validate school has entities
            teacher_count = len(self.school_teachers.get(v2_school_id, set()))
            parent_count = len(self.school_parents.get(v2_school_id, set()))
            student_count = len(self.school_students.get(v2_school_id, set()))
            
            validation_results["school_summaries"][v2_school_id] = {
                "name": school_mapping.metadata.get("name", ""),
                "curriculum": curriculum_mapping.curriculum_name,
                "teachers": teacher_count,
                "parents": parent_count,
                "students": student_count
            }
            
            # Warnings for unusual patterns
            if teacher_count == 0:
                validation_results["warnings"].append(f"School '{school_mapping.metadata.get('name')}' has no teachers")
            
            if student_count == 0:
                validation_results["warnings"].append(f"School '{school_mapping.metadata.get('name')}' has no students")
            
            if student_count > 0 and parent_count == 0:
                validation_results["warnings"].append(f"School '{school_mapping.metadata.get('name')}' has students but no parents")
        
        return validation_results
    
    def get_session_summary(self) -> Dict[str, Any]:
        """Get comprehensive session summary"""
        duration = datetime.now() - self.start_time
        
        return {
            "session_id": self.session_id,
            "current_phase": self.current_phase.value,
            "duration_seconds": duration.total_seconds(),
            "start_time": self.start_time.isoformat(),
            "statistics": self.stats,
            "mappings_count": {
                "schools": len(self.school_mappings),
                "teachers": len(self.teacher_mappings),
                "parents": len(self.parent_mappings),
                "students": len(self.student_mappings)
            },
            "validation_errors": len(self.validation_errors),
            "validation_warnings": len(self.validation_warnings),
            "school_curriculums": len(self.school_curriculums)
        }


# Global session instance
migration_session: Optional[MigrationSession] = None


def get_migration_session() -> Optional[MigrationSession]:
    """Get the current migration session"""
    return migration_session


def create_migration_session(session_id: Optional[str] = None) -> MigrationSession:
    """Create a new migration session"""
    global migration_session
    migration_session = MigrationSession(session_id)
    return migration_session


def clear_migration_session():
    """Clear the current migration session"""
    global migration_session
    migration_session = None
