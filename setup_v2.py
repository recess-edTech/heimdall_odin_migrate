"""
Setup script to prepare V2 database with required default data
This should be run before migration to ensure necessary reference data exists
"""

import logging
import asyncio
from typing import Dict, Any, List

from .db_utils.db_utils import db_manager
from .config.config import DEFAULT_VALUES

logger = logging.getLogger(__name__)


class V2DatabaseSetup:
    """Sets up V2 database with required default data"""
    
    def __init__(self):
        self.setup_log = []
    
    async def setup_v2_database(self) -> Dict[str, Any]:
        """Main method to setup V2 database"""
        logger.info("Setting up V2 database with required defaults...")
        
        try:
            await self._setup_permission_modules()
            await self._setup_permissions()
            await self._setup_grade_systems()
            await self._setup_curriculums()
            await self._setup_class_levels()
            await self._setup_classes()
            await self._setup_subjects()
            
            logger.info("V2 database setup completed successfully")
            return {"success": True, "log": self.setup_log}
            
        except Exception as e:
            logger.error(f"V2 database setup failed: {e}")
            return {"success": False, "error": str(e), "log": self.setup_log}
    
    async def _setup_permission_modules(self):
        """Setup default permission modules"""
        logger.info("Setting up permission modules...")
        
        modules = [
            {"name": "attendance", "alias": "Attendance Management", "is_active": True},
            {"name": "billing", "alias": "Billing & Fees", "is_active": True},
            {"name": "reports", "alias": "Reports & Analytics", "is_active": True},
            {"name": "user_management", "alias": "User Management", "is_active": True},
            {"name": "communication", "alias": "Communication", "is_active": True},
            {"name": "academics", "alias": "Academic Management", "is_active": True},
        ]
        
        for module in modules:
            try:
                # Check if exists
                existing = await db_manager.execute_query(
                    "SELECT id FROM permission_modules WHERE name = :name LIMIT 1",
                    {"name": module["name"]}
                )
                
                if not existing:
                    await db_manager.insert_record("permission_modules", module)
                    logger.info(f"Created permission module: {module['alias']}")
                
            except Exception as e:
                logger.error(f"Error creating permission module {module['name']}: {e}")
        
        self.setup_log.append("Permission modules setup completed")
    
    async def _setup_permissions(self):
        """Setup default permissions"""
        logger.info("Setting up permissions...")
        
        # Get module IDs
        modules = await db_manager.execute_query("SELECT id, name FROM permission_modules")
        module_map = {m['name']: m['id'] for m in modules}
        
        permissions = [
            # Attendance permissions
            {"name": "view_attendance", "alias": "View Attendance", "module_id": module_map.get("attendance"), "scope": "ACCOUNT"},
            {"name": "manage_attendance", "alias": "Manage Attendance", "module_id": module_map.get("attendance"), "scope": "ACCOUNT"},
            
            # User management permissions
            {"name": "view_users", "alias": "View Users", "module_id": module_map.get("user_management"), "scope": "ACCOUNT"},
            {"name": "manage_users", "alias": "Manage Users", "module_id": module_map.get("user_management"), "scope": "ACCOUNT"},
            
            # Academic permissions
            {"name": "view_academics", "alias": "View Academic Data", "module_id": module_map.get("academics"), "scope": "ACCOUNT"},
            {"name": "manage_academics", "alias": "Manage Academic Data", "module_id": module_map.get("academics"), "scope": "ACCOUNT"},
            
            # Billing permissions
            {"name": "view_billing", "alias": "View Billing", "module_id": module_map.get("billing"), "scope": "ACCOUNT"},
            {"name": "manage_billing", "alias": "Manage Billing", "module_id": module_map.get("billing"), "scope": "ACCOUNT"},
            
            # Reports permissions
            {"name": "view_reports", "alias": "View Reports", "module_id": module_map.get("reports"), "scope": "ACCOUNT"},
            {"name": "generate_reports", "alias": "Generate Reports", "module_id": module_map.get("reports"), "scope": "ACCOUNT"},
            
            # Communication permissions
            {"name": "send_notifications", "alias": "Send Notifications", "module_id": module_map.get("communication"), "scope": "ACCOUNT"},
            {"name": "manage_newsletters", "alias": "Manage Newsletters", "module_id": module_map.get("communication"), "scope": "ACCOUNT"},
        ]
        
        for permission in permissions:
            if permission["module_id"] is None:
                continue
                
            try:
                existing = await db_manager.execute_query(
                    "SELECT id FROM permissions WHERE name = :name LIMIT 1",
                    {"name": permission["name"]}
                )
                
                if not existing:
                    await db_manager.insert_record("permissions", permission)
                    logger.info(f"Created permission: {permission['alias']}")
                    
            except Exception as e:
                logger.error(f"Error creating permission {permission['name']}: {e}")
        
        self.setup_log.append("Permissions setup completed")
    
    async def _setup_grade_systems(self):
        """Setup default grade systems"""
        logger.info("Setting up grade systems...")
        
        grade_systems = [
            {
                "name": "Kenya 8-4-4 Standard",
                "description": "Standard Kenyan grading system",
                "grade_type": "LETTER",
                "is_default": True,
                "is_predefined": True,
                "country": "Kenya"
            },
            {
                "name": "Percentage System",
                "description": "0-100 percentage grading",
                "grade_type": "NUMERIC",
                "is_default": False,
                "is_predefined": True,
                "country": "Kenya"
            }
        ]
        
        for grade_system in grade_systems:
            try:
                existing = await db_manager.execute_query(
                    "SELECT id FROM grade_systems WHERE name = :name LIMIT 1",
                    {"name": grade_system["name"]}
                )
                
                if not existing:
                    grade_system_id = await db_manager.insert_record("grade_systems", grade_system)
                    
                    # Add grades for the system
                    if grade_system["name"] == "Kenya 8-4-4 Standard":
                        await self._create_kenyan_grades(grade_system_id)
                    elif grade_system["name"] == "Percentage System":
                        await self._create_percentage_grades(grade_system_id)
                    
                    logger.info(f"Created grade system: {grade_system['name']}")
                    
            except Exception as e:
                logger.error(f"Error creating grade system {grade_system['name']}: {e}")
        
        self.setup_log.append("Grade systems setup completed")
    
    async def _create_kenyan_grades(self, grade_system_id: int):
        """Create Kenyan grade system grades"""
        kenyan_grades = [
            {"name": "A", "min_score": 80.0, "max_score": 100.0, "remark": "Excellent"},
            {"name": "A-", "min_score": 75.0, "max_score": 79.0, "remark": "Very Good"},
            {"name": "B+", "min_score": 70.0, "max_score": 74.0, "remark": "Good"},
            {"name": "B", "min_score": 65.0, "max_score": 69.0, "remark": "Good"},
            {"name": "B-", "min_score": 60.0, "max_score": 64.0, "remark": "Above Average"},
            {"name": "C+", "min_score": 55.0, "max_score": 59.0, "remark": "Above Average"},
            {"name": "C", "min_score": 50.0, "max_score": 54.0, "remark": "Average"},
            {"name": "C-", "min_score": 45.0, "max_score": 49.0, "remark": "Below Average"},
            {"name": "D+", "min_score": 40.0, "max_score": 44.0, "remark": "Below Average"},
            {"name": "D", "min_score": 35.0, "max_score": 39.0, "remark": "Poor"},
            {"name": "D-", "min_score": 30.0, "max_score": 34.0, "remark": "Poor"},
            {"name": "E", "min_score": 0.0, "max_score": 29.0, "remark": "Fail"},
        ]
        
        for grade in kenyan_grades:
            grade["grade_system_id"] = grade_system_id
            await db_manager.insert_record("grades", grade)
    
    async def _create_percentage_grades(self, grade_system_id: int):
        """Create percentage grade system grades"""
        percentage_grades = [
            {"name": "90-100", "min_score": 90.0, "max_score": 100.0, "remark": "Excellent"},
            {"name": "80-89", "min_score": 80.0, "max_score": 89.0, "remark": "Very Good"},
            {"name": "70-79", "min_score": 70.0, "max_score": 79.0, "remark": "Good"},
            {"name": "60-69", "min_score": 60.0, "max_score": 69.0, "remark": "Above Average"},
            {"name": "50-59", "min_score": 50.0, "max_score": 59.0, "remark": "Average"},
            {"name": "40-49", "min_score": 40.0, "max_score": 49.0, "remark": "Below Average"},
            {"name": "0-39", "min_score": 0.0, "max_score": 39.0, "remark": "Fail"},
        ]
        
        for grade in percentage_grades:
            grade["grade_system_id"] = grade_system_id
            await db_manager.insert_record("grades", grade)
    
    async def _setup_curriculums(self):
        """Setup default curriculums"""
        logger.info("Setting up curriculums...")
        
        # Get default grade system ID
        grade_systems = await db_manager.execute_query(
            "SELECT id FROM grade_systems WHERE is_default = true LIMIT 1"
        )
        default_grade_system_id = grade_systems[0]['id'] if grade_systems else 1
        
        curriculums = [
            {
                "name": "kenya_8_4_4",
                "alias": "Kenya 8-4-4 System",
                "country": "Kenya",
                "is_active": True,
                "grade_system_id": default_grade_system_id
            },
            {
                "name": "british_curriculum",
                "alias": "British Curriculum",
                "country": "Kenya",
                "is_active": True,
                "grade_system_id": default_grade_system_id
            },
            {
                "name": "american_curriculum",
                "alias": "American Curriculum",
                "country": "Kenya",
                "is_active": True,
                "grade_system_id": default_grade_system_id
            }
        ]
        
        for curriculum in curriculums:
            try:
                existing = await db_manager.execute_query(
                    "SELECT id FROM curriculums WHERE name = :name LIMIT 1",
                    {"name": curriculum["name"]}
                )
                
                if not existing:
                    await db_manager.insert_record("curriculums", curriculum)
                    logger.info(f"Created curriculum: {curriculum['alias']}")
                    
            except Exception as e:
                logger.error(f"Error creating curriculum {curriculum['name']}: {e}")
        
        self.setup_log.append("Curriculums setup completed")
    
    async def _setup_class_levels(self):
        """Setup default class levels"""
        logger.info("Setting up class levels...")
        
        # Get default curriculum ID
        curriculums = await db_manager.execute_query(
            "SELECT id FROM curriculums WHERE name = 'kenya_8_4_4' LIMIT 1"
        )
        default_curriculum_id = curriculums[0]['id'] if curriculums else 1
        
        class_levels = [
            {"name": "PRIMARY", "min_age": 6, "max_age": 14, "curriculum_id": default_curriculum_id},
            {"name": "JUNIOR SECONDARY", "min_age": 14, "max_age": 16, "curriculum_id": default_curriculum_id},
            {"name": "SENIOR SECONDARY", "min_age": 16, "max_age": 18, "curriculum_id": default_curriculum_id},
        ]
        
        for class_level in class_levels:
            try:
                existing = await db_manager.execute_query(
                    "SELECT id FROM class_levels WHERE name = :name AND curriculum_id = :curriculum_id LIMIT 1",
                    {"name": class_level["name"], "curriculum_id": class_level["curriculum_id"]}
                )
                
                if not existing:
                    await db_manager.insert_record("class_levels", class_level)
                    logger.info(f"Created class level: {class_level['name']}")
                    
            except Exception as e:
                logger.error(f"Error creating class level {class_level['name']}: {e}")
        
        self.setup_log.append("Class levels setup completed")
    
    async def _setup_classes(self):
        """Setup default classes"""
        logger.info("Setting up classes...")
        
        # Get curriculum and level IDs
        curriculums = await db_manager.execute_query("SELECT id FROM curriculums WHERE name = 'kenya_8_4_4' LIMIT 1")
        curriculum_id = curriculums[0]['id'] if curriculums else 1
        
        levels = await db_manager.execute_query("SELECT id, name FROM class_levels WHERE curriculum_id = :curriculum_id", {"curriculum_id": curriculum_id})
        level_map = {level['name']: level['id'] for level in levels}
        
        # Kenyan 8-4-4 classes
        classes = [
            # Primary classes
            {"name": "Class 1", "curriculum_id": curriculum_id, "order": 1, "level_id": level_map.get("PRIMARY", 1)},
            {"name": "Class 2", "curriculum_id": curriculum_id, "order": 2, "level_id": level_map.get("PRIMARY", 1)},
            {"name": "Class 3", "curriculum_id": curriculum_id, "order": 3, "level_id": level_map.get("PRIMARY", 1)},
            {"name": "Class 4", "curriculum_id": curriculum_id, "order": 4, "level_id": level_map.get("PRIMARY", 1)},
            {"name": "Class 5", "curriculum_id": curriculum_id, "order": 5, "level_id": level_map.get("PRIMARY", 1)},
            {"name": "Class 6", "curriculum_id": curriculum_id, "order": 6, "level_id": level_map.get("PRIMARY", 1)},
            {"name": "Class 7", "curriculum_id": curriculum_id, "order": 7, "level_id": level_map.get("PRIMARY", 1)},
            {"name": "Class 8", "curriculum_id": curriculum_id, "order": 8, "level_id": level_map.get("PRIMARY", 1)},
            
            # Junior Secondary
            {"name": "Grade 7", "curriculum_id": curriculum_id, "order": 9, "level_id": level_map.get("JUNIOR SECONDARY", 1)},
            {"name": "Grade 8", "curriculum_id": curriculum_id, "order": 10, "level_id": level_map.get("JUNIOR SECONDARY", 1)},
            {"name": "Grade 9", "curriculum_id": curriculum_id, "order": 11, "level_id": level_map.get("JUNIOR SECONDARY", 1)},
            
            # Senior Secondary
            {"name": "Grade 10", "curriculum_id": curriculum_id, "order": 12, "level_id": level_map.get("SENIOR SECONDARY", 1)},
            {"name": "Grade 11", "curriculum_id": curriculum_id, "order": 13, "level_id": level_map.get("SENIOR SECONDARY", 1)},
            {"name": "Grade 12", "curriculum_id": curriculum_id, "order": 14, "level_id": level_map.get("SENIOR SECONDARY", 1)},
        ]
        
        for cls in classes:
            try:
                existing = await db_manager.execute_query(
                    "SELECT id FROM classes WHERE name = :name AND curriculum_id = :curriculum_id LIMIT 1",
                    {"name": cls["name"], "curriculum_id": cls["curriculum_id"]}
                )
                
                if not existing:
                    await db_manager.insert_record("classes", cls)
                    logger.info(f"Created class: {cls['name']}")
                    
            except Exception as e:
                logger.error(f"Error creating class {cls['name']}: {e}")
        
        self.setup_log.append("Classes setup completed")
    
    async def _setup_subjects(self):
        """Setup default subjects"""
        logger.info("Setting up subjects...")
        
        # Get curriculum ID
        curriculums = await db_manager.execute_query("SELECT id FROM curriculums WHERE name = 'kenya_8_4_4' LIMIT 1")
        curriculum_id = curriculums[0]['id'] if curriculums else 1
        
        subjects = [
            # Core subjects
            {"name": "Mathematics", "curriculum_id": curriculum_id},
            {"name": "English", "curriculum_id": curriculum_id},
            {"name": "Kiswahili", "curriculum_id": curriculum_id},
            {"name": "Science", "curriculum_id": curriculum_id},
            {"name": "Social Studies", "curriculum_id": curriculum_id},
            
            # Secondary subjects
            {"name": "Physics", "curriculum_id": curriculum_id},
            {"name": "Chemistry", "curriculum_id": curriculum_id},
            {"name": "Biology", "curriculum_id": curriculum_id},
            {"name": "History", "curriculum_id": curriculum_id},
            {"name": "Geography", "curriculum_id": curriculum_id},
            {"name": "Computer Studies", "curriculum_id": curriculum_id},
            
            # Creative subjects
            {"name": "Art and Craft", "curriculum_id": curriculum_id},
            {"name": "Music", "curriculum_id": curriculum_id},
            {"name": "Physical Education", "curriculum_id": curriculum_id},
            
            # Languages
            {"name": "French", "curriculum_id": curriculum_id},
            {"name": "German", "curriculum_id": curriculum_id},
            {"name": "Arabic", "curriculum_id": curriculum_id},
        ]
        
        for subject in subjects:
            try:
                existing = await db_manager.execute_query(
                    "SELECT id FROM subjects WHERE name = :name AND curriculum_id = :curriculum_id LIMIT 1",
                    {"name": subject["name"], "curriculum_id": subject["curriculum_id"]}
                )
                
                if not existing:
                    await db_manager.insert_record("subjects", subject)
                    logger.info(f"Created subject: {subject['name']}")
                    
            except Exception as e:
                logger.error(f"Error creating subject {subject['name']}: {e}")
        
        self.setup_log.append("Subjects setup completed")


# Global setup instance
v2_setup = V2DatabaseSetup()


async def main():
    """Main function to run V2 database setup"""
    import argparse
    from .utils import setup_logging
    
    parser = argparse.ArgumentParser(description="Setup V2 database with required defaults")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    
    args = parser.parse_args()
    
    setup_logging("DEBUG" if args.verbose else "INFO")
    
    try:
        result = await v2_setup.setup_v2_database()
        
        if result["success"]:
            print("✅ V2 Database setup completed successfully!")
            for log_entry in result["log"]:
                print(f"  - {log_entry}")
        else:
            print(f"❌ V2 Database setup failed: {result['error']}")
            
    except Exception as e:
        logger.error(f"Setup failed: {e}")
        print(f"❌ Setup failed: {e}")
    finally:
        db_manager.close_connections()


if __name__ == "__main__":
    asyncio.run(main())
