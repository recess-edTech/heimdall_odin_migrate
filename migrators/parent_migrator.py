"""
Parent migration from V1 to V2 schema
Creates User records and Parent records for each V1 parent
"""

import logging
from typing import Dict, Any, List, Optional

from ..db_utils import db_manager
from ..user_utils import user_manager
from .school_migrator import school_migrator

logger = logging.getLogger(__name__)


class ParentMigrator:
    """Handles migration of parents from V1 to V2"""
    
    def __init__(self):
        self.parent_mappings = {}  # Maps V1 parent IDs to V2 user IDs
        self.migrated_count = 0
        self.failed_count = 0
        
    async def migrate_parents(self) -> Dict[str, Any]:
        """Main method to migrate all parents"""
        logger.info("Starting parent migration...")
        
        try:
            # Get all parents from V1
            v1_parents = await self._get_v1_parents()
            logger.info(f"Found {len(v1_parents)} parents in V1 database")
            
            # Migrate each parent
            for parent in v1_parents:
                success = await self._migrate_single_parent(parent)
                if success:
                    self.migrated_count += 1
                else:
                    self.failed_count += 1
            
            result = {
                "success": True,
                "migrated": self.migrated_count,
                "failed": self.failed_count,
                "total": len(v1_parents),
                "mappings": self.parent_mappings
            }
            
            logger.info(f"Parent migration completed: {self.migrated_count} successful, {self.failed_count} failed")
            return result
            
        except Exception as e:
            logger.error(f"Parent migration failed: {e}")
            return {"success": False, "error": str(e)}
    
    async def _get_v1_parents(self) -> List[Dict[str, Any]]:
        """Get all parents from V1 database"""
        query = """
            SELECT 
                id, firstName, middleName, lastName, email, phoneNumber,
                secondaryPhoneNumber, isFirstTimePasswordChanged, isLoginBarred,
                relationship, profileImage, password, isDeleted, schoolId,
                lastLoggedIn
            FROM "Parent"
            WHERE isDeleted = false
            ORDER BY schoolId, firstName
        """
        
        return await db_manager.execute_query(query, engine_version="v1")
    
    async def _migrate_single_parent(self, v1_parent: Dict[str, Any]) -> bool:
        """Migrate a single parent from V1 to V2"""
        try:
            # Get V2 school ID from mapping
            v2_school_id = school_migrator.get_v2_school_id(v1_parent['schoolid'])
            if not v2_school_id:
                logger.error(f"No V2 school found for V1 school ID: {v1_parent['schoolid']}")
                return False
            
            # Step 1: Create User record
            user_id = await user_manager.create_parent_user(v1_parent, v2_school_id)
            if not user_id:
                logger.error(f"Failed to create user for parent: {v1_parent['firstname']} {v1_parent['lastname']}")
                return False
            
            # Step 2: Create Parent record
            parent_data = {
                "user_id": user_id,
                "occupation": None,  # V1 doesn't have occupation
                "type": self._map_parent_type(v1_parent.get('relationship')),
                "address": None,  # V1 doesn't have parent address separate from school
            }
            
            # Remove None values
            parent_data = {k: v for k, v in parent_data.items() if v is not None}
            
            # Insert parent
            await db_manager.insert_record("parents", parent_data)
            
            # Store mapping
            self.parent_mappings[v1_parent['id']] = user_id
            logger.info(f"Migrated parent: {v1_parent['firstname']} {v1_parent['lastname']} (User ID: {user_id})")
            return True
                
        except Exception as e:
            logger.error(f"Error migrating parent {v1_parent.get('firstname', '')} {v1_parent.get('lastname', '')}: {e}")
            logger.error(f"Parent data: {v1_parent}")
            return False
    
    def _map_parent_type(self, relationship: Optional[str]) -> str:
        """Map V1 relationship to V2 parent type"""
        if not relationship:
            return "GUARDIAN"
            
        relationship_lower = relationship.lower()
        
        if "father" in relationship_lower or "dad" in relationship_lower:
            return "FATHER"
        elif "mother" in relationship_lower or "mom" in relationship_lower or "mum" in relationship_lower:
            return "MOTHER"
        else:
            return "GUARDIAN"
    
    def get_v2_user_id(self, v1_parent_id: str) -> Optional[int]:
        """Get V2 user ID from V1 parent ID"""
        return self.parent_mappings.get(v1_parent_id)
    
    def get_mapping_stats(self) -> Dict[str, int]:
        """Get statistics about parent migration"""
        return {
            "total_migrated": self.migrated_count,
            "total_failed": self.failed_count,
            "total_mappings": len(self.parent_mappings)
        }


# Global parent migrator instance  
parent_migrator = ParentMigrator()
