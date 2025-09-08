"""
User creation utilities for migrating from V1 to V2 schema
Handles the central challenge of creating User records for Teachers, Parents, Students
"""

import logging
import uuid
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
from enum import Enum

from ..db_utils import db_manager
from ..config import DEFAULT_VALUES

logger = logging.getLogger(__name__)


class UserType(Enum):
    TEACHER = "TEACHER"
    PARENT = "PARENT"
    STUDENT = "STUDENT"
    SCHOOL_ADMIN = "SCHOOL_ADMIN"


@dataclass
class UserData:
    """Data structure for user information"""
    first_name: str
    middle_name: Optional[str]
    last_name: str
    email: Optional[str]
    phone_number: Optional[str]
    password: Optional[str]
    user_type: UserType
    school_id: int
    profile_picture: Optional[str] = None
    country: str = "Kenya"
    is_active: bool = True
    is_verified: bool = False
    v1_id: Optional[str] = None  # Store V1 ID for mapping


class UserManager:
    """Manages user creation and mapping between V1 and V2"""
    
    def __init__(self):
        self.user_mappings = {}  # Maps V1 IDs to V2 User IDs
        self.email_counter = {}  # Counter for duplicate emails
        self.phone_counter = {}  # Counter for duplicate phones
        
    async def create_user(self, user_data: UserData) -> Optional[int]:
        """Create a user in V2 database and return the user ID"""
        try:
            # Handle email uniqueness
            email = await self._ensure_unique_email(user_data.email, user_data.user_type)
            
            # Handle phone uniqueness  
            phone = await self._ensure_unique_phone(user_data.phone_number, user_data.user_type)
            
            # Generate UUID
            user_uuid = str(uuid.uuid4())
            
            # Prepare user data for insertion
            insert_data = {
                "uuid": user_uuid,
                "first_name": user_data.first_name,
                "middle_name": user_data.middle_name,
                "last_name": user_data.last_name,
                "email": email,
                "phone_number": phone,
                "password": user_data.password,
                "is_password_set": bool(user_data.password),
                "profile_picture": user_data.profile_picture,
                "type": user_data.user_type.value,
                "is_active": user_data.is_active,
                "school_id": user_data.school_id,
                "is_verified": user_data.is_verified,
                "country": user_data.country,
            }
            
            # Remove None values
            insert_data = {k: v for k, v in insert_data.items() if v is not None}
            
            # Insert user
            user_id = await db_manager.insert_record("users", insert_data)
            
            if user_id and user_data.v1_id:
                self.user_mappings[user_data.v1_id] = user_id
                
            logger.info(f"Created {user_data.user_type.value} user: {user_data.first_name} {user_data.last_name} (ID: {user_id})")
            return user_id
            
        except Exception as e:
            logger.error(f"Error creating user {user_data.first_name} {user_data.last_name}: {e}")
            logger.error(f"User data: {user_data}")
            return None
    
    async def _ensure_unique_email(self, email: Optional[str], user_type: UserType) -> Optional[str]:
        """Ensure email is unique, handle duplicates by appending counter"""
        if not email:
            return None
            
        # Check if email already exists
        existing = await db_manager.execute_query(
            "SELECT id FROM users WHERE email = :email LIMIT 1",
            {"email": email}
        )
        
        if not existing:
            return email
            
        # Email exists, create a unique variant
        if email not in self.email_counter:
            self.email_counter[email] = 1
        else:
            self.email_counter[email] += 1
            
        # Split email to insert counter before @
        local_part, domain = email.split('@', 1)
        unique_email = f"{local_part}+{user_type.value.lower()}{self.email_counter[email]}@{domain}"
        
        logger.warning(f"Email {email} already exists, using {unique_email}")
        return unique_email
    
    async def _ensure_unique_phone(self, phone: Optional[str], user_type: UserType) -> Optional[str]:
        """Ensure phone is unique, handle duplicates by appending counter"""
        if not phone:
            return None
            
        # Clean phone number (remove spaces, dashes, etc.)
        clean_phone = ''.join(filter(str.isdigit, phone))
        
        # Check if phone already exists
        existing = await db_manager.execute_query(
            "SELECT id FROM users WHERE phone_number = :phone LIMIT 1",
            {"phone": clean_phone}
        )
        
        if not existing:
            return clean_phone
            
        # Phone exists, create a unique variant
        if clean_phone not in self.phone_counter:
            self.phone_counter[clean_phone] = 1
        else:
            self.phone_counter[clean_phone] += 1
            
        unique_phone = f"{clean_phone}{self.phone_counter[clean_phone]}"
        
        logger.warning(f"Phone {clean_phone} already exists, using {unique_phone}")
        return unique_phone
    
    async def create_teacher_user(self, v1_teacher: Dict[str, Any], school_id: int) -> Optional[int]:
        """Create user for V1 teacher record"""
        user_data = UserData(
            first_name=v1_teacher.get('firstName', ''),
            middle_name=v1_teacher.get('middleName'),
            last_name=v1_teacher.get('lastName', ''),
            email=v1_teacher.get('email'),
            phone_number=v1_teacher.get('phoneNumber'),
            password=v1_teacher.get('password'),
            user_type=UserType.TEACHER,
            school_id=school_id,
            profile_picture=v1_teacher.get('profileImage'),
            is_active=not v1_teacher.get('isDeleted', False),
            v1_id=v1_teacher['id']
        )
        
        return await self.create_user(user_data)
    
    async def create_parent_user(self, v1_parent: Dict[str, Any], school_id: int) -> Optional[int]:
        """Create user for V1 parent record"""
        user_data = UserData(
            first_name=v1_parent.get('firstName', ''),
            middle_name=v1_parent.get('middleName'),
            last_name=v1_parent.get('lastName', ''),
            email=v1_parent.get('email'),
            phone_number=v1_parent.get('phoneNumber'),
            password=v1_parent.get('password'),
            user_type=UserType.PARENT,
            school_id=school_id,
            profile_picture=v1_parent.get('profileImage'),
            is_active=not v1_parent.get('isDeleted', False),
            v1_id=v1_parent['id']
        )
        
        return await self.create_user(user_data)
    
    async def create_student_user(self, v1_student: Dict[str, Any], school_id: int) -> Optional[int]:
        """Create user for V1 student record"""
        # Students in V1 might not have email/password, that's ok
        user_data = UserData(
            first_name=v1_student.get('firstName', ''),
            middle_name=v1_student.get('middleName'),
            last_name=v1_student.get('lastName', ''),
            email=None,  # Students typically don't have emails in V1
            phone_number=None,  # Students typically don't have phones in V1
            password=None,  # Students typically don't have passwords in V1
            user_type=UserType.STUDENT,
            school_id=school_id,
            profile_picture=v1_student.get('profileImage'),
            is_active=not v1_student.get('isDeleted', False),
            v1_id=v1_student['id']
        )
        
        return await self.create_user(user_data)
    
    def get_v2_user_id(self, v1_id: str) -> Optional[int]:
        """Get V2 user ID from V1 ID"""
        return self.user_mappings.get(v1_id)
    
    def get_mapping_stats(self) -> Dict[str, int]:
        """Get statistics about user mappings"""
        return {
            "total_users_created": len(self.user_mappings),
            "unique_emails_generated": len(self.email_counter),
            "unique_phones_generated": len(self.phone_counter)
        }


# Global user manager instance
user_manager = UserManager()
