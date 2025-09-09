# V1 to V2 Database Migration System

A comprehensive Python-based migration system for migrating data from V1 Prisma schema to V2 Prisma schema. This system handles the key architectural change where V1 uses separate authentication tables for teachers, parents, and students, while V2 uses a centralized User table with role-based relationships.

## 🏗️ Architecture Overview

### Key Changes V1 → V2
- **V1**: Separate `Teacher`, `Parent`, `Student` tables with direct authentication
- **V2**: Central `User` table with role-based relationships via `UserRole`
- **Challenge**: Creating User records for every migrated teacher, parent, and student

### Migration Flow
```
1. Schools → 2. Teachers → 3. Parents → 4. Students
```

Each step creates necessary User records and maintains referential integrity.

## 📋 Prerequisites

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Environment Configuration

Create a `.env` file with your database configurations:

```bash
# V1 Database (Source)
V1_DB_HOST=localhost
V1_DB_PORT=5432
V1_DB_NAME=v1_database
V1_DB_USER=your_user
V1_DB_PASSWORD=your_password

# V2 Database (Target)
V2_DB_HOST=localhost
V2_DB_PORT=5432
V2_DB_NAME=v2_database
V2_DB_USER=your_user
V2_DB_PASSWORD=your_password
```

### 3. Setup V2 Database Structure

Before running migrations, initialize the V2 database with required reference data:

```bash
python setup_v2.py
```

This creates:
- Permission modules and permissions
- Grade systems and levels  
- Default curriculums
- Class levels
- Subject categories and subjects
- User types and roles

## 🚀 Quick Start

### Basic Migration

```bash
# Full migration (recommended)
python migrate.py

# Dry run (preview changes)
python migrate.py --dry-run

# Migrate specific entities
python migrate.py --entities schools,teachers

# Skip confirmation prompts
python migrate.py --force
```

### Check Migration Status

```bash
# Analyze source schemas
python analyze_schemas.py

# View migration progress
python migrate.py --status
```

## 🔧 Core Components

### Database Utilities (`db_utils.py`)
- `DatabaseManager`: Handles V1/V2 database connections
- Async and sync connection support
- Query execution and bulk operations
- Connection pooling and error handling

### User Management (`user_utils.py`)  
- `UserManager`: Central user creation logic
- Email/phone uniqueness handling
- Role-based user creation (Teacher/Parent/Student)
- Password generation and hashing

### Individual Migrators
- `SchoolMigrator`: Schools and admin users
- `TeacherMigrator`: Teachers with User records
- `ParentMigrator`: Parents with User records  
- `StudentMigrator`: Students with parent relationships

### Configuration (`config.py`)
- Database connection settings
- Migration order and dependencies
- Default values and mappings

## 📊 Migration Details

### User Creation Loop Pattern

For each migrated entity (Teacher/Parent/Student), the system:

1. **Checks Uniqueness**: Verifies email/phone not already used
2. **Resolves Conflicts**: Adds counter suffixes for duplicates  
3. **Creates User Record**: With appropriate role and profile data
4. **Links Relationships**: Connects to schools, classes, parents as needed

### Sequential Processing

```bash
Schools First   → Provides school mappings for other entities
Teachers Next   → Creates teacher users and assignments  
Parents Then    → Creates parent users with school links
Students Last   → Creates student users with parent relationships
```

### Data Transformations

- **Phone Numbers**: Standardized format with country codes
- **Addresses**: Mapped from V1 structure to V2 components
- **Names**: Split full names into first/middle/last components
- **Emails**: Generated for missing email addresses
- **Passwords**: Auto-generated secure passwords for all users

## 🔍 Troubleshooting

### Common Issues

**Database Connection Errors**
```bash
# Check connection settings
python -c "from config import V1_DB_CONFIG, V2_DB_CONFIG; print('V1:', V1_DB_CONFIG); print('V2:', V2_DB_CONFIG)"

# Test connections individually  
python -c "from db_utils import DatabaseManager; dm = DatabaseManager(); dm.connect_v1_sync()"
```

**Migration Failures**
```bash
# Check logs for detailed error messages
tail -f migration.log

# Run with verbose output
python migrate.py --verbose --dry-run
```

**Duplicate Data**
```bash
# Check for existing data in V2
python -c "from db_utils import DatabaseManager; dm = DatabaseManager(); print(dm.execute_query('SELECT COUNT(*) FROM \"User\"', db='v2'))"

# Clean V2 database if needed (⚠️ DESTRUCTIVE)
python setup_v2.py --clean
```

### Performance Optimization

- **Batch Processing**: Migrators use bulk inserts for better performance
- **Connection Pooling**: Async connections reduce overhead  
- **Progress Tracking**: Real-time progress indicators
- **Memory Management**: Processes data in chunks

## 🧪 Testing

### Unit Tests
```bash
# Run all tests
python -m pytest tests/

# Run specific test categories
python -m pytest tests/unit/
python -m pytest tests/integration/
```

### Integration Tests  
```bash
# Test with sample data
python -m pytest tests/integration/test_full_migration.py

# Test individual migrators
python -m pytest tests/integration/test_teacher_migrator.py
```

## 📈 Monitoring

### Progress Tracking

The migration system provides detailed progress information:

- **Entity Counts**: Shows total entities to migrate
- **Success Rates**: Tracks successful vs failed migrations  
- **Error Details**: Logs specific failure reasons
- **Time Estimates**: Provides completion time estimates

### Validation

After migration:

```bash
# Verify data integrity
python migrate.py --validate

# Compare record counts
python analyze_schemas.py --compare-counts

# Test user authentication  
python -c "from user_utils import UserManager; um = UserManager(); um.verify_user_login('test@example.com')"
```

## 🔐 Security Considerations

- **Password Generation**: Auto-generated secure 12-character passwords
- **Data Sanitization**: Input validation and SQL injection prevention
- **Connection Security**: SSL/TLS encryption for database connections
- **Audit Trail**: Complete migration logs for compliance

## 📚 Advanced Usage

### Custom Migration Scripts

Create custom migrators by extending base classes:

```python
from migrators.base_migrator import BaseMigrator

class CustomMigrator(BaseMigrator):
    async def migrate_entity(self, entity_data):
        # Custom migration logic
        pass
```

### Selective Migration

```bash
# Migrate only specific schools
python migrate.py --filter "school_id IN (1,2,3)"

# Migrate users created after date
python migrate.py --filter "created_at > '2024-01-01'"

# Skip validation for faster processing  
python migrate.py --skip-validation
```

## 🤝 Contributing

1. Fork the repository
2. Create feature branch: `git checkout -b feature/new-migrator`
3. Add tests for new functionality
4. Run test suite: `python -m pytest`
5. Submit pull request with detailed description

## 📝 License

This migration system is part of the V1→V2 database migration project.

- **`config.py`** - Configuration settings and database connections
- **`db_utils.py`** - Database utilities and connection management
- **`migrate.py`** - Main migration runner script
- **`analyze_schemas.py`** - Database schema analysis tool
- **`utils.py`** - Utility functions for data transformation and logging

## Features

### Database Migration
- **Schema Analysis** - Compares database structures
- **Batch Processing** - Processes data in configurable batches
- **Data Transformation** - Transforms data between different schemas
- **Error Handling** - Robust error handling with detailed logging
- **Backup Creation** - Creates backups before migration
- **Validation** - Validates migration results

### File Migration
- **JSON Files** - Migrates JSON configuration files
- **CSV Samples** - Migrates sample data files
- **Configurable Paths** - Flexible source and destination paths

### Safety Features
- **Dry Run Mode** - Test migrations without making changes
- **Transaction Safety** - Uses database transactions for data integrity
- **Logging** - Comprehensive logging of all operations
- **Migration Reports** - Detailed reports of migration results

## Configuration

### Database Settings

Set these environment variables in your `.env` file:

```
# Heimdall Database
HEIMDALL_DB_HOST=localhost
HEIMDALL_DB_PORT=5432
HEIMDALL_DB_NAME=heimdall
HEIMDALL_DB_USER=postgres
HEIMDALL_DB_PASSWORD=your_password

# Odin Database  
ODIN_DB_HOST=localhost
ODIN_DB_PORT=5432
ODIN_DB_NAME=odin
ODIN_DB_USER=postgres
ODIN_DB_PASSWORD=your_password
```

### Migration Settings

```
MIGRATION_BATCH_SIZE=1000    # Number of records to process at once
DRY_RUN=False               # Set to True for dry-run mode
LOG_LEVEL=INFO              # Logging level (DEBUG, INFO, WARNING, ERROR)
```

## Customization

### Adding Data Transformations

Edit the transformation functions in `utils.py`:

```python
def transform_user_data(df: pd.DataFrame) -> pd.DataFrame:
    # Add your custom transformations here
    df['new_column'] = df['old_column'].apply(lambda x: transform_logic(x))
    return df
```

### Custom Table Mappings

Modify the `get_table_mappings` method in `migrate.py`:

```python
async def get_table_mappings(self, schema_analysis: Dict) -> Dict[str, str]:
    mappings = {}
    # Add custom table mappings
    mappings["heimdall_users"] = "odin_users"
    mappings["heimdall_schools"] = "odin_institutions"
    return mappings
```

## Troubleshooting

### Common Issues

1. **Connection Errors**
   - Verify database credentials in `.env` file
   - Ensure databases are running and accessible

2. **Schema Mismatches**
   - Run schema analysis first
   - Add necessary transformations in `utils.py`

3. **Memory Issues with Large Data**
   - Reduce `MIGRATION_BATCH_SIZE` in config
   - Consider running migrations table by table

### Getting Help

1. Run with `--verbose` flag for detailed logging
2. Check the generated log files
3. Review the schema comparison reports

## Safety Recommendations

1. **Always run dry-run first** to understand what will be migrated
2. **Backup your databases** before running the actual migration
3. **Test with a small dataset** first
4. **Monitor the migration** process and check logs regularly
5. **Validate results** after migration completes

## File Structure

```
migrator/
├── __init__.py              # Package initialization
├── config.py                # Configuration settings
├── db_utils.py             # Database utilities
├── migrate.py              # Main migration script
├── analyze_schemas.py      # Schema analysis tool
├── utils.py                # Utility functions
├── requirements.txt        # Python dependencies
├── .env.example           # Environment template
└── README.md              # This file
```
