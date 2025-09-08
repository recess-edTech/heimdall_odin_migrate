# Heimdall to Odin Migration Scripts

This directory contains Python scripts for migrating data from the Heimdall project to the Odin project.

## Setup

### 1. Install Dependencies

```bash
cd migrator
pip install -r requirements.txt
```

### 2. Configure Environment

Copy the example environment file and configure your database connections:

```bash
cp .env.example .env
```

Edit the `.env` file with your actual database credentials and settings.

### 3. Verify Database Connections

Make sure both Heimdall and Odin databases are accessible and running.

## Usage

### 1. Analyze Database Schemas

Before running the migration, analyze the database schemas to understand the structure:

```bash
python -m migrator.analyze_schemas
```

This will:
- Connect to both databases
- List all tables and their schemas
- Compare structures between Heimdall and Odin
- Generate detailed JSON reports

### 2. Run Migration (Dry Run)

First, run the migration in dry-run mode to see what would be migrated:

```bash
python -m migrator.migrate --dry-run --verbose
```

### 3. Run Actual Migration

Once you're satisfied with the dry-run results:

```bash
python -m migrator.migrate --verbose
```

## Scripts Overview

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
