#!/usr/bin/env python3
"""
Migration Status Checker
Provides comprehensive status information about the migration system
"""

import asyncio
import sys
from datetime import datetime
from typing import Dict, Any

from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from sqlalchemy import text

from config import V1_DB_CONFIG, V2_DB_CONFIG
from db_utils import DatabaseManager


console = Console()


async def check_database_connection(db_config: Dict[str, Any], name: str) -> Dict[str, Any]:
    """Check database connection and basic info"""
    result = {
        "name": name,
        "connected": False,
        "error": None,
        "version": None,
        "size": None,
        "tables": 0
    }
    
    try:
        db_manager = DatabaseManager()
        
        if name == "V1":
            engine = await db_manager.connect_v1_async()
        else:
            engine = await db_manager.connect_v2_async()
            
        # Test connection with a simple query using SQLAlchemy async engine
        async with engine.begin() as conn:
            # Get PostgreSQL version
            version_result = await conn.execute(text("SELECT version() as version"))
            version_row = version_result.fetchone()
            result["version"] = version_row[0] if version_row else "Unknown"
            result["connected"] = True
            
            # Get database size
            size_query = text(f"""
                SELECT pg_size_pretty(pg_database_size('{db_config["database"]}')) as size
            """)
            size_result = await conn.execute(size_query)
            size_row = size_result.fetchone()
            result["size"] = size_row[0] if size_row else "Unknown"
            
            # Count tables
            tables_result = await conn.execute(text("""
                SELECT COUNT(*) as table_count 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
            """))
            tables_row = tables_result.fetchone()
            result["tables"] = tables_row[0] if tables_row else 0
        
    except Exception as e:
        result["error"] = str(e)
    
    return result


async def get_migration_counts() -> Dict[str, Dict[str, int]]:
    """Get record counts for migration entities"""
    counts = {"v1": {}, "v2": {}}
    
    try:
        db_manager = DatabaseManager()
        
        # V1 counts
        v1_engine = await db_manager.connect_v1_async()
        
        async with v1_engine.begin() as conn:
            # Schools
            schools_result = await conn.execute(text('SELECT COUNT(*) as count FROM "School"'))
            schools_row = schools_result.fetchone()
            counts["v1"]["schools"] = schools_row[0] if schools_row else 0
            
            # Teachers  
            teachers_result = await conn.execute(text('SELECT COUNT(*) as count FROM "Teacher"'))
            teachers_row = teachers_result.fetchone()
            counts["v1"]["teachers"] = teachers_row[0] if teachers_row else 0
            
            # Parents
            parents_result = await conn.execute(text('SELECT COUNT(*) as count FROM "Parent"'))
            parents_row = parents_result.fetchone()
            counts["v1"]["parents"] = parents_row[0] if parents_row else 0
            
            # Students
            students_result = await conn.execute(text('SELECT COUNT(*) as count FROM "Student"'))
            students_row = students_result.fetchone()
            counts["v1"]["students"] = students_row[0] if students_row else 0
        
        # V2 counts
        v2_engine = await db_manager.connect_v2_async()
        
        async with v2_engine.begin() as conn:
            # Schools
            v2_schools_result = await conn.execute(text('SELECT COUNT(*) as count FROM "School"'))
            v2_schools_row = v2_schools_result.fetchone()
            counts["v2"]["schools"] = v2_schools_row[0] if v2_schools_row else 0
            
            # Users (all types)
            v2_users_result = await conn.execute(text('SELECT COUNT(*) as count FROM "User"'))
            v2_users_row = v2_users_result.fetchone()
            counts["v2"]["users"] = v2_users_row[0] if v2_users_row else 0
            
            # Teachers (via UserRole)
            v2_teachers_result = await conn.execute(text("""
                SELECT COUNT(*) as count 
                FROM "UserRole" ur 
                JOIN "Role" r ON ur."roleId" = r.id 
                WHERE r.name = 'teacher'
            """))
            v2_teachers_row = v2_teachers_result.fetchone()
            counts["v2"]["teachers"] = v2_teachers_row[0] if v2_teachers_row else 0
            
            # Parents (via UserRole)
            v2_parents_result = await conn.execute(text("""
                SELECT COUNT(*) as count 
                FROM "UserRole" ur 
                JOIN "Role" r ON ur."roleId" = r.id 
                WHERE r.name = 'parent'
            """))
            v2_parents_row = v2_parents_result.fetchone()
            counts["v2"]["parents"] = v2_parents_row[0] if v2_parents_row else 0
            
            # Students (via UserRole)
            v2_students_result = await conn.execute(text("""
                SELECT COUNT(*) as count 
                FROM "UserRole" ur 
                JOIN "Role" r ON ur."roleId" = r.id 
                WHERE r.name = 'student'
            """))
            v2_students_row = v2_students_result.fetchone()
            counts["v2"]["students"] = v2_students_row[0] if v2_students_row else 0
        
    except Exception as e:
        console.print(f"[red]Error getting migration counts: {e}[/red]")
    
    return counts


def create_connection_table(v1_status: Dict, v2_status: Dict) -> Table:
    """Create database connection status table"""
    table = Table(title="Database Connection Status", show_header=True)
    table.add_column("Database", style="cyan")
    table.add_column("Status", style="green") 
    table.add_column("Version", style="yellow")
    table.add_column("Size", style="blue")
    table.add_column("Tables", style="magenta")
    
    # V1 row
    v1_status_str = "✅ Connected" if v1_status["connected"] else f"❌ Failed: {v1_status['error']}"
    table.add_row(
        "V1 (Source)",
        v1_status_str,
        v1_status["version"][:50] + "..." if v1_status["version"] else "N/A",
        v1_status["size"] or "N/A",
        str(v1_status["tables"])
    )
    
    # V2 row
    v2_status_str = "✅ Connected" if v2_status["connected"] else f"❌ Failed: {v2_status['error']}"
    table.add_row(
        "V2 (Target)",
        v2_status_str,
        v2_status["version"][:50] + "..." if v2_status["version"] else "N/A", 
        v2_status["size"] or "N/A",
        str(v2_status["tables"])
    )
    
    return table


def create_migration_table(counts: Dict) -> Table:
    """Create migration counts comparison table"""
    table = Table(title="Migration Progress", show_header=True)
    table.add_column("Entity", style="cyan")
    table.add_column("V1 (Source)", style="yellow")
    table.add_column("V2 (Target)", style="green")
    table.add_column("Progress", style="blue")
    
    entities = ["schools", "teachers", "parents", "students"]
    
    for entity in entities:
        v1_count = counts.get("v1", {}).get(entity, 0)
        v2_count = counts.get("v2", {}).get(entity, 0)
        
        if entity == "teachers":
            # For teachers, compare V1 teachers to V2 teacher roles
            progress = f"{v2_count}/{v1_count}" if v1_count > 0 else "0/0"
        elif entity == "parents":
            # For parents, compare V1 parents to V2 parent roles  
            progress = f"{v2_count}/{v1_count}" if v1_count > 0 else "0/0"
        elif entity == "students":
            # For students, compare V1 students to V2 student roles
            progress = f"{v2_count}/{v1_count}" if v1_count > 0 else "0/0"
        else:
            # For schools, direct comparison
            progress = f"{v2_count}/{v1_count}" if v1_count > 0 else "0/0"
        
        table.add_row(
            entity.title(),
            str(v1_count),
            str(v2_count),
            progress
        )
    
    # Add total users row
    total_users = counts.get("v2", {}).get("users", 0)
    table.add_row(
        "Total Users",
        "-",
        str(total_users),
        "All roles"
    )
    
    return table


async def main():
    """Main status check function"""
    console.print("\n[bold blue]🔍 Migration System Status Check[/bold blue]\n")
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console
    ) as progress:
        
        # Check database connections
        task1 = progress.add_task("Checking database connections...", total=None)
        
        v1_status, v2_status = await asyncio.gather(
            check_database_connection(V1_DB_CONFIG, "V1"),
            check_database_connection(V2_DB_CONFIG, "V2")
        )
        
        progress.update(task1, description="✅ Database connections checked")
        
        # Get migration counts
        task2 = progress.add_task("Gathering migration statistics...", total=None)
        counts = await get_migration_counts()
        progress.update(task2, description="✅ Migration statistics gathered")
    
    # Display results
    console.print()
    console.print(create_connection_table(v1_status, v2_status))
    console.print()
    console.print(create_migration_table(counts))
    
    # Summary panel
    both_connected = v1_status["connected"] and v2_status["connected"]
    
    if both_connected:
        status_text = "✅ Both databases are accessible and ready for migration"
        panel_style = "green"
    else:
        status_text = "❌ Database connection issues detected. Check configuration."
        panel_style = "red"
    
    console.print()
    console.print(Panel(
        f"[bold]{status_text}[/bold]\n\n"
        f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        title="Migration System Status",
        border_style=panel_style
    ))
    
    # Exit with error code if connections failed
    if not both_connected:
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
