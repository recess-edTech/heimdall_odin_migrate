
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
            conn = await db_manager.connect_v1_async()
        else:
            conn = await db_manager.connect_v2_async()
            
        
        version_result = await conn.fetchrow("SELECT version() as version")
        result["version"] = version_result["version"]
        result["connected"] = True
        
        
        size_query = f"""
        SELECT pg_size_pretty(pg_database_size('{db_config["database"]}')) as size
        """
        size_result = await conn.fetchrow(size_query)
        result["size"] = size_result["size"]
        
        
        tables_result = await conn.fetchrow("""
        SELECT COUNT(*) as table_count 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
        """)
        result["tables"] = tables_result["table_count"]
        
        await conn.close()
        
    except Exception as e:
        result["error"] = str(e)
    
    return result


async def get_migration_counts() -> Dict[str, Dict[str, int]]:
    """Get record counts for migration entities"""
    counts = {"v1": {}, "v2": {}}
    
    try:
        db_manager = DatabaseManager()
        
        
        v1_conn = await db_manager.connect_v1_async()
        
        
        schools_result = await v1_conn.fetchrow('SELECT COUNT(*) as count FROM "School"')
        counts["v1"]["schools"] = schools_result["count"]
        
        
        teachers_result = await v1_conn.fetchrow('SELECT COUNT(*) as count FROM "Teacher"')
        counts["v1"]["teachers"] = teachers_result["count"]
        
        
        parents_result = await v1_conn.fetchrow('SELECT COUNT(*) as count FROM "Parent"')
        counts["v1"]["parents"] = parents_result["count"]
        
        
        students_result = await v1_conn.fetchrow('SELECT COUNT(*) as count FROM "Student"')
        counts["v1"]["students"] = students_result["count"]
        
        await v1_conn.close()
        
        
        v2_conn = await db_manager.connect_v2_async()
        
        
        v2_schools_result = await v2_conn.fetchrow('SELECT COUNT(*) as count FROM "School"')
        counts["v2"]["schools"] = v2_schools_result["count"]
        
        
        v2_users_result = await v2_conn.fetchrow('SELECT COUNT(*) as count FROM "User"')
        counts["v2"]["users"] = v2_users_result["count"]
        
        
        v2_teachers_result = await v2_conn.fetchrow("""
        SELECT COUNT(*) as count 
        FROM "UserRole" ur 
        JOIN "Role" r ON ur."roleId" = r.id 
        WHERE r.name = 'teacher'
        """)
        counts["v2"]["teachers"] = v2_teachers_result["count"]
        
        
        v2_parents_result = await v2_conn.fetchrow("""
        SELECT COUNT(*) as count 
        FROM "UserRole" ur 
        JOIN "Role" r ON ur."roleId" = r.id 
        WHERE r.name = 'parent'
        """)
        counts["v2"]["parents"] = v2_parents_result["count"]
        
        
        v2_students_result = await v2_conn.fetchrow("""
        SELECT COUNT(*) as count 
        FROM "UserRole" ur 
        JOIN "Role" r ON ur."roleId" = r.id 
        WHERE r.name = 'student'
        """)
        counts["v2"]["students"] = v2_students_result["count"]
        
        await v2_conn.close()
        
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
    
    
    v1_status_str = "✅ Connected" if v1_status["connected"] else f"❌ Failed: {v1_status['error']}"
    table.add_row(
        "V1 (Source)",
        v1_status_str,
        v1_status["version"][:50] + "..." if v1_status["version"] else "N/A",
        v1_status["size"] or "N/A",
        str(v1_status["tables"])
    )
    
    
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
            
            progress = f"{v2_count}/{v1_count}" if v1_count > 0 else "0/0"
        elif entity == "parents":
            
            progress = f"{v2_count}/{v1_count}" if v1_count > 0 else "0/0"
        elif entity == "students":
            
            progress = f"{v2_count}/{v1_count}" if v1_count > 0 else "0/0"
        else:
            
            progress = f"{v2_count}/{v1_count}" if v1_count > 0 else "0/0"
        
        table.add_row(
            entity.title(),
            str(v1_count),
            str(v2_count),
            progress
        )
    
    
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
        
        
        task1 = progress.add_task("Checking database connections...", total=None)
        
        v1_status, v2_status = await asyncio.gather(
            check_database_connection(V1_DB_CONFIG, "V1"),
            check_database_connection(V2_DB_CONFIG, "V2")
        )
        
        progress.update(task1, description="✅ Database connections checked")
        
        
        task2 = progress.add_task("Gathering migration statistics...", total=None)
        counts = await get_migration_counts()
        progress.update(task2, description="✅ Migration statistics gathered")
    
    
    console.print()
    console.print(create_connection_table(v1_status, v2_status))
    console.print()
    console.print(create_migration_table(counts))
    
    
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
    
    
    if not both_connected:
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
