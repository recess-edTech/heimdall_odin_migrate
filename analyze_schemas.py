#!/usr/bin/env python3
"""
Schema analysis script to understand the structure of Heimdall and Odin databases
"""

import asyncio
import json
from pathlib import Path
from datetime import datetime

from .db_utils.db_utils import db_manager
from .utils import setup_logging

setup_logging("INFO")


async def analyze_database_schema(engine, db_name: str):
    """Analyze database schema and return detailed information"""
    
    schema_info = {
        "database": db_name,
        "tables": [],
        "analysis_timestamp": datetime.now().isoformat()
    }
    
    # Get all tables
    tables = await db_manager.get_table_list(engine)
    print(f"\n{db_name} database has {len(tables)} tables:")
    
    for table in tables:
        print(f"  - {table}")
        
        # Get table schema
        columns = await db_manager.get_table_schema(engine, table)
        
        # Get row count
        try:
            count_result = await db_manager.execute_query(
                engine, 
                f"SELECT COUNT(*) as count FROM {table}"
            )
            row_count = count_result[0]["count"] if count_result else 0
        except Exception as e:
            row_count = f"Error: {e}"
        
        table_info = {
            "name": table,
            "columns": columns,
            "row_count": row_count
        }
        
        schema_info["tables"].append(table_info)
    
    return schema_info


async def compare_schemas(heimdall_schema, odin_schema):
    """Compare schemas between Heimdall and Odin"""
    
    heimdall_tables = {table["name"]: table for table in heimdall_schema["tables"]}
    odin_tables = {table["name"]: table for table in odin_schema["tables"]}
    
    comparison = {
        "common_tables": [],
        "heimdall_only": [],
        "odin_only": [],
        "schema_differences": []
    }
    
    all_tables = set(heimdall_tables.keys()) | set(odin_tables.keys())
    
    for table_name in sorted(all_tables):
        if table_name in heimdall_tables and table_name in odin_tables:
            comparison["common_tables"].append(table_name)
            
            # Compare column schemas
            heimdall_cols = {col["column_name"]: col for col in heimdall_tables[table_name]["columns"]}
            odin_cols = {col["column_name"]: col for col in odin_tables[table_name]["columns"]}
            
            column_diff = {
                "table": table_name,
                "common_columns": list(set(heimdall_cols.keys()) & set(odin_cols.keys())),
                "heimdall_only_columns": list(set(heimdall_cols.keys()) - set(odin_cols.keys())),
                "odin_only_columns": list(set(odin_cols.keys()) - set(heimdall_cols.keys())),
                "type_differences": []
            }
            
            # Check for type differences in common columns
            for col_name in column_diff["common_columns"]:
                h_col = heimdall_cols[col_name]
                o_col = odin_cols[col_name]
                
                if h_col["data_type"] != o_col["data_type"]:
                    column_diff["type_differences"].append({
                        "column": col_name,
                        "heimdall_type": h_col["data_type"],
                        "odin_type": o_col["data_type"]
                    })
            
            comparison["schema_differences"].append(column_diff)
            
        elif table_name in heimdall_tables:
            comparison["heimdall_only"].append(table_name)
        else:
            comparison["odin_only"].append(table_name)
    
    return comparison


async def main():
    """Main analysis function"""
    print("Starting database schema analysis...")
    
    try:
        # Connect to databases
        heimdall_engine = await db_manager.connect_heimdall_async()
        odin_engine = await db_manager.connect_odin_async()
        
        # Analyze schemas
        print("\nAnalyzing Heimdall database...")
        heimdall_schema = await analyze_database_schema(heimdall_engine, "Heimdall")
        
        print("\nAnalyzing Odin database...")
        odin_schema = await analyze_database_schema(odin_engine, "Odin")
        
        # Compare schemas
        print("\nComparing schemas...")
        comparison = await compare_schemas(heimdall_schema, odin_schema)
        
        # Print summary
        print("\n" + "="*60)
        print("SCHEMA COMPARISON SUMMARY")
        print("="*60)
        print(f"Common tables: {len(comparison['common_tables'])}")
        print(f"Heimdall-only tables: {len(comparison['heimdall_only'])}")
        print(f"Odin-only tables: {len(comparison['odin_only'])}")
        
        if comparison['common_tables']:
            print(f"\nCommon tables:")
            for table in comparison['common_tables']:
                print(f"  - {table}")
        
        if comparison['heimdall_only']:
            print(f"\nTables only in Heimdall:")
            for table in comparison['heimdall_only']:
                print(f"  - {table}")
        
        if comparison['odin_only']:
            print(f"\nTables only in Odin:")
            for table in comparison['odin_only']:
                print(f"  - {table}")
        
        # Show schema differences for common tables
        print(f"\nSchema differences in common tables:")
        for diff in comparison['schema_differences']:
            if (diff['heimdall_only_columns'] or 
                diff['odin_only_columns'] or 
                diff['type_differences']):
                
                print(f"\n  Table: {diff['table']}")
                if diff['heimdall_only_columns']:
                    print(f"    Heimdall-only columns: {diff['heimdall_only_columns']}")
                if diff['odin_only_columns']:
                    print(f"    Odin-only columns: {diff['odin_only_columns']}")
                if diff['type_differences']:
                    print(f"    Type differences:")
                    for type_diff in diff['type_differences']:
                        print(f"      - {type_diff['column']}: {type_diff['heimdall_type']} -> {type_diff['odin_type']}")
        
        # Save detailed analysis to files
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Save individual schemas
        with open(f"heimdall_schema_{timestamp}.json", "w") as f:
            json.dump(heimdall_schema, f, indent=2, default=str)
        
        with open(f"odin_schema_{timestamp}.json", "w") as f:
            json.dump(odin_schema, f, indent=2, default=str)
        
        # Save comparison
        with open(f"schema_comparison_{timestamp}.json", "w") as f:
            json.dump(comparison, f, indent=2, default=str)
        
        print(f"\nDetailed analysis saved to:")
        print(f"  - heimdall_schema_{timestamp}.json")
        print(f"  - odin_schema_{timestamp}.json")
        print(f"  - schema_comparison_{timestamp}.json")
        
    except Exception as e:
        print(f"Error during analysis: {e}")
        raise
    finally:
        db_manager.close_connections()


if __name__ == "__main__":
    asyncio.run(main())
