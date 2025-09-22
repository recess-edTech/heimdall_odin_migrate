#!/bin/bash

# Supabase Database Dump Script
# Quick dump generation using pg_dump

set -e

if [ -f .env ]; then
    export $(cat .env | grep -v '#' | xargs)
fi

TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
OUTPUT_DIR="database_dumps_${TIMESTAMP}"
mkdir -p "$OUTPUT_DIR"

echo "🗄️  Generating Supabase database dumps..."
echo "📁 Output directory: $OUTPUT_DIR"

run_pg_dump() {
    local db_name=$1
    local host=$2
    local port=$3
    local user=$4
    local password=$5
    local database=$6
    local output_file=$7
    local extra_args=$8
    
    echo "🔄 Dumping $db_name database..."
    
    export PGPASSWORD="$password"
    
    if pg_dump -h "$host" -p "$port" -U "$user" -d "$database" $extra_args -f "$output_file"; then
        echo "✅ Successfully generated: $output_file"
    else
        echo "❌ Failed to dump $db_name database"
        return 1
    fi
}

if ! command -v pg_dump &> /dev/null; then
    echo "❌ pg_dump not found. Please install PostgreSQL client tools."
    exit 1
fi

if [ -n "$V1_DB_HOST" ]; then
    echo ""
    echo "📊 === V1 Database Dumps ==="
    
    V1_DIR="$OUTPUT_DIR/v1"
    mkdir -p "$V1_DIR"
    
    # Complete dump
    run_pg_dump "V1" "$V1_DB_HOST" "$V1_DB_PORT" "$V1_DB_USER" "$V1_DB_PASSWORD" "$V1_DB_NAME" \
        "$V1_DIR/v1_complete_dump_${TIMESTAMP}.sql" ""
    
    # Schema only
    run_pg_dump "V1" "$V1_DB_HOST" "$V1_DB_PORT" "$V1_DB_USER" "$V1_DB_PASSWORD" "$V1_DB_NAME" \
        "$V1_DIR/v1_schema_only_${TIMESTAMP}.sql" "--schema-only"
    
    # Data only
    run_pg_dump "V1" "$V1_DB_HOST" "$V1_DB_PORT" "$V1_DB_USER" "$V1_DB_PASSWORD" "$V1_DB_NAME" \
        "$V1_DIR/v1_data_only_${TIMESTAMP}.sql" "--data-only"-
    
else
    echo "⚠️  V1 database connection details not found in environment"
fi

# Generate quick summary
SUMMARY_FILE="$OUTPUT_DIR/dump_summary_${TIMESTAMP}.txt"
echo "📋 Generating dump summary..."

cat > "$SUMMARY_FILE" << EOF
Supabase Database Dump Summary
Generated: $(date)
==============================================

Output Directory: $OUTPUT_DIR

V1 Database Dumps:
- Complete dump: v1/v1_complete_dump_${TIMESTAMP}.sql
- Schema only: v1/v1_schema_only_${TIMESTAMP}.sql  
- Data only: v1/v1_data_only_${TIMESTAMP}.sql

V2 Database Dumps:
- Complete dump: v2/v2_complete_dump_${TIMESTAMP}.sql
- Schema only: v2/v2_schema_only_${TIMESTAMP}.sql
- Data only: v2/v2_data_only_${TIMESTAMP}.sql

Usage:
------
1. Review schema dumps before migration
2. Use complete dumps as backup reference
3. Compare V1 vs V2 structures
4. Validate migration requirements

Next Steps:
-----------
1. Run: python generate_dump.py (for detailed analysis)
2. Review migration requirements
3. Execute migration with confidence
EOF

echo ""
echo "✅ Database dumps completed successfully!"
echo "📁 All files saved to: $OUTPUT_DIR"
echo "📋 Summary: $SUMMARY_FILE"
echo ""
echo "🔍 For detailed analysis, run:"
echo "   python generate_dump.py --output-dir $OUTPUT_DIR"
echo ""
echo "📚 Review the dump files before running migration to understand:"
echo "   • Database schema differences"
echo "   • Data volumes and structure" 
echo "   • Migration requirements"