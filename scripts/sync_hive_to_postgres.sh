#!/bin/bash
# sync_hive_to_postgres.sh
#
# Converts Hive SQL schema and sample data to PostgreSQL-compatible SQL and loads them into the superset-db container.
# Usage: ./scripts/sync_hive_to_postgres.sh
#
# Re-run this script whenever you update Hive schemas or sample data to keep PostgreSQL in sync.

set -e

# Paths
HIVE_SCHEMA="$(dirname "$0")/../hive/01_schema.sql"
HIVE_DATA="$(dirname "$0")/../hive/03_sample_data.sql"
TMP_SCHEMA="/tmp/pg_schema.sql"
TMP_DATA="/tmp/pg_data.sql"

# Convert Hive SQL to PostgreSQL SQL (basic replacements, extend as needed)
sed -e 's/`/"/g' \
    -e 's/STRING/VARCHAR(255)/gI' \
    -e 's/DOUBLE/DOUBLE PRECISION/gI' \
    -e 's/BOOLEAN/BOOLEAN/gI' \
    -e 's/INT/INTEGER/gI' \
    -e 's/DECIMAL([0-9, ]*)/NUMERIC\1/gI' \
    -e 's/COMMENT \'[^']*\'//gI' \
    -e 's/ROW FORMAT DELIMITED.*$//gI' \
    -e 's/STORED AS.*$//gI' \
    -e 's/TBLPROPERTIES.*$//gI' \
    "$HIVE_SCHEMA" > "$TMP_SCHEMA"

# Convert sample data (if needed, here just copy)
cp "$HIVE_DATA" "$TMP_DATA"

# Load into PostgreSQL (superset-db)
echo "Loading schema into PostgreSQL..."
docker compose exec -T superset-db psql -U superset -d superset -f "$TMP_SCHEMA"
echo "Loading sample data into PostgreSQL..."
docker compose exec -T superset-db psql -U superset -d superset -f "$TMP_DATA"

echo "Sync complete."
