#!/bin/bash

# Load environment variables from .env file
if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
else
    echo ".env file not found!"
    exit 1
fi

# Configuration from .env
DB_USER=${POSTGRES_USER:-user}
DB_NAME=${POSTGRES_DB:-testdb}
TABLE_NAME=${GOMIGRATE_SOURCE_TABLES:-source_users}

echo "Updating table '$TABLE_NAME' with 100 rows and 20 columns..."

# --- 1. Define 20 Columns ---
# id (SERIAL PK), name, email, and 17 additional data columns
COLS_DEF="id SERIAL PRIMARY KEY, name VARCHAR(100), email VARCHAR(100)"
COLS_LIST="name, email"

for i in {4..20}; do
    COLS_DEF="$COLS_DEF, col_$i VARCHAR(100)"
    COLS_LIST="$COLS_LIST, col_$i"
done

# --- 2. Build the SQL Query ---
SQL_QUERY=$(cat <<EOF
DROP TABLE IF EXISTS $TABLE_NAME;
CREATE TABLE $TABLE_NAME ($COLS_DEF);
EOF
)

# Generate 100 rows of INSERT values
INSERT_STMT="INSERT INTO $TABLE_NAME ($COLS_LIST) VALUES "

for r in {1..100}; do
    ROW="('User $r', 'user$r@example.com'"
    for c in {4..20}; do
        ROW="$ROW, 'val_${r}_${c}'"
    done
    ROW="$ROW)"

    if [ $r -lt 100 ]; then
        INSERT_STMT="$INSERT_STMT $ROW,"
    else
        INSERT_STMT="$INSERT_STMT $ROW;"
    fi
done

# Combine the SQL
FULL_QUERY="$SQL_QUERY $INSERT_STMT"

# Execute SQL inside the docker container
docker exec -i $(docker-compose ps -q postgres) psql -U $DB_USER -d $DB_NAME -c "$FULL_QUERY"

if [ $? -eq 0 ]; then
    echo "Successfully seeded $TABLE_NAME with 100 rows and 20 columns."
    echo "Total columns: 20 (id + 19 data columns)"
    echo "Total rows: 100"
else
    echo "Error seeding database."
    exit 1
fi
