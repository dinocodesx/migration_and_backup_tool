#!/bin/bash
set -e

# GoMigrate Demo Script
# This script demonstrates: Build -> Seed -> Backup -> Verify -> Restore

echo "🚀 Starting GoMigrate Demo..."

# 1. Build the tool
echo "📦 Building gomigrate..."
make build

# 2. Start PostgreSQL (if not running)
echo "🐘 Starting PostgreSQL via Docker..."
docker-compose up -d postgres
echo "Waiting for Postgres to be ready..."
sleep 5

# 3. Seed data
echo "🌱 Seeding test data..."
DATABASE_URL="postgres://user:password@localhost:5432/testdb?sslmode=disable" go run demo/seed/main.go

# 4. Perform Backup
echo "💾 Performing backup..."
./gomigrate backup --config demo/demo_config.yaml

# 5. Verify Backup
echo "🔍 Verifying backup..."
./gomigrate verify --config demo/demo_config.yaml --manifest demo/backups/manifest.json

# 6. Setup for Restore (Drop and recreate table)
echo "🧹 Preparing for restore (dropping demo_users)..."
docker exec -it $(docker-compose ps -q postgres) psql -U user -d testdb -c "DROP TABLE demo_users;"

# 7. Perform Restore
echo "🔄 Performing restore..."
./gomigrate restore --config demo/demo_config.yaml --manifest demo/backups/manifest.json

# 8. Final Count Check
echo "✅ Demo complete! Checking final record count..."
COUNT=$(docker exec -it $(docker-compose ps -q postgres) psql -U user -d testdb -t -c "SELECT COUNT(*) FROM demo_users;")
echo "Final record count in demo_users: $COUNT"

if [ "${COUNT//[[:space:]]/}" == "1000" ]; then
    echo "🎉 SUCCESS: All 1000 records restored correctly!"
else
    echo "❌ FAILURE: Record count mismatch."
fi
