#!/bin/bash
set -e

# GoMigrate Demo Script
# This script demonstrates: Build -> Seed -> Backup -> Verify -> Restore

echo "🚀 Starting GoMigrate Demo..."

# 1. Version Check
echo "🏷️  Checking version..."
./gomigrate version

# 2. Build the tool
echo "📦 Building gomigrate..."
make build

# 3. Start Infrastructure
echo "🐘 Starting Infrastructure via Docker..."
docker-compose up -d postgres mongodb
echo "Waiting for services to be ready..."
sleep 10

# 4. Seed data
echo "🌱 Seeding test data into Postgres..."
DATABASE_URL="postgres://user:password@localhost:5432/testdb?sslmode=disable" go run demo/seed/main.go

# 5. Perform Cross-DB Migration (Postgres -> MongoDB)
echo "🚚 Performing migration: Postgres -> MongoDB..."
./gomigrate migrate --config demo/config/migrate-postgres-mongo.yaml

# 6. Check status (Checkpoint)
echo "📍 Checking checkpoint status..."
./gomigrate status --config demo/config/migrate-postgres-mongo.yaml

# 7. Verify Migration in MongoDB
echo "🔍 Checking record count in MongoDB..."
# Simple check using mongosh if available
docker exec -it $(docker-compose ps -q mongodb) mongosh testdb --quiet --eval "db.demo_users.countDocuments()"

# 8. Perform Backup (Postgres -> Parquet)
echo "💾 Performing backup (Postgres -> Parquet)..."
./gomigrate backup --config demo/config/backup-restore.yaml

# 9. Verify Backup Integrity
echo "🔍 Verifying backup..."
./gomigrate verify --config demo/config/backup-restore.yaml --manifest manifest.json

# 10. Setup for Restore (Drop and recreate table)
echo "🧹 Preparing for restore (dropping demo_users in Postgres)..."
docker exec -it $(docker-compose ps -q postgres) psql -U user -d testdb -c "DROP TABLE demo_users;"

# 11. Perform Restore
echo "🔄 Performing restore..."
./gomigrate restore --config demo/config/backup-restore.yaml --manifest manifest.json

# 12. Final Count Check
echo "✅ Demo complete! Checking final record count in Postgres..."
COUNT=$(docker exec -it $(docker-compose ps -q postgres) psql -U user -d testdb -t -c "SELECT COUNT(*) FROM demo_users;")
echo "Final record count in demo_users: $COUNT"

if [ "${COUNT//[[:space:]]/}" == "1000" ]; then
    echo "🎉 SUCCESS: Demo completed successfully!"
else
    echo "❌ FAILURE: Record count mismatch."
fi
