package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/jackc/pgx/v5"
)

func main() {
	connStr := os.Getenv("DATABASE_URL")
	if connStr == "" {
		connStr = "postgres://user:password@localhost:5432/testdb?sslmode=disable"
	}

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		log.Fatalf("Unable to connect to database: %v\n", err)
	}
	defer conn.Close(ctx)

	fmt.Println("Seeding database...")

	// Create table
	_, err = conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS demo_users (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT UNIQUE NOT NULL,
			bio TEXT,
			created_at TIMESTAMPTZ DEFAULT NOW()
		)`)
	if err != nil {
		log.Fatalf("CreateTable failed: %v\n", err)
	}

	// Insert data
	batch := &pgx.Batch{}
	for i := 1; i <= 1000; i++ {
		batch.Queue("INSERT INTO demo_users (name, email, bio) VALUES ($1, $2, $3)",
			fmt.Sprintf("User %d", i),
			fmt.Sprintf("user%d@example.com", i),
			fmt.Sprintf("This is a bio for user %d to add some weight to the parquet file.", i))
	}

	br := conn.SendBatch(ctx, batch)
	if err := br.Close(); err != nil {
		log.Fatalf("Batch insert failed: %v\n", err)
	}

	fmt.Println("Seeded 1000 records into demo_users table.")
}
