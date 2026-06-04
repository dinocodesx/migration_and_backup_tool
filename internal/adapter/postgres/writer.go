package postgres

import (
	"context"
	"fmt"

	"github.com/dinocodesx/gomigrate/internal/record"
	"github.com/dinocodesx/gomigrate/internal/schema"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Writer implements high-performance data ingestion for PostgreSQL.
// It is designed to work with a single target table and uses the binary
// COPY protocol (via pgx.CopyFrom) for maximum throughput.
type Writer struct {
	// db is the connection pool used for COPY operations.
	db *pgxpool.Pool
	// table is the name of the target table.
	table string
}

// NewWriter creates a new Writer instance for the specified target table.
func NewWriter(db *pgxpool.Pool, table string) *Writer {
	return &Writer{db: db, table: table}
}

// WriteBatch inserts a slice of records into the target table using the
// PostgreSQL COPY protocol. This is significantly faster than individual
// INSERT statements.
//
// It extracts the column names from the first record in the batch and
// expects all subsequent records in the same batch to have an identical
// field set.
func (w *Writer) WriteBatch(ctx context.Context, batch []*record.Record) (int, error) {
	if len(batch) == 0 {
		return 0, nil
	}

	columns := make([]string, 0, len(batch[0].Data))
	for col := range batch[0].Data {
		columns = append(columns, col)
	}

	rows := make([][]any, len(batch))
	for i, rec := range batch {
		row := make([]any, len(columns))
		for j, col := range columns {
			row[j] = rec.Data[col]
		}
		rows[i] = row
	}

	count, err := w.db.CopyFrom(
		ctx,
		pgx.Identifier{w.table},
		columns,
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return 0, fmt.Errorf("failed to copy from rows: %w", err)
	}

	return int(count), nil
}

// ApplySchema creates the target table if it does not exist. It maps canonical
// gomigrate types back to PostgreSQL-specific types and handles primary key
// and nullability constraints.
func (w *Writer) ApplySchema(ctx context.Context, s *schema.Schema) error {
	tableName := pgx.Identifier{s.Name}.Sanitize()
	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (", tableName)
	for i, col := range s.Columns {
		pgType := mapToPostgresType(col.Type)
		query += fmt.Sprintf("%s %s", col.Name, pgType)
		if col.PrimaryKey {
			query += " PRIMARY KEY"
		}
		if !col.Nullable {
			query += " NOT NULL"
		}
		if i < len(s.Columns)-1 {
			query += ", "
		}
	}
	query += ")"

	_, err := w.db.Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to apply schema: %w", err)
	}
	return nil
}

// mapToPostgresType converts a canonical gomigrate type string to a valid
// PostgreSQL data type string for use in CREATE TABLE statements.
func mapToPostgresType(t string) string {
	switch t {
	case "int64":
		return "bigint"
	case "string":
		return "text"
	case "bool":
		return "boolean"
	case "float64":
		return "double precision"
	case "timestamp":
		return "timestamptz"
	default:
		return "text"
	}
}
