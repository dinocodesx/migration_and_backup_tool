package postgres

import (
	"context"
	"fmt"

	"github.com/dinocodesx/migration_and_backup_tool/internal/record"
	"github.com/dinocodesx/migration_and_backup_tool/internal/schema"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Writer handles bulk writing to PostgreSQL using the COPY protocol.
type Writer struct {
	db    *pgxpool.Pool
	table string
}

func NewWriter(db *pgxpool.Pool, table string) *Writer {
	return &Writer{db: db, table: table}
}

func (w *Writer) WriteBatch(ctx context.Context, batch []*record.Record) (int, error) {
	if len(batch) == 0 {
		return 0, nil
	}

	// Extract column names from the first record
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

func (w *Writer) ApplySchema(ctx context.Context, s *schema.Schema) error {
	// Simple CREATE TABLE implementation
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
