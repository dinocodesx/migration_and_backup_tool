package postgres

import (
	"context"
	"fmt"

	"github.com/dinocodesx/gomigrate/internal/adapter"
	"github.com/dinocodesx/gomigrate/internal/record"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Reader handles parallel reading from PostgreSQL.
type Reader struct {
	db *pgxpool.Pool
}

func NewReader(db *pgxpool.Pool) *Reader {
	return &Reader{db: db}
}

func (r *Reader) Partitions(ctx context.Context, table string, n int) ([]adapter.Partition, error) {
	// Simple PK range partitioning. For more complex cases, ctid or other strategies can be used.
	var min, max int64
	tableName := pgx.Identifier{table}.Sanitize()
	query := fmt.Sprintf("SELECT MIN(id), MAX(id) FROM %s", tableName) // Assuming 'id' is integer PK
	err := r.db.QueryRow(ctx, query).Scan(&min, &max)
	if err != nil {
		return nil, fmt.Errorf("failed to get min/max PK: %w", err)
	}

	if max <= min {
		return []adapter.Partition{{ID: "p0", Table: table, Start: min, End: max}}, nil
	}

	partitions := make([]adapter.Partition, 0, n)
	step := (max - min) / int64(n)
	if step == 0 {
		step = 1
	}

	for i := 0; i < n; i++ {
		start := min + int64(i)*step
		end := start + step
		if i == n-1 {
			end = max + 1 // Ensure we cover the last record
		}
		partitions = append(partitions, adapter.Partition{
			ID:    fmt.Sprintf("p%d", i),
			Table: table,
			Start: start,
			End:   end,
		})
	}

	return partitions, nil
}

func (r *Reader) ReadPartition(ctx context.Context, p adapter.Partition, ch chan<- *record.Record, errCh chan<- error) {
	// Use Identifier for safe table name quoting
	tableName := pgx.Identifier{p.Table}.Sanitize()
	query := fmt.Sprintf("SELECT * FROM %s WHERE id >= $1 AND id < $2", tableName)
	rows, err := r.db.Query(ctx, query, p.Start, p.End)
	if err != nil {
		errCh <- fmt.Errorf("failed to read partition %s: %w", p.ID, err)
		return
	}
	defer rows.Close()

	fieldDescriptions := rows.FieldDescriptions()

	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			errCh <- err
			return
		}

		data := make(map[string]any)
		var id string
		for i, fd := range fieldDescriptions {
			data[string(fd.Name)] = values[i]
			if string(fd.Name) == "id" {
				id = fmt.Sprintf("%v", values[i])
			}
		}

		rec := &record.Record{
			ID:   id,
			Data: data,
			Metadata: record.RecordMetadata{
				SourceTable: p.Table,
				PartitionID: p.ID,
				Offset:      values[0], // Assuming first column is ID/Offset
			},
		}

		select {
		case ch <- rec:
		case <-ctx.Done():
			return
		}
	}

	if err := rows.Err(); err != nil {
		errCh <- err
		return
	}

	// Signal successful completion by sending nil to errCh
	errCh <- nil
}
