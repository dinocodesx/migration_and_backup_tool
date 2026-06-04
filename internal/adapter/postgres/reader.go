package postgres

import (
	"context"
	"fmt"

	"github.com/dinocodesx/gomigrate/internal/adapter"
	"github.com/dinocodesx/gomigrate/internal/record"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Reader handles parallel reading from PostgreSQL by partitioning tables
// based on primary key ranges.
type Reader struct {
	db *pgxpool.Pool
}

// NewReader creates a new Reader backed by the provided PostgreSQL connection pool.
func NewReader(db *pgxpool.Pool) *Reader {
	return &Reader{db: db}
}

// Partitions calculates n roughly equal partitions for the specified table
// based on the minimum and maximum values of the 'id' (primary key) column.
//
// If the table is empty, it returns an empty slice.
// If the ID range is too small for the requested number of partitions,
// it returns a single partition covering the entire range.
func (r *Reader) Partitions(ctx context.Context, table string, n int) ([]adapter.Partition, error) {
	tableName := pgx.Identifier{table}.Sanitize()
	query := fmt.Sprintf("SELECT MIN(id), MAX(id) FROM %s", tableName)

	// Use nullable pointers so that an empty table returns (nil, nil) instead
	// of causing a scan error.
	var minID, maxID *int64
	if err := r.db.QueryRow(ctx, query).Scan(&minID, &maxID); err != nil {
		return nil, fmt.Errorf("failed to get min/max PK for table %s: %w", table, err)
	}

	// Empty table — nothing to partition.
	if minID == nil || maxID == nil {
		return []adapter.Partition{}, nil
	}
	min, max := *minID, *maxID

	// Single record or range is zero — return a single partition.
	if max <= min {
		return []adapter.Partition{{ID: "p0", Table: table, Start: min, End: max + 1}}, nil
	}

	if n <= 0 {
		n = 1
	}
	step := (max - min) / int64(n)
	if step == 0 {
		step = 1
	}

	partitions := make([]adapter.Partition, 0, n)
	for i := 0; i < n; i++ {
		start := min + int64(i)*step
		end := start + step
		if i == n-1 {
			end = max + 1 // inclusive upper bound
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

// ReadPartition executes a SELECT query for a specific ID range and streams
// each resulting row as a record.Record onto the provided channel.
//
// It sanitizes the table name and uses parameterized queries for the range bounds.
// The channel is closed when reading is complete or an error occurs.
// It respects context cancellation.
func (r *Reader) ReadPartition(ctx context.Context, p adapter.Partition, ch chan<- *record.Record) error {
	defer close(ch)

	tableName := pgx.Identifier{p.Table}.Sanitize()
	query := fmt.Sprintf("SELECT * FROM %s WHERE id >= $1 AND id < $2", tableName)

	rows, err := r.db.Query(ctx, query, p.Start, p.End)
	if err != nil {
		return fmt.Errorf("failed to query partition %s of table %s: %w", p.ID, p.Table, err)
	}
	defer rows.Close()

	fieldDescs := rows.FieldDescriptions()

	for rows.Next() {
		// Check for context cancellation between rows.
		if ctx.Err() != nil {
			return ctx.Err()
		}

		values, err := rows.Values()
		if err != nil {
			return fmt.Errorf("failed to scan row in partition %s: %w", p.ID, err)
		}

		data := make(map[string]any, len(fieldDescs))
		var id string
		var offset any

		for i, fd := range fieldDescs {
			colName := string(fd.Name)
			data[colName] = values[i]
			if colName == "id" {
				id = fmt.Sprintf("%v", values[i])
				offset = values[i]
			}
		}
		// Fallback: if no "id" column, use the first column as offset.
		if offset == nil && len(values) > 0 {
			offset = values[0]
		}

		rec := &record.Record{
			ID:   id,
			Data: data,
			Metadata: record.RecordMetadata{
				SourceTable: p.Table,
				PartitionID: p.ID,
				Offset:      offset,
			},
		}

		select {
		case ch <- rec:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("row iteration error in partition %s: %w", p.ID, err)
	}

	return nil
}
