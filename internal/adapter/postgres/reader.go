package postgres

import (
	"context"
	"fmt"

	"github.com/dinocodesx/gomigrate/internal/adapter"
	"github.com/dinocodesx/gomigrate/internal/record"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Reader encapsulates the logic for extracting data from PostgreSQL in parallel.
// It leverages primary-key range partitioning to allow multiple workers to
// read non-overlapping segments of a table concurrently.
type Reader struct {
	// db is the connection pool used for queries.
	db *pgxpool.Pool
}

// NewReader creates a new Reader instance using the provided connection pool.
func NewReader(db *pgxpool.Pool) *Reader {
	return &Reader{db: db}
}

// Partitions splits a table into 'n' roughly equal segments based on its
// primary key range. It queries the MIN and MAX of the 'id' column to determine
// the global range and then divides it into contiguous, non-overlapping intervals.
//
// This method assumes the table has an integer primary key named 'id'.
// If the table is empty, it returns an empty slice of partitions.
func (r *Reader) Partitions(ctx context.Context, table string, n int) ([]adapter.Partition, error) {
	tableName := pgx.Identifier{table}.Sanitize()
	query := fmt.Sprintf("SELECT MIN(id), MAX(id) FROM %s", tableName)

	var minID, maxID *int64
	if err := r.db.QueryRow(ctx, query).Scan(&minID, &maxID); err != nil {
		return nil, fmt.Errorf("failed to get min/max PK for table %s: %w", table, err)
	}

	if minID == nil || maxID == nil {
		return []adapter.Partition{}, nil
	}
	min, max := *minID, *maxID

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
			end = max + 1
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

// ReadPartition executes a range query for the specified partition and streams
// the results into the provided channel as Record objects.
//
// It performs a 'SELECT *' within the PK boundaries. Each row is converted
// into a map-based representation within the Record. The channel is closed
// automatically once all rows are processed or an error occurs.
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
