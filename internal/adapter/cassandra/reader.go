package cassandra

import (
	"context"
	"fmt"
	"math/big"
	"strings"

	"github.com/dinocodesx/gomigrate/internal/adapter"
	"github.com/dinocodesx/gomigrate/internal/record"
	"github.com/gocql/gocql"
)

// Reader handles parallel reading from Cassandra by partitioning the token space.
type Reader struct {
	session  *gocql.Session
	keyspace string
}

// NewReader creates a new Reader for the specified keyspace, backed by the
// provided Cassandra session.
func NewReader(session *gocql.Session, keyspace string) *Reader {
	return &Reader{session: session, keyspace: keyspace}
}

// Partitions divides the entire Murmur3 token range ([-2^63, 2^63-1]) into
// n equal sub-ranges. This allows for parallel scanning of the table
// across different coordinators and vnodes.
func (r *Reader) Partitions(ctx context.Context, table string, n int) ([]adapter.Partition, error) {
	if n <= 0 {
		n = 1
	}

	// Murmur3 token range: [-2^63, 2^63-1]
	minToken := big.NewInt(-1)
	minToken.Lsh(minToken, 63) // -2^63

	maxToken := big.NewInt(1)
	maxToken.Lsh(maxToken, 63).Sub(maxToken, big.NewInt(1)) // 2^63-1

	totalRange := big.NewInt(0).Sub(maxToken, minToken)
	numPartitions := big.NewInt(int64(n))
	step := big.NewInt(0).Div(totalRange, numPartitions)

	partitions := make([]adapter.Partition, 0, n)
	current := big.NewInt(0).Set(minToken)

	for i := 0; i < n; i++ {
		next := big.NewInt(0).Add(current, step)
		if i == n-1 {
			next.Set(maxToken)
		}

		partitions = append(partitions, adapter.Partition{
			ID:    fmt.Sprintf("%s-%d", table, i),
			Table: table,
			Start: current.Int64(),
			End:   next.Int64(),
		})
		current.Set(next)
	}

	return partitions, nil
}

// ReadPartition streams every record within the specified token range onto ch.
// It uses the token() function in the WHERE clause to perform an efficient
// range scan.
//
// Records are identified by a composite key consisting of their partition and
// clustering columns. UUIDs are automatically normalized to their string
// representation for cross-database compatibility.
func (r *Reader) ReadPartition(ctx context.Context, p adapter.Partition, ch chan<- *record.Record) error {
	defer close(ch)

	ksMetadata, err := r.session.KeyspaceMetadata(r.keyspace)
	if err != nil {
		return fmt.Errorf("failed to get keyspace metadata: %w", err)
	}
	tableMetadata, ok := ksMetadata.Tables[p.Table]
	if !ok {
		return fmt.Errorf("table %s not found", p.Table)
	}

	pkCols := make([]string, len(tableMetadata.PartitionKey))
	for i, col := range tableMetadata.PartitionKey {
		pkCols[i] = col.Name
	}
	pkList := fmt.Sprintf("token(%s)", strings.Join(pkCols, ","))

	query := fmt.Sprintf("SELECT *, %s as gomigrate_token FROM %s WHERE %s >= ? AND %s < ?", pkList, p.Table, pkList, pkList)
	iter := r.session.Query(query, p.Start, p.End).WithContext(ctx).PageSize(1000).Iter()

	for {
		row := make(map[string]any)
		if !iter.MapScan(row) {
			break
		}

		token := row["gomigrate_token"]
		delete(row, "gomigrate_token")

		// Identify PK values for the record ID and normalize UUIDs to strings
		var pkValues []string
		for _, col := range tableMetadata.PartitionKey {
			val := row[col.Name]
			if uuid, ok := val.(gocql.UUID); ok {
				val = uuid.String()
				row[col.Name] = val
			}
			pkValues = append(pkValues, fmt.Sprintf("%v", val))
		}
		for _, col := range tableMetadata.ClusteringColumns {
			val := row[col.Name]
			if uuid, ok := val.(gocql.UUID); ok {
				val = uuid.String()
				row[col.Name] = val
			}
			pkValues = append(pkValues, fmt.Sprintf("%v", val))
		}

		// Normalize other UUIDs in the row
		for k, v := range row {
			if uuid, ok := v.(gocql.UUID); ok {
				row[k] = uuid.String()
			}
		}

		rec := &record.Record{
			ID:   strings.Join(pkValues, ":"),
			Data: row,
			Metadata: record.RecordMetadata{
				SourceTable: p.Table,
				SourceDB:    r.keyspace,
				PartitionID: p.ID,
				Offset:      token,
			},
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case ch <- rec:
		}
	}

	if err := iter.Close(); err != nil {
		return fmt.Errorf("cassandra iteration error: %w", err)
	}

	return nil
}
