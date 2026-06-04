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

// Reader provides the logic for parallel data extraction from Cassandra.
// It uses token-based range queries to scan the entire Murmur3 token space
// without performing heavy full-table scans that would overload coordinators.
type Reader struct {
	// session is the gocql connection session.
	session *gocql.Session
	// keyspace is the source keyspace.
	keyspace string
}

// NewReader creates a new Reader instance for the given session and keyspace.
func NewReader(session *gocql.Session, keyspace string) *Reader {
	return &Reader{session: session, keyspace: keyspace}
}

// Partitions divides the Murmur3 token range ([-2^63, 2^63-1]) into 'n'
// non-overlapping intervals. Each interval corresponds to a gomigrate Partition.
// This approach ensures that data is read in a distributed fashion across the cluster.
func (r *Reader) Partitions(ctx context.Context, table string, n int) ([]adapter.Partition, error) {
	if n <= 0 {
		n = 1
	}

	minToken := big.NewInt(-1)
	minToken.Lsh(minToken, 63)

	maxToken := big.NewInt(1)
	maxToken.Lsh(maxToken, 63).Sub(maxToken, big.NewInt(1))

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

// ReadPartition executes a CQL query using the 'token()' function to filter
// records within a specific token range. It handles UUID normalization to
// strings to maintain cross-database compatibility within the Record structure.
//
// The result set is paged (default size 1000) and streamed into the channel.
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
