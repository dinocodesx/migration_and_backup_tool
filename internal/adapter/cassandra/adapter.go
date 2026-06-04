// Package cassandra provides a production-grade Cassandra implementation of the
// gomigrate adapter interfaces. It supports Apache Cassandra and ScyllaDB
// clusters using the gocql driver, focusing on token-aware parallel extraction
// and efficient batch ingestion.
package cassandra

import (
	"context"
	"fmt"
	"time"

	"github.com/dinocodesx/gomigrate/internal/adapter"
	"github.com/dinocodesx/gomigrate/internal/config"
	"github.com/dinocodesx/gomigrate/internal/record"
	"github.com/dinocodesx/gomigrate/internal/schema"
	"github.com/gocql/gocql"
)

// CassandraAdapter implements both adapter.SourceAdapter and adapter.TargetAdapter.
// It manages the gocql.Session and orchestrates partition-based reads and
// batch-based writes to a Cassandra cluster.
type CassandraAdapter struct {
	// session is the active Cassandra connection session.
	session *gocql.Session
	// keyspace is the target keyspace for all operations.
	keyspace string
	// reader handles partition discovery and range-based data extraction.
	reader *Reader
	// writer handles schema application and batch data insertion.
	writer *Writer
}

// NewCassandraAdapter returns an uninitialized CassandraAdapter.
func NewCassandraAdapter() *CassandraAdapter {
	return &CassandraAdapter{}
}

// Type returns the adapter identifier "cassandra".
func (a *CassandraAdapter) Type() string { return "cassandra" }

// Connect establishes a connection session to the Cassandra cluster.
// It configures token-aware host selection and establishes authentication
// if provided in the configuration.
func (a *CassandraAdapter) Connect(ctx context.Context, cfg config.DBConfig) error {
	hosts := cfg.Hosts
	if len(hosts) == 0 && cfg.Host != "" {
		hosts = []string{cfg.Host}
	}
	if len(hosts) == 0 {
		return fmt.Errorf("no cassandra hosts provided in config")
	}

	cluster := gocql.NewCluster(hosts...)
	if cfg.Port > 0 {
		cluster.Port = cfg.Port
	}
	cluster.Keyspace = cfg.Keyspace
	cluster.Timeout = 10 * time.Second
	cluster.ConnectTimeout = 10 * time.Second
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())

	if cfg.User != "" || cfg.Password != "" {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: cfg.User,
			Password: cfg.Password,
		}
	}

	session, err := cluster.CreateSession()
	if err != nil {
		return fmt.Errorf("failed to create cassandra session: %w", err)
	}

	a.session = session
	a.keyspace = cfg.Keyspace
	a.reader = NewReader(session, cfg.Keyspace)
	return nil
}

// Partitions splits the entire Murmur3 token space into non-overlapping segments.
func (a *CassandraAdapter) Partitions(ctx context.Context, table string, n int) ([]adapter.Partition, error) {
	return a.reader.Partitions(ctx, table, n)
}

// ReadPartition extracts all records from a specific token range.
func (a *CassandraAdapter) ReadPartition(ctx context.Context, p adapter.Partition, ch chan<- *record.Record) error {
	return a.reader.ReadPartition(ctx, p, ch)
}

// Schema introspects the Cassandra metadata catalogs to retrieve table information.
func (a *CassandraAdapter) Schema(ctx context.Context, table string) (*schema.Schema, error) {
	return GetSchema(ctx, a.session, a.keyspace, table)
}

// WriteBatch performs a bulk insert of records using unlogged batches.
func (a *CassandraAdapter) WriteBatch(ctx context.Context, batch []*record.Record) (int, error) {
	if len(batch) == 0 {
		return 0, nil
	}
	if a.writer == nil {
		a.writer = NewWriter(a.session, a.keyspace, batch[0].Metadata.SourceTable)
	}
	return a.writer.WriteBatch(ctx, batch)
}

// ApplySchema creates the target table using CQL.
func (a *CassandraAdapter) ApplySchema(ctx context.Context, s *schema.Schema) error {
	a.writer = NewWriter(a.session, a.keyspace, s.Name)
	return a.writer.ApplySchema(ctx, s)
}

// Close gracefully closes the Cassandra session.
func (a *CassandraAdapter) Close() error {
	if a.session != nil {
		a.session.Close()
		a.session = nil
	}
	return nil
}

// Ensure interface compliance at compile time.
var _ adapter.SourceAdapter = (*CassandraAdapter)(nil)
var _ adapter.TargetAdapter = (*CassandraAdapter)(nil)
