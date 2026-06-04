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
type CassandraAdapter struct {
	session  *gocql.Session
	keyspace string
	reader   *Reader
	writer   *Writer
}

// NewCassandraAdapter creates an unconnected CassandraAdapter.
func NewCassandraAdapter() *CassandraAdapter {
	return &CassandraAdapter{}
}

// Type returns the adapter's database identifier.
func (a *CassandraAdapter) Type() string { return "cassandra" }

// Connect opens a connection pool and validates connectivity.
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
	// Writer will be initialized lazily or via ApplySchema
	return nil
}

// Partitions delegates to the Reader.
func (a *CassandraAdapter) Partitions(ctx context.Context, table string, n int) ([]adapter.Partition, error) {
	return a.reader.Partitions(ctx, table, n)
}

// ReadPartition delegates to the Reader.
func (a *CassandraAdapter) ReadPartition(ctx context.Context, p adapter.Partition, ch chan<- *record.Record) error {
	return a.reader.ReadPartition(ctx, p, ch)
}

// Schema introspects the given table.
func (a *CassandraAdapter) Schema(ctx context.Context, table string) (*schema.Schema, error) {
	return GetSchema(ctx, a.session, a.keyspace, table)
}

// WriteBatch writes a batch of records to the target table.
func (a *CassandraAdapter) WriteBatch(ctx context.Context, batch []*record.Record) (int, error) {
	if len(batch) == 0 {
		return 0, nil
	}
	if a.writer == nil {
		a.writer = NewWriter(a.session, a.keyspace, batch[0].Metadata.SourceTable)
	}
	return a.writer.WriteBatch(ctx, batch)
}

// ApplySchema creates (or ensures) the target table and caches the writer.
func (a *CassandraAdapter) ApplySchema(ctx context.Context, s *schema.Schema) error {
	a.writer = NewWriter(a.session, a.keyspace, s.Name)
	return a.writer.ApplySchema(ctx, s)
}

// Close releases all connections.
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
