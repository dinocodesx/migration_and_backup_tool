package postgres

import (
	"context"
	"fmt"

	"github.com/dinocodesx/gomigrate/internal/adapter"
	"github.com/dinocodesx/gomigrate/internal/config"
	"github.com/dinocodesx/gomigrate/internal/record"
	"github.com/dinocodesx/gomigrate/internal/schema"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresAdapter struct {
	pool        *pgxpool.Pool
	reader      *Reader
	targetTable string
}

func NewPostgresAdapter() *PostgresAdapter {
	return &PostgresAdapter{}
}

func (a *PostgresAdapter) Type() string {
	return "postgres"
}

func (a *PostgresAdapter) Connect(ctx context.Context, cfg config.DBConfig) error {
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%d/%s",
		cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Database)

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return fmt.Errorf("failed to connect to postgres: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("failed to ping postgres: %w", err)
	}

	a.pool = pool
	a.reader = NewReader(pool)
	return nil
}

func (a *PostgresAdapter) Partitions(ctx context.Context, table string, n int) ([]adapter.Partition, error) {
	return a.reader.Partitions(ctx, table, n)
}

func (a *PostgresAdapter) ReadPartition(ctx context.Context, p adapter.Partition, ch chan<- *record.Record, errCh chan<- error) {
	a.reader.ReadPartition(ctx, p, ch, errCh)
}

func (a *PostgresAdapter) Schema(ctx context.Context, table string) (*schema.Schema, error) {
	return GetSchema(ctx, a.pool, table)
}

func (a *PostgresAdapter) WriteBatch(ctx context.Context, batch []*record.Record) (int, error) {
	if len(batch) == 0 {
		return 0, nil
	}
	table := a.targetTable
	if table == "" {
		table = batch[0].Metadata.SourceTable
	}
	writer := NewWriter(a.pool, table)
	return writer.WriteBatch(ctx, batch)
}

func (a *PostgresAdapter) ApplySchema(ctx context.Context, s *schema.Schema) error {
	a.targetTable = s.Name
	writer := NewWriter(a.pool, s.Name)
	return writer.ApplySchema(ctx, s)
}

func (a *PostgresAdapter) Close() error {
	if a.pool != nil {
		a.pool.Close()
	}
	return nil
}
