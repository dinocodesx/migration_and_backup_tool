package integration

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/dinocodesx/gomigrate/internal/adapter/cassandra"
	"github.com/dinocodesx/gomigrate/internal/checkpoint"
	"github.com/dinocodesx/gomigrate/internal/config"
	"github.com/dinocodesx/gomigrate/internal/migration"
	"github.com/dinocodesx/gomigrate/internal/pipeline"
	"github.com/gocql/gocql"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.uber.org/zap"
)

func TestCassandraToCassandraMigration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	// 1. Spin up Cassandra container
	cassandraContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "cassandra:4.1",
			ExposedPorts: []string{"9042/tcp"},
			WaitingFor: wait.ForLog("Created default superuser role 'cassandra'").
				WithStartupTimeout(120 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("failed to start container: %v", err)
	}
	defer cassandraContainer.Terminate(ctx)

	host, _ := cassandraContainer.Host(ctx)
	port, _ := cassandraContainer.MappedPort(ctx, "9042")
	p, _ := strconv.Atoi(port.Port())

	cluster := gocql.NewCluster(host)
	cluster.Port = p
	cluster.Timeout = 20 * time.Second
	session, err := cluster.CreateSession()
	if err != nil {
		t.Fatalf("failed to connect to cassandra: %v", err)
	}
	defer session.Close()

	// 2. Setup keyspace and seed source table
	err = session.Query("CREATE KEYSPACE test_ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}").Exec()
	if err != nil {
		t.Fatalf("failed to create keyspace: %v", err)
	}

	err = session.Query("CREATE TABLE test_ks.source_users (id int PRIMARY KEY, name text, email text)").Exec()
	if err != nil {
		t.Fatalf("failed to create source table: %v", err)
	}

	numRows := 100
	for i := 1; i <= numRows; i++ {
		err = session.Query("INSERT INTO test_ks.source_users (id, name, email) VALUES (?, ?, ?)",
			i, fmt.Sprintf("User %d", i), fmt.Sprintf("user%d@example.com", i)).Exec()
		if err != nil {
			t.Fatalf("failed to seed data: %v", err)
		}
	}

	// 3. Run migration
	store, err := checkpoint.NewStore("test_checkpoint_cassandra.bolt")
	if err != nil {
		t.Fatalf("failed to create checkpoint store: %v", err)
	}
	defer store.Close()

	cfg := config.DBConfig{
		Type:     "cassandra",
		Hosts:    []string{host},
		Port:     p,
		Keyspace: "test_ks",
	}

	src := cassandra.NewCassandraAdapter()
	if err := src.Connect(ctx, cfg); err != nil {
		t.Fatalf("failed to connect src: %v", err)
	}
	dst := cassandra.NewCassandraAdapter()
	if err := dst.Connect(ctx, cfg); err != nil {
		t.Fatalf("failed to connect dst: %v", err)
	}

	s, err := src.Schema(ctx, "source_users")
	if err != nil {
		t.Fatalf("failed to get schema: %v", err)
	}
	s.Name = "target_users"
	if err := dst.ApplySchema(ctx, s); err != nil {
		t.Fatalf("failed to apply schema: %v", err)
	}

	concurrency := config.ConcurrencyConfig{
		NumReaders:      2,
		NumTransformers: 1,
		NumWriters:      2,
		BatchSize:       10,
		BatchTimeout:    1 * time.Second,
	}

	mapper := migration.NewSchemaMapper(src.Type(), dst.Type())
	orch := pipeline.NewOrchestrator(concurrency, store, mapper, zap.NewNop())
	if err := orch.Migrate(ctx, "test-op-cassandra", src, dst, "source_users"); err != nil {
		t.Fatalf("migration failed: %v", err)
	}

	// 4. Verify
	var count int
	if err := session.Query("SELECT COUNT(*) FROM test_ks.target_users").Scan(&count); err != nil {
		t.Fatalf("failed to count target rows: %v", err)
	}

	if count != numRows {
		t.Errorf("expected %d rows, got %d", numRows, count)
	}
}
