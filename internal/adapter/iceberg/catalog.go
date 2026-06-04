package iceberg

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// CatalogClient handles HTTP interactions with the Iceberg REST Catalog.
// It follows the Apache Iceberg REST OpenAPI specification for metadata
// operations, table management, and atomic snapshot commits.
type CatalogClient struct {
	// BaseURL is the root endpoint of the Iceberg REST Catalog (e.g., http://localhost:8181).
	BaseURL    string
	// HTTPClient is used for executing REST requests with a configured timeout.
	HTTPClient *http.Client
}

// TableResponse represents the JSON response from the /v1/namespaces/{ns}/tables/{table} endpoint.
type TableResponse struct {
	// Metadata contains the actual table definition and current state.
	Metadata       Metadata `json:"metadata"`
	// MetadataLocation is the path to the current metadata JSON file.
	MetadataLocation string   `json:"metadata-location"`
}

// Metadata contains the core Iceberg table configuration, including its
// current schema, location, and snapshot history.
type Metadata struct {
	// Schema is the current active schema for the table.
	Schema         IcebergSchema   `json:"schema"`
	// Location is the base directory (on S3/GCS/Local) where data files are stored.
	Location       string          `json:"location"`
	// LastColumnID is used to assign unique IDs to new columns during schema evolution.
	LastColumnID   int             `json:"last-column-id"`
	// CurrentSnapshotID is the ID of the latest committed snapshot.
	CurrentSnapshotID int64        `json:"current-snapshot-id"`
	// Snapshots is a history of all point-in-time states of the table.
	Snapshots      []Snapshot      `json:"snapshots"`
	// Manifests is a list of paths to manifest files (used for data discovery).
	Manifests      []string        `json:"manifest-list,omitempty"`
}

// Snapshot represents a single committed version of the table state.
type Snapshot struct {
	// SnapshotID is the unique identifier for this snapshot.
	SnapshotID   int64             `json:"snapshot-id"`
	// TimestampMS is when the snapshot was committed (UTC milliseconds).
	TimestampMS  int64             `json:"timestamp-ms"`
	// Summary contains metadata about the operation (e.g., "operation": "append").
	Summary      map[string]string `json:"summary"`
	// ManifestList is the path to the manifest list file for this snapshot.
	ManifestList string            `json:"manifest-list"`
}

// IcebergSchema defines the structural definition of an Iceberg table.
type IcebergSchema struct {
	// Type is always "struct" for a table schema.
	Type     string         `json:"type"`
	// Fields is a list of columns defined in the table.
	Fields   []IcebergField `json:"fields"`
	// SchemaID is a unique identifier for this specific version of the schema.
	SchemaID int            `json:"schema-id"`
}

// IcebergField represents a single column within an Iceberg schema.
type IcebergField struct {
	// ID is the unique field identifier, used for stable schema evolution.
	ID       int    `json:"id"`
	// Name is the name of the column.
	Name     string `json:"name"`
	// Type can be a primitive string (e.g., "long") or a nested object (e.g., struct/list).
	Type     any    `json:"type"`
	// Required specifies if the field can contain null values.
	Required bool   `json:"required"`
}

// NewCatalogClient initializes a new CatalogClient with a default HTTP timeout.
func NewCatalogClient(baseURL string) *CatalogClient {
	return &CatalogClient{
		BaseURL: baseURL,
		HTTPClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// GetTable fetches the full metadata of a specific Iceberg table from the catalog.
func (c *CatalogClient) GetTable(ctx context.Context, namespace, table string) (*TableResponse, error) {
	url := fmt.Sprintf("%s/v1/namespaces/%s/tables/%s", c.BaseURL, namespace, table)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("catalog error (%d): %s", resp.StatusCode, string(body))
	}

	var tableResp TableResponse
	if err := json.NewDecoder(resp.Body).Decode(&tableResp); err != nil {
		return nil, err
	}

	return &tableResp, nil
}

// CreateTable registers a new table with the catalog. It expects a payload
// containing the name, initial schema, and storage location.
func (c *CatalogClient) CreateTable(ctx context.Context, namespace string, request any) error {
	url := fmt.Sprintf("%s/v1/namespaces/%s/tables", c.BaseURL, namespace)
	data, err := json.Marshal(request)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to create table (%d): %s", resp.StatusCode, string(body))
	}

	return nil
}

// CommitSnapshot pushes a new snapshot update to the catalog. This is an
// atomic operation that adds the provided data files to the current table state.
func (c *CatalogClient) CommitSnapshot(ctx context.Context, namespace, table string, commit any) error {
	url := fmt.Sprintf("%s/v1/namespaces/%s/tables/%s/snapshots", c.BaseURL, namespace, table)
	data, err := json.Marshal(commit)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("commit failed (%d): %s", resp.StatusCode, string(body))
	}

	return nil
}
