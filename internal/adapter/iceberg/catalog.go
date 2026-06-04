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

// CatalogClient handles interactions with the Iceberg REST Catalog.
type CatalogClient struct {
	BaseURL    string
	HTTPClient *http.Client
}

// TableResponse represents the metadata of an Iceberg table.
type TableResponse struct {
	Metadata       Metadata `json:"metadata"`
	MetadataLocation string   `json:"metadata-location"`
}

// Metadata contains the core table configuration and state.
type Metadata struct {
	Schema         IcebergSchema   `json:"schema"`
	Location       string          `json:"location"`
	LastColumnID   int             `json:"last-column-id"`
	CurrentSnapshotID int64        `json:"current-snapshot-id"`
	Snapshots      []Snapshot      `json:"snapshots"`
	Manifests      []string        `json:"manifest-list,omitempty"` // simplified
}

// Snapshot represents a point-in-time state of the table.
type Snapshot struct {
	SnapshotID   int64             `json:"snapshot-id"`
	TimestampMS  int64             `json:"timestamp-ms"`
	Summary      map[string]string `json:"summary"`
	ManifestList string            `json:"manifest-list"`
}

// IcebergSchema defines the Iceberg table schema structure.
type IcebergSchema struct {
	Type     string         `json:"type"`
	Fields   []IcebergField `json:"fields"`
	SchemaID int            `json:"schema-id"`
}

// IcebergField represents a single field in an Iceberg schema.
type IcebergField struct {
	ID       int    `json:"id"`
	Name     string `json:"name"`
	Type     any    `json:"type"` // Can be string or complex object
	Required bool   `json:"required"`
}

// NewCatalogClient creates a new client for the Iceberg REST Catalog.
func NewCatalogClient(baseURL string) *CatalogClient {
	return &CatalogClient{
		BaseURL: baseURL,
		HTTPClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// GetTable retrieves table metadata from the catalog.
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

// CreateTable creates a new Iceberg table via the catalog.
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

// CommitSnapshot commits a new set of data files to a table.
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
