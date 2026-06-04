package secrets

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	vault "github.com/hashicorp/vault/api"
)

// VaultProvider resolves secrets using HashiCorp Vault's KV v2 secrets engine.
// It authenticates via the AppRole auth method, which is suitable for
// non-interactive workloads such as CI/CD pipelines and Kubernetes Jobs.
type VaultProvider struct {
	client    *vault.Client
	mountPath string // KV v2 mount path, e.g. "secret"
}

// NewVaultProvider configures and authenticates a Vault client.
//
// Required opts keys:
//
//   - "addr"        — Vault server URL, e.g. "https://vault.example.com"
//   - "role_id"     — AppRole role ID
//   - "secret_id"   — AppRole secret ID
//
// Optional opts keys:
//
//   - "mount_path"  — KV v2 mount path (default: "secret")
//   - "namespace"   — Vault Enterprise namespace (leave empty for OSS)
func NewVaultProvider(opts map[string]string) (*VaultProvider, error) {
	addr, ok := opts["addr"]
	if !ok || addr == "" {
		return nil, fmt.Errorf("vault provider: missing required option 'addr'")
	}
	roleID, ok := opts["role_id"]
	if !ok || roleID == "" {
		return nil, fmt.Errorf("vault provider: missing required option 'role_id'")
	}
	secretID, ok := opts["secret_id"]
	if !ok || secretID == "" {
		return nil, fmt.Errorf("vault provider: missing required option 'secret_id'")
	}

	cfg := vault.DefaultConfig()
	cfg.Address = addr
	cfg.HttpClient = &http.Client{Timeout: 10 * time.Second}

	client, err := vault.NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("vault provider: failed to create client: %w", err)
	}

	if ns := opts["namespace"]; ns != "" {
		client.SetNamespace(ns)
	}

	// Authenticate with AppRole.
	loginData := map[string]interface{}{
		"role_id":   roleID,
		"secret_id": secretID,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	resp, err := client.Logical().WriteWithContext(ctx, "auth/approle/login", loginData)
	if err != nil {
		return nil, fmt.Errorf("vault provider: AppRole login failed: %w", err)
	}
	if resp == nil || resp.Auth == nil {
		return nil, fmt.Errorf("vault provider: AppRole login returned no auth")
	}
	client.SetToken(resp.Auth.ClientToken)

	mountPath := opts["mount_path"]
	if mountPath == "" {
		mountPath = "secret"
	}

	return &VaultProvider{client: client, mountPath: mountPath}, nil
}

// Resolve fetches a secret from Vault KV v2.
//
// The key format is "path/to/secret:field", where the part before the colon
// is the KV path (relative to the mount) and the part after is the field name.
// Example: "database/prod:password"
func (v *VaultProvider) Resolve(key string) (string, error) {
	parts := strings.SplitN(key, ":", 2)
	if len(parts) != 2 {
		return "", fmt.Errorf("vault provider: key %q must be in 'path:field' format", key)
	}
	kvPath, field := parts[0], parts[1]

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// KV v2 API path is "<mount>/data/<path>".
	apiPath := fmt.Sprintf("%s/data/%s", v.mountPath, kvPath)
	secret, err := v.client.Logical().ReadWithContext(ctx, apiPath)
	if err != nil {
		return "", fmt.Errorf("vault provider: failed to read %q: %w", apiPath, err)
	}
	if secret == nil || secret.Data == nil {
		return "", fmt.Errorf("vault provider: no data at path %q", apiPath)
	}

	// KV v2 wraps values in a "data" sub-map.
	dataMap, ok := secret.Data["data"].(map[string]interface{})
	if !ok {
		return "", fmt.Errorf("vault provider: unexpected data format at %q", apiPath)
	}

	val, ok := dataMap[field]
	if !ok {
		return "", fmt.Errorf("vault provider: field %q not found in secret %q", field, kvPath)
	}

	str, ok := val.(string)
	if !ok {
		return "", fmt.Errorf("vault provider: field %q is not a string", field)
	}

	return str, nil
}
