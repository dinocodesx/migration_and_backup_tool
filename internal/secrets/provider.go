// Package secrets provides an abstraction layer for securely retrieving
// credentials at runtime. It supports multiple backend providers (environment
// variables, files, and HashiCorp Vault) so the caller never needs to
// hard-code or manage secrets directly.
package secrets

import (
	"fmt"
	"os"
	"strings"
)

// Provider is the interface that every secrets backend must implement.
// Resolve maps a logical secret key to its plaintext value.
type Provider interface {
	// Resolve returns the plaintext secret for the given key.
	// It returns an error if the key is unknown or cannot be fetched.
	Resolve(key string) (string, error)
}

// NewProvider constructs the appropriate Provider from a type identifier.
//
//   - "env"  — reads from environment variables (default)
//   - "file" — reads from a flat key=value file
//   - "vault" — reads from HashiCorp Vault (requires Vault config in DBConfig.Params)
//
// Any unrecognised type falls back to the env provider.
func NewProvider(providerType string, opts map[string]string) (Provider, error) {
	switch strings.ToLower(providerType) {
	case "file":
		path, ok := opts["path"]
		if !ok || path == "" {
			return nil, fmt.Errorf("secrets: file provider requires 'path' option")
		}
		return NewFileProvider(path)
	case "vault":
		return NewVaultProvider(opts)
	default: // "env" or empty
		return &EnvProvider{}, nil
	}
}

// ─────────────────────────────────────────────────────────────
// Env Provider
// ─────────────────────────────────────────────────────────────

// EnvProvider resolves secrets by reading environment variables.
// The key is expected to be an exact environment variable name, or a
// ${VAR_NAME} style interpolation expression.
type EnvProvider struct{}

// Resolve fetches the value of an environment variable. Supports both
// bare names ("MY_SECRET") and ${MY_SECRET} syntax.
func (p *EnvProvider) Resolve(key string) (string, error) {
	name := strings.TrimSpace(key)
	if strings.HasPrefix(name, "${") && strings.HasSuffix(name, "}") {
		name = name[2 : len(name)-1]
	}
	val := os.Getenv(name)
	if val == "" {
		return "", fmt.Errorf("secrets: env variable %q is not set or empty", name)
	}
	return val, nil
}

// ─────────────────────────────────────────────────────────────
// File Provider
// ─────────────────────────────────────────────────────────────

// FileProvider resolves secrets from a flat "KEY=VALUE" text file.
// Blank lines and lines starting with '#' are ignored.
type FileProvider struct {
	secrets map[string]string
}

// NewFileProvider loads all key-value pairs from a secrets file into memory.
func NewFileProvider(path string) (*FileProvider, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("secrets: cannot read file %q: %w", path, err)
	}

	m := make(map[string]string)
	for i, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("secrets: file %q line %d: expected KEY=VALUE format", path, i+1)
		}
		m[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
	}

	return &FileProvider{secrets: m}, nil
}

// Resolve looks up the key in the loaded secrets map.
func (p *FileProvider) Resolve(key string) (string, error) {
	val, ok := p.secrets[key]
	if !ok {
		return "", fmt.Errorf("secrets: key %q not found in secrets file", key)
	}
	return val, nil
}

// Interpolate replaces all ${KEY} placeholders in a string with resolved secret values.
// Placeholders that cannot be resolved are left unchanged and an error is returned.
func Interpolate(s string, p Provider) (string, error) {
	var lastErr error
	result := os.Expand(s, func(key string) string {
		val, err := p.Resolve(key)
		if err != nil {
			lastErr = err
			return "${" + key + "}" // leave placeholder intact on failure
		}
		return val
	})
	return result, lastErr
}
