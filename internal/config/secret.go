package config

import (
	"context"
	"fmt"
	"os"
)

// SecretProvider defines the interface for retrieving sensitive credentials.
type SecretProvider interface {
	// GetSecret retrieves the secret value for the given key.
	GetSecret(ctx context.Context, key string) (string, error)
}

// EnvSecretProvider retrieves secrets from environment variables.
type EnvSecretProvider struct{}

func (p *EnvSecretProvider) GetSecret(ctx context.Context, key string) (string, error) {
	val := os.Getenv(key)
	if val == "" {
		return "", fmt.Errorf("environment variable %s not set", key)
	}
	return val, nil
}

// FileSecretProvider retrieves secrets from files (e.g., Kubernetes secrets).
type FileSecretProvider struct {
	BaseDir string
}

func (p *FileSecretProvider) GetSecret(ctx context.Context, key string) (string, error) {
	data, err := os.ReadFile(fmt.Sprintf("%s/%s", p.BaseDir, key))
	if err != nil {
		return "", fmt.Errorf("failed to read secret file %s: %w", key, err)
	}
	return string(data), nil
}

// TODO: Implement VaultSecretProvider and AWSSecretProvider
