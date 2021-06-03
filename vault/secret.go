package vault

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	platform2 "github.com/influxdata/influxdb/v2/kit/platform"

	"github.com/hashicorp/vault/api"
	platform "github.com/influxdata/influxdb/v2"
)

var _ platform.SecretService = (*SecretService)(nil)

// SecretService is service for storing user secrets
type SecretService struct {
	Client *api.Client
}

// Config may setup the vault client configuration. If any field is a zero
// value, it will be ignored and the default used.
type Config struct {
	Address       string
	AgentAddress  string
	ClientTimeout time.Duration
	MaxRetries    int
	Token         string
	TLSConfig
}

// TLSConfig is the configuration for TLS.
type TLSConfig struct {
	CACert             string
	CAPath             string
	ClientCert         string
	ClientKey          string
	InsecureSkipVerify bool
	TLSServerName      string
}

func (c Config) assign(apiCFG *api.Config) error {
	if c.Address != "" {
		apiCFG.Address = c.Address
	}

	if c.AgentAddress != "" {
		apiCFG.AgentAddress = c.AgentAddress
	}

	if c.ClientTimeout > 0 {
		apiCFG.Timeout = c.ClientTimeout
	}

	if c.MaxRetries > 0 {
		apiCFG.MaxRetries = c.MaxRetries
	}

	if c.TLSServerName != "" {
		err := apiCFG.ConfigureTLS(&api.TLSConfig{
			CACert:        c.CACert,
			CAPath:        c.CAPath,
			ClientCert:    c.ClientCert,
			ClientKey:     c.ClientKey,
			TLSServerName: c.TLSServerName,
			Insecure:      c.InsecureSkipVerify,
		})
		if err != nil {
			return err
		}
	}

	return nil
}

// ConfigOptFn is a functional input option to configure a vault service.
type ConfigOptFn func(Config) Config

// WithConfig provides a configuration to the service constructor.
func WithConfig(config Config) ConfigOptFn {
	return func(Config) Config {
		return config
	}
}

// WithTLSConfig allows one to set the TLS config only.
func WithTLSConfig(tlsCFG TLSConfig) ConfigOptFn {
	return func(cfg Config) Config {
		cfg.TLSConfig = tlsCFG
		return cfg
	}
}

// NewSecretService creates an instance of a SecretService.
// The service is configured using the standard vault environment variables.
// https://www.vaultproject.io/docs/commands/index.html#environment-variables
func NewSecretService(cfgOpts ...ConfigOptFn) (*SecretService, error) {
	explicitConfig := Config{}
	for _, o := range cfgOpts {
		explicitConfig = o(explicitConfig)
	}

	cfg := api.DefaultConfig()
	if cfg.Error != nil {
		return nil, cfg.Error
	}

	err := explicitConfig.assign(cfg)
	if err != nil {
		return nil, err
	}

	c, err := api.NewClient(cfg)
	if err != nil {
		return nil, err
	}

	if explicitConfig.Token != "" {
		c.SetToken(explicitConfig.Token)
	}

	return &SecretService{
		Client: c,
	}, nil
}

// LoadSecret retrieves the secret value v found at key k for organization orgID.
func (s *SecretService) LoadSecret(ctx context.Context, orgID platform2.ID, k string) (string, error) {
	data, _, err := s.loadSecrets(ctx, orgID)
	if err != nil {
		return "", err
	}

	if v, ok := data[k]; ok {
		return v, nil
	}

	return "", fmt.Errorf("secret not found")
}

// loadSecrets retrieves a map of secrets for an organization and the version of the secrets retrieved.
// The version is used to ensure that concurrent updates will not overwrite one another.
func (s *SecretService) loadSecrets(ctx context.Context, orgID platform2.ID) (map[string]string, int, error) {
	// TODO(desa): update url construction
	sec, err := s.Client.Logical().Read(fmt.Sprintf("/secret/data/%s", orgID))
	if err != nil {
		return nil, -1, err
	}

	m := map[string]string{}
	if sec == nil {
		return m, 0, nil
	}

	data, ok := sec.Data["data"].(map[string]interface{})
	if !ok {
		return nil, -1, fmt.Errorf("value found in secret data is not map[string]interface{}")
	}

	for k, v := range data {
		val, ok := v.(string)
		if !ok {
			continue
		}
		m[k] = val
	}

	metadata, ok := sec.Data["metadata"].(map[string]interface{})
	if !ok {
		return nil, -1, fmt.Errorf("value found in secret metadata is not map[string]interface{}")
	}

	var version int
	switch v := metadata["version"].(type) {
	case json.Number:
		ver, err := v.Int64()
		if err != nil {
			return nil, -1, err
		}
		version = int(ver)
	case string:
		ver, err := strconv.Atoi(v)
		if err != nil {
			return nil, -1, fmt.Errorf("version provided is not a valid integer: %v", err)
		}
		version = ver
	case int:
		version = v
	default:
		return nil, -1, fmt.Errorf("version provided is %T not a string or int", v)
	}

	return m, version, nil
}

// GetSecretKeys retrieves all secret keys that are stored for the organization orgID.
func (s *SecretService) GetSecretKeys(ctx context.Context, orgID platform2.ID) ([]string, error) {
	data, _, err := s.loadSecrets(ctx, orgID)
	if err != nil {
		return nil, err
	}

	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}

	return keys, nil
}

// PutSecret stores the secret pair (k,v) for the organization orgID.
func (s *SecretService) PutSecret(ctx context.Context, orgID platform2.ID, k string, v string) error {
	data, ver, err := s.loadSecrets(ctx, orgID)
	if err != nil {
		return err
	}

	data[k] = v

	return s.putSecrets(ctx, orgID, data, ver)
}

// putSecrets will set all provided data values for the organization orgID.
// If version is negative, the write will overwrite all specified values.
// If version is 0, the write will only be allowed if the keys do not exists.
// If version is non-zero, the write will only be allowed if the keys current
// version in vault matches the version specified.
func (s *SecretService) putSecrets(ctx context.Context, orgID platform2.ID, data map[string]string, version int) error {
	m := map[string]interface{}{"data": data}

	if version >= 0 {
		m["options"] = map[string]interface{}{"cas": version}
	}

	if _, err := s.Client.Logical().Write(fmt.Sprintf("/secret/data/%s", orgID), m); err != nil {
		return err
	}

	return nil
}

// PutSecrets puts all provided secrets and overwrites any previous values.
func (s *SecretService) PutSecrets(ctx context.Context, orgID platform2.ID, m map[string]string) error {
	return s.putSecrets(ctx, orgID, m, -1)
}

// PatchSecrets patches all provided secrets and updates any previous values.
func (s *SecretService) PatchSecrets(ctx context.Context, orgID platform2.ID, m map[string]string) error {
	data, ver, err := s.loadSecrets(ctx, orgID)
	if err != nil {
		return err
	}

	for k, v := range m {
		data[k] = v
	}

	return s.putSecrets(ctx, orgID, data, ver)
}

// DeleteSecret removes a single secret from the secret store.
func (s *SecretService) DeleteSecret(ctx context.Context, orgID platform2.ID, ks ...string) error {
	data, ver, err := s.loadSecrets(ctx, orgID)
	if err != nil {
		return err
	}

	for _, k := range ks {
		delete(data, k)
	}

	return s.putSecrets(ctx, orgID, data, ver)
}
