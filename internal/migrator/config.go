package migrator

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

const (
	connectionKindAuto       = "auto"
	connectionKindStandalone = "standalone"
	connectionKindReplicaSet = "replica_set"
	connectionKindDocumentDB = "documentdb"
)

// Config holds runtime settings for a single snapshot migration.
type Config struct {
	Source               MongoConnectionConfig `json:"source"`
	Target               MongoConnectionConfig `json:"target"`
	JobID                string                `json:"job_id"`
	MetaCollectionPrefix string                `json:"meta_prefix"`
	BatchSize            int                   `json:"batch_size"`
	LogPercentStep       int                   `json:"log_percent_step"`
}

// MongoConnectionConfig controls how each MongoDB endpoint is initialized.
type MongoConnectionConfig struct {
	URI                      string `json:"uri"`
	Database                 string `json:"database"`
	Kind                     string `json:"kind"`
	ReplicaSet               string `json:"replica_set"`
	DirectConnection         *bool  `json:"direct_connection"`
	RetryWrites              *bool  `json:"retry_writes"`
	ReadPreference           string `json:"read_preference"`
	AppName                  string `json:"app_name"`
	AuthSource               string `json:"auth_source"`
	TLS                      *bool  `json:"tls"`
	TLSCAFile                string `json:"tls_ca_file"`
	TLSInsecureSkipVerify    bool   `json:"tls_insecure_skip_verify"`
	ConnectTimeoutMS         int    `json:"connect_timeout_ms"`
	ServerSelectionTimeoutMS int    `json:"server_selection_timeout_ms"`
	SocketTimeoutMS          int    `json:"socket_timeout_ms"`
	MaxPoolSize              uint64 `json:"max_pool_size"`
	MinPoolSize              uint64 `json:"min_pool_size"`
}

func LoadConfig(path string) (Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return Config{}, fmt.Errorf("open config file %q: %w", path, err)
	}
	defer f.Close()

	var cfg Config
	dec := json.NewDecoder(f)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("decode config file %q: %w", path, err)
	}
	if err := ensureSingleJSONObject(dec); err != nil {
		return Config{}, fmt.Errorf("decode config file %q: %w", path, err)
	}

	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		return Config{}, fmt.Errorf("validate config file %q: %w", path, err)
	}

	return cfg, nil
}

func ensureSingleJSONObject(dec *json.Decoder) error {
	var tail interface{}
	if err := dec.Decode(&tail); err == io.EOF {
		return nil
	} else if err != nil {
		return err
	}
	return errors.New("config file must contain exactly one JSON object")
}

func (c *Config) applyDefaults() {
	if c.JobID == "" {
		c.JobID = "default"
	}
	if c.MetaCollectionPrefix == "" {
		c.MetaCollectionPrefix = "__mongomery"
	}
	if c.BatchSize == 0 {
		c.BatchSize = 1000
	}
	if c.LogPercentStep == 0 {
		c.LogPercentStep = 5
	}

	c.Source.applyDefaults()
	c.Target.applyDefaults()
}

func (c Config) Validate() error {
	if err := c.Source.Validate("source"); err != nil {
		return err
	}
	if err := c.Target.Validate("target"); err != nil {
		return err
	}
	if c.Source.URI == c.Target.URI && c.Source.Database == c.Target.Database {
		return errors.New("source and target must not be the same database")
	}
	if c.JobID == "" {
		return errors.New("job-id is required")
	}
	if c.MetaCollectionPrefix == "" {
		return errors.New("meta-prefix is required")
	}
	if c.BatchSize <= 0 {
		return fmt.Errorf("batch-size must be > 0, got %d", c.BatchSize)
	}
	if c.LogPercentStep <= 0 || c.LogPercentStep > 100 {
		return fmt.Errorf("log-percent-step must be between 1 and 100, got %d", c.LogPercentStep)
	}

	return nil
}

func (c *MongoConnectionConfig) applyDefaults() {
	c.Kind = strings.ToLower(strings.TrimSpace(c.Kind))
	if c.Kind == "" {
		c.Kind = connectionKindAuto
	}

	switch c.Kind {
	case connectionKindStandalone:
		if c.DirectConnection == nil {
			c.DirectConnection = boolPtr(true)
		}
	case connectionKindReplicaSet:
		if c.DirectConnection == nil {
			c.DirectConnection = boolPtr(false)
		}
	case connectionKindDocumentDB:
		if c.DirectConnection == nil {
			c.DirectConnection = boolPtr(false)
		}
		if c.TLS == nil {
			c.TLS = boolPtr(true)
		}
		if c.RetryWrites == nil {
			c.RetryWrites = boolPtr(false)
		}
		if c.ReplicaSet == "" {
			c.ReplicaSet = "rs0"
		}
		if c.ReadPreference == "" {
			c.ReadPreference = "secondaryPreferred"
		}
	}
}

func (c MongoConnectionConfig) Validate(field string) error {
	if c.URI == "" {
		return fmt.Errorf("%s.uri is required", field)
	}
	if c.Database == "" {
		return fmt.Errorf("%s.database is required", field)
	}
	switch c.Kind {
	case connectionKindAuto, connectionKindStandalone, connectionKindReplicaSet, connectionKindDocumentDB:
	default:
		return fmt.Errorf("%s.kind must be one of auto, standalone, replica_set, documentdb", field)
	}

	if c.ReadPreference != "" {
		if _, err := parseReadPreference(c.ReadPreference); err != nil {
			return fmt.Errorf("%s.read_preference: %w", field, err)
		}
	}

	if c.ConnectTimeoutMS < 0 {
		return fmt.Errorf("%s.connect_timeout_ms must be >= 0", field)
	}
	if c.ServerSelectionTimeoutMS < 0 {
		return fmt.Errorf("%s.server_selection_timeout_ms must be >= 0", field)
	}
	if c.SocketTimeoutMS < 0 {
		return fmt.Errorf("%s.socket_timeout_ms must be >= 0", field)
	}
	if c.MinPoolSize > c.MaxPoolSize && c.MaxPoolSize > 0 {
		return fmt.Errorf("%s.min_pool_size must be <= max_pool_size", field)
	}
	if c.TLS != nil && !*c.TLS && (c.TLSCAFile != "" || c.TLSInsecureSkipVerify) {
		return fmt.Errorf("%s.tls cannot be false when tls_ca_file or tls_insecure_skip_verify is set", field)
	}

	return nil
}

func boolPtr(v bool) *bool {
	return &v
}
