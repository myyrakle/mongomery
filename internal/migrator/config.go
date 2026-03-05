package migrator

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
)

// Config holds runtime settings for a single snapshot migration.
type Config struct {
	SourceURI            string `json:"source_uri"`
	TargetURI            string `json:"target_uri"`
	SourceDB             string `json:"source_db"`
	TargetDB             string `json:"target_db"`
	JobID                string `json:"job_id"`
	MetaCollectionPrefix string `json:"meta_prefix"`
	BatchSize            int    `json:"batch_size"`
	LogPercentStep       int    `json:"log_percent_step"`
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

	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		return Config{}, fmt.Errorf("validate config file %q: %w", path, err)
	}

	return cfg, nil
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
}

func (c Config) Validate() error {
	if c.SourceURI == "" {
		return errors.New("source-uri is required")
	}
	if c.TargetURI == "" {
		return errors.New("target-uri is required")
	}
	if c.SourceDB == "" {
		return errors.New("source-db is required")
	}
	if c.TargetDB == "" {
		return errors.New("target-db is required")
	}
	if c.SourceURI == c.TargetURI && c.SourceDB == c.TargetDB {
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
