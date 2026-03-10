package migrator

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
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
	SkipMalformedDocs    *bool                 `json:"skip_malformed_documents"`
}

// MongoConnectionConfig controls how each MongoDB endpoint is initialized.
type MongoConnectionConfig struct {
	FullURI                  string   `json:"full_uri"`
	Host                     string   `json:"host"`
	UseSRV                   bool     `json:"srv"`
	Username                 string   `json:"username"`
	Password                 string   `json:"password"`
	Database                 string   `json:"database"`
	Kind                     string   `json:"kind"`
	ReplicaSet               string   `json:"replica_set"`
	DirectConnection         *bool    `json:"direct_connection"`
	RetryWrites              *bool    `json:"retry_writes"`
	ReadPreference           string   `json:"read_preference"`
	Compressors              []string `json:"compressors"`
	ZlibCompressionLevel     *int     `json:"zlib_compression_level"`
	ZstdCompressionLevel     *int     `json:"zstd_compression_level"`
	AppName                  string   `json:"app_name"`
	AuthSource               string   `json:"auth_source"`
	SkipCollections          []string `json:"skip_collections"`
	TLS                      *bool    `json:"tls"`
	TLSCAFile                string   `json:"tls_ca_file"`
	TLSInsecureSkipVerify    bool     `json:"tls_insecure_skip_verify"`
	ConnectTimeoutMS         int      `json:"connect_timeout_ms"`
	ServerSelectionTimeoutMS int      `json:"server_selection_timeout_ms"`
	SocketTimeoutMS          int      `json:"socket_timeout_ms"`
	MaxPoolSize              uint64   `json:"max_pool_size"`
	MinPoolSize              uint64   `json:"min_pool_size"`
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
	if c.SkipMalformedDocs == nil {
		c.SkipMalformedDocs = boolPtr(true)
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
	if c.Source.EndpointKey() == c.Target.EndpointKey() {
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

func (c Config) ShouldSkipMalformedDocuments() bool {
	return c.SkipMalformedDocs != nil && *c.SkipMalformedDocs
}

func (c *MongoConnectionConfig) applyDefaults() {
	c.Kind = strings.ToLower(strings.TrimSpace(c.Kind))
	if c.Kind == "" {
		c.Kind = connectionKindAuto
	}
	if strings.TrimSpace(c.Database) == "" && strings.TrimSpace(c.FullURI) != "" {
		if parsedURI, err := url.Parse(strings.TrimSpace(c.FullURI)); err == nil {
			c.Database = strings.TrimPrefix(parsedURI.Path, "/")
		}
	}

	switch c.Kind {
	case connectionKindStandalone:
		if c.DirectConnection == nil {
			// mongodb+srv does not support directConnection=true.
			// 기본값은 standalone에서 true이지만, SRV URI인 경우엔 false로 보정한다.
			if c.isFullSRVURI() || c.UseSRV {
				c.DirectConnection = boolPtr(false)
			} else {
				c.DirectConnection = boolPtr(true)
			}
		}
		if c.UseSRV && c.TLS == nil {
			c.TLS = boolPtr(true)
		}
	case connectionKindReplicaSet:
		if c.DirectConnection == nil {
			c.DirectConnection = boolPtr(false)
		}
		if c.UseSRV && c.TLS == nil {
			c.TLS = boolPtr(true)
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

	c.Compressors = normalizeCompressors(c.Compressors)
}

func (c MongoConnectionConfig) Validate(field string) error {
	if _, err := c.resolvedHosts(); err != nil {
		return fmt.Errorf("%s: %w", field, err)
	}
	if c.Database == "" {
		return fmt.Errorf("%s.database is required", field)
	}

	if c.Username == "" && c.Password != "" {
		return fmt.Errorf("%s.password requires %s.username", field, "username")
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
	if err := validateCompressors(c.Compressors); err != nil {
		return fmt.Errorf("%s.compressors: %w", field, err)
	}
	if c.ZlibCompressionLevel != nil {
		if *c.ZlibCompressionLevel < -1 || *c.ZlibCompressionLevel > 9 {
			return fmt.Errorf("%s.zlib_compression_level must be between -1 and 9", field)
		}
	}
	if c.ZstdCompressionLevel != nil {
		if *c.ZstdCompressionLevel < -131072 || *c.ZstdCompressionLevel > 22 {
			return fmt.Errorf("%s.zstd_compression_level must be between -131072 and 22", field)
		}
	}
	effectiveCompressors := c.effectiveCompressors()
	if c.ZlibCompressionLevel != nil && !containsCompressor(effectiveCompressors, "zlib") {
		return fmt.Errorf("%s.zlib_compression_level requires compressors to include zlib", field)
	}
	if c.ZstdCompressionLevel != nil && !containsCompressor(effectiveCompressors, "zstd") {
		return fmt.Errorf("%s.zstd_compression_level requires compressors to include zstd", field)
	}

	if c.UseSRV {
		if c.DirectConnection != nil && *c.DirectConnection {
			return fmt.Errorf("%s.direct_connection cannot be true when srv is enabled; use false or omit", field)
		}
	}

	if c.DirectConnection != nil && *c.DirectConnection && c.isFullSRVURI() {
		return fmt.Errorf("%s.direct_connection cannot be true with mongodb+srv full_uri; set false or omit direct_connection", field)
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
	if field == "target" && len(c.SkipCollections) > 0 {
		return fmt.Errorf("target.skip_collections is not supported; use source.skip_collections instead")
	}

	return nil
}

func (c MongoConnectionConfig) resolvedHosts() ([]string, error) {
	if strings.TrimSpace(c.FullURI) != "" {
		return c.resolvedHostsFromURI()
	}
	if c.UseSRV {
		return c.resolvedHostForSRV()
	}
	return c.resolvedHostsFromHostCSV()
}

func (c MongoConnectionConfig) resolvedHostForSRV() ([]string, error) {
	raw := strings.TrimSpace(c.Host)
	if raw == "" {
		return nil, errors.New("host is required when srv=true")
	}
	if strings.Contains(raw, ",") {
		return nil, errors.New("srv=true supports single host only")
	}
	if strings.Contains(raw, ":") {
		return nil, errors.New("srv=true expects host without port")
	}
	if strings.Contains(raw, "://") {
		return nil, fmt.Errorf("srv host %q must not include scheme", raw)
	}
	return []string{raw}, nil
}

func (c MongoConnectionConfig) resolvedHostsFromURI() ([]string, error) {
	fullURI := strings.TrimSpace(c.FullURI)
	parsed, err := url.Parse(fullURI)
	if err != nil {
		return nil, fmt.Errorf("full_uri is invalid: %w", err)
	}
	switch parsed.Scheme {
	case "mongodb", "mongodb+srv":
	default:
		return nil, fmt.Errorf("full_uri has unsupported scheme %q", parsed.Scheme)
	}
	if parsed.Host == "" {
		return nil, errors.New("full_uri must include at least one host")
	}

	parts := strings.Split(parsed.Host, ",")
	hosts := make([]string, 0, len(parts))
	for _, host := range parts {
		h := strings.TrimSpace(host)
		if h == "" {
			continue
		}
		hosts = append(hosts, h)
	}
	if len(hosts) == 0 {
		return nil, errors.New("full_uri must include at least one host")
	}

	return hosts, nil
}

func (c MongoConnectionConfig) isFullSRVURI() bool {
	fullURI := strings.TrimSpace(c.FullURI)
	if fullURI == "" {
		return false
	}
	parsed, err := url.Parse(fullURI)
	if err != nil {
		return false
	}
	return parsed.Scheme == "mongodb+srv"
}

func (c MongoConnectionConfig) resolvedHostsFromHostCSV() ([]string, error) {
	raw := strings.TrimSpace(c.Host)
	if raw == "" {
		return nil, errors.New("host is required")
	}

	parts := strings.Split(raw, ",")
	hosts := make([]string, 0, len(parts))
	for _, host := range parts {
		h := strings.TrimSpace(host)
		if h == "" {
			continue
		}
		hosts = append(hosts, h)
	}

	if len(hosts) == 0 {
		return nil, errors.New("host is required")
	}

	for _, host := range hosts {
		if strings.Contains(host, "://") {
			return nil, fmt.Errorf("host value %q contains a URI scheme, use host only", host)
		}
	}

	return hosts, nil
}

func (c MongoConnectionConfig) EndpointKey() string {
	hosts, err := c.resolvedHosts()
	if err != nil {
		return ""
	}
	normalized := make([]string, len(hosts))
	for i, host := range hosts {
		normalized[i] = strings.ToLower(strings.TrimSpace(host))
	}
	// keep order stable while still normalizing whitespace/case;
	// if hosts are repeated with different order in a CSV, users can pass in one canonical order.

	return fmt.Sprintf("%s/%s", strings.Join(normalized, ","), c.Database)
}

func boolPtr(v bool) *bool {
	return &v
}

func normalizeCompressors(in []string) []string {
	if len(in) == 0 {
		return nil
	}

	out := make([]string, 0, len(in))
	seen := make(map[string]struct{}, len(in))
	for _, compressor := range in {
		normalized := strings.ToLower(strings.TrimSpace(compressor))
		if normalized == "" {
			continue
		}
		if _, ok := seen[normalized]; ok {
			continue
		}
		seen[normalized] = struct{}{}
		out = append(out, normalized)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func validateCompressors(compressors []string) error {
	for _, compressor := range compressors {
		switch compressor {
		case "snappy", "zlib", "zstd":
		default:
			return fmt.Errorf("unsupported value %q (allowed: snappy, zlib, zstd)", compressor)
		}
	}
	return nil
}

func containsCompressor(compressors []string, target string) bool {
	for _, compressor := range compressors {
		if compressor == target {
			return true
		}
	}
	return false
}

func (c MongoConnectionConfig) effectiveCompressors() []string {
	if len(c.Compressors) > 0 {
		return c.Compressors
	}

	fullURI := strings.TrimSpace(c.FullURI)
	if fullURI == "" {
		return nil
	}
	parsed, err := url.Parse(fullURI)
	if err != nil {
		return nil
	}

	raw := parsed.Query().Get("compressors")
	if raw == "" {
		return nil
	}
	return normalizeCompressors(strings.Split(raw, ","))
}
