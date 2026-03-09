package migrator

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadConfig_AppliesDefaults(t *testing.T) {
	t.Parallel()

	cfg := mustLoadConfigFromJSON(t, `{
		"source": {"host":"source:27017","database":"app"},
		"target": {"host":"target:27017","database":"app_copy"}
	}`)

	if cfg.JobID != "default" {
		t.Fatalf("expected default job_id, got %q", cfg.JobID)
	}
	if cfg.MetaCollectionPrefix != "__mongomery" {
		t.Fatalf("expected default meta_prefix, got %q", cfg.MetaCollectionPrefix)
	}
	if cfg.BatchSize != 1000 {
		t.Fatalf("expected default batch_size=1000, got %d", cfg.BatchSize)
	}
	if cfg.LogPercentStep != 5 {
		t.Fatalf("expected default log_percent_step=5, got %d", cfg.LogPercentStep)
	}
	if cfg.Source.Kind != connectionKindAuto {
		t.Fatalf("expected default source kind auto, got %q", cfg.Source.Kind)
	}
	if cfg.Target.Kind != connectionKindAuto {
		t.Fatalf("expected default target kind auto, got %q", cfg.Target.Kind)
	}
}

func TestLoadConfig_AcceptsFullURIWithoutHost(t *testing.T) {
	t.Parallel()

	cfg := mustLoadConfigFromJSON(t, `{
		"source": {"full_uri":"mongodb://source:27017,source2:27017","database":"app"},
		"target": {"full_uri":"mongodb+srv://target.example.com","database":"app_copy"}
	}`)

	if cfg.Source.Host != "" {
		t.Fatalf("expected source.host to be optional when full_uri provided")
	}
	if cfg.Target.Host != "" {
		t.Fatalf("expected target.host to be optional when full_uri provided")
	}
}

func TestLoadConfig_UsesDatabaseFromFullURIPathWhenMissing(t *testing.T) {
	t.Parallel()

	cfg := mustLoadConfigFromJSON(t, `{
		"source": {"full_uri":"mongodb+srv://source.example.com/app","kind":"replica_set"},
		"target": {"full_uri":"mongodb+srv://target.example.com/app_copy","kind":"replica_set"}
	}`)
	if cfg.Source.Database != "app" {
		t.Fatalf("expected source.database inferred from full_uri path, got %q", cfg.Source.Database)
	}
	if cfg.Target.Database != "app_copy" {
		t.Fatalf("expected target.database inferred from full_uri path, got %q", cfg.Target.Database)
	}
}

func TestLoadConfig_RejectsMissingConnectionInfo(t *testing.T) {
	t.Parallel()

	_, err := loadConfigFromJSON(t, `{
		"source": {"database":"app"},
		"target": {"host":"target:27017","database":"app_copy"}
	}`)
	if err == nil {
		t.Fatalf("expected error for missing source host and full_uri")
	}
	if !strings.Contains(err.Error(), "source: host is required") {
		t.Fatalf("expected host required error, got: %v", err)
	}
}

func TestLoadConfig_RejectsInvalidFullURI(t *testing.T) {
	t.Parallel()

	_, err := loadConfigFromJSON(t, `{
		"source": {"full_uri":"http://source:27017","database":"app"},
		"target": {"host":"target:27017","database":"app_copy"}
	}`)
	if err == nil {
		t.Fatalf("expected error for invalid full_uri")
	}
	if !strings.Contains(err.Error(), "full_uri has unsupported scheme") {
		t.Fatalf("expected unsupported full_uri scheme error, got: %v", err)
	}
}

func TestLoadConfig_AppliesStandaloneAndReplicaDefaults(t *testing.T) {
	t.Parallel()

	cfg := mustLoadConfigFromJSON(t, `{
		"source": {"host":"source:27017","database":"app","kind":"standalone"},
		"target": {"host":"target1:27017,target2:27017","database":"app_copy","kind":"replica_set"}
	}`)

	if cfg.Source.DirectConnection == nil || !*cfg.Source.DirectConnection {
		t.Fatalf("expected standalone source direct_connection=true default")
	}
	if cfg.Target.DirectConnection == nil || *cfg.Target.DirectConnection {
		t.Fatalf("expected replica_set target direct_connection=false default")
	}
}

func TestLoadConfig_AdjustsStandaloneSRVDefaultDirectConnection(t *testing.T) {
	t.Parallel()

	cfg := mustLoadConfigFromJSON(t, `{
		"source": {
			"full_uri":"mongodb+srv://source-cluster.example.com/app",
			"database":"app",
			"kind":"standalone"
		},
		"target": {
			"host":"target:27017",
			"database":"app_copy",
			"kind":"replica_set"
		}
	}`)

	if cfg.Source.DirectConnection == nil || *cfg.Source.DirectConnection {
		t.Fatalf("expected standalone full_uri mongodb+srv default direct_connection=false")
	}
}

func TestLoadConfig_RejectsStandaloneSRVWithDirectConnectionTrue(t *testing.T) {
	t.Parallel()

	_, err := loadConfigFromJSON(t, `{
		"source": {
			"full_uri":"mongodb+srv://source-cluster.example.com/app",
			"database":"app",
			"kind":"standalone",
			"direct_connection": true
		},
		"target": {
			"host":"target:27017",
			"database":"app_copy",
			"kind":"replica_set"
		}
	}`)
	if err == nil {
		t.Fatalf("expected error for mongodb+srv with direct_connection=true")
	}
	if !strings.Contains(err.Error(), "source.direct_connection cannot be true with mongodb+srv full_uri") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadConfig_AppliesDocumentDBDefaults(t *testing.T) {
	t.Parallel()

	cfg := mustLoadConfigFromJSON(t, `{
		"source": {"host":"docdb-cluster.amazonaws.com:27017","database":"app","kind":"documentdb"},
		"target": {"host":"target:27017","database":"app_copy"}
	}`)

	if cfg.Source.TLS == nil || !*cfg.Source.TLS {
		t.Fatalf("expected documentdb source tls=true default")
	}
	if cfg.Source.RetryWrites == nil || *cfg.Source.RetryWrites {
		t.Fatalf("expected documentdb source retry_writes=false default")
	}
	if cfg.Source.ReplicaSet != "rs0" {
		t.Fatalf("expected documentdb source replica_set=rs0 default, got %q", cfg.Source.ReplicaSet)
	}
	if cfg.Source.ReadPreference != "secondaryPreferred" {
		t.Fatalf("expected documentdb source read_preference default, got %q", cfg.Source.ReadPreference)
	}
	if cfg.Source.DirectConnection == nil || *cfg.Source.DirectConnection {
		t.Fatalf("expected documentdb source direct_connection=false default")
	}
}

func TestLoadConfig_RejectsUnknownField(t *testing.T) {
	t.Parallel()

	_, err := loadConfigFromJSON(t, `{
		"source": {"host":"source:27017","database":"app"},
		"target": {"host":"target:27017","database":"app_copy"},
		"unknown_field": true
	}`)
	if err == nil {
		t.Fatalf("expected error for unknown field")
	}
	if !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("expected unknown field error, got %v", err)
	}
}

func TestLoadConfig_RejectsInvalidKind(t *testing.T) {
	t.Parallel()

	_, err := loadConfigFromJSON(t, `{
		"source": {"host":"source:27017","database":"app","kind":"bad"},
		"target": {"host":"target:27017","database":"app_copy"}
	}`)
	if err == nil {
		t.Fatalf("expected error for invalid source.kind")
	}
	if !strings.Contains(err.Error(), "source.kind") {
		t.Fatalf("expected source.kind in error, got %v", err)
	}
}

func TestLoadConfig_RejectsSameSourceAndTarget(t *testing.T) {
	t.Parallel()

	_, err := loadConfigFromJSON(t, `{
		"source": {"host":"same:27017","database":"app"},
		"target": {"host":"same:27017","database":"app"}
	}`)
	if err == nil {
		t.Fatalf("expected error for same source and target")
	}
	if !strings.Contains(err.Error(), "source and target must not be the same database") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadConfig_RejectsMultipleJSONDocuments(t *testing.T) {
	t.Parallel()

	_, err := loadConfigFromJSON(t, `{
		"source": {"host":"source:27017","database":"app"},
		"target": {"host":"target:27017","database":"app_copy"}
	}{"x":1}`)
	if err == nil {
		t.Fatalf("expected error for multiple json documents")
	}
	if !strings.Contains(err.Error(), "exactly one JSON object") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadConfig_RejectsHostWithUriLikeFormat(t *testing.T) {
	t.Parallel()

	_, err := loadConfigFromJSON(t, `{
		"source": {"host":"mongodb://source:27017","database":"app"},
		"target": {"host":"target:27017","database":"app_copy"}
	}`)
	if err == nil {
		t.Fatalf("expected error for host containing uri scheme")
	}
	if !strings.Contains(err.Error(), "host value") {
		t.Fatalf("expected host scheme error, got: %v", err)
	}
}

func TestLoadConfig_AcceptsSRVHostMode(t *testing.T) {
	t.Parallel()

	cfg := mustLoadConfigFromJSON(t, `{
		"source": {
			"host":"aims-aws-kr-prod.clrl7.mongodb.net",
			"srv": true,
			"username": "aims",
			"password": "pwd",
			"database":"acloset-prod",
			"kind":"replica_set"
		},
		"target": {"host":"127.0.0.1:27018","database":"app_copy","kind":"standalone","direct_connection":true}
	}`)
	if cfg.Source.UseSRV != true {
		t.Fatalf("expected source.srv=true")
	}
	if cfg.Source.DirectConnection == nil || *cfg.Source.DirectConnection {
		t.Fatalf("expected source direct_connection=false for srv mode")
	}
}

func TestLoadConfig_RejectsSRVWithPort(t *testing.T) {
	t.Parallel()

	_, err := loadConfigFromJSON(t, `{
		"source": {
			"host":"aims-aws-kr-prod.clrl7.mongodb.net:27017",
			"srv": true,
			"database":"acloset-prod",
			"kind":"replica_set"
		},
		"target": {"host":"127.0.0.1:27018","database":"app_copy","kind":"standalone","direct_connection":true}
	}`)
	if err == nil {
		t.Fatalf("expected error for srv mode with port")
	}
	if !strings.Contains(err.Error(), "srv=true expects host without port") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadConfig_NormalizesCompressors(t *testing.T) {
	t.Parallel()

	cfg := mustLoadConfigFromJSON(t, `{
		"source": {
			"host":"source:27017",
			"database":"app",
			"compressors":[" ZSTD ","snappy","zstd","zlib"]
		},
		"target": {"host":"target:27017","database":"app_copy"}
	}`)
	if len(cfg.Source.Compressors) != 3 {
		t.Fatalf("expected normalized unique compressors, got %v", cfg.Source.Compressors)
	}
	if cfg.Source.Compressors[0] != "zstd" || cfg.Source.Compressors[1] != "snappy" || cfg.Source.Compressors[2] != "zlib" {
		t.Fatalf("unexpected normalized compressors: %v", cfg.Source.Compressors)
	}
}

func TestLoadConfig_RejectsInvalidCompressor(t *testing.T) {
	t.Parallel()

	_, err := loadConfigFromJSON(t, `{
		"source": {
			"host":"source:27017",
			"database":"app",
			"compressors":["gzip"]
		},
		"target": {"host":"target:27017","database":"app_copy"}
	}`)
	if err == nil {
		t.Fatalf("expected error for invalid compressor")
	}
	if !strings.Contains(err.Error(), "source.compressors") {
		t.Fatalf("expected source.compressors in error, got: %v", err)
	}
}

func TestLoadConfig_RejectsZlibLevelWithoutZlibCompressor(t *testing.T) {
	t.Parallel()

	_, err := loadConfigFromJSON(t, `{
		"source": {
			"host":"source:27017",
			"database":"app",
			"compressors":["zstd"],
			"zlib_compression_level":6
		},
		"target": {"host":"target:27017","database":"app_copy"}
	}`)
	if err == nil {
		t.Fatalf("expected error for zlib_compression_level without zlib compressor")
	}
	if !strings.Contains(err.Error(), "source.zlib_compression_level requires compressors to include zlib") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadConfig_AcceptsZlibLevelWhenFullURIHasZlibCompressor(t *testing.T) {
	t.Parallel()

	cfg := mustLoadConfigFromJSON(t, `{
		"source": {
			"full_uri":"mongodb://source:27017/app?compressors=zstd,zlib",
			"zlib_compression_level":6
		},
		"target": {"host":"target:27017","database":"app_copy"}
	}`)
	if cfg.Source.ZlibCompressionLevel == nil || *cfg.Source.ZlibCompressionLevel != 6 {
		t.Fatalf("expected zlib_compression_level=6, got %v", cfg.Source.ZlibCompressionLevel)
	}
}

func mustLoadConfigFromJSON(t *testing.T, raw string) Config {
	t.Helper()
	cfg, err := loadConfigFromJSON(t, raw)
	if err != nil {
		t.Fatalf("load config failed: %v", err)
	}
	return cfg
}

func loadConfigFromJSON(t *testing.T, raw string) (Config, error) {
	t.Helper()

	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "config.json")
	if err := os.WriteFile(path, []byte(raw), 0o644); err != nil {
		t.Fatalf("write temp config: %v", err)
	}
	return LoadConfig(path)
}
