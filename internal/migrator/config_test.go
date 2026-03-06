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

func TestLoadConfig_AppliesStandaloneAndReplicaDefaults(t *testing.T) {
	t.Parallel()

	cfg := mustLoadConfigFromJSON(t, `{
		"source": {"host":"source:27017","database":"app","kind":"standalone"},
		"target": {"hosts":["target1:27017","target2:27017"],"database":"app_copy","kind":"replica_set"}
	}`)

	if cfg.Source.DirectConnection == nil || !*cfg.Source.DirectConnection {
		t.Fatalf("expected standalone source direct_connection=true default")
	}
	if cfg.Target.DirectConnection == nil || *cfg.Target.DirectConnection {
		t.Fatalf("expected replica_set target direct_connection=false default")
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
		"source": {"hosts":["same:27017"],"database":"app"},
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
