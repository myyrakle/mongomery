package migrator

import (
	"os"
	"path/filepath"
	"testing"
)

func TestParseReadPreference_SupportedValues(t *testing.T) {
	t.Parallel()

	values := []string{
		"primary",
		"primaryPreferred",
		"secondary",
		"secondaryPreferred",
		"nearest",
	}

	for _, value := range values {
		value := value
		t.Run(value, func(t *testing.T) {
			t.Parallel()
			if _, err := parseReadPreference(value); err != nil {
				t.Fatalf("expected %q to be valid read preference: %v", value, err)
			}
		})
	}
}

func TestParseReadPreference_InvalidValue(t *testing.T) {
	t.Parallel()
	if _, err := parseReadPreference("invalid"); err == nil {
		t.Fatalf("expected parseReadPreference to fail")
	}
}

func TestBuildTLSConfig_ReturnsNilWhenNoTLSOptions(t *testing.T) {
	t.Parallel()

	tlsConfig, err := buildTLSConfig(MongoConnectionConfig{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tlsConfig != nil {
		t.Fatalf("expected nil tls config when tls options are not provided")
	}
}

func TestBuildTLSConfig_InvalidCAFile(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "bad.pem")
	if err := os.WriteFile(path, []byte("not-a-certificate"), 0o644); err != nil {
		t.Fatalf("write test file: %v", err)
	}

	_, err := buildTLSConfig(MongoConnectionConfig{
		TLS:       boolPtr(true),
		TLSCAFile: path,
	})
	if err == nil {
		t.Fatalf("expected tls config build to fail for invalid pem")
	}
}
