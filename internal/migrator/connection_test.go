package migrator

import (
	"net/url"
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

func TestBuildConnectionURI_WithCredentialsAndHost(t *testing.T) {
	t.Parallel()

	cfg := MongoConnectionConfig{
		Host:     "mongo-primary:27017,mongo-secondary:27017",
		Username: "admin",
		Password: "p@ss",
		Database: "appdb",
		TLS:      boolPtr(true),
	}
	uri, err := buildConnectionURI(cfg)
	if err != nil {
		t.Fatalf("buildConnectionURI failed: %v", err)
	}
	parsed, err := url.Parse(uri)
	if err != nil {
		t.Fatalf("parse generated uri %q: %v", uri, err)
	}
	if parsed.Scheme != "mongodb" {
		t.Fatalf("expected scheme mongodb, got %q", parsed.Scheme)
	}
	if parsed.Host != "mongo-primary:27017,mongo-secondary:27017" {
		t.Fatalf("unexpected host string %q", parsed.Host)
	}
	if parsed.Path != "/appdb" {
		t.Fatalf("unexpected path %q", parsed.Path)
	}
	username := parsed.User.Username()
	password, _ := parsed.User.Password()
	if username != "admin" || password != "p@ss" {
		t.Fatalf("unexpected credentials: %q / %q", username, password)
	}
	if parsed.Query().Get("tls") != "true" {
		t.Fatalf("expected tls=true, got %q", parsed.Query().Get("tls"))
	}
}

func TestBuildConnectionURI_UsesFullURI(t *testing.T) {
	t.Parallel()

	full := "mongodb://admin:secret@fulluri:27017/mydb?tls=true"
	cfg := MongoConnectionConfig{
		FullURI:  full,
		Database: "should_be_unused_for_uri_construction",
	}
	uri, err := buildConnectionURI(cfg)
	if err != nil {
		t.Fatalf("buildConnectionURI failed: %v", err)
	}
	if uri != full {
		t.Fatalf("expected full uri passthrough, expected %q got %q", full, uri)
	}
}

func TestBuildConnectionURI_RejectsMissingHost(t *testing.T) {
	t.Parallel()

	_, err := buildConnectionURI(MongoConnectionConfig{Database: "app"})
	if err == nil {
		t.Fatalf("expected buildConnectionURI to fail with missing host")
	}
}
