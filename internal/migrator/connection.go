package migrator

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

func buildClientOptions(cfg MongoConnectionConfig) (*options.ClientOptions, error) {
	uri, err := buildConnectionURI(cfg)
	if err != nil {
		return nil, err
	}

	clientOptions := options.Client().ApplyURI(uri)

	if cfg.AppName != "" {
		clientOptions.SetAppName(cfg.AppName)
	}
	if cfg.ReplicaSet != "" {
		clientOptions.SetReplicaSet(cfg.ReplicaSet)
	}
	if cfg.DirectConnection != nil {
		clientOptions.SetDirect(*cfg.DirectConnection)
	}
	if cfg.RetryWrites != nil {
		clientOptions.SetRetryWrites(*cfg.RetryWrites)
	}
	if cfg.ReadPreference != "" {
		rp, err := parseReadPreference(cfg.ReadPreference)
		if err != nil {
			return nil, err
		}
		clientOptions.SetReadPreference(rp)
	}
	if cfg.AuthSource != "" {
		cred := options.Credential{AuthSource: cfg.AuthSource}
		if clientOptions.Auth != nil {
			cred = *clientOptions.Auth
			cred.AuthSource = cfg.AuthSource
		}
		clientOptions.SetAuth(cred)
	}
	if cfg.ConnectTimeoutMS > 0 {
		clientOptions.SetConnectTimeout(time.Duration(cfg.ConnectTimeoutMS) * time.Millisecond)
	}
	if cfg.ServerSelectionTimeoutMS > 0 {
		clientOptions.SetServerSelectionTimeout(time.Duration(cfg.ServerSelectionTimeoutMS) * time.Millisecond)
	}
	if cfg.SocketTimeoutMS > 0 {
		clientOptions.SetSocketTimeout(time.Duration(cfg.SocketTimeoutMS) * time.Millisecond)
	}
	if cfg.MaxPoolSize > 0 {
		clientOptions.SetMaxPoolSize(cfg.MaxPoolSize)
	}
	if cfg.MinPoolSize > 0 {
		clientOptions.SetMinPoolSize(cfg.MinPoolSize)
	}

	tlsConfig, err := buildTLSConfig(cfg)
	if err != nil {
		return nil, err
	}
	if tlsConfig != nil {
		clientOptions.SetTLSConfig(tlsConfig)
	}

	return clientOptions, nil
}

func buildConnectionURI(cfg MongoConnectionConfig) (string, error) {
	if strings.TrimSpace(cfg.FullURI) != "" {
		return strings.TrimSpace(cfg.FullURI), nil
	}

	hosts, err := cfg.resolvedHosts()
	if err != nil {
		return "", err
	}

	uri := &url.URL{
		Scheme: "mongodb",
		Host:   strings.Join(hosts, ","),
		Path:   "/" + cfg.Database,
	}

	if cfg.Username != "" {
		if cfg.Password != "" {
			uri.User = url.UserPassword(cfg.Username, cfg.Password)
		} else {
			uri.User = url.User(cfg.Username)
		}
	}

	query := uri.Query()
	if cfg.ReplicaSet != "" {
		query.Set("replicaSet", cfg.ReplicaSet)
	}
	if cfg.TLS != nil {
		query.Set("tls", strconv.FormatBool(*cfg.TLS))
	}

	if len(query) > 0 {
		uri.RawQuery = query.Encode()
	}

	return uri.String(), nil
}

func buildTLSConfig(cfg MongoConnectionConfig) (*tls.Config, error) {
	tlsEnabled := cfg.TLS != nil && *cfg.TLS
	if !tlsEnabled && cfg.TLSCAFile == "" && !cfg.TLSInsecureSkipVerify {
		return nil, nil
	}

	tlsConfig := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: cfg.TLSInsecureSkipVerify, //nolint:gosec // explicitly controlled by config
	}

	if cfg.TLSCAFile != "" {
		caCert, err := os.ReadFile(cfg.TLSCAFile)
		if err != nil {
			return nil, fmt.Errorf("read tls_ca_file %q: %w", cfg.TLSCAFile, err)
		}
		caPool := x509.NewCertPool()
		if ok := caPool.AppendCertsFromPEM(caCert); !ok {
			return nil, fmt.Errorf("parse tls_ca_file %q: no valid certificates found", cfg.TLSCAFile)
		}
		tlsConfig.RootCAs = caPool
	}

	return tlsConfig, nil
}

func parseReadPreference(s string) (*readpref.ReadPref, error) {
	normalized := strings.ToLower(strings.TrimSpace(s))
	switch normalized {
	case "primary":
		return readpref.Primary(), nil
	case "primarypreferred":
		return readpref.PrimaryPreferred(), nil
	case "secondary":
		return readpref.Secondary(), nil
	case "secondarypreferred":
		return readpref.SecondaryPreferred(), nil
	case "nearest":
		return readpref.Nearest(), nil
	default:
		return nil, fmt.Errorf("unsupported read preference %q", s)
	}
}
