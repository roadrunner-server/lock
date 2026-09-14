package lock

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

type TLSConfig struct {
	Cert   string `mapstructure:"cert"`
	Key    string `mapstructure:"key"`
	RootCa string `mapstructure:"root_ca"`
}

// tlsConfig builds the client TLS configuration. A nil conf keeps the connection plaintext.
func tlsConfig(conf *TLSConfig) (*tls.Config, error) {
	if conf == nil {
		return nil, nil
	}

	cfg := &tls.Config{MinVersion: tls.VersionTLS12}

	// Validate rejects a certificate without its key.
	if conf.Cert != "" {
		// The handshake loads the pair, so a renewed certificate needs no restart.
		cfg.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			cert, err := tls.LoadX509KeyPair(conf.Cert, conf.Key)
			if err != nil {
				return nil, err
			}
			return new(cert), nil
		}
	}

	if conf.RootCa == "" {
		return cfg, nil
	}

	pool, err := x509.SystemCertPool()
	if err != nil {
		return nil, err
	}
	rootCa, err := os.ReadFile(conf.RootCa)
	if err != nil {
		return nil, err
	}
	if !pool.AppendCertsFromPEM(rootCa) {
		return nil, fmt.Errorf("no certificate found in %q", conf.RootCa)
	}
	cfg.RootCAs = pool

	return cfg, nil
}
