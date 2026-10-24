package caimandb

// tls_mtls.go builds the *tls.Config shared by AdminServer
// (http_admin.go) and QueryServer (http_query.go) when TLSEnabled is
// set. It adds two things on top of the plain server.ListenAndServeTLS
// this codebase already had:
//
//  1. Mutual TLS: when Config.TLSClientCAFile is set, the server
//     requires and verifies a client certificate signed by that CA
//     (tls.RequireAndVerifyClientCert) in addition to presenting its
//     own. TLSClientCAFile empty means "server-side TLS only",
//     preserving the exact previous behavior.
//  2. Certificate rotation without a restart: the serving certificate
//     is held behind an atomic pointer and served via
//     tls.Config.GetCertificate, with a background goroutine that
//     re-stats TLSCertFile/TLSKeyFile every TLSCertReloadInterval and
//     reloads them if either file's mtime changed. Existing
//     connections keep whatever cert they already negotiated; only new
//     handshakes see the rotated one.
import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

// rotatingCertStore holds the currently-served certificate behind an
// atomic pointer so GetCertificate (called concurrently, once per TLS
// handshake) never races with reload() swapping it out.
type rotatingCertStore struct {
	certFile, keyFile string
	current           atomic.Pointer[tls.Certificate]
	certModTime       time.Time
	keyModTime        time.Time
}

func newRotatingCertStore(certFile, keyFile string) (*rotatingCertStore, error) {
	rc := &rotatingCertStore{certFile: certFile, keyFile: keyFile}
	if err := rc.reload(); err != nil {
		return nil, err
	}
	return rc, nil
}

// reload re-reads cert+key from disk unconditionally and swaps them in.
// Called once at startup (via newRotatingCertStore) and again by
// watch() whenever mtimes change.
func (rc *rotatingCertStore) reload() error {
	cert, err := tls.LoadX509KeyPair(rc.certFile, rc.keyFile)
	if err != nil {
		return fmt.Errorf("loading TLS cert/key: %w", err)
	}
	if certInfo, err := os.Stat(rc.certFile); err == nil {
		rc.certModTime = certInfo.ModTime()
	}
	if keyInfo, err := os.Stat(rc.keyFile); err == nil {
		rc.keyModTime = keyInfo.ModTime()
	}
	rc.current.Store(&cert)
	return nil
}

// changed reports whether either file's mtime has moved since the last
// successful reload, without re-reading file contents -- cheap enough
// to call on every watch() tick.
func (rc *rotatingCertStore) changed() bool {
	certInfo, err := os.Stat(rc.certFile)
	if err != nil {
		return false
	}
	keyInfo, err := os.Stat(rc.keyFile)
	if err != nil {
		return false
	}
	return !certInfo.ModTime().Equal(rc.certModTime) || !keyInfo.ModTime().Equal(rc.keyModTime)
}

func (rc *rotatingCertStore) getCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	return rc.current.Load(), nil
}

// watch runs until stop is closed, checking for a changed cert/key on
// disk every interval and reloading in place. A reload failure (e.g. an
// operator mid-way through writing a new cert file) is logged and the
// previously-loaded certificate keeps serving -- it does NOT bring the
// server down or stop serving TLS.
func (rc *rotatingCertStore) watch(interval time.Duration, stop <-chan struct{}) {
	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			if !rc.changed() {
				continue
			}
			if err := rc.reload(); err != nil {
				log().Warn("TLS certificate rotation failed, keeping previous certificate",
					zap.String("cert_file", rc.certFile), zap.Error(err))
				continue
			}
			log().Info("TLS certificate rotated", zap.String("cert_file", rc.certFile))
		}
	}
}

// buildServerTLSConfig assembles the *tls.Config an http.Server should
// use for cfg.TLSEnabled, including mTLS (if cfg.TLSClientCAFile is
// set) and hot cert rotation. stop should be closed when the owning
// server shuts down, to stop the rotation goroutine. The returned
// rotatingCertStore is only returned so callers can trigger an
// immediate reload in tests; production code just needs the
// *tls.Config.
func buildServerTLSConfig(cfg *Config, stop <-chan struct{}) (*tls.Config, *rotatingCertStore, error) {
	store, err := newRotatingCertStore(cfg.TLSCertFile, cfg.TLSKeyFile)
	if err != nil {
		return nil, nil, err
	}
	safeGo("tls_cert_rotation", func() {
		store.watch(cfg.TLSCertReloadInterval, stop)
	})

	tlsConfig := &tls.Config{
		GetCertificate: store.getCertificate,
		MinVersion:     tls.VersionTLS12,
	}

	if cfg.TLSClientCAFile != "" {
		caBytes, err := os.ReadFile(cfg.TLSClientCAFile)
		if err != nil {
			return nil, nil, fmt.Errorf("reading TLS client CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caBytes) {
			return nil, nil, fmt.Errorf("TLS client CA file %s contains no valid PEM certificates", cfg.TLSClientCAFile)
		}
		tlsConfig.ClientCAs = pool
		tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
	}

	return tlsConfig, store, nil
}
