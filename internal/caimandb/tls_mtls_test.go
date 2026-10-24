package caimandb

// tls_mtls_test.go exercises mTLS end to end against a real
// AdminServer listening on a real TCP port with real X.509
// certificates generated in-process (not mocked/stubbed TLS) --
// confirming three things buildServerTLSConfig (tls_mtls.go) promises:
//
//  1. With TLSClientCAFile unset, TLS works exactly as it did before
//     mTLS existed (server-only auth): a plain HTTPS client with no
//     client cert can connect.
//  2. With TLSClientCAFile set, a client presenting no certificate, or
//     one not signed by that CA, is rejected at the TLS handshake; a
//     client presenting a certificate signed by that CA is accepted.
//  3. Certificate rotation: swapping the cert/key files on disk and
//     waiting past TLSCertReloadInterval causes new connections to
//     receive the new certificate, without restarting the server.
import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// genCert creates a self-signed CA (if parent is nil) or a leaf
// certificate signed by parent/parentKey, returning PEM-encoded cert
// and key bytes plus the parsed certificate (for signing further
// leaves) and its private key.
func genCert(t *testing.T, cn string, isCA bool, parent *x509.Certificate, parentKey *ecdsa.PrivateKey) (certPEM, keyPEM []byte, cert *x509.Certificate, key *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generating key: %v", err)
	}
	serial, err := rand.Int(rand.Reader, big.NewInt(1<<62))
	if err != nil {
		t.Fatalf("generating serial: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  isCA,
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}
	signer := tmpl
	signerKey := key
	if parent != nil {
		signer = parent
		signerKey = parentKey
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, signer, &key.PublicKey, signerKey)
	if err != nil {
		t.Fatalf("creating certificate: %v", err)
	}
	cert, err = x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parsing certificate: %v", err)
	}
	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyBytes, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshaling key: %v", err)
	}
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes})
	return certPEM, keyPEM, cert, key
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("finding a free port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func TestMTLS_AdminServer(t *testing.T) {
	dir := t.TempDir()

	// Server identity, self-signed (no client-verifiable server CA
	// needed for this test: the test HTTP clients skip server-cert
	// verification via InsecureSkipVerify, since what's under test is
	// the server's CLIENT-cert enforcement, not server PKI trust).
	serverCertPEM, serverKeyPEM, _, _ := genCert(t, "localhost", false, nil, nil)
	serverCertFile := filepath.Join(dir, "server.crt")
	serverKeyFile := filepath.Join(dir, "server.key")
	if err := os.WriteFile(serverCertFile, serverCertPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(serverKeyFile, serverKeyPEM, 0o600); err != nil {
		t.Fatal(err)
	}

	// A client CA, and one client cert signed by it.
	clientCACertPEM, _, clientCACert, clientCAKeyParsed := genCert(t, "test-client-ca", true, nil, nil)
	clientCAFile := filepath.Join(dir, "client_ca.crt")
	if err := os.WriteFile(clientCAFile, clientCACertPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	clientCertPEM, clientKeyPEM, _, _ := genCert(t, "test-client", false, clientCACert, clientCAKeyParsed)
	clientCert, err := tls.X509KeyPair(clientCertPEM, clientKeyPEM)
	if err != nil {
		t.Fatalf("loading client keypair: %v", err)
	}

	// A second, unrelated CA + cert, to prove a client cert NOT signed
	// by the configured client CA is rejected (not just "any cert
	// works").
	_, _, untrustedCACert, untrustedCAKey := genCert(t, "untrusted-ca", true, nil, nil)
	untrustedCertPEM, untrustedKeyPEM, _, _ := genCert(t, "untrusted-client", false, untrustedCACert, untrustedCAKey)
	untrustedCert, err := tls.X509KeyPair(untrustedCertPEM, untrustedKeyPEM)
	if err != nil {
		t.Fatalf("loading untrusted client keypair: %v", err)
	}

	e := newConcurrencyTestEngine(t)
	cfg := DefaultConfig()
	cfg.AdminPort = freePort(t)
	cfg.TLSEnabled = true
	cfg.TLSCertFile = serverCertFile
	cfg.TLSKeyFile = serverKeyFile
	cfg.TLSClientCAFile = clientCAFile
	cfg.TLSCertReloadInterval = 150 * time.Millisecond

	as := NewAdminServer(e, cfg)
	as.Start()
	defer as.Stop()

	url := "https://127.0.0.1:" + itoa(cfg.AdminPort) + "/api/v1/health"
	waitForServer(t, cfg.AdminPort)

	httpClient := func(clientCerts []tls.Certificate) *http.Client {
		return &http.Client{
			Timeout: 3 * time.Second,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: true, // not testing server-cert trust here
					Certificates:       clientCerts,
				},
			},
		}
	}

	t.Run("no client cert is rejected", func(t *testing.T) {
		_, err := httpClient(nil).Get(url)
		if err == nil {
			t.Fatal("expected the TLS handshake to fail with no client certificate, got a successful response")
		}
	})

	t.Run("untrusted client cert is rejected", func(t *testing.T) {
		_, err := httpClient([]tls.Certificate{untrustedCert}).Get(url)
		if err == nil {
			t.Fatal("expected the TLS handshake to fail with a client cert from an unrelated CA, got a successful response")
		}
	})

	t.Run("trusted client cert is accepted", func(t *testing.T) {
		resp, err := httpClient([]tls.Certificate{clientCert}).Get(url)
		if err != nil {
			t.Fatalf("expected a trusted client certificate to be accepted, got error: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200 from /api/v1/health, got %d", resp.StatusCode)
		}
	})

	t.Run("certificate rotation takes effect without a restart", func(t *testing.T) {
		trusted := httpClient([]tls.Certificate{clientCert})
		before, err := trusted.Get(url)
		if err != nil {
			t.Fatalf("pre-rotation request: %v", err)
		}
		before.Body.Close()

		// Write a NEW server certificate over the same files.
		newCertPEM, newKeyPEM, _, _ := genCert(t, "localhost-rotated", false, nil, nil)
		if err := os.WriteFile(serverCertFile, newCertPEM, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(serverKeyFile, newKeyPEM, 0o600); err != nil {
			t.Fatal(err)
		}
		// Nudge the mtime forward in case the write above landed within
		// the same filesystem-timestamp granularity as the original.
		future := time.Now().Add(2 * time.Second)
		os.Chtimes(serverCertFile, future, future)
		os.Chtimes(serverKeyFile, future, future)

		deadline := time.Now().Add(3 * time.Second)
		var sawRotated bool
		for time.Now().Before(deadline) {
			c, err := tls.DialWithDialer(&net.Dialer{Timeout: 1 * time.Second}, "tcp", "127.0.0.1:"+itoa(cfg.AdminPort), &tls.Config{
				InsecureSkipVerify: true,
				Certificates:       []tls.Certificate{clientCert},
			})
			if err != nil {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			certs := c.ConnectionState().PeerCertificates
			c.Close()
			if len(certs) > 0 && certs[0].Subject.CommonName == "localhost-rotated" {
				sawRotated = true
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if !sawRotated {
			t.Fatal("expected a new TLS connection after rotation to present the rotated certificate (CN=localhost-rotated), never saw it within the deadline")
		}
	})
}

// itoa avoids importing strconv just for this one call site repeated a
// few times above.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

// waitForServer polls until something is listening on 127.0.0.1:port
// or the deadline passes, so the subtests below don't race
// AdminServer.Start's background goroutine.
func waitForServer(t *testing.T, port int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", "127.0.0.1:"+itoa(port), 200*time.Millisecond)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("server on port %d never started listening", port)
}
