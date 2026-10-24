package caimandb

// tls_raft.go implements a raft.StreamLayer (the interface
// hashicorp/raft uses for its actual node-to-node network I/O) that
// wraps every connection -- both accepted (server side) and dialed
// (client side, since every Raft node dials every other node) -- in
// mutual TLS. It's used in place of the plain raft.NewTCPTransport
// call in cluster.go's initRaftWithCustomLogStore when
// Config.RaftTLSEnabled is set.
//
// This reuses rotatingCertStore from tls_mtls.go for the same hot
// cert-rotation behavior the admin/query HTTP servers already have --
// a node's Raft identity certificate can be rotated without a restart
// (and therefore without a Raft leadership disruption).
//
// Hostname verification is intentionally skipped: Raft addresses are
// bind/advertise IP:port pairs, which won't generally match a
// certificate's DNS SAN the way an HTTPS server's hostname would.
// Trust is instead established purely by chain verification against
// RaftTLSCAFile via VerifyPeerCertificate -- every node's cert must
// still be signed by the configured cluster CA, or the handshake is
// rejected; only the "does the name match" check is dropped.
import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/hashicorp/raft"
)

// verifyPeerChain builds tls.Config.VerifyPeerCertificate: it
// reconstructs and verifies the certificate chain presented by the
// peer against caPool, ignoring hostname/SAN matching (see package doc
// above). Returning a non-nil error fails the handshake.
func verifyPeerChain(caPool *x509.CertPool) func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
	return func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
		if len(rawCerts) == 0 {
			return fmt.Errorf("raft mTLS: peer presented no certificate")
		}
		leaf, err := x509.ParseCertificate(rawCerts[0])
		if err != nil {
			return fmt.Errorf("raft mTLS: parsing peer certificate: %w", err)
		}
		intermediates := x509.NewCertPool()
		for _, raw := range rawCerts[1:] {
			cert, err := x509.ParseCertificate(raw)
			if err != nil {
				continue
			}
			intermediates.AddCert(cert)
		}
		_, err = leaf.Verify(x509.VerifyOptions{
			Roots:         caPool,
			Intermediates: intermediates,
			KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageAny},
		})
		if err != nil {
			return fmt.Errorf("raft mTLS: peer certificate not signed by the configured cluster CA: %w", err)
		}
		return nil
	}
}

// raftTLSConfig builds the shared server+client tls.Config for the
// Raft mTLS stream layer: certificate rotation via rotatingCertStore,
// and mutual trust against a single cluster CA (RaftTLSCAFile) used
// both as ClientCAs (accepting inbound peers) and as the chain root
// VerifyPeerCertificate checks against (dialing outbound peers).
// InsecureSkipVerify+VerifyPeerCertificate together mean "skip Go's
// built-in hostname check, but still fail closed if
// VerifyPeerCertificate doesn't explicitly approve the chain" -- this
// is not the same as disabling verification.
func raftTLSConfig(cfg *Config, stop <-chan struct{}) (*tls.Config, error) {
	store, err := newRotatingCertStore(cfg.RaftTLSCertFile, cfg.RaftTLSKeyFile)
	if err != nil {
		return nil, fmt.Errorf("raft mTLS cert: %w", err)
	}
	safeGo("raft_tls_cert_rotation", func() {
		store.watch(cfg.TLSCertReloadInterval, stop)
	})

	caBytes, err := os.ReadFile(cfg.RaftTLSCAFile)
	if err != nil {
		return nil, fmt.Errorf("reading raft TLS CA file: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caBytes) {
		return nil, fmt.Errorf("raft TLS CA file %s contains no valid PEM certificates", cfg.RaftTLSCAFile)
	}

	return &tls.Config{
		MinVersion:     tls.VersionTLS12,
		GetCertificate: store.getCertificate,
		GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			return store.getCertificate(nil)
		},
		ClientCAs:             pool,
		ClientAuth:            tls.RequireAndVerifyClientCert,
		RootCAs:               pool,
		InsecureSkipVerify:    true, // hostname check skipped; chain check enforced below
		VerifyPeerCertificate: verifyPeerChain(pool),
	}, nil
}

// tlsStreamLayer implements raft.StreamLayer over TLS: Accept/Close/Addr
// delegate to a tls.Listener, and Dial wraps the outbound TCP
// connection with tls.Client using the same shared tls.Config.
type tlsStreamLayer struct {
	ln        net.Listener
	tlsConfig *tls.Config
}

func newTLSStreamLayer(bindAddr string, tlsConfig *tls.Config) (*tlsStreamLayer, error) {
	ln, err := tls.Listen("tcp", bindAddr, tlsConfig)
	if err != nil {
		return nil, fmt.Errorf("raft mTLS listen on %s: %w", bindAddr, err)
	}
	return &tlsStreamLayer{ln: ln, tlsConfig: tlsConfig}, nil
}

func (t *tlsStreamLayer) Accept() (net.Conn, error) { return t.ln.Accept() }
func (t *tlsStreamLayer) Close() error              { return t.ln.Close() }
func (t *tlsStreamLayer) Addr() net.Addr            { return t.ln.Addr() }

func (t *tlsStreamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: timeout}
	return tls.DialWithDialer(dialer, "tcp", string(address), t.tlsConfig)
}
