# Runbook: TLS Certificate Rotation or Expiry

Covers both TLS surfaces added this session: the admin/query HTTP APIs
(`tls_mtls.go`, `Config.TLSEnabled`/`TLSCertFile`/`TLSKeyFile`/
`TLSClientCAFile`) and inter-node Raft traffic
(`tls_raft.go`, `Config.RaftTLSEnabled`/`RaftTLSCertFile`/
`RaftTLSKeyFile`/`RaftTLSCAFile`).

## The good news: rotation doesn't need a restart

Both surfaces already support hot rotation
(`rotatingCertStore` in `tls_mtls.go`, shared by both): each node
re-checks its cert/key files' mtime every `TLSCertReloadInterval`
(default 10 minutes) and reloads in place if they changed. **New**
TLS handshakes after that point use the new cert; connections already
established keep whatever cert they negotiated until they naturally
reconnect.

## Planned rotation (cert is expiring soon, not yet expired)

1. Generate the new certificate, signed by the same CA the old one
   was (for Raft mTLS, this must be the cluster CA every node trusts
   -- `RaftTLSCAFile`; for admin/query mTLS, the CA in
   `TLSClientCAFile` if you're using client-cert auth there too).
2. Write the new cert+key **over the same file paths**
   `TLSCertFile`/`TLSKeyFile` (or `RaftTLSCertFile`/`RaftTLSKeyFile`)
   already point to -- don't change the path, or the running process
   won't find it.
3. Wait up to `TLSCertReloadInterval` (or trigger however your
   deployment signals a faster recheck, if you've built one -- this
   repo's rotation is purely poll-based, there's no SIGHUP hook as of
   this session).
4. Confirm the new cert is live: connect and check the served
   certificate's expiry/serial, or check logs for the "TLS certificate
   rotated" info-level message (`tls_mtls.go`'s `watch` logs this on
   every successful reload).
5. Do this on **every node** if it's the Raft/cluster CA that changed
   (all nodes need to trust the same CA) -- a rolling rotation across
   nodes with the *same* CA is fine since verification is chain-based,
   not "must exactly match one specific leaf cert".

## Emergency: a certificate already expired, or was compromised

**Expired**: TLS handshakes will start failing immediately for any new
connection once the served cert is past `NotAfter` -- this looks like
a sudden connectivity outage on that surface (admin/query API, or
node-to-node Raft), not a slow degradation. Follow the planned
rotation steps above, but skip waiting for the poll interval if
you've got a way to restart the process faster (a restart also picks
up a new cert immediately, since startup always does a fresh load).

**Compromised** (a private key leaked): this needs more than just
rotating the cert -- if you're using `TLSClientCAFile`/`RaftTLSCAFile`
based on a compromised CA (not just a compromised leaf cert), you need
to reissue a *new CA* and redistribute both new CA and new leaf certs
to every node/client, since revocation-list checking isn't part of
this implementation (`tls_mtls.go`/`tls_raft.go` verify chain-of-trust
only, no CRL/OCSP check). Rotating just the leaf cert under the same
(compromised) CA does not fix a compromised CA.

## Common mistakes to avoid

- **Changing the file path instead of overwriting in place.** The
  rotation watcher only checks the paths from `Config` at startup --
  it won't pick up a cert at a new path without a config change +
  restart.
- **Forgetting `RaftTLSCAFile` needs to match across every node.**
  Rotating one node's Raft identity cert under a *different* CA than
  the rest of the cluster trusts will make that node unable to
  rejoin -- every node verifies peers against its own configured
  `RaftTLSCAFile`.
- **Assuming client-cert (mTLS) rotation on the admin/query API
  requires touching `TLSClientCAFile`.** You only need to rotate
  `TLSClientCAFile` if the *CA* is changing. Individual client
  certs signed by an unchanged CA can rotate independently, on
  whatever schedule the client side manages -- the server doesn't
  need to do anything for that.
