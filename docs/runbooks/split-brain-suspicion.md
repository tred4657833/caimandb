# Runbook: Split-Brain Suspicion / More Than One Node Thinks It's Leader

Raft is designed so at most one node can be leader for a given term,
but a *stale* former leader can briefly still believe it's leader
during a network partition, until its lease/heartbeat times out. This
runbook is for when that suspicion goes beyond "briefly, during a
normal election" into "writes look inconsistent across nodes" or
"more than one node has believed it's leader for more than a few
seconds".

## Step 1: confirm it's real, not a monitoring artifact

Check `caimandb_cluster_status{role="leader"}` across every node
(this metric was added in a recent session -- see the CHANGELOG; if
your deployment predates it, you won't have it and should upgrade
before relying on this runbook). More than one node reporting
`leader=1` for more than a few seconds is the real signal -- a brief
overlap during an election is normal and expected.

Also check `caimandb_raft_replication_lag` on every node. A genuinely
partitioned former-leader will show its own lag climbing (it stopped
hearing from anyone) even while it still thinks it's leader.

## Step 2: this codebase already defends against split-brain at the write layer -- confirm that defense is actually working

This is not a from-scratch design question: real network-partition
resilience against exactly this scenario is already 
`network_partition_test.go`, `network_partition_asymmetric_test.go`,
and `network_partition_scenario_test.go`, and mTLS on the Raft
transport (`tls_raft.go`, if `RaftTLSEnabled`) additionally requires
every inter-node connection to present a certificate signed by the
cluster CA, which closes off a rogue/unauthorized process pretending
to be a cluster member. If you're seeing real split-brain symptoms
despite this, that's either:

- A genuine bug (worth reporting with `CLUSTER STATUS` output from
  every node, timestamps, and `caimandb_raft_replication_lag`
  history around the incident), or
- A **client-side** routing problem: something in front of the
  cluster (a load balancer, a client library) is sending writes to a
  node that isn't actually the current leader, and that node is
  *rejecting* them (`ErrNotRaftLeader` -- see `cluster.go`) but
  something downstream is silently retrying against the wrong node
  repeatedly, or misreporting the error as success. Check your
  client/proxy layer's handling of `ErrNotRaftLeader` before assuming
  the database itself is inconsistent.

## Step 3: while investigating, don't write around the problem

Resist the urge to manually route writes to "whichever node answers"
during this investigation -- if there really are two nodes both
accepting writes as leader (which shouldn't be possible given Raft's
term/quorum guarantees, but is exactly what you're checking for),
adding more uncoordinated writes makes reconciliation harder, not
easier.

## Step 4: if you confirm a real split-brain (not just a routing bug)

This would be a serious bug in this codebase's Raft integration, not
an expected operational scenario -- Raft's whole design point is that
this shouldn't be reachable under normal partition conditions the
existing chaos tests cover. Recovery:

1. Identify the true current leader by term number (`CLUSTER STATUS`
   reports the Raft term; the higher term wins) -- don't just pick the
   node that answers first.
2. Isolate (stop, or network-block) every node reporting itself as
   leader with a lower term.
3. Once only the true leader remains reachable, let followers rejoin
   one at a time, watching `caimandb_raft_replication_lag` for each to
   confirm it's actually catching up cleanly rather than re-diverging.
4. Any writes accepted by the stale/false leader during the split need
   manual reconciliation against the true leader's data -- there's no
   automatic merge for this. Diff the affected blocks by hand.

## After resolution

File this as a bug against this project regardless of root cause --
even a client-routing-layer cause is worth documenting so the next
person recognizes the symptom faster.
