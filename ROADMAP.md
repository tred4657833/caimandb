# CaimanDB Roadmap

This roadmap describes the direction of the project rather than a fixed release schedule.

## 1. Reliability

- Expand crash and stress scenarios.
- Strengthen WAL recovery coverage.
- Exercise transaction recovery under process termination.
- Improve cluster failover testing.
- Add regression tests for data-integrity failures.

## 2. NQL

- Keep the core syntax small and readable.
- Complete consistency between parser, executor and documentation.
- Expand filtering, grouping, pagination and distinct operations.
- Improve error messages and command help.
- Keep Spanish, English and German syntax references synchronized.

## 3. Engine architecture

- Separate storage, transaction, query, cluster and administration responsibilities.
- Reduce coupling around the central engine.
- Improve package boundaries and public APIs.

## 4. Backup and recovery

- Improve backup and restore workflows.
- Expand snapshots and point-in-time recovery.
- Document operational recovery procedures.

## 5. Security

- Continue TLS and mTLS support.
- Improve RBAC and ACL capabilities.
- Add certificate rotation workflows.
- Review authentication and authorization paths.

## 6. Observability

- Expand metrics and health endpoints.
- Track latency percentiles.
- Expose WAL and Raft health information.
- Improve structured operational logs.

## 7. Performance

Performance work will follow correctness and resilience improvements.

Areas of interest include:

- Batch processing.
- Query execution.
- Index performance.
- Shard distribution.
- WAL throughput.
- Memory and CPU efficiency.

## How to contribute to the roadmap

Open an issue with a concrete proposal, reproduction, benchmark, design document or implementation. Large changes should be discussed before implementation so contributors and maintainers can agree on the direction.
