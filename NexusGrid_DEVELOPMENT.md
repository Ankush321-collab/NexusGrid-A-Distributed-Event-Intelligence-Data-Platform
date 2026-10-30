# NexusGrid Development Guide

## Phase-Wise Build, Test, and Evidence Plan

This guide turns NexusGrid from an architecture into a verifiable distributed-systems project. Each phase has a narrow goal, required deliverables, test requirements, and an exit gate. Do not begin the next phase merely because code compiles; advance only when the stated evidence exists.

## Operating Rules

1. Start locally with one instance of each dependency. Distribution comes after correctness.
2. Treat every cross-system interaction as fallible: use retries, idempotency, reconciliation, and observability.
3. Document delivery semantics before optimizing throughput.
4. Do not build sharding, Kubernetes autoscaling, or a large dashboard before the end-to-end ingestion path works.
5. Record assumptions, configuration, data volume, benchmark environment, and results for every performance claim.

---

## Phase 0 — Foundations and Contracts

### Goal

Create a reproducible workspace and establish the contracts every service will share.

### Build

- Set up a TypeScript monorepo or equivalent multi-service repository.
- Configure formatting, linting, type checking, unit testing, and CI.
- Create local dependency infrastructure for PostgreSQL, Kafka, ClickHouse, Redis, and S3-compatible object storage.
- Define environment configuration with validated schemas.
- Publish OpenAPI definitions for ingestion, query, identity, and alert operations.
- Define the canonical event envelope, schema versioning rules, error format, and correlation IDs.
- Write `docs/delivery-guarantees.md` specifying at-least-once delivery, duplicate handling, ordering scope, and replay expectations.

### Tests

- A clean clone can start local dependencies.
- Contract validation rejects an invalid event envelope.
- Every service can emit a correlation ID and structured log.
- CI runs formatting, types, unit tests, and contract tests.

### Exit gate

- Local development environment starts consistently.
- Event and API contracts are versioned in source control.
- Delivery semantics are documented before Kafka code is written.

### Evidence artifacts

- Local startup guide
- OpenAPI specification
- Event JSON schema
- CI result
- Delivery-guarantees document

---

## Phase 1 — Tenant Identity and Transactional Metadata

### Goal

Build secure multi-tenant foundations in PostgreSQL.

### Build

- Implement tenants, users, projects, API keys, event schemas, and audit logs.
- Hash API keys at rest; support creation, rotation, revocation, and project scoping.
- Use migrations and constraints for all metadata tables.
- Add service-layer authorization checks and tenant context propagation.
- Add cursor pagination for list endpoints.
- Add composite indexes based on actual API access patterns.

### Tests

- Authentication success, failure, revoked-key, and wrong-project cases.
- Cross-tenant access attempts must fail.
- Migration up/down or forward-only migration strategy is tested.
- Query plans confirm expected indexes for high-frequency lookups.
- Transactional audit records are written for API-key changes.

### Exit gate

- A tenant can create a project and API key, then authenticate an API request.
- Automated authorization tests prove tenant isolation.
- Metadata queries have captured query plans.

### Evidence artifacts

- ER diagram
- Migration history
- Authorization test report
- Sample `EXPLAIN ANALYZE` outputs

---

## Phase 2 — Ingestion API and Validation

### Goal

Accept valid events safely and reject invalid, oversized, unauthenticated, or over-limit requests predictably.

### Build

- Implement `POST /v1/events` and `POST /v1/events:batch`.
- Authenticate tenant and project context.
- Validate content type, event envelope, schema version, timestamps, body size, batch size, and event count.
- Add request IDs, event IDs, idempotency keys, and stable error codes.
- Use Redis for tenant-scoped rate limits and short-lived idempotency or deduplication keys.
- Return explicit outcomes for accepted, partially accepted, rate-limited, and invalid batches.

### Tests

- Valid single-event and batch ingestion.
- Schema mismatch and unknown schema version.
- Duplicate event and repeated idempotency key.
- Concurrent requests at rate-limit boundary.
- Payload size, event count, and malformed JSON limits.
- Tenant and project mismatch.

### Exit gate

- An authenticated client can submit a valid event and receive a traceable acknowledgement.
- Invalid input never reaches Kafka.
- Rate-limit, dedupe, and validation outcomes are observable in metrics.

### Evidence artifacts

- API examples and error catalogue
- Integration-test report
- Rate-limit metrics dashboard

---

## Phase 3 — Kafka Publish, Consumer, and Failure Handling

### Goal

Create a durable asynchronous event path with explicit retry and replay behaviour.

### Build

- Define main event topic, retry topic strategy, and dead-letter topic.
- Choose a partition key, initially tenant and project scoped, and document ordering guarantees.
- Publish accepted events to Kafka with headers for schema version, trace ID, retry count, and event ID.
- Implement stream workers as a consumer group.
- Classify errors as retriable, non-retriable, or poison-message failures.
- Send exhausted failures to a DLQ with diagnostic context.
- Provide a controlled replay tool.
- Implement an outbox publisher for PostgreSQL-originated integration events where transactional publication is required.

### Tests

- Worker restart during a batch.
- Duplicate Kafka delivery.
- Retriable ClickHouse or network failure.
- Invalid event routes to DLQ.
- DLQ event is inspected, corrected if appropriate, and replayed.
- Consumer lag increases under load and decreases after capacity is restored.

### Exit gate

- The team can demonstrate accepted events reaching a worker, a failed event reaching the DLQ, and an operator replaying it.
- Duplicate events do not create incorrect analytical counts within the defined idempotency strategy.
- Consumer lag is visible in monitoring.

### Evidence artifacts

- Topic and partition design
- Retry and DLQ runbook
- Replay demonstration record
- Consumer-lag dashboard

---

## Phase 4 — ClickHouse Analytical Storage

### Goal

Persist processable events for efficient tenant-scoped time-series analytics.

### Build

- Define ClickHouse event schema with tenant, project, event time, received time, event name, identifiers, and typed properties.
- Select partitioning and ordering keys based on defined query patterns.
- Write workers that batch inserts and preserve event identifiers for idempotency or reconciliation.
- Add retention TTL policy.
- Archive accepted raw events to object storage where required.
- Build at least one materialized view only after measuring a recurring aggregation query.

### Tests

- Batch writes from worker to ClickHouse.
- Repeated message processing does not inflate results beyond documented duplicate semantics.
- Query correctness across time windows and tenants.
- Retention behavior on test data.
- Worker recovery after ClickHouse temporary unavailability.

### Exit gate

- A complete event reaches ClickHouse from the public ingestion API.
- Data is isolated by tenant in query results.
- At least one measured query profile and one documented table design decision exist.

### Evidence artifacts

- ClickHouse DDL
- Insert throughput report
- Query profile output
- Raw-archive verification

---

## Phase 5 — Query API and Analytics

### Goal

Expose safe, low-latency analytics queries to applications and dashboards.

### Build

- Implement time-range, tenant-scoped aggregate query endpoints.
- Support filters, grouping, percentile metrics, and cursor pagination where suitable.
- Define an allowlist of aggregation and filter operations; do not accept unrestricted client SQL.
- Implement query limits, timeout, scan-cost protections, and maximum cardinality safeguards.
- Cache safe repeated results in Redis with explicit key structure and TTL.
- Add cache invalidation strategy or acceptance of controlled staleness.

### Tests

- Tenant isolation under all query parameters.
- Time-range boundaries and timezone handling.
- Pagination stability.
- Query timeout and overly expensive query rejection.
- Cache hit, miss, expiration, and stampede protection.
- Percentile and aggregate correctness against reference data.

### Exit gate

- Dashboard-style requests return correct tenant-scoped data.
- Expensive or unsafe queries fail predictably without harming the database.
- Cache behavior is measured rather than assumed.

### Evidence artifacts

- Query API specification
- Query latency report
- Cache-hit dashboard
- Expensive-query protection test

---

## Phase 6 — Alerting and Scheduled Analytics

### Goal

Create reliable alerts from analytical data without noisy or duplicate notifications.

### Build

- Create alert rules for thresholds such as error rate, latency percentile, and event volume.
- Implement alert evaluation scheduler.
- Store alert state, deduplication keys, trigger history, and notification attempts in PostgreSQL.
- Support cooldown windows and recovery notifications.
- Produce scheduled tenant summaries from predefined analytics queries.

### Tests

- Threshold crossing and recovery.
- Alert suppression during cooldown.
- Duplicate evaluation does not create duplicate notifications.
- Missing data or delayed events are handled explicitly.
- Tenant boundary on alert query and notification destination.

### Exit gate

- One error-rate alert can be triggered, deduplicated, recovered, and audited.
- Scheduled summaries operate on a tenant-isolated query path.

### Evidence artifacts

- Alert state-machine diagram
- Alert integration tests
- Trigger and recovery demonstration

---

## Phase 7 — Reliability and Reconciliation

### Goal

Prove recovery behavior when distributed dependencies fail or disagree.

### Build

- Create reconciliation jobs comparing Kafka offsets, worker outcomes, ClickHouse inserts, and raw archive records.
- Implement idempotent retry logic with bounded backoff.
- Write service shutdown handling so in-flight work is committed, retried, or safely released.
- Create backup and restore scripts for PostgreSQL metadata and ClickHouse analytical data.
- Define RPO and RTO targets appropriate to the local or cloud environment.

### Tests

- Kill worker during batch processing.
- Restart Kafka consumer after offset edge case.
- Simulate ClickHouse outage during writes.
- Simulate Redis unavailability and confirm safe degraded behavior.
- Restore PostgreSQL and ClickHouse test backups.
- Reconcile intentionally injected missing or duplicated events.

### Exit gate

- Failures are detectable, recoverable, and documented.
- Backup restoration is demonstrated rather than described.
- Reconciliation produces a bounded, explainable discrepancy report.

### Evidence artifacts

- Failure-test report
- Recovery runbooks
- Backup/restore record
- Reconciliation report

---

## Phase 8 — Containers and Kubernetes

### Goal

Deploy the application services as secure, observable Kubernetes workloads.

### Build

- Create minimal Docker images with non-root runtime users.
- Deploy API, ingestion, worker, query, and alert services.
- Separate application and infrastructure namespaces.
- Configure ConfigMaps, Secrets, readiness probes, liveness probes, resource requests, and resource limits.
- Add PodDisruptionBudgets and graceful shutdown handling where useful.
- Add Horizontal Pod Autoscalers only after selecting a measurable scaling signal.
- Add network policies and restricted service accounts.
- Package deployments with Helm.

### Tests

- New deployment becomes ready only after dependency connectivity and initialization checks pass.
- Rolling update preserves availability within the defined limit.
- Pod termination does not lose acknowledged work.
- HPA responds to the selected measured signal.
- Network policy blocks disallowed service paths.

### Exit gate

- Core services run in Kubernetes with probes, limits, logs, and metrics.
- Deployment, restart, and rolling-update behaviour are demonstrated.
- Kubernetes configuration is reproducible from source control.

### Evidence artifacts

- Helm chart
- Kubernetes manifest inventory
- Rolling-update evidence
- Resource and autoscaling charts

---

## Phase 9 — Edge Networking and Cloud Infrastructure

### Goal

Expose the platform securely through managed infrastructure and reproducible IaC.

### Build

- Provision cloud networking, managed Kubernetes, DNS records, ingress, certificates, and load balancer configuration using Terraform.
- Configure hostnames for API, ingestion, and dashboard paths.
- Enable HTTPS and automated certificate renewal.
- Keep Kafka, PostgreSQL, Redis, and ClickHouse managed or outside Kubernetes initially when that reduces operational risk.
- Store all environment-specific configuration outside application source.

### Tests

- HTTPS requests reach the intended ingress route.
- Invalid host, path, and certificate cases fail safely.
- Certificate renewal path is documented and verified in a non-production environment.
- Terraform plan and apply are reviewed and repeatable.

### Exit gate

- A protected public endpoint serves a tested deployment.
- Infrastructure can be recreated from Terraform with documented manual prerequisites.

### Evidence artifacts

- Terraform modules
- DNS and ingress diagram
- HTTPS verification record
- Infrastructure deployment guide

---

## Phase 10 — Observability, Load, and Scale Benchmarks

### Goal

Measure the real limits of NexusGrid and explain bottlenecks.

### Build

- Instrument service metrics for request rate, errors, p50/p95/p99 latency, Kafka lag, retries, DLQ volume, cache hit rate, database pool saturation, ClickHouse query time, and worker batch size.
- Add OpenTelemetry traces spanning API intake, Kafka publication, consumer processing, ClickHouse insert, and query response.
- Build Grafana dashboards for platform and tenant-level views.
- Write k6 scenarios for steady load, burst load, batch ingestion, rate-limit behavior, and query traffic.
- Define one controlled benchmark environment at a time.

### Tests

- Begin at 1,000 events per second or a lower environment-appropriate target.
- Run steady-state and burst tests.
- Identify bottleneck with metrics before changing partition count, worker count, or database configuration.
- Run a hot-tenant test and report impact on other tenants.
- Compare ClickHouse query performance before and after a materialized view, cache, or ordering-key change.

### Exit gate

- Benchmark reports include workload, dataset size, host resources, configuration, duration, throughput, latency percentiles, error rate, and bottleneck analysis.
- Scaling conclusions are grounded in measurements.

### Evidence artifacts

- k6 scripts and HTML/JSON reports
- Grafana dashboard exports
- Trace screenshots or sampled trace data
- Benchmark methodology document

---

## Phase 11 — Database Scaling Experiments

### Goal

Demonstrate controlled data-system scaling rather than adding complexity without evidence.

### Build

- Begin PostgreSQL with a single primary database.
- Design a tenant shard-map model and routing interface.
- Prototype shard routing with a limited test dataset.
- Simulate a hot tenant and capture effects on latency and throughput.
- Demonstrate a controlled tenant migration or resharding workflow.
- Analyze ClickHouse partition, sorting-key, and materialized-view choices under benchmark load.
- Evaluate Redis hot-key and cache-stampede mitigation strategies.

### Tests

- Routing chooses the correct shard for every tenant.
- Cross-shard operations fail or use an explicitly designed aggregation path.
- Tenant migration preserves metadata and query correctness.
- Hot-shard condition is detected and recorded.
- Cache-stampede test does not overload ClickHouse.

### Exit gate

- The project shows a working shard map and a controlled migration demonstration.
- Sharding limitations and trade-offs are documented honestly.

### Evidence artifacts

- Sharding design document
- Tenant-migration test
- Hot-shard benchmark
- Query optimization comparison

---

## Phase 12 — Final Portfolio Release

### Goal

Package NexusGrid as an engineering project that another developer can run, inspect, test, and evaluate.

### Required deliverables

- Accurate top-level README
- Architecture and sequence diagrams
- Data contracts and delivery semantics
- Local setup and Kubernetes deployment guides
- API documentation and SDK example
- Benchmark methodology and measured results
- Failure, recovery, and incident runbooks
- Security and tenant-isolation test results
- Backup and restore procedure
- Known limitations and next scaling steps

### Final review checklist

- No capability is described as complete without code and evidence.
- No throughput or availability claim lacks a benchmark or test.
- Delivery semantics are stated without exaggeration.
- The data-store responsibilities remain distinct.
- Tenant isolation is tested automatically.
- All runbooks are executable by someone other than the original author.

---

## Suggested 24-Week Sequence

| Weeks | Focus | Phase outcome |
|---:|---|---|
| 1–2 | Foundations, contracts, local dependencies | Phase 0 complete |
| 3–4 | Tenant identity and metadata | Phase 1 complete |
| 5–6 | Ingestion API and controls | Phase 2 complete |
| 7–8 | Kafka, consumers, retries, DLQ | Phase 3 complete |
| 9–10 | ClickHouse writes and analytics | Phase 4 complete |
| 11–12 | Query API and basic dashboard | Phase 5 complete |
| 13–14 | Alerts and reliability | Phases 6–7 complete |
| 15–17 | Containers and Kubernetes | Phase 8 complete |
| 18–19 | Cloud networking and Terraform | Phase 9 complete |
| 20–21 | Observability and benchmarks | Phase 10 complete |
| 22–23 | Sharding and database experiments | Phase 11 complete |
| 24 | Documentation, demo, and portfolio release | Phase 12 complete |

This timeline is a planning reference, not proof of completion. Mark a phase complete only when its exit gate and evidence artifacts are met.
