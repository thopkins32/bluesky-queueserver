# RFC 0001: Bluesky Experiment Controller Product Boundary

| Field | Value |
|---|---|
| Status | Draft |
| Created | 2026-09-03 |
| Decision scope | Product boundary and foundational architecture |
| Target product | Working name: Bluesky Experiment Controller |
| Related planning document | [Experiment Controller Redesign Proposal](EXPERIMENT_CONTROLLER_REDESIGN.md) |
| Reference implementation | [`bluesky_queueserver._experiment_controller`](src/bluesky_queueserver/_experiment_controller/) |

## Abstract

This RFC proposes replacing QueueServer's general remote-execution model with an explicitly incompatible, single-controller experiment-execution product. The new product will use one monorepo and one product version, a declared and versioned operation catalog instead of remote Python namespace access, a private worker-process boundary, and durable transactional state with an auditable control lease.

The existing QueueServer remains available to current users and receives maintenance appropriate to that product. The new controller does not inherit QueueServer's public ZMQ protocol, interactive kernel access, arbitrary script or function execution, Redis queue model, profile-loading semantics, or CLI compatibility.

This parent RFC fixes the product boundary and core invariants. It does not standardize the final HTTP schema, authentication provider, worker SDK, hardware operation catalog, or detailed pause/stop protocol. Those require follow-up RFCs informed by an actual instrument workflow.

## Terminology

The key words **MUST**, **MUST NOT**, **SHOULD**, **SHOULD NOT**, and **MAY** describe requirements on the proposed product.

- **Controller**: the sole owner of durable queue state, authorization decisions, scheduling, worker lifecycle, and controller events for one instrument deployment.
- **Worker**: a trusted process running a declared instrument environment and a reviewed operation catalog.
- **Operation**: a versioned, typed action submitted to the controller. It is not an arbitrary Python function call.
- **Controller event**: a durable lifecycle or audit record such as `operation.started`. It is not a Bluesky Event document.
- **Bluesky document**: a RunEngine document such as `start`, `descriptor`, `event`, or `stop`.
- **Control lease**: persisted, time-bounded authority held by one authenticated principal for queue mutation and execution control.
- **Worker revision**: the immutable deployment or artifact revision reported by a worker when it starts.

## Context

QueueServer combines useful execution and isolation behavior with compatibility surfaces that are expensive to secure and evolve together. Those surfaces include public direct ZMQ, remote interactive kernels, arbitrary scripts and functions, runtime namespace reflection, multiple independently versioned API packages, and Redis-backed mutable queue records.

Those capabilities are not requirements for the proposed product. Preserving them would retain the current coupling while preventing a smaller and more explicit security boundary.

The desired service instead needs to answer a narrow set of operational questions:

1. Which reviewed operation may run?
2. Who requested and authorized it?
3. Which immutable parameters and worker revision were used?
4. What is the operation's durable lifecycle state?
5. Is dispatch safe after a worker or controller failure?
6. Which controller events and Bluesky runs are associated with the operation?
7. What happened after a restart?

## Decision

### 1. Create an incompatible product boundary

The project MUST be a new product rather than a compatibility mode or public-v2 surface inside QueueServer.

The production implementation SHOULD move to a new repository under a working name such as `bluesky-experiment-controller`. Current QueueServer and the new controller MUST NOT share a queue store, worker process, live instrument authority, or public protocol. Migration is deployment-level replacement, not an in-process compatibility bridge.

The internal prototype in this repository is an intentional contract probe. Its location does not reverse the new-product decision, establish a public `bluesky_queueserver` API, or imply that the production controller should import the legacy manager.

### 2. Use one monorepo and one active controller

The initial product MUST use one repository and one product version for the protocol, controller, worker SDK, web boundary, storage, tests, and deployment examples. Protocol and implementation changes can then be reviewed and released atomically.

Each instrument deployment MUST have exactly one active controller process. The first release will not support active-active controllers, horizontal web workers that share mutable controller state, or distributed scheduling. A later HA design requires a separate RFC and a storage/leadership model that preserves the invariants in this document.

The worker remains a separate process; "one controller" does not mean executing Bluesky inside the controller process.

The product MUST rely on the deployment's standard process supervisor rather than reproduce QueueServer's custom Manager Watchdog as a first-release subsystem. A controller-only death while its same-host worker and supervisor remain healthy is possible, but it is a narrow process-local failure rather than the primary availability model. Deterministic controller crashes, deadlocks, and dependency failures must be addressed through release testing and fixes; automatic restart is containment for residual faults, not a substitute for correctness.

### 3. Replace namespace access with a versioned operation catalog

A worker deployment MUST publish an explicit catalog of operation descriptors. Each descriptor contains at least:

- a stable `operation_id`;
- an explicit `operation_version`;
- a closed input schema;
- deployment metadata sufficient to identify the worker revision.

The pair `(operation_id, operation_version)` MUST be unique within a worker catalog. A submission MUST identify that pair and provide a JSON object containing only schema-declared parameters.

The controller MUST validate the operation identity, version, JSON representation, and input schema before creating an operation record or contacting the worker. The worker MUST validate the same operation descriptor at its trust boundary before interacting with Bluesky or Ophyd.

The public API MUST NOT accept Python expressions, arbitrary function names, uploaded scripts, object traversal paths, or runtime namespace references. Logical device identifiers, where needed, are resolved by reviewed worker code into permitted device objects.

### 4. Keep the worker protocol private and the environment declared

The controller MUST launch a configured worker command without a shell. The worker command selects a deployed, reviewed environment; the controller MUST NOT install packages, update Git repositories, invoke environment managers, or run arbitrary deployment hooks through its public API.

At startup, the worker MUST report:

- its worker protocol version;
- its worker revision;
- its operation catalog.

The controller MUST reject unsupported protocol versions, malformed responses, and duplicate operation identities before accepting submissions. Request correlation identifiers MUST be preserved across the private boundary.

The worker protocol is private implementation detail. It MUST NOT be exposed as a public network API. The exact newline-delimited JSON protocol used by the prototype is evidence that the process boundary works, not a commitment that the production transport can never change.

The production controller SHOULD remain import-independent of Bluesky, Ophyd, and instrument startup packages. Those dependencies belong in the declared worker environment.

### 5. Make controller state transactional and durable

The initial storage backend MUST be SQLite in WAL mode on a local persistent filesystem. SQLite over NFS is unsupported. PostgreSQL or distributed coordination is deferred until a demonstrated HA or multi-controller requirement exists.

Every queue or dispatch mutation MUST atomically persist:

- the domain-state change;
- the incremented queue revision;
- the acting principal;
- an append-only controller event.

Clients that mutate queue or dispatch state MUST provide the queue revision they observed. A stale revision MUST fail with both the expected and current revisions and MUST NOT partially mutate state.

An operation receives one stable `operation_uid` when submitted. That identifier MUST remain unchanged across every lifecycle transition, including failure and an unknown outcome. Parameters, operation ID and version, submitting principal, and worker revision MUST remain attributable to that identity.

Controller event IDs MUST be durable, monotonically increasing cursors. A future SSE endpoint will resume after a supplied event ID rather than relying on a transient ZMQ or WebSocket stream.

Controller events and Bluesky documents are separate data planes. Controller events describe authorization and operation lifecycle. Bluesky documents remain the RunEngine's data stream. The controller MUST preserve returned run UIDs so downstream systems can correlate an operation with its Bluesky runs; this RFC does not make the controller a data catalog.

### 6. Bind mutation authority to an authenticated control lease

The production public boundary MUST authenticate callers. It converts the authenticated principal into the subject used by the controller; clients MUST NOT be permitted to assert an arbitrary subject.

One persisted control lease grants time-bounded authority for queue mutation and execution control. Submission, dispatch, recovery acknowledgement, and future control actions MUST require both the matching principal and active lease identifier.

Lease acquisition, renewal, expiry, handoff, revocation, and privileged override MUST be auditable. An expired lease prevents new mutations but MUST NOT rewrite ownership or the outcome of work that is already running. Detailed handoff, revocation, and administrative override semantics require a follow-up RFC before production use.

### 7. Treat process loss and ambiguous execution conservatively

The target lifecycle is explicit and durable:

```text
submitted
  -> queued
  -> claimed
  -> running
  -> succeeded | failed | cancelled | aborted | interrupted | unknown
```

The first implementation MAY combine adjacent internal states while preserving their externally meaningful transitions. It MUST NOT report a running operation as completed merely because the controller or worker restarted.

Controller-process restart is not a normal execution path and does not promise uninterrupted queue processing. If the controller connection disappears while an operation is claimed or running, the worker MUST accept no new work. It MUST follow an orphan policy defined by the reviewed operation/runtime contract: finish the current operation, pause at a safe checkpoint, or request a safe stop. The parent RFC does not select one universal action because abruptly terminating a worker is not necessarily a safe hardware stop.

The deployment supervisor MAY restart the controller. The restarted controller MUST load durable state and reconcile the operation UID, worker revision, and the worker session or execution-attempt identity defined by the lifecycle RFC. A proven match MAY allow the controller to resume observing the current operation. It MUST NOT silently authorize dispatch of the next queue item. The reconciliation decision and resulting state MUST be durably recorded before dispatch can resume.

If the worker is absent, identities do not match, or the outcome cannot be proven, the operation MUST become `unknown` or `interrupted`, automatic dispatch MUST stop, and recovery MUST require an authenticated operator acknowledgement. Worker transport loss after claim is always such an ambiguous outcome unless a later reconciliation supplies authoritative evidence.

An explicit retry is new operator intent. It MUST have an auditable identity and MUST NOT overwrite the unknown operation. The detailed relationship between an operation, execution attempts, worker sessions, and retries is deferred to the lifecycle/recovery RFC.

The prototype also blocks dispatch after an ordinary operation failure. A follow-up lifecycle RFC may make that policy dependent on a reviewed operation's failure classification, but ambiguous process or transport loss MUST always block.

### 8. Expose one typed public API

The production controller will expose:

- HTTPS JSON commands and queries;
- typed request and response models generated from one contract;
- Server-Sent Events for durable controller events with cursor-based resumption;
- health and readiness endpoints that distinguish controller, storage, and worker state.

The first release has no public direct ZMQ, raw worker protocol, interactive kernel, or WebSocket requirement. Authentication and authorization integration are required before hardware use but are specified separately from this parent decision.

## Normative invariants

Every conforming implementation MUST preserve these invariants:

1. One instrument has at most one active controller authority.
2. Public callers can request only catalog-declared, schema-valid operations.
3. The controller never evaluates caller-supplied Python or traverses a worker namespace.
4. Every operation has one stable UID and records its submitted parameters, actor, operation version, and worker revision.
5. Queue and dispatch mutations use optimistic revision checks.
6. A queue/dispatch mutation, its new revision, and its controller event commit in one transaction.
7. Controller events have durable cursor IDs and are distinct from Bluesky documents.
8. Loss of the controller connection prevents the worker from accepting new work.
9. Controller restart never implicitly authorizes dispatch of the next item; reconciliation is recorded first.
10. Worker transport loss after claim produces an unknown outcome unless authoritative reconciliation proves otherwise.
11. Unknown work is never automatically retried, requeued, or declared successful.
12. Recovery that restores dispatch is explicit, authenticated, and audited.
13. The controller and worker do not bypass device, IOC, or facility safety interlocks.
14. QueueServer and the new product never share live control authority during migration.

## Prototype evidence

The internal prototype establishes that the smallest control path is feasible without Redis, public ZMQ, IPython, or a web stack.

| Decision | Prototype evidence | Remaining production work |
|---|---|---|
| Typed contract | Frozen dataclasses, explicit states and errors, Draft 7 input validation | Select the production schema/model generation strategy |
| Explicit catalog | One `simulated-count` operation, version `1`, with a closed parameter schema | Define one real instrument workflow and worker SDK |
| Durable state | Versioned SQLite schema, WAL mode, queue revisions, stable operation records, append-only events | Migration tooling, backup guidance, production operational limits |
| Control lease | Persisted lease checked on submission, dispatch, and recovery | Authenticated principal integration, handoff, revocation, override |
| Worker isolation | Separate interpreter process with strict request/response correlation and reported revision | Independently built worker artifact and deployment supervision |
| Safe ambiguity | Transport loss records `unknown`, blocks dispatch, and requires acknowledgement | Worker reconciliation protocol and operator UX |
| Bluesky correlation | Simulated RunEngine execution returns start-document run UIDs | Data-document routing and data-catalog integration boundary |
| Restart evidence | Reopening the file-backed database returns the same terminal record and ordered events | Standard process supervision, worker orphan policy, exact worker-session reconciliation, and a complete crash matrix |

The reference implementation is intentionally narrower than this RFC. It uses a trusted subject string, runs the worker with the controller's interpreter, has no HTTP or SSE server, exposes one simulator operation, and supports no hardware.

## Non-goals

The first product release will not provide:

- QueueServer protocol, queue, history, CLI, or SDK compatibility;
- public ZMQ or raw worker access;
- IPython worker mode or Jupyter console attachment;
- dynamic plan or device discovery;
- arbitrary function execution or uploaded scripts;
- self-updating startup repositories;
- direct PV mutation or monitoring outside declared operations;
- a Bluesky document store or data catalog;
- active-active controllers or SQLite over NFS;
- a replacement for EPICS, device, PLC, or facility safety interlocks.

An offline archival export tool MAY be designed later. It MUST NOT become a runtime compatibility bridge or a second live control path.

## Alternatives considered

### Evolve QueueServer in place

Rejected. Compatibility would retain the dynamic execution, transport, package-version, and storage boundaries this product is intended to remove. It would also make the security posture ambiguous to existing clients.

### Keep a generic plan/function execution API

Rejected. An allowlisted but generic Python namespace remains difficult to review, version, document, and authorize at operation granularity. Explicit operations provide a smaller contract and make deployment revision part of the record.

### Retain Redis as the primary domain store

Rejected for the initial single-controller product. SQLite provides transactions, revisions, event cursors, and simpler local operation in one persistence boundary. Redis may still be used by unrelated facility services; it is not the controller's source of truth.

### Let the controller manage worker environments

Rejected. Package installation and source updates are deployment responsibilities. Exposing them through the controller would recreate arbitrary remote execution with deployment credentials.

### Add HA and multiple active controllers immediately

Rejected. Consensus, leadership, fencing, and distributed storage substantially expand the safety and operational model. One active controller is sufficient to validate the product and real workflows.

### Preserve QueueServer's custom Watchdog and transparent continuation

Rejected as a first-release requirement. The legacy topology can replace only the Manager while retaining its sibling Worker, but this mainly protects against a localized Manager defect or a signal directed at that process. The processes still share a host and usually a service or resource boundary, so this is not general host or deployment resilience. Known deterministic defects should be removed through testing, while residual failures should be contained by standard supervision and fail-closed reconciliation. Heuristically reconstructing state and continuing the queue hides ambiguity rather than resolving it.

### Add HTTP, authentication, and database frameworks to the prototype

Rejected for the contract probe. The prototype first had to prove operation identity, durable transitions, worker isolation, and failure semantics. Production HTTP and identity integration remain required, but they should be built after this parent decision is reviewed.

## Consequences

### Benefits

- A smaller public attack surface and explicit trust boundary.
- Reproducible operation and worker version attribution.
- Durable concurrency and recovery semantics instead of inferred process state.
- One source of truth for controller contracts and releases.
- A migration path that does not destabilize current QueueServer users.

### Costs

- Existing QueueServer clients and queues are not directly compatible.
- Facilities must deploy and operate a separate product and state store.
- Each supported instrument workflow needs reviewed operation definitions.
- Authentication, worker deployment, safe control actions, and data integration still require deliberate design.
- A single-controller SQLite deployment does not provide HA.
- A controller restart overlapping active work may stop queue progress and require reconciliation or operator action instead of continuing transparently.

## Migration posture

QueueServer remains the supported system for existing deployments until a facility deliberately adopts the new controller. A pilot deployment MUST use a separate state store, worker, endpoint, and control authority. The two systems MUST NOT simultaneously control the same live instrument.

Migration tooling, if required, should export historical information offline. It must not mirror mutable queues or forward live commands between products.

## Safety and security considerations

This controller is an execution coordinator, not a physical safety system. Hardware and facility interlocks remain authoritative. Operation code MUST use supported Bluesky/Ophyd and facility control surfaces and MUST NOT bypass server-side validation or interlocks.

A production threat model must cover authentication, authorization, lease theft, replay, stale revisions, worker impersonation, local IPC access, database file permissions, event retention, secrets, denial of service, and deployment artifact provenance. The prototype demonstrates none of those controls beyond lease state, strict private framing, and deterministic failure handling.

No hardware pilot may begin until authenticated identity, facility authorization, reviewed operations, safe stop behavior, worker recovery, and deployment ownership are defined and exercised against simulation or a test IOC.

## Acceptance criteria for this RFC

Accepting this RFC means agreement that:

1. the new system is an incompatible product, not QueueServer v2 compatibility work;
2. one monorepo and one active controller per instrument are the initial deployment model;
3. clients submit only explicit, versioned, schema-valid operations;
4. the worker is a private, declared environment and reports its revision;
5. controller state, revisions, leases, and controller events are durable and transactional;
6. controller restart does not imply transparent queue continuation, and ambiguous execution outcomes block dispatch until explicit audited recovery;
7. public access will use a typed HTTPS/SSE boundary rather than direct ZMQ;
8. the current prototype is evidence, not a production API commitment.

Acceptance does not approve hardware operation, production deployment, or the unresolved follow-up designs below.

## Required follow-up decisions

1. Select one actual NSLS-II workflow and define its operation catalog, simulator, and acceptance scenarios.
2. Specify operation registration, schema generation, logical device resolution, and worker SDK packaging.
3. Specify execution attempts, worker-session identity, worker orphan policy, supervisor responsibilities, pause/stop/cancel semantics, reconciliation, and restart behavior.
4. Specify HTTP resources, idempotency, errors, SSE event envelopes, and retention.
5. Select authentication integration and define authorization, lease handoff, revocation, and override policy.
6. Define immutable worker artifacts, deployment supervision, readiness, rollback, and provenance.
7. Define Bluesky document routing and the run-UID/data-catalog boundary.
8. Define SQLite backup, migration, corruption recovery, and the evidence threshold for PostgreSQL or HA.
