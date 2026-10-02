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

This RFC proposes replacing QueueServer's general remote-execution model with an explicitly incompatible, single-controller experiment-execution product. The new product will use one monorepo and one product version, a declared and versioned operation catalog instead of remote Python namespace access, a private and subordinate worker-process boundary for dependency and failure isolation, and durable transactional state with an auditable control lease.

The existing QueueServer remains available to current users and receives maintenance appropriate to that product. The new controller does not inherit QueueServer's public ZMQ protocol, interactive kernel access, arbitrary script or function execution, Redis queue model, profile-loading semantics, or CLI compatibility.

This parent RFC fixes the product boundary and core invariants. It does not standardize the final HTTP schema, authentication provider, worker SDK, hardware operation catalog, or detailed pause/stop protocol. Those require follow-up RFCs informed by an actual instrument workflow.

## Terminology

The key words **MUST**, **MUST NOT**, **SHOULD**, **SHOULD NOT**, and **MAY** describe requirements on the proposed product.

- **Controller**: the sole owner of durable queue state, authorization decisions, scheduling, worker lifecycle, and controller events for one instrument deployment.
- **Worker**: a trusted, disposable execution process running a declared instrument environment and reviewed operation catalog. It owns RunEngine and device objects while executing controller-authorized work, but owns no durable queue, authorization policy, public API, or recovery authority.
- **Operation**: a versioned, typed action submitted to the controller. It is not an arbitrary Python function call.
- **Execution attempt**: one controller-authorized invocation of an operation. Every retry creates a new attempt identity and never overwrites evidence from an earlier attempt.
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

Bluesky and Ophyd execution MUST remain in a separate worker process. This boundary exists because the controller and instrument stack have different dependency and failure domains: a worker package conflict, native-library failure, memory leak, or blocked operation must not also own or corrupt the durable control plane. The boundary also lets a deployment select an instrument environment without importing that environment into the public controller process.

The process split MUST NOT be represented as a physical-safety mechanism, an HA design, or—when both processes run as the same operating-system principal—a security sandbox. Hardware interlocks remain authoritative, and stronger containment requires deployment-level credentials or operating-system isolation.

The worker is subordinate to the controller and disposable. Only the controller owns durable state and public authority; the worker has no queue, authentication surface, database, or public endpoint. The first release MUST NOT reproduce QueueServer's independently recoverable Manager/Worker topology or promise transparent continuation across controller restart.

The product MUST rely on the deployment's standard process supervisor rather than reproduce QueueServer's custom Manager Watchdog. A controller-only death is a narrow process-local failure, not the primary availability model. Deterministic controller crashes, deadlocks, and dependency failures must be addressed through release testing and fixes; automatic restart is containment for residual faults, not a substitute for correctness.

The deployment supervisor MUST NOT independently restart a worker. Only the controller may launch a worker, and only after it has established that no previous worker retains execution authority.

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

The controller MUST launch a configured worker command without a shell. The command selects a deployed, reviewed environment; the controller MUST NOT install packages, update Git repositories, invoke environment managers, or run arbitrary deployment hooks through its public API.

At startup, the worker MUST report:

- its worker protocol version;
- its worker revision;
- its operation catalog.

The controller MUST reject unsupported protocol versions, malformed responses, and duplicate operation identities before accepting submissions. Every dispatch MUST carry the stable operation UID, a new execution-attempt identity, and a request correlation identifier across the private boundary.

The worker MUST service a private control and liveness path while an operation is executing. A blocking execute request/response loop that cannot concurrently receive a stop request or detect controller loss is insufficient for production. The detailed transport is private implementation detail and MUST NOT be exposed as a public network API.

A malformed response, timeout, EOF, process exit, or correlation failure after an operation is claimed is an ambiguous execution outcome and MUST be handled according to Section 7. The exact newline-delimited JSON protocol used by the prototype proves only that a subprocess boundary works; it is not the production protocol commitment.

The production controller SHOULD remain import-independent of Bluesky, Ophyd, and instrument startup packages. Those dependencies belong in the declared worker environment. The worker boundary does not replace authentication or operating-system isolation at the public or deployment boundary.

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

The first implementation MAY combine adjacent internal states while preserving their externally meaningful transitions. Before contacting the worker, the controller MUST durably record a new execution-attempt identity, the claim, the acting principal, the worker revision, and the corresponding controller event. It MUST NOT report a running operation as completed merely because either process restarted.

After claim, any transport failure, protocol failure, timeout, process exit, or missing response that makes the outcome unprovable MUST record the attempt as `unknown` or `interrupted`, block automatic dispatch, and require authenticated operator recovery. It MUST NOT automatically retry, requeue, or declare the attempt successful.

If the controller connection disappears while an attempt is claimed or running, the worker MUST accept no new work. It MUST apply the reviewed operation's fixed orphan policy—finish the current operation, request a safe stop, or reach another explicitly defined safe terminal condition—and then exit. The worker's control/liveness path MUST be able to detect controller loss while execution is active. Abruptly terminating a worker is not assumed to be a safe hardware stop.

The first release MUST NOT reconnect to a surviving worker or resume observing its in-memory execution after controller restart. A restarted controller MUST load any claimed or running attempt, record it as `unknown` or `interrupted`, keep dispatch blocked, and expose the condition for operator recovery. It MUST NOT launch a replacement worker until the previous worker's execution authority is proven to have ended. An acknowledgement alone is not proof that the prior worker is gone.

The deployment supervisor MAY restart the controller, but restart does not authorize queue continuation. The worker-exit evidence, fail-closed state transition, recovery acknowledgement, and any subsequent worker launch MUST be durably auditable. A future proposal for reconnectable workers would require a separate RFC with a stronger identity, result-recovery, and fencing model.

An explicit retry is new operator intent. It MUST receive a new auditable execution-attempt identity and MUST NOT overwrite the unknown operation or attempt. The detailed relationship between operations and attempts remains part of the lifecycle follow-up design.

The prototype also blocks dispatch after an ordinary operation failure. A follow-up lifecycle RFC may make that policy dependent on a reviewed operation's failure classification, but ambiguous process or protocol loss MUST always block.

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
5. Every dispatch has a durable execution-attempt identity before the worker is contacted.
6. Queue and dispatch mutations use optimistic revision checks.
7. A queue/dispatch mutation, its new revision, and its controller event commit in one transaction.
8. Controller events have durable cursor IDs and are distinct from Bluesky documents.
9. The worker owns no durable control state and accepts work only from its controller connection.
10. Loss of the controller connection prevents new work, invokes the fixed orphan policy, and ends that worker's authority.
11. The first release never reconnects to or resumes observing a surviving worker after controller restart.
12. A claimed or running attempt found after controller restart becomes `unknown` or `interrupted`, and dispatch remains blocked.
13. A replacement worker cannot start until the previous worker's execution authority is proven to have ended.
14. Any post-claim transport or protocol failure that makes the result unprovable produces an unknown outcome.
15. Unknown work is never automatically retried, requeued, or declared successful.
16. Recovery that restores dispatch is explicit, authenticated, and audited.
17. The controller and worker do not bypass device, IOC, or facility safety interlocks.
18. QueueServer and the new product never share live control authority during migration.

## Prototype evidence

The internal prototype establishes that the smallest control path is feasible without Redis, public ZMQ, IPython, or a web stack.

| Decision | Prototype evidence | Remaining production work |
|---|---|---|
| Typed contract | Frozen dataclasses, explicit states and errors, Draft 7 input validation | Select the production schema/model generation strategy |
| Explicit catalog | One `simulated-count` operation, version `1`, with a closed parameter schema | Define one real instrument workflow and worker SDK |
| Durable state | Versioned SQLite schema, WAL mode, queue revisions, stable operation records, append-only events | Add execution attempts, migration tooling, backup guidance, and production operational limits |
| Control lease | Persisted lease checked on submission, dispatch, and recovery | Add authenticated principal integration, handoff, revocation, and override |
| Worker isolation | Separate interpreter process with strict request/response correlation and reported revision | Build an independent immutable worker artifact and add concurrent control/liveness, orphan exit, and execution-authority fencing |
| Safe ambiguity | Transport loss records `unknown`, blocks dispatch, and requires acknowledgement | Treat every post-claim protocol failure consistently and define the operator recovery UX |
| Bluesky correlation | Simulated RunEngine execution returns start-document run UIDs | Define the data-document routing and data-catalog boundary |
| Restart evidence | Reopening the file-backed database returns the same terminal record and ordered events | Add fail-closed handling of nonterminal attempts, prohibit worker reattachment, prove prior-worker exit, and exercise the complete crash matrix |

The reference implementation is intentionally narrower than this RFC. It uses a trusted subject string, runs the worker with the controller's interpreter, has a blocking request/response loop with no concurrent control or liveness path, has no HTTP or SSE server, exposes one simulator operation, and supports no hardware.

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
- reconnecting to or resuming observation of a surviving worker after controller restart;
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

### Run Bluesky inside the controller process

Rejected for the initial production architecture. A single process with a dedicated execution thread would remove IPC and independent-lifecycle failures, but it would also place the public API, durable state owner, beamline dependencies, native libraries, and operation execution in one failure and dependency domain. The separate worker is retained only for dependency and failure containment; it is not presented as a security sandbox, HA mechanism, or physical-safety boundary.

This decision should be re-examined if the selected workflow demonstrates that the controller and instrument environments can be deployed together and that whole-service failure has acceptable operational consequences. The product MUST NOT maintain both in-process and subprocess production modes merely for optionality.

### Add HA and multiple active controllers immediately

Rejected. Consensus, leadership, fencing, and distributed storage substantially expand the safety and operational model. One active controller is sufficient to validate the product and real workflows.

### Preserve QueueServer's custom Watchdog or transparent continuation

Rejected. The legacy topology attempts to replace the Manager while retaining and reconnecting to its sibling Worker. That protects mainly against a localized Manager defect while introducing independent-lifetime, identity, and reconciliation failure modes. The new worker is a subordinate execution capsule: after controller loss it follows its fixed orphan policy and exits, and the restarted controller fails closed instead of reconstructing or continuing the prior execution.

### Add HTTP, authentication, and database frameworks to the prototype

Rejected for the contract probe. The prototype first had to prove operation identity, durable transitions, worker isolation, and failure semantics. Production HTTP and identity integration remain required, but they should be built after this parent decision is reviewed.

## Consequences

### Benefits

- A smaller public attack surface and explicit code/dependency boundary.
- Durable controller state and operator visibility survive an ordinary worker failure.
- Reproducible operation and worker version attribution.
- Durable concurrency and fail-closed recovery semantics instead of inferred process state.
- One source of truth for controller contracts and releases.
- A migration path that does not destabilize current QueueServer users.

### Costs

- Existing QueueServer clients and queues are not directly compatible.
- Facilities must deploy and operate a separate product and state store.
- Each supported instrument workflow needs reviewed operation definitions.
- The worker boundary requires a private protocol, concurrent control/liveness handling, process supervision, execution-attempt records, orphan behavior, and fencing.
- Authentication, worker deployment, safe control actions, and data integration still require deliberate design.
- A single-controller SQLite deployment does not provide HA.
- A controller restart overlapping active work stops queue progress and requires explicit operator recovery; the first release does not reconnect to the old worker.

## Migration posture

QueueServer remains the supported system for existing deployments until a facility deliberately adopts the new controller. A pilot deployment MUST use a separate state store, worker, endpoint, and control authority. The two systems MUST NOT simultaneously control the same live instrument.

Migration tooling, if required, should export historical information offline. It must not mirror mutable queues or forward live commands between products.

## Safety and security considerations

This controller is an execution coordinator, not a physical safety system. Hardware and facility interlocks remain authoritative. Operation code MUST use supported Bluesky/Ophyd and facility control surfaces and MUST NOT bypass server-side validation or interlocks.

A worker process running on the same host and as the same operating-system principal as the controller is not a security sandbox. If compromise containment is required, the deployment must add distinct credentials, filesystem and network permissions, or an operating-system isolation boundary. The application-level process split is justified by dependency and failure containment, not by an unsupported security claim.

A production threat model must cover authentication, authorization, lease theft, replay, stale revisions, worker impersonation, local IPC access, database file permissions, event retention, secrets, denial of service, and deployment artifact provenance. The prototype demonstrates none of those controls beyond lease state, strict private framing, and deterministic transport-failure handling.

No hardware pilot may begin until authenticated identity, facility authorization, reviewed operations, safe stop behavior, worker orphan behavior, execution-authority fencing, operator recovery, and deployment ownership are defined and exercised against simulation or a test IOC.

## Acceptance criteria for this RFC

Accepting this RFC means agreement that:

1. the new system is an incompatible product, not QueueServer v2 compatibility work;
2. one monorepo and one active controller per instrument are the initial deployment model;
3. clients submit only explicit, versioned, schema-valid operations;
4. the worker is a private, subordinate process in a declared environment and reports its revision;
5. the process split exists for dependency and failure containment, not as a physical-safety, HA, or same-user security boundary;
6. controller state, revisions, leases, execution attempts, and controller events are durable and transactional;
7. controller restart never reconnects to the old worker in the first release, and ambiguous execution blocks dispatch until the old authority is fenced and recovery is explicitly acknowledged;
8. public access will use a typed HTTPS/SSE boundary rather than direct ZMQ;
9. the current prototype is evidence, not a production API commitment.

Acceptance does not approve hardware operation, production deployment, or the unresolved follow-up designs below.

## Required follow-up decisions

1. Select one actual NSLS-II workflow and define its operation catalog, simulator or test IOC, safe-stop behavior, orphan policy, and acceptance scenarios.
2. Specify operation registration, schema generation, logical device resolution, and worker SDK packaging.
3. Specify durable execution attempts, concurrent worker control/liveness, worker-exit evidence, execution-authority fencing, pause/stop/cancel semantics, fail-closed startup, and operator recovery. Worker reattachment is out of scope for the first release.
4. Specify the minimum HTTP resources, idempotency rules, errors, SSE event envelopes, and retention needed by the selected workflow.
5. Select one authentication integration and define authorization, lease handoff, revocation, and override policy.
6. Define immutable worker artifacts, deployment supervision, readiness, rollback, and provenance.
7. Define Bluesky document routing and the run-UID/data-catalog boundary.
8. Define SQLite backup, migration, corruption recovery, and the evidence threshold for PostgreSQL or HA.
