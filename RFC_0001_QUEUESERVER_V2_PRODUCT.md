# RFC 0001: Bluesky QueueServer v2 Product Boundary

| Field | Value |
|---|---|
| Status | Draft |
| Created | 2026-09-03 |
| Last revised | 2026-10-05 |
| Decision scope | Next-generation product boundary and foundational architecture |
| Product brand | Bluesky QueueServer |
| Architecture generation | QueueServer v2 |
| Target distribution | `bluesky-queueserver` 2.0.0 |
| Project plan | [QueueServer v2 Project Plan](QUEUESERVER_V2_PROJECT_PLAN.md) |
| Reference prototype | [`bluesky_queueserver._experiment_controller`](src/bluesky_queueserver/_experiment_controller/) |

## Decision

Replace the current QueueServer general remote-execution architecture with an intentionally incompatible next major generation: Bluesky QueueServer v2.

QueueServer v2 uses one versioned operation catalog, one durable transactional state store, one authenticated control lease, one active controller per instrument, and one private subordinate worker process. It exposes a typed HTTPS/SSE API and fails closed whenever a claimed execution outcome cannot be proven.

The current QueueServer `0.x` line remains available for existing deployments and receives maintenance appropriate to that generation. QueueServer v2 is a breaking successor, not a compatibility mode, and this RFC does not approve operation on live hardware.

The key words **MUST**, **MUST NOT**, **SHOULD**, **SHOULD NOT**, and **MAY** describe normative requirements.

## Product model

### One product and one active authority

The initial product MUST use one repository and one product version for protocol models, controller, worker SDK, web API, storage, tests, and deployment examples.

The released v2 product SHOULD retain the canonical `bluesky-queueserver` project identity, Python distribution, and `bluesky_queueserver` import namespace. The target release is `2.0.0`, making the architectural break explicit. Development MAY use a dedicated branch or temporary incubation repository, but v2 MUST NOT become a permanently separate sibling product. The current `0.x` generation requires a documented maintenance branch and support policy.

Each instrument deployment MUST have exactly one active controller. The controller is the sole owner of authentication decisions, the control lease, queue state, scheduling, execution attempts, recovery state, and controller events. The first release has no active-active controller, horizontal web workers sharing mutable state, or distributed scheduling.

Current QueueServer and QueueServer v2 MUST NOT share a queue, worker, endpoint, state store, or simultaneous authority over the same live instrument.

### Explicit versioned operations

Public callers submit:

- a stable `operation_id`;
- an explicit `operation_version`;
- a closed JSON parameter object.

The worker publishes an explicit catalog of operation descriptors and its immutable worker revision at startup. The controller validates the catalog before accepting submissions. The controller and worker both validate operation identity, version, JSON representation, and schema before execution.

The public API MUST NOT accept Python expressions, arbitrary callable names, uploaded scripts, object traversal paths, or runtime namespace references. Reviewed worker code resolves logical identifiers into permitted device objects.

`operation_version` identifies the public schema and meaning. `worker_revision` identifies the exact profile, adapter, dependencies, and implementation artifact. Compatible implementation changes update the worker revision; breaking public semantics require a new operation version.

#### Profile-backed operation adapters

Registered operations MUST NOT require wholesale profile-collection cleanup. A worker MAY load a declared immutable profile collection in its established startup order and then load a small beamline-owned adapter that binds selected existing plans and devices to explicit operation descriptors.

The adapter remains inside the private worker boundary. It MUST:

- publish only reviewed registrations;
- resolve request values through explicit device maps;
- fail startup when required symbols are absent;
- propagate failures and preserve cleanup;
- define safe-stop and controller-loss orphan behavior;
- include the profile source and environment lock in the worker revision.

A callable or device name in reviewed deployment code is allowed. A callable name, object path, expression, or arbitrary file path supplied by a remote client is not.

Offline tooling MAY inspect QueueServer annotations, generated plan/device lists, permission files, and queue history to create candidate adapters or catalog diffs. A developer MUST review and commit a registration before it becomes remotely callable. The production API MUST NOT expose a generic `execute_plan(name, args, kwargs)` operation or automatically publish discovered namespace objects.

### Durable state and concurrency

The initial store MUST be SQLite in WAL mode on a local persistent filesystem. SQLite over NFS is unsupported.

Every queue, queue-execution, or dispatch mutation MUST atomically persist:

- the domain change;
- the incremented queue revision;
- the authenticated actor or durable scheduler authorization;
- an append-only controller event.

External mutations MUST include the queue revision observed by the client. A stale revision fails without partial mutation.

The durable domain includes:

- **Operation**: immutable submitted intent with one stable UID.
- **Queue execution**: authorization for the scheduler to consume an editable queue until empty, stopped, or blocked.
- **Execution attempt**: one controller-authorized invocation of an operation; retries never overwrite earlier evidence.
- **Control lease**: time-bounded mutation authority held by one authenticated principal.
- **Controller event**: append-only lifecycle or audit record with a monotonic cursor.

### Control lease and editable queue execution

The public boundary MUST derive the principal from authentication; clients cannot assert an arbitrary subject.

Submitting, cancelling, replacing, or reordering pending operations; starting or stopping queue execution; manually dispatching work; controlling execution; and acknowledging recovery require the matching active lease.

Starting queue execution durably records its UID, initiating principal, policy, admitted operations, and queue revision. The controller MAY continue dispatching admitted work after the browser disconnects or the initiating lease expires.

Whoever currently holds the lease MAY add, cancel, replace, or reorder pending work while execution continues. Claimed and running work is immutable and can be affected only through defined execution controls. Scheduler claims, queue edits, and completion serialize through the same queue revision.

Queue execution completes only when no attempt is active and no admitted operation remains queued. The first release stops on every failed, interrupted, or unknown attempt and never retries automatically.

### Private subordinate worker

Bluesky and Ophyd execute in one private worker process. The split exists for dependency and ordinary failure containment, not as a physical-safety mechanism, HA design, or same-user security sandbox.

The worker owns the RunEngine, device objects, reviewed operation catalog, and at most one execution attempt. It owns no durable queue, database, authentication policy, public endpoint, or recovery authority.

The controller launches a configured worker command without a shell. The controller never installs packages, updates source repositories, invokes environment managers, or runs deployment hooks through its public API. Deployment updates are external operations.

The worker protocol is private and MUST retain request correlation. It MUST provide a control and liveness path that remains responsive while an operation executes; a blocking execute-only request loop is insufficient.

The deployment supervisor manages the controller, not an independently restarting worker. Only the controller may launch a worker, and only after proving that no previous worker retains execution authority.

### Fail-closed lifecycle

The target operation lifecycle is:

```text
submitted
  -> queued
  -> claimed
  -> running
  -> succeeded | failed | cancelled | aborted | interrupted | unknown
```

Before worker contact, the controller MUST durably record a new attempt, claim, actor, worker revision, queue revision, and event.

After claim, any timeout, EOF, process exit, malformed response, correlation failure, or other unprovable result becomes `unknown` or `interrupted`. Dispatch stops. The controller MUST NOT automatically retry, requeue, or infer success.

If the controller connection disappears during an attempt, the worker accepts no new work, applies the operation's fixed orphan policy, and exits. Abrupt process termination is not assumed to be a safe hardware stop.

The first release MUST NOT reconnect to or resume observing a surviving worker. On restart, a claimed or running attempt becomes `unknown` or `interrupted`, dispatch remains blocked, and no replacement worker launches until the prior execution authority is proven ended. Recovery requires authenticated operator acknowledgement, but acknowledgement alone is not fencing evidence.

### Public API and data planes

The public surface consists of:

- HTTPS JSON commands and queries;
- typed request and response models generated from one contract;
- Server-Sent Events with durable cursor resumption;
- health and readiness endpoints that distinguish controller, storage, worker, fencing, and dispatch state.

There is no public ZMQ, raw worker protocol, interactive kernel, or first-release WebSocket requirement.

Controller events and Bluesky documents are separate data planes. Controller events describe authorization and lifecycle. Bluesky documents remain the RunEngine data stream. The controller preserves run UIDs for correlation but is not a data catalog.

## Normative invariants

1. One instrument has at most one active controller authority.
2. Public callers can request only catalog-declared, schema-valid operations.
3. The controller never evaluates caller-supplied Python or traverses a worker namespace.
4. Every operation and attempt retains stable identity and provenance.
5. Starting or externally mutating queue execution requires the authenticated control lease.
6. Lease expiry prevents new external mutations but does not revoke admitted work.
7. Pending work is editable; claimed and running work is immutable.
8. Scheduler dispatch is limited to durably admitted work.
9. State, revision, actor or scheduler authorization, and event commit in one transaction.
10. The worker owns no durable control state and accepts work only from its controller connection.
11. Controller loss invokes the fixed orphan policy and ends that worker's authority.
12. The first release never reattaches to a surviving worker.
13. A replacement worker cannot start until the previous authority is fenced.
14. Every unprovable post-claim outcome blocks dispatch and becomes unknown or interrupted.
15. Failed, interrupted, and unknown work is never automatically retried.
16. Recovery is explicit, authenticated, and audited.
17. The controller and worker never bypass device, IOC, PLC, or facility interlocks.
18. Current QueueServer and QueueServer v2 never share live control authority.

## Rationale and rejected alternatives

| Choice | Decision |
|---|---|
| Preserve QueueServer `0.x` compatibility in v2 | Rejected: compatibility retains the dynamic execution, transport, storage, and package boundaries being removed. |
| Public generic plan/function API | Rejected: an allowlisted Python namespace remains an unstable and overly broad product contract. |
| Require profile cleanup before adoption | Rejected: selected workflows may be exposed through private profile-backed adapters. |
| Redis as the domain store | Rejected initially: SQLite provides the required transaction, revision, and event boundary for one controller. |
| Bluesky inside the controller process | Rejected initially: it couples public authority and durable state to beamline dependencies and execution failures. |
| Controller-managed worker environments | Rejected: package installation and source updates are deployment responsibilities. |
| Custom Watchdog or worker reattachment | Rejected: independent lifetime and reconstruction add ambiguity; v1 fails closed. |
| Immediate HA or active-active controllers | Rejected: leadership, fencing, and distributed storage are not justified for the first product. |

## Non-goals

The first product does not provide:

- QueueServer `0.x` protocol, queue, history, CLI, or SDK compatibility;
- dynamic public plan/device discovery;
- arbitrary functions, scripts, or Python expressions;
- an interactive IPython worker;
- controller-managed profile or environment updates;
- direct PV monitoring or mutation outside registered operations;
- a Bluesky document store or data catalog;
- active-active controllers or SQLite over NFS;
- worker reattachment after controller restart;
- a replacement for EPICS, Tango, PLC, device, or facility safety systems.

## Migration and safety posture

Adoption is workflow-by-workflow. A v2 pilot worker MAY privately load an existing profile collection and register one selected workflow. Only the reachable path requires immediate contract hardening; unrelated expert and commissioning helpers remain outside the catalog.

Interactive Bluesky or current QueueServer may remain in use for workflows not yet migrated, but cutover must provide separate state, endpoints, workers, and control authority. No runtime bridge mirrors queues or forwards live commands between generations.

A worker running under the same operating-system principal as the controller is not a security sandbox. Stronger containment requires deployment-level credentials, filesystem and network permissions, or operating-system isolation.

No hardware pilot may begin until identity, authorization, the selected operation, safe stop, orphan behavior, fencing, operator recovery, deployment ownership, and the failure matrix are exercised against simulation or a test IOC.

## Prototype status

The internal `_experiment_controller` prototype proves a typed controller/worker contract, strict catalog validation, SQLite WAL storage, queue revisions, a persisted lease, one simulated operation, separate-process RunEngine execution, run-UID correlation, durable events, and conservative transport-loss handling.

It does not implement the target queue-execution scheduler, execution attempts, profile adapter, independent worker environment, concurrent control/liveness, safe stop, orphan exit, fencing, authenticated HTTP/SSE, or production deployment. The [QueueServer v2 project plan](QUEUESERVER_V2_PROJECT_PLAN.md) defines the implementation sequence.

## Acceptance

Accepting this RFC approves the QueueServer v2 product boundary and invariants above. It does not approve live hardware operation, production deployment, compatibility with QueueServer `0.x`, or the prototype's exact Python and stdio interfaces.
