QueueServer V2
==============

.. warning::

   QueueServer V2 is approved for the Ophyd simulator only.  The controller/
   worker split and the authority locks prove software authority; they are not
   physical-safety mechanisms and do not replace IOC, PLC, device, or facility
   interlocks.  No live hardware is approved by this implementation.

QueueServer V2 is an additive, incompatible control path.  Existing QueueServer
``0.x`` commands, protocols, and state remain separate.  There is no queue,
worker, endpoint, or runtime bridge between the generations.

Supported operation
-------------------

The sole production catalog entry is ``simulated-count`` version ``1``.  It
requires ``queueserver:control`` and has the fixed ``request-stop`` orphan
policy.  Its request is a closed JSON object:

.. code-block:: json

   {
     "operation_id": "simulated-count",
     "operation_version": "1",
     "parameters": {
       "detectors": ["det"],
       "num": 1,
       "delay": 0.0
     }
   }

``detectors`` must contain exactly one ``det`` value.  ``num`` is an integer
from 1 through 10.  ``delay`` is a finite number from 0 through 10 seconds.
The result is ``{"run_uids": ["..."]}``.  The worker maps ``det`` only to
``ophyd.sim.hw().det``, owns one RunEngine, and executes
``bluesky.plans.count``.  Bluesky documents remain in the worker data path;
the controller stores only run UIDs and lifecycle evidence.

Identity and authorization
--------------------------

Every ``/api/v2`` endpoint requires an OIDC bearer JWT signed with ``RS256``.
The controller verifies the configured issuer, audience, JWKS, ``sub``,
``exp``, and ``iat`` claims, honors ``nbf``, and allows at most 30 seconds of
clock skew.  The nonempty ``sub`` claim is the principal.  Only the standard,
space-delimited ``scope`` claim is read.

The fixed scopes are:

* ``queueserver:read`` for every query and event stream;
* ``queueserver:control`` for lease actions, queue edits, queue execution,
  safe stop, and recovery; it implies read;
* ``queueserver:admin`` for lease override; it implies control and read.

``/health`` and ``/ready`` are unauthenticated and expose coarse component
state only.  They never expose principals, tokens, lease identities, worker
PIDs, or IPC details.  Interactive kernels, arbitrary plans or functions,
namespace traversal, script upload, public ZMQ, and WebSockets are absent.

HTTP API
--------

Queries:

* ``GET /health``
* ``GET /ready``
* ``GET /api/v2/openapi.json``
* ``GET /api/v2/catalog``
* ``GET /api/v2/control-lease``
* ``GET /api/v2/queue``
* ``GET /api/v2/queue-executions/{uid}``
* ``GET /api/v2/operations/{uid}``
* ``GET /api/v2/attempts/{uid}``
* ``GET /api/v2/events?after=<cursor>&limit=<n>``
* ``GET /api/v2/events/stream``

Commands:

* ``POST /api/v2/control-lease``
* ``POST /api/v2/control-lease/renew``
* ``POST /api/v2/control-lease/release``
* ``POST /api/v2/control-lease/override``
* ``POST /api/v2/operations``
* ``DELETE /api/v2/operations/{uid}``
* ``PUT /api/v2/operations/{uid}``
* ``POST /api/v2/queue/reorder``
* ``POST /api/v2/queue-executions``
* ``POST /api/v2/queue-executions/{uid}/stop``
* ``POST /api/v2/attempts/{uid}/safe-stop``
* ``POST /api/v2/recovery/acknowledge``

Every mutation requires an ``Idempotency-Key`` matching
``[A-Za-z0-9._:-]{1,128}``.  Its scope is principal, HTTP method, and concrete
request path.  The request hash also covers the canonical body and
``If-Match``.  An exact replay returns the original status, body, and ETag;
conflicting reuse returns HTTP 409.

Queue-changing commands require ``If-Match: "qrev-N"`` and the active lease to
belong to the authenticated principal.  Missing preconditions return 428 and
stale revisions return 412 with the current ETag.  Queue-bearing responses
carry the resulting ETag.  All errors use
``{"error":{"code":...,"message":...,"details":...,"request_id":...}}``
and every response carries ``X-Request-ID``.

The SSE stream uses durable event IDs.  Supply either ``after`` or
``Last-Event-ID``; supplying disagreeing values is rejected.  Reconnection
returns every later event in order.  Idle streams receive a comment heartbeat
every 15 seconds and close when the JWT expires.

Queue and stop semantics
------------------------

A lease authorizes new external mutations but is not scheduler authority.
Work admitted to a running queue execution continues after client disconnect or
lease expiry.  Pending work may be added, cancelled, replaced, or completely
reordered.  Claimed and running work is immutable.

``stop`` on a queue execution means stop after the current attempt.  A safe-stop
request targets only the current attempt and requests a pause at the next
RunEngine checkpoint.  A matching acknowledgement plus completed cleanup
produces ``aborted``.  A valid success or failure remains authoritative if it
wins the race.  Missing, malformed, timed-out, or uncorrelated evidence becomes
``unknown`` and blocks dispatch; no attempt is retried automatically.

Configuration and startup
-------------------------

Use ``deployment/v2/controller.example.yml`` and ``worker.example.yml`` as the
schema examples.  Controller configuration contains the instrument ID, an
absolute local database path, the worker argv list, OIDC issuer/audience/JWKS,
and TLS certificate/key paths.  A worker command is never interpreted by a
shell.  The controller appends the IPC descriptor, worker-instance UID,
worker-lock path, and instrument ID; those values are not accepted from HTTP or
worker YAML.

The deployed simulator worker config selects ``simulated-count`` and records a
64-character lowercase environment-lock SHA-256.  Profile mode is a deployment
boundary: it requires a startup directory and adapter file, runs top-level
``*.py`` and ``*.ipy`` files in lexical order in an isolated IPython shell, then
calls exactly ``register_operations(registry, profile)`` from the adapter.

The worker revision is a SHA-256 over canonical provenance containing the
installed distribution version, private protocol version, operation schemas
and policies, Bluesky and Ophyd versions, environment lock digest, and profile
and adapter source digests.  Simulator profile and adapter hashes are null.

Storage and authority
---------------------

The V2 database is a separate SQLite file with application ID ``0x51535632``.
It uses foreign keys, a 5-second busy timeout, WAL journaling, and FULL
synchronous writes on a canonical local filesystem.  NFS, CIFS/SMB, SSHFS, 9P,
Ceph, GlusterFS, and unidentifiable filesystems are rejected.

Sibling ``.controller.lock`` and ``.worker.lock`` files are owned by the service
user, not group/world writable, opened without following symlinks, and held by
``flock``.  The controller lock establishes the sole service authority.  The
worker lock is held until RunEngine cleanup and process exit.  Successfully
acquiring that worker lock is the only supported proof that prior worker
authority ended; a PID or operator statement is not proof.

``/health`` means the process responds.  ``/ready`` is HTTP 200 only when the
controller lock, storage, compatible worker, fence state, and dispatch state are
ready.  Otherwise it is HTTP 503 with only coarse component fields.

Backup, migration, and rollback
-------------------------------

Stop the HTTPS service before state-changing administration.  The commands are:

.. code-block:: console

   qserver-v2-admin check --config /etc/bluesky-queueserver-v2/controller.yml
   qserver-v2-admin backup --config /etc/bluesky-queueserver-v2/controller.yml --output /srv/backup/qsv2.sqlite
   qserver-v2-admin migrate --config /etc/bluesky-queueserver-v2/controller.yml --backup /srv/backup/pre-migrate.sqlite
   qserver-v2-admin restore --config /etc/bluesky-queueserver-v2/controller.yml --backup /srv/backup/qsv2.sqlite

Migration, backup, and restore require exclusive controller and worker locks.
Backups use SQLite's backup API and include a SHA-256, application ID, schema
version, migration checksum, and installed package version manifest.  Restore
verifies all of those fields, replaces the database atomically, fsyncs the file
and directory, and creates a fenced ``restore.requires_review`` block.  A
rollback is a restore performed with the matching wheel and environment lock;
never downgrade a live database in place.

Recovery runbook
----------------

1. Leave the controller serving only health, query, and recovery state while a
   dispatch block is active.
2. Determine and correct the underlying worker or operation failure outside the
   public API.  Never infer completion from timing or a PID.
3. Wait for the old worker to release the canonical worker lock.  The controller
   records the successful exclusive lock probe as fence evidence.
4. Acquire the control lease with an authorized identity, read the current queue
   revision, and acknowledge recovery with a nonblank note.
5. Confirm ``/ready`` is healthy.  Remaining queued work is not resumed; start a
   new queue execution explicitly.

The systemd unit supervises only the controller.  It deliberately disables
forced termination and uses an unbounded stop timeout so RunEngine cleanup may
finish.  It never launches or restarts the worker independently.
