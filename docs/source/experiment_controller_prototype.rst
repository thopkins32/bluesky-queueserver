==========================================
Internal QueueServer v2 Contract Prototype
==========================================

.. warning::

   This is an internal, simulator-only QueueServer v2 contract prototype. It is
   not a production control service and does not support hardware.

The prototype demonstrates one complete control path: acquire a control lease,
submit a versioned operation, durably claim it from a FIFO queue, execute it in a
separate simulator process, and persist the result and lifecycle events. Run the
bounded demonstration with::

  python -m bluesky_queueserver._experiment_controller --num 1

The command prints one JSON object containing the stable operation UID, terminal
state, Bluesky run UIDs, queue revision, worker revision, and durable event IDs.
Use ``--database PATH`` to preserve the SQLite database after the command exits;
without it, the command uses and removes a temporary local database.

Prototype boundaries
====================

The controller exposes an internal typed Python API. Operation inputs are
validated against a closed Draft 7 JSON Schema before they are persisted. Queue
records, the active control lease, queue revision, dispatch block, and
append-only lifecycle events share one local SQLite transaction boundary. The
SQLite database uses write-ahead logging and is intended for one local
controller; this prototype makes no NFS or high-availability claim.

The prototype has no durable queue-execution record or automatic scheduler.
Each call dispatches at most one operation and requires the caller's live control
lease. It therefore does not yet support an overnight queue that continues
dispatching after lease expiry or lease-authorized edits to a running batch.

The trusted worker runs in a separate process using the same Python interpreter
as the controller. Requests and responses use a private newline-delimited JSON
protocol over the subprocess standard streams. The only operation is
``simulated-count`` version ``1``. It creates Ophyd simulated hardware and a
fresh Bluesky RunEngine inside the worker process. A later deployment may point
the worker client at an independently built environment, but this prototype
does not install, update, or manage that environment.

The request loop blocks while the operation executes. It therefore provides no
concurrent safe-stop channel, controller-liveness detection, orphan policy,
worker-instance fencing, or execution-attempt identity. The production design
requires those behaviors and does not reconnect to a surviving worker after a
controller restart.

A failed operation blocks further dispatch until an operator acknowledges the
block. Loss of the worker transport leaves the stable operation record in the
``unknown`` state, also blocks dispatch, and never automatically retries or
requeues the operation. The prototype does not yet map every malformed or
mismatched post-claim protocol response to that same fail-closed state.

Deliberate exclusions
=====================

This slice provides no HTTP service, authentication or authorization
integration, public ZMQ endpoint, QueueServer ``0.x`` compatibility gateway,
profile loading, arbitrary script or function execution, IPython console,
remote environment control, or hardware support. The caller supplies a trusted
subject string to exercise lease semantics. Importing the private child package
still imports the current distribution's parent package; the prototype does not
yet represent the final QueueServer v2 package or runtime boundary.
