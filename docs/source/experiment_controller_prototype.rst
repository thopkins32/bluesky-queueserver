========================================
Internal Experiment Controller Prototype
========================================

.. warning::

   This is an internal, simulator-only prototype. It is not a production control
   service and does not support hardware.

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

The trusted worker runs in a separate process using the same Python interpreter
as the controller. Requests and responses use a private newline-delimited JSON
protocol over the subprocess standard streams. The only operation is
``simulated-count`` version ``1``. It creates Ophyd simulated hardware and a
fresh Bluesky RunEngine inside the worker process. A later deployment may point
the worker client at an independently built environment, but this prototype
does not install, update, or manage that environment.

A failed operation blocks further dispatch until an operator acknowledges the
block. Loss of the worker transport leaves the stable operation record in the
``unknown`` state, also blocks dispatch, and never automatically retries or
requeues the operation.

Deliberate exclusions
=====================

This slice provides no HTTP service, authentication or authorization
integration, public ZMQ endpoint, legacy QueueServer compatibility gateway,
profile loading, arbitrary script or function execution, IPython console,
remote environment control, or hardware support. The caller supplies a trusted
subject string to exercise lease semantics. Importing the private child package
also still imports the existing distribution's parent package; independent
service deployment remains outside this prototype.
