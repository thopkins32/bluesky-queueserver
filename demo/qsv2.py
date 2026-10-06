"""Minimal HTTPS client for driving the QueueServer V2 demo.

Every call prints the request line with its precondition headers and the
response status, ETag and body, so the protocol is visible on screen.

Examples::

    qsv2 ready
    qsv2 lease acquire
    qsv2 submit --num 3 --delay 1
    qsv2 --stale submit            # deliberately send If-Match: "qrev-0"
    qsv2 start
    qsv2 stream
    qsv2 --as viewer submit        # 403: read scope cannot mutate
    qsv2 --as forged queue         # 401: unknown signing key
    qsv2 kill-worker               # SIGKILL the subordinate worker
    qsv2 ack --note "worker crash investigated"
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import ssl
import subprocess
import sys
from pathlib import Path
from uuid import uuid4

import httpx

STATE = Path(__file__).resolve().parent / "state"
BASE_URL = os.environ.get("QSV2_URL", "https://127.0.0.1:8443")


class Client:
    def __init__(self, *, identity: str, key: str | None, if_match: str | None, stale: bool) -> None:
        token = (STATE / f"token.{identity}").read_text().strip()
        self.http = httpx.Client(
            base_url=BASE_URL,
            verify=ssl.create_default_context(cafile=str(STATE / "server.crt")),
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        self.key = key
        self.if_match = '"qrev-0"' if stale else if_match

    def current_etag(self) -> str:
        return self.http.get("/api/v2/queue").headers["etag"]

    def show(self, response: httpx.Response, *, body: bool = True) -> httpx.Response:
        request = response.request
        extras = [f"{h}: {request.headers[h]}" for h in ("if-match", "idempotency-key") if h in request.headers]
        print(
            f"> {request.method} {request.url.raw_path.decode()}"
            + (f"   [{' | '.join(extras)}]" if extras else "")
        )
        etag = f"   ETag: {response.headers['etag']}" if "etag" in response.headers else ""
        print(f"< {response.status_code}{etag}")
        if body and response.content:
            print(json.dumps(response.json(), indent=2))
        return response

    def get(self, path: str, **params) -> httpx.Response:
        return self.show(self.http.get(path, params=params or None))

    def mutate(self, method: str, path: str, *, json_body=None, precondition: bool = True) -> httpx.Response:
        headers = {"Idempotency-Key": self.key or str(uuid4())}
        if precondition:
            headers["If-Match"] = self.if_match or self.current_etag()
        return self.show(self.http.request(method, path, json=json_body, headers=headers))

    def stream(self, after: int | None) -> None:
        params = {"after": after} if after is not None else None
        with self.http.stream("GET", "/api/v2/events/stream", params=params, timeout=None) as response:
            print("> GET /api/v2/events/stream   (Ctrl-C to stop)")
            print(f"< {response.status_code} {response.headers.get('content-type', '')}")
            event = {}
            for line in response.iter_lines():
                if line.startswith(":"):
                    print(line)
                elif line.startswith(("id: ", "event: ")):
                    field, _, value = line.partition(": ")
                    event[field] = value
                elif line.startswith("data: "):
                    data = json.loads(line.removeprefix("data: "))
                    actor = data.get("actor_kind") or "-"
                    if data.get("actor_kind") == "principal":
                        actor = data.get("actor_id") or actor
                    subject = (
                        data.get("operation_uid")
                        or data.get("attempt_uid")
                        or data.get("queue_execution_uid")
                        or ""
                    )
                    payload = {k: v for k, v in (data.get("payload") or {}).items() if v not in (None, {}, [])}
                    revision = data.get("queue_revision")
                    print(
                        f"  #{event.get('id'):>4}  qrev={revision!s:<4} {event.get('event'):<32} "
                        f"{actor:<10} {subject[:8]:<8} {json.dumps(payload) if payload else ''}"
                    )
                    event = {}


def cmd_kill_worker(_: Client, args: argparse.Namespace) -> None:
    pids = subprocess.run(["pgrep", "-f", "qserver-v2-worker"], capture_output=True, text=True).stdout.split()
    if not pids:
        sys.exit("no qserver-v2-worker process found")
    for pid in pids:
        os.kill(int(pid), signal.SIGKILL)
        print(f"sent SIGKILL to worker pid {pid}")


def submission(args: argparse.Namespace) -> dict:
    if args.raw:
        return json.loads(args.raw)
    return {
        "operation_id": args.op,
        "operation_version": args.version,
        "parameters": {"detectors": ["det"], "num": args.num, "delay": args.delay},
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="qsv2", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--as", dest="identity", default="operator", help="viewer | operator | admin | forged")
    parser.add_argument("--key", help="Idempotency-Key to reuse (default: fresh UUID per call)")
    parser.add_argument("--if-match", help="explicit If-Match ETag, e.g. '\"qrev-3\"'")
    parser.add_argument("--stale", action="store_true", help='send If-Match: "qrev-0" to provoke 412')
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("health").set_defaults(run=lambda c, a: c.get("/health"))
    sub.add_parser("ready").set_defaults(run=lambda c, a: c.get("/ready"))
    sub.add_parser("catalog").set_defaults(run=lambda c, a: c.get("/api/v2/catalog"))
    sub.add_parser("queue").set_defaults(run=lambda c, a: c.get("/api/v2/queue"))

    lease = sub.add_parser("lease").add_subparsers(dest="action", required=True)
    lease.add_parser("show").set_defaults(run=lambda c, a: c.get("/api/v2/control-lease"))
    acquire = lease.add_parser("acquire")
    acquire.add_argument("--ttl", type=int, default=300)
    acquire.set_defaults(
        run=lambda c, a: c.mutate(
            "POST", "/api/v2/control-lease", json_body={"ttl_seconds": a.ttl}, precondition=False
        )
    )
    renew = lease.add_parser("renew")
    renew.add_argument("--ttl", type=int, default=300)
    renew.set_defaults(
        run=lambda c, a: c.mutate(
            "POST", "/api/v2/control-lease/renew", json_body={"ttl_seconds": a.ttl}, precondition=False
        )
    )
    lease.add_parser("release").set_defaults(
        run=lambda c, a: c.mutate("POST", "/api/v2/control-lease/release", precondition=False)
    )
    override = lease.add_parser("override")
    override.add_argument("--reason", required=True)
    override.set_defaults(
        run=lambda c, a: c.mutate(
            "POST", "/api/v2/control-lease/override", json_body={"reason": a.reason}, precondition=False
        )
    )

    submit = sub.add_parser("submit")
    submit.add_argument("--op", default="simulated-count")
    submit.add_argument("--version", default="1")
    submit.add_argument("--num", type=int, default=1)
    submit.add_argument("--delay", type=float, default=0.0)
    submit.add_argument("--raw", help="literal JSON submission body (overrides the other options)")
    submit.set_defaults(run=lambda c, a: c.mutate("POST", "/api/v2/operations", json_body=submission(a)))

    replace = sub.add_parser("replace")
    replace.add_argument("uid")
    replace.add_argument("--op", default="simulated-count")
    replace.add_argument("--version", default="1")
    replace.add_argument("--num", type=int, default=1)
    replace.add_argument("--delay", type=float, default=0.0)
    replace.add_argument("--raw")
    replace.set_defaults(run=lambda c, a: c.mutate("PUT", f"/api/v2/operations/{a.uid}", json_body=submission(a)))

    cancel = sub.add_parser("cancel")
    cancel.add_argument("uid")
    cancel.set_defaults(run=lambda c, a: c.mutate("DELETE", f"/api/v2/operations/{a.uid}"))

    reorder = sub.add_parser("reorder")
    reorder.add_argument("uids", nargs="+")
    reorder.set_defaults(
        run=lambda c, a: c.mutate("POST", "/api/v2/queue/reorder", json_body={"operation_uids": a.uids})
    )

    sub.add_parser("start").set_defaults(
        run=lambda c, a: c.mutate("POST", "/api/v2/queue-executions", json_body={})
    )
    stop = sub.add_parser("stop")
    stop.add_argument("uid", help="queue execution uid")
    stop.set_defaults(run=lambda c, a: c.mutate("POST", f"/api/v2/queue-executions/{a.uid}/stop"))
    safe_stop = sub.add_parser("safe-stop")
    safe_stop.add_argument("uid", help="attempt uid")
    safe_stop.set_defaults(run=lambda c, a: c.mutate("POST", f"/api/v2/attempts/{a.uid}/safe-stop"))

    for name, path in (("op", "operations"), ("attempt", "attempts"), ("execution", "queue-executions")):
        show = sub.add_parser(name)
        show.add_argument("uid")
        show.set_defaults(run=lambda c, a, path=path: c.get(f"/api/v2/{path}/{a.uid}"))

    events = sub.add_parser("events")
    events.add_argument("--after", type=int, default=0)
    events.add_argument("--limit", type=int, default=100)
    events.set_defaults(run=lambda c, a: c.get("/api/v2/events", after=a.after, limit=a.limit))
    stream = sub.add_parser("stream")
    stream.add_argument("--after", type=int)
    stream.set_defaults(run=lambda c, a: c.stream(a.after))

    ack = sub.add_parser("ack")
    ack.add_argument("--note", required=True)
    ack.set_defaults(run=lambda c, a: c.mutate("POST", "/api/v2/recovery/acknowledge", json_body={"note": a.note}))

    sub.add_parser("kill-worker").set_defaults(run=cmd_kill_worker)
    return parser


def main() -> int:
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)
    args = build_parser().parse_args()
    client = Client(identity=args.identity, key=args.key, if_match=args.if_match, stale=args.stale)
    try:
        args.run(client, args)
    except KeyboardInterrupt:
        print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
