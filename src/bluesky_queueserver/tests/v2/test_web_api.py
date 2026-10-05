import asyncio
import json
import time
from uuid import uuid4

import httpx
import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from jwt.algorithms import RSAAlgorithm

from bluesky_queueserver.v2.auth import AuthenticationError, OidcAuthenticator
from bluesky_queueserver.v2.config import OidcConfig
from bluesky_queueserver.v2.contracts import SIMULATED_COUNT_DESCRIPTOR, AuthorizationScope, WorkerCatalog
from bluesky_queueserver.v2.controller import ControllerService
from bluesky_queueserver.v2.storage import SQLiteStore
from bluesky_queueserver.v2.web import create_app


def run(coroutine):
    return asyncio.run(coroutine)


def make_authenticator(tmp_path):
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    jwk = json.loads(RSAAlgorithm.to_jwk(private_key.public_key()))
    jwk["kid"] = "test-key"
    jwks_path = tmp_path / "jwks.json"
    jwks_path.write_text(json.dumps({"keys": [jwk]}), encoding="utf-8")
    config = OidcConfig(
        issuer="https://issuer.example",
        audience="queueserver",
        jwks_path=jwks_path,
    )
    return private_key, OidcAuthenticator(config)


def token_for(private_key, **overrides):
    now = int(time.time())
    claims = {
        "iss": "https://issuer.example",
        "aud": "queueserver",
        "sub": "operator",
        "iat": now,
        "exp": now + 300,
        "scope": "queueserver:control",
    }
    claims.update(overrides)
    return jwt.encode(claims, private_key, algorithm="RS256", headers={"kid": "test-key"})


def test_oidc_authenticator_verifies_identity_and_scope_hierarchy(tmp_path):
    async def scenario():
        private_key, authenticator = make_authenticator(tmp_path)
        try:
            principal = await authenticator.authenticate(f"Bearer {token_for(private_key)}")
            assert principal.subject == "operator"
            assert principal.scopes == {AuthorizationScope.READ, AuthorizationScope.CONTROL}

            admin = await authenticator.authenticate(
                f"Bearer {token_for(private_key, scope='queueserver:admin unrelated')}"
            )
            assert admin.scopes == set(AuthorizationScope)
        finally:
            await authenticator.close()

    run(scenario())


@pytest.mark.parametrize(
    "overrides",
    [
        {"iss": "https://wrong.example"},
        {"aud": "wrong"},
        {"exp": 0},
        {"sub": ""},
        {"sub": None},
        {"iat": "now"},
        {"nbf": int(time.time()) + 60},
    ],
)
def test_oidc_authenticator_rejects_invalid_claims(tmp_path, overrides):
    async def scenario():
        private_key, authenticator = make_authenticator(tmp_path)
        try:
            with pytest.raises(AuthenticationError):
                await authenticator.authenticate(f"Bearer {token_for(private_key, **overrides)}")
        finally:
            await authenticator.close()

    run(scenario())


def test_oidc_authenticator_rejects_bad_signature_algorithm_and_header(tmp_path):
    async def scenario():
        private_key, authenticator = make_authenticator(tmp_path)
        other_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        try:
            for authorization in (
                None,
                "Basic credentials",
                f"Bearer {token_for(other_key)}",
                "Bearer "
                + jwt.encode({"sub": "operator"}, "secret", algorithm="HS256", headers={"kid": "test-key"}),
            ):
                with pytest.raises(AuthenticationError):
                    await authenticator.authenticate(authorization)
        finally:
            await authenticator.close()

    run(scenario())


class ApiWorker:
    def __init__(self, lock_path):
        self.worker_instance_uid = str(uuid4())
        self.lock_path = lock_path
        self.pid = None
        self.catalog = WorkerCatalog(
            protocol_version="2",
            worker_revision="sha256:" + "1" * 64,
            worker_provenance={"provider": "test"},
            operations=[SIMULATED_COUNT_DESCRIPTOR],
        )

    async def start(self):
        return None

    async def start_execution(self, *, attempt, descriptor, parameters):
        raise AssertionError("API contract test must not start execution")

    async def request_safe_stop(self, *, attempt_uid):
        raise AssertionError("API contract test has no active attempt")

    async def close(self):
        return 0

    async def disconnect(self):
        return 0


def test_http_api_authentication_etag_idempotency_and_surface(tmp_path):
    async def scenario():
        private_key, authenticator = make_authenticator(tmp_path)
        store = SQLiteStore(tmp_path / "state.sqlite", instrument_id="instrument")
        service = ControllerService(store, worker=ApiWorker(store.worker_lock_path))
        await service.open()
        client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=create_app(service, authenticator)),
            base_url="https://testserver",
        )
        try:
            control_headers = {"Authorization": f"Bearer {token_for(private_key)}"}
            read_headers = {"Authorization": f"Bearer {token_for(private_key, scope='queueserver:read')}"}

            health = await client.get("/health")
            ready = await client.get("/ready")
            assert health.status_code == 200
            assert ready.status_code == 200
            assert set(ready.json()) == {
                "ready",
                "controller_lock",
                "storage",
                "worker",
                "fencing",
                "dispatch",
            }
            assert "x-request-id" in health.headers
            assert (await client.get("/docs")).status_code == 404
            assert (await client.get("/openapi.json")).status_code == 404
            unauthenticated = await client.get("/api/v2/queue")
            assert unauthenticated.status_code == 401, unauthenticated.text

            forbidden = await client.post(
                "/api/v2/control-lease",
                headers={**read_headers, "Idempotency-Key": "lease-read"},
                json={"ttl_seconds": 300},
            )
            assert forbidden.status_code == 403

            lease_headers = {**control_headers, "Idempotency-Key": "lease-1"}
            acquired = await client.post(
                "/api/v2/control-lease",
                headers=lease_headers,
                json={"ttl_seconds": 300},
            )
            replayed_lease = await client.post(
                "/api/v2/control-lease",
                headers=lease_headers,
                json={"ttl_seconds": 300},
            )
            assert acquired.status_code == replayed_lease.status_code == 201
            assert acquired.json() == replayed_lease.json()
            assert set(acquired.json()) == {"holder", "expires_at"}

            submission = {
                "operation_id": "simulated-count",
                "operation_version": "1",
                "parameters": {"detectors": ["det"], "num": 1, "delay": 0.0},
            }
            missing_precondition = await client.post(
                "/api/v2/operations",
                headers={**control_headers, "Idempotency-Key": "operation-missing"},
                json=submission,
            )
            assert missing_precondition.status_code == 428

            submit_headers = {
                **control_headers,
                "Idempotency-Key": "operation-1",
                "If-Match": '"qrev-0"',
            }
            submitted = await client.post("/api/v2/operations", headers=submit_headers, json=submission)
            assert submitted.status_code == 201
            assert submitted.headers["etag"] == '"qrev-1"'
            operation_uid = submitted.json()["operation_uid"]

            second = await client.post(
                "/api/v2/operations",
                headers={
                    **control_headers,
                    "Idempotency-Key": "operation-2",
                    "If-Match": '"qrev-1"',
                },
                json={**submission, "parameters": {"detectors": ["det"], "num": 2}},
            )
            assert second.status_code == 201
            assert second.headers["etag"] == '"qrev-2"'

            replay = await client.post("/api/v2/operations", headers=submit_headers, json=submission)
            assert replay.status_code == 201
            assert replay.headers["etag"] == '"qrev-1"'
            assert replay.json()["operation_uid"] == operation_uid

            conflict = await client.post(
                "/api/v2/operations",
                headers=submit_headers,
                json={**submission, "parameters": {"detectors": ["det"], "num": 3}},
            )
            assert conflict.status_code == 409
            assert conflict.json()["error"]["code"] == "idempotency_conflict"

            stale = await client.post(
                "/api/v2/operations",
                headers={
                    **control_headers,
                    "Idempotency-Key": "operation-stale",
                    "If-Match": '"qrev-0"',
                },
                json=submission,
            )
            assert stale.status_code == 412
            assert stale.headers["etag"] == '"qrev-2"'

            actor_injection = await client.post(
                "/api/v2/operations",
                headers={
                    **control_headers,
                    "Idempotency-Key": "operation-actor",
                    "If-Match": '"qrev-2"',
                },
                json={**submission, "subject": "mallory"},
            )
            assert actor_injection.status_code == 422
            assert (await client.get("/api/v2/queue", headers=read_headers)).headers["etag"] == '"qrev-2"'

            specification = await client.get("/api/v2/openapi.json", headers=read_headers)
            assert specification.status_code == 200
            paths = set(specification.json()["paths"])
            assert "/api/v2/operations" in paths
            assert not any(
                forbidden_word in path
                for path in paths
                for forbidden_word in ("zmq", "script", "function", "namespace", "worker", "plan")
            )
        finally:
            await client.aclose()
            await authenticator.close()
            await service.close()

    run(scenario())


class AdvancingClock:
    def __init__(self, *values):
        self._values = list(values)
        self._last = values[-1]

    def __call__(self):
        if self._values:
            self._last = self._values.pop(0)
        return self._last


def test_sse_backlog_cursor_heartbeat_and_token_expiry(tmp_path):
    async def scenario():
        private_key, authenticator = make_authenticator(tmp_path)
        store = SQLiteStore(tmp_path / "state.sqlite", instrument_id="instrument")
        service = ControllerService(store, worker=ApiWorker(store.worker_lock_path))
        await service.open()
        now = int(time.time())
        token = token_for(private_key, exp=now + 60)
        headers = {"Authorization": f"Bearer {token}"}
        client = httpx.AsyncClient(
            transport=httpx.ASGITransport(
                app=create_app(
                    service,
                    authenticator,
                    clock=AdvancingClock(now, now + 61),
                    sse_heartbeat_seconds=0.01,
                )
            ),
            base_url="https://testserver",
        )
        try:
            await service.acquire_control_lease(
                principal="operator",
                scopes=[AuthorizationScope.CONTROL],
            )
            response = await client.get("/api/v2/events/stream?after=0", headers=headers)
            assert response.status_code == 200
            assert "event: lease.acquired\n" in response.text

            latest = (await service.events())[-1].event_id
            reconnect_client = httpx.AsyncClient(
                transport=httpx.ASGITransport(
                    app=create_app(
                        service,
                        authenticator,
                        clock=AdvancingClock(now, now, now + 61),
                        sse_heartbeat_seconds=0.01,
                    )
                ),
                base_url="https://testserver",
            )
            try:
                heartbeat = await reconnect_client.get(
                    "/api/v2/events/stream",
                    headers={**headers, "Last-Event-ID": str(latest)},
                )
                assert heartbeat.status_code == 200
                assert ": heartbeat\n\n" in heartbeat.text
                disagreement = await reconnect_client.get(
                    "/api/v2/events/stream?after=0",
                    headers={**headers, "Last-Event-ID": str(latest)},
                )
                assert disagreement.status_code == 422
            finally:
                await reconnect_client.aclose()
        finally:
            await client.aclose()
            await authenticator.close()
            await service.close()

    run(scenario())
