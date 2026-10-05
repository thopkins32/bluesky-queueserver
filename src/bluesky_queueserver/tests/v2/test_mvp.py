import asyncio
import ipaddress
import json
import signal
import socket
import ssl
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import jwt
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from jwt.algorithms import RSAAlgorithm


def run(coroutine):
    return asyncio.run(coroutine)


def allocate_loopback_port():
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def write_identity_material(tmp_path):
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = issuer = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])
    certificate = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(private_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.now(UTC) - timedelta(minutes=1))
        .not_valid_after(datetime.now(UTC) + timedelta(hours=1))
        .add_extension(
            x509.SubjectAlternativeName(
                [x509.DNSName("localhost"), x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]
            ),
            critical=False,
        )
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(private_key, hashes.SHA256())
    )
    certificate_path = tmp_path / "server.crt"
    certificate_path.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
    private_key_path = tmp_path / "server.key"
    private_key_path.write_bytes(
        private_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    jwk = json.loads(RSAAlgorithm.to_jwk(private_key.public_key()))
    jwk["kid"] = "smoke-key"
    jwks_path = tmp_path / "jwks.json"
    jwks_path.write_text(json.dumps({"keys": [jwk]}), encoding="utf-8")
    return private_key, certificate_path, private_key_path, jwks_path


async def start_controller(config_path, port):
    executable = Path(sys.executable).with_name("start-qserver-v2")
    process = await asyncio.create_subprocess_exec(
        str(executable),
        "--config",
        str(config_path),
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    assert process.stdout is not None
    output = []
    while True:
        line = await asyncio.wait_for(process.stdout.readline(), timeout=20)
        if not line:
            raise AssertionError(f"controller exited before startup: {b''.join(output).decode(errors='replace')}")
        output.append(line)
        if b"Uvicorn running on" in line:
            return process


async def stop_controller(process):
    if process.returncode is None:
        process.send_signal(signal.SIGTERM)
        await asyncio.wait_for(process.wait(), timeout=30)
    assert process.returncode in {0, -signal.SIGTERM}


def test_https_simulated_count_smoke(tmp_path):
    async def scenario():
        private_key, certificate_path, private_key_path, jwks_path = write_identity_material(tmp_path)
        worker_config = tmp_path / "worker.yml"
        worker_config.write_text(
            "provider: simulated-count\nenvironment_lock_sha256: " + "a" * 64 + "\n",
            encoding="utf-8",
        )
        database_path = tmp_path / "state.sqlite"
        worker_executable = Path(sys.executable).with_name("qserver-v2-worker")
        controller_config = tmp_path / "controller.yml"
        controller_config.write_text(
            f"""
instrument_id: simulator
database_path: {database_path}
worker:
  command: [{worker_executable}, --config, {worker_config}]
oidc:
  issuer: https://issuer.example
  audience: queueserver
  jwks_path: {jwks_path}
tls:
  certificate_path: {certificate_path}
  private_key_path: {private_key_path}
offline_simulator: true
""",
            encoding="utf-8",
        )
        now = int(datetime.now(UTC).timestamp())
        token = jwt.encode(
            {
                "iss": "https://issuer.example",
                "aud": "queueserver",
                "sub": "operator",
                "iat": now,
                "exp": now + 300,
                "scope": "queueserver:control",
            },
            private_key,
            algorithm="RS256",
            headers={"kid": "smoke-key"},
        )
        headers = {"Authorization": f"Bearer {token}"}
        port = allocate_loopback_port()
        base_url = f"https://127.0.0.1:{port}"
        ssl_context = ssl.create_default_context(cafile=str(certificate_path))

        process = await start_controller(controller_config, port)
        operation_uid = None
        execution_uid = None
        try:
            async with httpx.AsyncClient(base_url=base_url, verify=ssl_context, timeout=20) as client:
                ready = await client.get("/ready")
                assert ready.status_code == 200
                lease = await client.post(
                    "/api/v2/control-lease",
                    headers={**headers, "Idempotency-Key": "smoke-lease"},
                    json={"ttl_seconds": 300},
                )
                assert lease.status_code == 201
                submitted = await client.post(
                    "/api/v2/operations",
                    headers={
                        **headers,
                        "Idempotency-Key": "smoke-operation",
                        "If-Match": '"qrev-0"',
                    },
                    json={
                        "operation_id": "simulated-count",
                        "operation_version": "1",
                        "parameters": {"detectors": ["det"], "num": 1, "delay": 0.0},
                    },
                )
                assert submitted.status_code == 201, submitted.text
                operation_uid = submitted.json()["operation_uid"]
                started = await client.post(
                    "/api/v2/queue-executions",
                    headers={
                        **headers,
                        "Idempotency-Key": "smoke-execution",
                        "If-Match": submitted.headers["etag"],
                    },
                    json={},
                )
                assert started.status_code == 201, started.text
                execution_uid = started.json()["queue_execution_uid"]

                seen = []
                async with client.stream("GET", "/api/v2/events/stream?after=0", headers=headers) as stream:
                    assert stream.status_code == 200
                    async for line in stream.aiter_lines():
                        if line.startswith("event: "):
                            seen.append(line.removeprefix("event: "))
                        if "operation.succeeded" in seen:
                            break
                assert "operation.running" in seen
                assert "operation.succeeded" in seen

                operation = await client.get(f"/api/v2/operations/{operation_uid}", headers=headers)
                execution = await client.get(f"/api/v2/queue-executions/{execution_uid}", headers=headers)
                assert operation.json()["state"] == "succeeded"
                assert len(operation.json()["result"]["run_uids"]) == 1
                assert execution.json()["state"] == "completed"
        finally:
            await stop_controller(process)

        restarted = await start_controller(controller_config, port)
        try:
            async with httpx.AsyncClient(base_url=base_url, verify=ssl_context, timeout=20) as client:
                operation = await client.get(f"/api/v2/operations/{operation_uid}", headers=headers)
                events = await client.get("/api/v2/events?after=0&limit=100", headers=headers)
                assert operation.status_code == 200
                assert operation.json()["state"] == "succeeded"
                assert any(event["event_type"] == "operation.succeeded" for event in events.json())
        finally:
            await stop_controller(restarted)

    run(scenario())
