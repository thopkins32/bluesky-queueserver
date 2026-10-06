"""Generate self-contained material for a local QueueServer V2 demo.

Writes into ``demo/state/``:

* a self-signed TLS certificate and key for ``127.0.0.1``;
* an RSA signing key and the matching JWKS document (stands in for the
  identity provider);
* bearer tokens for ``viewer`` (read), ``operator`` (control), ``admin``
  (admin) and ``forged`` (signed by an unrelated key);
* ``controller.yml`` and ``worker.yml`` pointing at the pixi environment.

Run with ``pixi run -e py312 python demo/setup.py``.
"""

from __future__ import annotations

import hashlib
import ipaddress
import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import jwt
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from jwt.algorithms import RSAAlgorithm

ISSUER = "https://identity.demo.nsls2.bnl.gov/"
AUDIENCE = "bluesky-queueserver-v2"
TOKEN_TTL = timedelta(hours=8)
SCOPES = {
    "viewer": "queueserver:read",
    "operator": "queueserver:control",
    "admin": "queueserver:admin",
}

STATE = Path(__file__).resolve().parent / "state"


def write_private_key(path: Path, key: rsa.RSAPrivateKey) -> None:
    path.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    path.chmod(0o600)


def write_tls_material() -> None:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])
    now = datetime.now(UTC)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=5))
        .not_valid_after(now + timedelta(days=30))
        .add_extension(
            x509.SubjectAlternativeName(
                [x509.DNSName("localhost"), x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]
            ),
            critical=False,
        )
        .sign(key, hashes.SHA256())
    )
    (STATE / "server.crt").write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
    write_private_key(STATE / "server.key", key)


def mint(key: rsa.RSAPrivateKey, *, kid: str, subject: str, scope: str) -> str:
    now = datetime.now(UTC)
    return jwt.encode(
        {
            "iss": ISSUER,
            "aud": AUDIENCE,
            "sub": subject,
            "iat": int(now.timestamp()),
            "exp": int((now + TOKEN_TTL).timestamp()),
            "scope": scope,
        },
        key,
        algorithm="RS256",
        headers={"kid": kid},
    )


def write_identity_material() -> None:
    signing_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    jwk = json.loads(RSAAlgorithm.to_jwk(signing_key.public_key()))
    jwk["kid"] = "demo-idp-key"
    (STATE / "jwks.json").write_text(json.dumps({"keys": [jwk]}, indent=2), encoding="utf-8")
    for subject, scope in SCOPES.items():
        (STATE / f"token.{subject}").write_text(
            mint(signing_key, kid="demo-idp-key", subject=subject, scope=scope)
        )
    # A syntactically valid token signed by a key the controller has never seen.
    rogue_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    (STATE / "token.forged").write_text(
        mint(rogue_key, kid="demo-idp-key", subject="operator", scope="queueserver:admin")
    )


def write_configs() -> None:
    bin_dir = Path(sys.executable).parent
    repo_root = STATE.parent.parent
    lock_digest = hashlib.sha256((repo_root / "pixi.lock").read_bytes()).hexdigest()
    worker_config = STATE / "worker.yml"
    worker_config.write_text(
        f"provider: simulated-count\nenvironment_lock_sha256: {lock_digest}\n",
        encoding="utf-8",
    )
    (STATE / "controller.yml").write_text(
        f"""instrument_id: demo-sim

database_path: {STATE / "controller.sqlite3"}

worker:
  command:
    - {bin_dir / "qserver-v2-worker"}
    - --config
    - {worker_config}
  startup_timeout_seconds: 20.0
  heartbeat_interval_seconds: 1.0
  heartbeat_timeout_seconds: 5.0

oidc:
  issuer: {ISSUER}
  audience: {AUDIENCE}
  jwks_path: {STATE / "jwks.json"}

tls:
  certificate_path: {STATE / "server.crt"}
  private_key_path: {STATE / "server.key"}

# Allows a local JWKS file instead of a JWKS URL. Simulator-only switch.
offline_simulator: true
""",
        encoding="utf-8",
    )


def main() -> int:
    STATE.mkdir(exist_ok=True)
    for stale in STATE.glob("controller.sqlite3*"):
        stale.unlink()
    write_tls_material()
    write_identity_material()
    write_configs()
    print(f"demo material written to {STATE}")
    print("start the controller with:")
    print(f"  pixi run -e py312 start-qserver-v2 --config {STATE / 'controller.yml'} --port 8443")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
