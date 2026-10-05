"""OIDC bearer-token authentication and authorization."""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Callable
from dataclasses import dataclass

import httpx
import jwt
from jwt import InvalidTokenError, PyJWKSet

from .config import OidcConfig
from .contracts import AuthorizationScope, effective_authorization_scopes


class AuthenticationError(RuntimeError):
    """A bearer token could not be authenticated."""


class PermissionDeniedError(RuntimeError):
    """An authenticated principal lacks a required scope."""


@dataclass(frozen=True)
class Principal:
    subject: str
    scopes: frozenset[AuthorizationScope]
    expires_at: int

    def require(self, scope: AuthorizationScope) -> None:
        if scope not in self.scopes:
            raise PermissionDeniedError(f"scope {scope.value!r} is required")


class OidcAuthenticator:
    def __init__(
        self,
        config: OidcConfig,
        *,
        clock: Callable[[], float] | None = None,
        http_client: httpx.AsyncClient | None = None,
    ):
        self.config = config
        self._clock = clock or time.time
        self._http_client = http_client
        self._owns_http_client = http_client is None
        self._jwks: PyJWKSet | None = None
        self._jwks_expires_at = 0.0
        self._cache_lock = asyncio.Lock()

    async def close(self) -> None:
        if self._owns_http_client and self._http_client is not None:
            await self._http_client.aclose()
        self._http_client = None

    async def authenticate(self, authorization: str | None) -> Principal:
        if authorization is None:
            raise AuthenticationError("bearer authentication is required")
        scheme, separator, token = authorization.partition(" ")
        if not separator or scheme.lower() != "bearer" or not token or " " in token:
            raise AuthenticationError("invalid bearer authorization header")
        try:
            header = jwt.get_unverified_header(token)
            if header.get("alg") != "RS256":
                raise AuthenticationError("token algorithm must be RS256")
            kid = header.get("kid")
            if type(kid) is not str or not kid:
                raise AuthenticationError("token header must contain kid")
            key = await self._signing_key(kid)
            claims = jwt.decode(
                token,
                key=key,
                algorithms=["RS256"],
                audience=self.config.audience,
                issuer=self.config.issuer,
                leeway=self.config.clock_skew_seconds,
                options={"require": ["iss", "aud", "sub", "exp", "iat"]},
            )
        except AuthenticationError:
            raise
        except (InvalidTokenError, KeyError, TypeError, ValueError) as exc:
            raise AuthenticationError("bearer token validation failed") from exc

        subject = claims.get("sub")
        if type(subject) is not str or not subject.strip():
            raise AuthenticationError("token subject must be a nonempty string")
        expires_at = claims.get("exp")
        issued_at = claims.get("iat")
        if type(expires_at) is not int or type(issued_at) is not int:
            raise AuthenticationError("token exp and iat claims must be integer timestamps")
        scope_claim = claims.get("scope", "")
        if type(scope_claim) is not str:
            raise AuthenticationError("token scope claim must be a space-delimited string")
        scopes = effective_authorization_scopes(scope_claim.split())
        return Principal(subject=subject, scopes=scopes, expires_at=expires_at)

    async def _signing_key(self, kid: str):
        async with self._cache_lock:
            now = self._clock()
            if self._jwks is None or now >= self._jwks_expires_at:
                await self._refresh_jwks(now)
            key = self._find_key(kid)
            if key is None and self.config.jwks_url is not None:
                await self._refresh_jwks(now)
                key = self._find_key(kid)
            if key is None:
                raise AuthenticationError("token signing key is not available")
            return key.key

    def _find_key(self, kid: str):
        if self._jwks is None:
            return None
        return next((key for key in self._jwks.keys if key.key_id == kid), None)

    async def _refresh_jwks(self, now: float) -> None:
        try:
            if self.config.jwks_path is not None:
                raw = self.config.jwks_path.read_text(encoding="utf-8")
                document = json.loads(raw)
            else:
                if self._http_client is None:
                    self._http_client = httpx.AsyncClient(timeout=self.config.http_timeout_seconds)
                assert self.config.jwks_url is not None
                response = await self._http_client.get(self.config.jwks_url)
                response.raise_for_status()
                document = response.json()
            if type(document) is not dict:
                raise ValueError("JWKS document must be an object")
            self._jwks = PyJWKSet.from_dict(document)
            if not self._jwks.keys:
                raise ValueError("JWKS contains no keys")
        except (OSError, json.JSONDecodeError, InvalidTokenError, httpx.HTTPError, TypeError, ValueError) as exc:
            self._jwks = None
            self._jwks_expires_at = 0
            raise AuthenticationError("could not load OIDC signing keys") from exc
        self._jwks_expires_at = now + self.config.cache_seconds
