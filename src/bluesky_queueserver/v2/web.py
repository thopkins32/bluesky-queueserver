"""Authenticated HTTPS and SSE public boundary."""

import json
import time
from collections.abc import Awaitable, Callable, Mapping
from typing import Annotated
from uuid import uuid4

from fastapi import Depends, FastAPI, Header, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel

from .auth import AuthenticationError, OidcAuthenticator, PermissionDeniedError, Principal
from .contracts import (
    AttemptView,
    AuthorizationScope,
    CatalogView,
    ControlLeaseView,
    ErrorBody,
    ErrorResponse,
    HealthView,
    LeaseRequest,
    MutationReason,
    OperationSubmission,
    OperationView,
    QueueExecutionStart,
    QueueExecutionView,
    QueueReorder,
    QueueSnapshot,
    ReadinessView,
    RecoveryAcknowledgement,
)
from .controller import (
    AuthorizationError,
    ControllerService,
    InvalidPreconditionError,
    MissingPreconditionError,
    queue_etag,
)
from .storage import (
    IdempotencyConflictError,
    IdempotencyRequest,
    LeaseConflictError,
    LeaseExpiredError,
    LeaseOwnershipError,
    QueueValidationError,
    RecordNotFoundError,
    RevisionConflictError,
    StateConflictError,
    StorageError,
    StoredHttpResponse,
)


class MissingIdempotencyKeyError(ValueError):
    """A mutating request omitted its required idempotency key."""


def _model_payload(model: BaseModel) -> dict[str, object]:
    return json.loads(model.model_dump_json())


def create_app(
    service: ControllerService,
    authenticator: OidcAuthenticator,
    *,
    clock: Callable[[], float] = time.time,
    sse_heartbeat_seconds: float = 15.0,
) -> FastAPI:
    if sse_heartbeat_seconds <= 0:
        raise ValueError("SSE heartbeat interval must be positive")
    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
    bearer = HTTPBearer(auto_error=False)

    @app.middleware("http")
    async def add_request_id(request: Request, call_next):
        request.state.request_id = str(uuid4())
        response = await call_next(request)
        response.headers["X-Request-ID"] = request.state.request_id
        return response

    def error_response(request: Request, *, status: int, code: str, message: str, details=None):
        body = ErrorResponse(
            error=ErrorBody(
                code=code,
                message=message,
                details=details,
                request_id=request.state.request_id,
            )
        )
        headers = {}
        if status == 401:
            headers["WWW-Authenticate"] = "Bearer"
        if isinstance(details, dict) and "current_revision" in details:
            headers["ETag"] = queue_etag(details["current_revision"])
        return JSONResponse(status_code=status, content=_model_payload(body), headers=headers)

    @app.exception_handler(AuthenticationError)
    async def authentication_error(request: Request, exc: AuthenticationError):
        return error_response(request, status=401, code="authentication_failed", message=str(exc))

    @app.exception_handler(PermissionDeniedError)
    @app.exception_handler(AuthorizationError)
    @app.exception_handler(LeaseOwnershipError)
    @app.exception_handler(LeaseExpiredError)
    async def authorization_error(request: Request, exc: Exception):
        return error_response(request, status=403, code="forbidden", message=str(exc))

    @app.exception_handler(RecordNotFoundError)
    async def not_found(request: Request, exc: RecordNotFoundError):
        return error_response(request, status=404, code="not_found", message=str(exc))

    @app.exception_handler(RevisionConflictError)
    async def stale_revision(request: Request, exc: RevisionConflictError):
        return error_response(
            request,
            status=412,
            code="stale_revision",
            message=str(exc),
            details={"current_revision": exc.current_revision},
        )

    @app.exception_handler(MissingPreconditionError)
    async def missing_precondition(request: Request, exc: MissingPreconditionError):
        return error_response(request, status=428, code="precondition_required", message=str(exc))

    @app.exception_handler(InvalidPreconditionError)
    @app.exception_handler(MissingIdempotencyKeyError)
    async def invalid_request(request: Request, exc: Exception):
        return error_response(request, status=422, code="invalid_request", message=str(exc))

    @app.exception_handler(RequestValidationError)
    async def request_validation(request: Request, exc: RequestValidationError):
        return error_response(
            request,
            status=422,
            code="validation_error",
            message="request validation failed",
            details={"errors": json.loads(json.dumps(exc.errors(), default=str))},
        )

    @app.exception_handler(LeaseConflictError)
    @app.exception_handler(IdempotencyConflictError)
    @app.exception_handler(StateConflictError)
    @app.exception_handler(QueueValidationError)
    async def conflict(request: Request, exc: Exception):
        code = "idempotency_conflict" if isinstance(exc, IdempotencyConflictError) else "state_conflict"
        return error_response(request, status=409, code=code, message=str(exc))

    @app.exception_handler(StorageError)
    @app.exception_handler(RuntimeError)
    async def unavailable(request: Request, exc: Exception):
        return error_response(request, status=503, code="unavailable", message=str(exc))

    async def authenticate(
        credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer)],
    ) -> Principal:
        authorization = None if credentials is None else f"{credentials.scheme} {credentials.credentials}"
        return await authenticator.authenticate(authorization)

    def require_scope(required: AuthorizationScope):
        async def dependency(principal: Annotated[Principal, Depends(authenticate)]) -> Principal:
            principal.require(required)
            return principal

        return dependency

    read_principal = require_scope(AuthorizationScope.READ)
    control_principal = require_scope(AuthorizationScope.CONTROL)
    admin_principal = require_scope(AuthorizationScope.ADMIN)

    async def idempotent(
        request: Request,
        principal: Principal,
        *,
        key: str | None,
        if_match: str | None,
        body: object,
        action: Callable[[], Awaitable[tuple[int, Mapping[str, object], str | None]]],
    ) -> JSONResponse:
        if key is None:
            raise MissingIdempotencyKeyError("Idempotency-Key is required")
        context = IdempotencyRequest(
            principal=principal.subject,
            method=request.method,
            target=request.url.path,
            key=key,
            body=body,
            if_match=if_match,
        )

        async def invoke(connection):
            status, response_body, etag = await action()
            return StoredHttpResponse(status=status, body=response_body, etag=etag)

        result = await service.store.run_idempotent_mutation(context, invoke)
        headers = {} if result.response.etag is None else {"ETag": result.response.etag}
        return JSONResponse(
            status_code=result.response.status,
            content=dict(result.response.body),
            headers=headers,
        )

    @app.get("/health", response_model=HealthView)
    async def health():
        return service.health()

    @app.get("/ready", response_model=ReadinessView)
    async def ready():
        view = await service.readiness()
        return JSONResponse(status_code=200 if view.ready else 503, content=_model_payload(view))

    @app.get("/api/v2/openapi.json", include_in_schema=False)
    async def openapi(principal: Annotated[Principal, Depends(read_principal)]):
        return JSONResponse(app.openapi())

    @app.get("/api/v2/catalog", response_model=CatalogView)
    async def catalog(principal: Annotated[Principal, Depends(read_principal)]):
        return service.catalog_view()

    @app.get("/api/v2/control-lease", response_model=ControlLeaseView)
    async def control_lease(principal: Annotated[Principal, Depends(read_principal)]):
        return await service.control_lease()

    @app.get("/api/v2/queue", response_model=QueueSnapshot)
    async def queue(request: Request, principal: Annotated[Principal, Depends(read_principal)]):
        view = await service.queue_view()
        return JSONResponse(content=_model_payload(view), headers={"ETag": queue_etag(view.revision)})

    @app.get("/api/v2/queue-executions/{queue_execution_uid}", response_model=QueueExecutionView)
    async def queue_execution(
        queue_execution_uid: str,
        request: Request,
        principal: Annotated[Principal, Depends(read_principal)],
    ):
        view = await service.queue_execution_view(queue_execution_uid)
        revision = await service.store.current_revision()
        return JSONResponse(content=_model_payload(view), headers={"ETag": queue_etag(revision)})

    @app.get("/api/v2/operations/{operation_uid}", response_model=OperationView)
    async def operation(
        operation_uid: str,
        request: Request,
        principal: Annotated[Principal, Depends(read_principal)],
    ):
        view = await service.operation_view(operation_uid)
        revision = await service.store.current_revision()
        return JSONResponse(content=_model_payload(view), headers={"ETag": queue_etag(revision)})

    @app.get("/api/v2/attempts/{attempt_uid}", response_model=AttemptView)
    async def attempt(
        attempt_uid: str,
        request: Request,
        principal: Annotated[Principal, Depends(read_principal)],
    ):
        view = await service.attempt_view(attempt_uid)
        revision = await service.store.current_revision()
        return JSONResponse(content=_model_payload(view), headers={"ETag": queue_etag(revision)})

    @app.get("/api/v2/events")
    async def events(
        principal: Annotated[Principal, Depends(read_principal)],
        after: Annotated[int, Query(ge=0)] = 0,
        limit: Annotated[int, Query(ge=1, le=1000)] = 100,
    ):
        return [_model_payload(event) for event in await service.events(after=after, limit=limit)]

    @app.get("/api/v2/events/stream")
    async def event_stream(
        request: Request,
        principal: Annotated[Principal, Depends(read_principal)],
        after: Annotated[int | None, Query(ge=0)] = None,
        last_event_id: Annotated[str | None, Header(alias="Last-Event-ID")] = None,
    ):
        header_cursor = None
        if last_event_id is not None:
            try:
                header_cursor = int(last_event_id)
            except ValueError as exc:
                raise InvalidPreconditionError("Last-Event-ID must be a nonnegative integer") from exc
            if header_cursor < 0:
                raise InvalidPreconditionError("Last-Event-ID must be a nonnegative integer")
        if after is not None and header_cursor is not None and after != header_cursor:
            raise InvalidPreconditionError("after and Last-Event-ID disagree")
        cursor = after if after is not None else (header_cursor or 0)

        async def stream():
            nonlocal cursor
            while clock() < principal.expires_at:
                events = await service.events(after=cursor, limit=100)
                if not events:
                    remaining = principal.expires_at - clock()
                    if remaining <= 0:
                        return
                    events = await service.wait_for_events(
                        after=cursor,
                        limit=100,
                        timeout=min(sse_heartbeat_seconds, remaining),
                    )
                    if not events:
                        yield ": heartbeat\n\n"
                        continue
                for event in events:
                    cursor = event.event_id
                    yield (f"id: {event.event_id}\nevent: {event.event_type}\ndata: {event.model_dump_json()}\n\n")
                if await request.is_disconnected():
                    return

        return StreamingResponse(stream(), media_type="text/event-stream", headers={"Cache-Control": "no-cache"})

    @app.post("/api/v2/control-lease", response_model=ControlLeaseView)
    async def acquire_lease(
        request: Request,
        body: LeaseRequest,
        principal: Annotated[Principal, Depends(control_principal)],
        idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
    ):
        async def action():
            view = await service.acquire_control_lease(
                principal=principal.subject,
                scopes=principal.scopes,
                ttl_seconds=body.ttl_seconds,
            )
            return 201, _model_payload(view), None

        return await idempotent(
            request,
            principal,
            key=idempotency_key,
            if_match=None,
            body=body.model_dump(mode="json"),
            action=action,
        )

    @app.post("/api/v2/control-lease/renew", response_model=ControlLeaseView)
    async def renew_lease(
        request: Request,
        body: LeaseRequest,
        principal: Annotated[Principal, Depends(control_principal)],
        idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
    ):
        async def action():
            view = await service.renew_control_lease(
                principal=principal.subject,
                scopes=principal.scopes,
                ttl_seconds=body.ttl_seconds,
            )
            return 200, _model_payload(view), None

        return await idempotent(
            request, principal, key=idempotency_key, if_match=None, body=body.model_dump(), action=action
        )

    @app.post("/api/v2/control-lease/release")
    async def release_lease(
        request: Request,
        principal: Annotated[Principal, Depends(control_principal)],
        idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
    ):
        async def action():
            await service.release_control_lease(principal=principal.subject, scopes=principal.scopes)
            return 200, {"released": True}, None

        return await idempotent(request, principal, key=idempotency_key, if_match=None, body={}, action=action)

    @app.post("/api/v2/control-lease/override", response_model=ControlLeaseView)
    async def override_lease(
        request: Request,
        body: MutationReason,
        principal: Annotated[Principal, Depends(admin_principal)],
        idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
    ):
        async def action():
            view = await service.override_control_lease(
                administrator=principal.subject,
                scopes=principal.scopes,
                reason=body.reason,
            )
            return 200, _model_payload(view), None

        return await idempotent(
            request, principal, key=idempotency_key, if_match=None, body=body.model_dump(), action=action
        )

    @app.post("/api/v2/operations", response_model=OperationView)
    async def submit_operation(
        request: Request,
        body: OperationSubmission,
        principal: Annotated[Principal, Depends(control_principal)],
        if_match: Annotated[str | None, Header(alias="If-Match")] = None,
        idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
    ):
        async def action():
            record, revision = await service.submit_operation(
                principal=principal.subject,
                scopes=principal.scopes,
                if_match=if_match,
                submission=body,
            )
            view = await service.operation_view(record.operation_uid)
            return 201, _model_payload(view), queue_etag(revision)

        return await idempotent(
            request,
            principal,
            key=idempotency_key,
            if_match=if_match,
            body=body.model_dump(mode="json"),
            action=action,
        )

    @app.delete("/api/v2/operations/{operation_uid}", response_model=OperationView)
    async def cancel_operation(
        operation_uid: str,
        request: Request,
        principal: Annotated[Principal, Depends(control_principal)],
        if_match: Annotated[str | None, Header(alias="If-Match")] = None,
        idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
    ):
        async def action():
            record, revision = await service.cancel_operation(
                principal=principal.subject,
                scopes=principal.scopes,
                if_match=if_match,
                operation_uid=operation_uid,
            )
            view = await service.operation_view(record.operation_uid)
            return 200, _model_payload(view), queue_etag(revision)

        return await idempotent(request, principal, key=idempotency_key, if_match=if_match, body={}, action=action)

    @app.put("/api/v2/operations/{operation_uid}", response_model=OperationView)
    async def replace_operation(
        operation_uid: str,
        request: Request,
        body: OperationSubmission,
        principal: Annotated[Principal, Depends(control_principal)],
        if_match: Annotated[str | None, Header(alias="If-Match")] = None,
        idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
    ):
        async def action():
            record, revision = await service.replace_operation(
                principal=principal.subject,
                scopes=principal.scopes,
                if_match=if_match,
                operation_uid=operation_uid,
                submission=body,
            )
            view = await service.operation_view(record.operation_uid)
            return 200, _model_payload(view), queue_etag(revision)

        return await idempotent(
            request,
            principal,
            key=idempotency_key,
            if_match=if_match,
            body=body.model_dump(mode="json"),
            action=action,
        )

    @app.post("/api/v2/queue/reorder", response_model=QueueSnapshot)
    async def reorder_queue(
        request: Request,
        body: QueueReorder,
        principal: Annotated[Principal, Depends(control_principal)],
        if_match: Annotated[str | None, Header(alias="If-Match")] = None,
        idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
    ):
        async def action():
            snapshot = await service.reorder_queue(
                principal=principal.subject,
                scopes=principal.scopes,
                if_match=if_match,
                reorder=body,
            )
            view = await service.queue_view()
            return 200, _model_payload(view), queue_etag(snapshot.revision)

        return await idempotent(
            request,
            principal,
            key=idempotency_key,
            if_match=if_match,
            body=body.model_dump(mode="json"),
            action=action,
        )

    @app.post("/api/v2/queue-executions", response_model=QueueExecutionView)
    async def start_queue_execution(
        request: Request,
        body: QueueExecutionStart,
        principal: Annotated[Principal, Depends(control_principal)],
        if_match: Annotated[str | None, Header(alias="If-Match")] = None,
        idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
    ):
        async def action():
            record, revision = await service.start_queue_execution(
                principal=principal.subject,
                scopes=principal.scopes,
                if_match=if_match,
                policy=body.policy,
            )
            view = await service.queue_execution_view(record.queue_execution_uid)
            return 201, _model_payload(view), queue_etag(revision)

        return await idempotent(
            request,
            principal,
            key=idempotency_key,
            if_match=if_match,
            body=body.model_dump(mode="json"),
            action=action,
        )

    @app.post("/api/v2/queue-executions/{queue_execution_uid}/stop", response_model=QueueExecutionView)
    async def stop_queue_execution(
        queue_execution_uid: str,
        request: Request,
        principal: Annotated[Principal, Depends(control_principal)],
        if_match: Annotated[str | None, Header(alias="If-Match")] = None,
        idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
    ):
        async def action():
            _, revision = await service.stop_queue_execution(
                principal=principal.subject,
                scopes=principal.scopes,
                if_match=if_match,
                queue_execution_uid=queue_execution_uid,
            )
            view = await service.queue_execution_view(queue_execution_uid)
            return 200, _model_payload(view), queue_etag(revision)

        return await idempotent(request, principal, key=idempotency_key, if_match=if_match, body={}, action=action)

    @app.post("/api/v2/attempts/{attempt_uid}/safe-stop", response_model=AttemptView)
    async def safe_stop(
        attempt_uid: str,
        request: Request,
        principal: Annotated[Principal, Depends(control_principal)],
        if_match: Annotated[str | None, Header(alias="If-Match")] = None,
        idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
    ):
        async def action():
            _, revision = await service.safe_stop_attempt(
                principal=principal.subject,
                scopes=principal.scopes,
                if_match=if_match,
                attempt_uid=attempt_uid,
            )
            view = await service.attempt_view(attempt_uid)
            return 200, _model_payload(view), queue_etag(revision)

        return await idempotent(request, principal, key=idempotency_key, if_match=if_match, body={}, action=action)

    @app.post("/api/v2/recovery/acknowledge", response_model=QueueSnapshot)
    async def acknowledge_recovery(
        request: Request,
        body: RecoveryAcknowledgement,
        principal: Annotated[Principal, Depends(control_principal)],
        if_match: Annotated[str | None, Header(alias="If-Match")] = None,
        idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
    ):
        async def action():
            revision = await service.acknowledge_recovery(
                principal=principal.subject,
                scopes=principal.scopes,
                if_match=if_match,
                acknowledgement=body,
            )
            view = await service.queue_view()
            return 200, _model_payload(view), queue_etag(revision)

        return await idempotent(
            request,
            principal,
            key=idempotency_key,
            if_match=if_match,
            body=body.model_dump(mode="json"),
            action=action,
        )

    return app
