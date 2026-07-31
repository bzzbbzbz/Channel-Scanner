"""FastAPI application for the authenticated read-only admin dashboard."""

from __future__ import annotations

from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable
from urllib.parse import parse_qs

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from starlette.middleware.sessions import SessionMiddleware

from src.admin.passwords import verify_password
from src.admin.service import AdminDashboardService
from src.config.settings import AdminSettings


class LoginAttemptLimiter:
    """Small process-local guard against repeated password guessing."""

    def __init__(self, limit: int = 5, window: timedelta = timedelta(minutes=15)) -> None:
        self._limit = limit
        self._window = window
        self._attempts: dict[str, deque[datetime]] = defaultdict(deque)

    def allowed(self, client: str, now: datetime) -> bool:
        attempts = self._recent(client, now)
        return len(attempts) < self._limit

    def failed(self, client: str, now: datetime) -> None:
        self._recent(client, now).append(now)

    def succeeded(self, client: str) -> None:
        self._attempts.pop(client, None)

    def _recent(self, client: str, now: datetime) -> deque[datetime]:
        attempts = self._attempts[client]
        boundary = now - self._window
        while attempts and attempts[0] <= boundary:
            attempts.popleft()
        return attempts


def create_admin_app(settings: AdminSettings, session_factory) -> FastAPI:
    """Build the dashboard ASGI app without exposing application mutation services."""
    if not settings.username or not settings.password_hash or not settings.session_secret:
        raise ValueError("ADMIN_USERNAME, ADMIN_PASSWORD_HASH, and ADMIN_SESSION_SECRET are required")

    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
    app.add_middleware(
        SessionMiddleware,
        secret_key=settings.session_secret,
        https_only=settings.secure_cookies,
        same_site="lax",
    )
    dashboard = AdminDashboardService(session_factory)
    limiter = LoginAttemptLimiter()

    def authenticated(request: Request) -> bool:
        return request.session.get("admin") == settings.username

    def client_key(request: Request) -> str:
        return request.client.host if request.client else "unknown"

    @app.get("/", include_in_schema=False)
    async def root() -> RedirectResponse:
        return RedirectResponse("/admin", status_code=303)

    @app.get("/admin/login", response_class=HTMLResponse, include_in_schema=False)
    async def login_page(request: Request):
        if authenticated(request):
            return RedirectResponse("/admin", status_code=303)
        return HTMLResponse(_login_page())

    @app.post("/admin/login", include_in_schema=False)
    async def login(request: Request) -> RedirectResponse:
        now = datetime.now(timezone.utc)
        client = client_key(request)
        if not limiter.allowed(client, now):
            return RedirectResponse("/admin/login?error=rate", status_code=303)
        fields = parse_qs((await request.body()).decode())
        username = fields.get("username", [""])[0]
        password = fields.get("password", [""])[0]
        if username != settings.username or not verify_password(password, settings.password_hash):
            limiter.failed(client, now)
            return RedirectResponse("/admin/login?error=invalid", status_code=303)
        limiter.succeeded(client)
        request.session["admin"] = settings.username
        return RedirectResponse("/admin", status_code=303)

    @app.post("/admin/logout", include_in_schema=False)
    async def logout(request: Request) -> RedirectResponse:
        request.session.clear()
        return RedirectResponse("/admin/login", status_code=303)

    @app.get("/admin", response_class=HTMLResponse, include_in_schema=False)
    async def dashboard_page(request: Request):
        if not authenticated(request):
            return RedirectResponse("/admin/login", status_code=303)
        return HTMLResponse(_dashboard_page())

    @app.get("/admin/api/metrics", include_in_schema=False)
    async def metrics(request: Request, period: str = "24h", start: str | None = None, end: str | None = None) -> JSONResponse:
        if not authenticated(request):
            raise HTTPException(status_code=401, detail="Authentication required")
        range_start, range_end = await _resolve_range(dashboard, period, start, end)
        return JSONResponse(await dashboard.metrics(range_start, range_end))

    return app


async def _resolve_range(dashboard: AdminDashboardService, period: str, start: str | None, end: str | None) -> tuple[datetime, datetime]:
    now = datetime.now(timezone.utc)
    if period == "24h":
        return now - timedelta(hours=24), now
    if period == "7d":
        return now - timedelta(days=7), now
    if period == "all":
        return (await dashboard.first_event_at()) or (now - timedelta(hours=24)), now
    if period != "custom" or not start or not end:
        raise HTTPException(status_code=422, detail="Choose a valid dashboard period")
    try:
        range_start = _parse_datetime(start)
        range_end = _parse_datetime(end)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail="Use ISO-8601 UTC timestamps") from exc
    if range_start >= range_end or range_end > now + timedelta(minutes=1):
        raise HTTPException(status_code=422, detail="The selected date range is invalid")
    return range_start, range_end


def _parse_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("Timezone is required")
    return parsed.astimezone(timezone.utc)


def _login_page() -> str:
    return """<!doctype html><html lang="ru"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Channel Scanner Admin</title><style>body{margin:0;min-height:100vh;display:grid;place-items:center;background:#0c1220;color:#edf2f7;font:16px system-ui,sans-serif}.box{width:min(390px,calc(100% - 40px));padding:32px;border:1px solid #2a3a55;border-radius:18px;background:#131d30;box-sizing:border-box}h1{font-size:24px;margin:0 0 4px}p{color:#aebed3;margin:0 0 28px}label{display:grid;gap:7px;margin:16px 0;color:#dce6f5}input{border:1px solid #41536f;border-radius:9px;background:#0c1220;color:#fff;padding:12px;font:inherit}button{margin-top:10px;width:100%;border:0;border-radius:9px;padding:12px;background:#55d6be;color:#06251f;font-weight:700;font:inherit;cursor:pointer}</style></head><body><main class="box"><h1>Channel Scanner</h1><p>Административная панель</p><form method="post" action="/admin/login"><label>Логин<input name="username" autocomplete="username" required></label><label>Пароль<input name="password" type="password" autocomplete="current-password" required></label><button type="submit">Войти</button></form></main></body></html>"""


def _dashboard_page() -> str:
    return (Path(__file__).with_name("dashboard.html")).read_text(encoding="utf-8")
