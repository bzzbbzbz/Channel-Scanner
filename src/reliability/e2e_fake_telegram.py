"""Content-free Telegram Bot API fake for the isolated BL-22 stage-6 E2E."""

from __future__ import annotations

import hashlib
import json
import time
import asyncio
from dataclasses import dataclass, field
from urllib.parse import parse_qs

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse


@dataclass
class FakeState:
    plan: list[int | str] = field(default_factory=list)
    calls: list[dict[str, int | str | bool]] = field(default_factory=list)
    next_message_id: int = 1000


state = FakeState()
app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)


@app.get("/health")
async def health() -> dict[str, bool]:
    return {"ok": True}


@app.post("/control/reset")
async def reset(request: Request) -> dict[str, bool]:
    payload = await request.json()
    plan = payload.get("plan", [])
    if not isinstance(plan, list) or any(code not in {200, 429, 500, "accept_timeout"} for code in plan):
        return JSONResponse({"ok": False}, status_code=422)
    state.plan = list(plan)
    state.calls.clear()
    state.next_message_id = 1000
    return {"ok": True}


@app.get("/control/state")
async def get_state() -> dict:
    return {"calls": list(state.calls), "remaining_plan": len(state.plan)}


@app.post("/bot{token}/sendMessage")
async def send_message(token: str, request: Request) -> JSONResponse:
    del token
    raw = await request.body()
    content_type = request.headers.get("content-type", "")
    if "application/json" in content_type:
        values = json.loads(raw or b"{}")
    else:
        parsed = parse_qs(raw.decode())
        values = {key: entries[-1] for key, entries in parsed.items()}
    text = str(values.get("text", ""))
    chat_id = int(values.get("chat_id", 0))
    status = state.plan.pop(0) if state.plan else 200
    call: dict[str, int | str | bool] = {
        "ordinal": len(state.calls) + 1,
        "status": status,
        "chat_id": chat_id,
        "text_sha256": hashlib.sha256(text.encode()).hexdigest(),
        "text_bytes": len(text.encode()),
    }
    state.calls.append(call)
    if status == "accept_timeout":
        message_id = state.next_message_id
        state.next_message_id += 1
        call.update(accepted=True, message_id=message_id)
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            raise
        return JSONResponse({"ok": False}, status_code=504)
    if status == 500:
        return JSONResponse(
            {"ok": False, "error_code": 500, "description": "Internal Server Error"},
            status_code=500,
        )
    if status == 429:
        return JSONResponse(
            {
                "ok": False,
                "error_code": 429,
                "description": "Too Many Requests",
                "parameters": {"retry_after": 30},
            },
            status_code=429,
        )

    message_id = state.next_message_id
    state.next_message_id += 1
    call.update(accepted=True, message_id=message_id)
    return JSONResponse(
        {
            "ok": True,
            "result": {
                "message_id": message_id,
                "date": int(time.time()),
                "chat": {"id": chat_id, "type": "private"},
                "text": text,
            },
        }
    )


def main() -> None:
    uvicorn.run(app, host="0.0.0.0", port=8081, log_level="warning", access_log=False)


if __name__ == "__main__":
    main()
