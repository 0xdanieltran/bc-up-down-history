"""DeTrade / BC.Game Up-Down history client (REST + WebSocket)."""
from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import ssl
import threading
import time
import uuid
import zlib
from dataclasses import dataclass
from typing import Any, Callable
from urllib.parse import urlencode

import requests
import websockets

from .db import Database

logger = logging.getLogger(__name__)

API_BASE = "https://api.detrade.com"
WS_URL = "wss://websocket.detrade.com/ws"
# Price ticks grow fast (~0.5s). Pattern analytics only needs settled rounds.
STORE_TICKS = os.getenv("STORE_TICKS", "0").strip().lower() in {"1", "true", "yes"}
TICK_KEEP_LAST = int(os.getenv("TICK_KEEP_LAST", "5000"))
ROUND_KEEP_LAST = int(os.getenv("ROUND_KEEP_LAST", "50000"))
MAINT_EVERY_MESSAGES = int(os.getenv("MAINT_EVERY_MESSAGES", "400"))
TOKEN_MIN_TTL_SEC = int(os.getenv("TOKEN_MIN_TTL_SEC", "3600"))  # reuse until near expiry
RATE_LIMIT_BACKOFF_SEC = int(os.getenv("RATE_LIMIT_BACKOFF_SEC", "90"))

# Process-wide guest token cache (avoids "Frequent visits" / code 20009).
_token_cache: dict[str, Any] = {"token": None, "user_id": None, "expires_at": 0.0, "cooldown_until": 0.0}
_token_lock = threading.Lock()


def _jwt_exp(token: str) -> float | None:
    try:
        parts = token.split(".")
        if len(parts) < 2:
            return None
        payload = parts[1] + "=" * (-len(parts[1]) % 4)
        data = json.loads(base64.urlsafe_b64decode(payload.encode("utf-8")))
        exp = data.get("exp")
        return float(exp) if exp is not None else None
    except Exception:
        return None


class RateLimitedError(RuntimeError):
    """Raised when DeTrade returns frequent-visit / rate-limit codes."""


DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://bc.game",
    "Referer": "https://bc.game/",
    "Content-Type": "application/json",
    "Account-Type": "0",
    "Accept-Language": "en",
    "Device-Id": '""',
}


@dataclass
class PeriodConfig:
    symbol: str
    label: str
    period: int
    period_id: str
    min_amount: float | None = None
    max_amount: float | None = None


def _encode_ws(msg: dict[str, Any]) -> bytes:
    payload = {**msg, "cid": str(uuid.uuid4()), "reqId": str(uuid.uuid4())}
    return zlib.compress(json.dumps(payload, separators=(",", ":")).encode("utf-8"))


def _decode_ws(raw: bytes | str) -> dict[str, Any] | None:
    try:
        if isinstance(raw, str):
            return json.loads(raw)
        try:
            data = zlib.decompress(raw)
        except zlib.error:
            data = raw
        return json.loads(data.decode("utf-8"))
    except Exception:
        logger.exception("Failed to decode websocket payload")
        return None


class DetradeClient:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.token: str | None = None
        self.user_id: str | None = None

    def _request(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        """HTTP helper with retries for flaky edge/CDN disconnects."""
        timeout = kwargs.pop("timeout", 60)
        last_err: Exception | None = None
        for attempt in range(5):
            try:
                r = self.session.request(method, url, timeout=timeout, **kwargs)
                r.raise_for_status()
                return r
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
                last_err = exc
                time.sleep(0.6 * (attempt + 1))
        assert last_err is not None
        raise last_err

    def create_temporary_session(self, *, force: bool = False) -> str:
        """Get a guest token, reusing a process-wide cache when possible."""
        now = time.time()
        with _token_lock:
            if _token_cache["cooldown_until"] > now and not _token_cache.get("token"):
                wait = int(_token_cache["cooldown_until"] - now)
                raise RateLimitedError(
                    f"Rate limited (frequent visits). Retry in ~{max(wait, 1)}s."
                )
            if (
                not force
                and _token_cache.get("token")
                and float(_token_cache.get("expires_at") or 0) - now > TOKEN_MIN_TTL_SEC
            ):
                self.token = _token_cache["token"]
                self.user_id = _token_cache.get("user_id")
                self.session.headers["Authorization"] = self.token
                return self.token

        # Outside lock for network I/O; serialize creations lightly.
        with _token_lock:
            # Double-check after waiting for lock
            now = time.time()
            if (
                not force
                and _token_cache.get("token")
                and float(_token_cache.get("expires_at") or 0) - now > TOKEN_MIN_TTL_SEC
            ):
                self.token = _token_cache["token"]
                self.user_id = _token_cache.get("user_id")
                self.session.headers["Authorization"] = self.token
                return self.token

            last_err: Exception | None = None
            for attempt in range(4):
                now = time.time()
                if _token_cache["cooldown_until"] > now:
                    wait = int(_token_cache["cooldown_until"] - now)
                    raise RateLimitedError(
                        f"Rate limited (frequent visits). Retry in ~{max(wait, 1)}s."
                    )
                try:
                    r = self._request("POST", f"{API_BASE}/api/user/temporary/create", json={})
                    body = r.json()
                    code = body.get("code")
                    if code == 0:
                        data = body["data"]
                        token = data["token"]
                        exp = _jwt_exp(token) or (time.time() + 6 * 3600)
                        _token_cache.update(
                            {
                                "token": token,
                                "user_id": str(data.get("userId")),
                                "expires_at": exp,
                                "cooldown_until": 0.0,
                            }
                        )
                        self.token = token
                        self.user_id = str(data.get("userId"))
                        self.session.headers["Authorization"] = token
                        return token
                    if code == 20009 or "frequent" in str(body.get("msg", "")).lower():
                        # Do NOT sleep here — that would block the FastAPI event loop.
                        # Callers (async scrape loop) wait out the cooldown.
                        backoff = RATE_LIMIT_BACKOFF_SEC * (attempt + 1)
                        _token_cache["cooldown_until"] = time.time() + backoff
                        last_err = RateLimitedError(
                            f"Frequent visits (code 20009). Backing off {backoff}s."
                        )
                        logger.warning("%s", last_err)
                        raise last_err
                    raise RuntimeError(f"temp login failed: {body}")
                except RateLimitedError:
                    raise
                except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
                    last_err = exc
                    time.sleep(1.2 * (attempt + 1))
            if isinstance(last_err, RateLimitedError):
                raise last_err
            raise RuntimeError(f"temp login failed: {last_err}")

    def ensure_auth(self) -> str:
        return self.create_temporary_session(force=False)

    def list_updown_symbols(self) -> list[dict[str, Any]]:
        self.ensure_auth()
        r = self._request("GET", f"{API_BASE}/api/transaction/symbol/list", params={"type": 4})
        body = r.json()
        if body.get("code") != 0:
            raise RuntimeError(body)
        return body.get("data") or []

    def list_periods(self, symbol: str = "BTC/USD") -> list[PeriodConfig]:
        self.ensure_auth()
        r = self._request(
            "GET",
            f"{API_BASE}/api/transaction/updown/symbolPeriod/list",
            params={"symbol": symbol},
        )
        body = r.json()
        if body.get("code") != 0:
            raise RuntimeError(body)
        out: list[PeriodConfig] = []
        for item in body.get("data") or []:
            out.append(
                PeriodConfig(
                    symbol=item["symbol"],
                    label=item["label"],
                    period=int(item["period"]),
                    period_id=str(item["id"]),
                    min_amount=item.get("minAmount"),
                    max_amount=item.get("maxAmount"),
                )
            )
        return out

    def fetch_kline_history(self, symbol: str = "BTC/USD", seconds: int = 220) -> list[dict[str, Any]]:
        self.ensure_auth()
        sym = symbol.replace("/", "-")
        r = self._request(
            "GET",
            f"{API_BASE}/api/data/kline/history/ticker/latest",
            params={"symbol": sym, "seconds": seconds},
        )
        body = r.json()
        if body.get("code") != 0:
            raise RuntimeError(body)
        return body.get("data") or []

    def fetch_statistics(self) -> dict[str, Any]:
        self.ensure_auth()
        r = self._request("GET", f"{API_BASE}/api/transaction/updown/order/statistics")
        body = r.json()
        if body.get("code") != 0:
            raise RuntimeError(body)
        return body.get("data") or {}


ProgressCallback = Callable[[dict[str, Any]], None]


async def scrape_live_history(
    db: Database,
    *,
    symbol: str = "BTC/USD",
    label: str | None = None,
    duration_sec: int = 120,
    on_event: ProgressCallback | None = None,
) -> dict[str, Any]:
    """
    Connect to DeTrade trade websocket, subscribe to Up/Down contest ticker,
    and persist previousRoundResult + settled game snapshots into SQLite.

    duration_sec <= 0 means run until cancelled.
    """
    client = DetradeClient()
    # Auth + REST are blocking; run off the event loop so the dashboard stays responsive.
    token = await asyncio.to_thread(client.create_temporary_session)
    periods = await asyncio.to_thread(client.list_periods, symbol)
    if not periods:
        raise RuntimeError(f"No updown periods for {symbol}")

    period = next((p for p in periods if p.label == label), periods[0]) if label else periods[0]
    run_id = db.start_run(period.symbol, period.label)

    # Seed ticks from REST history (useful for price charts even before WS fills).
    if STORE_TICKS:
        try:
            kline = client.fetch_kline_history(period.symbol, seconds=300)
            ticks = [
                (int(x["t"]), float(x["p"]))
                for x in kline
                if x.get("t") and x.get("p") is not None
            ]
            db.upsert_ticks(period.symbol, ticks)
        except Exception:
            logger.exception("kline seed failed")

    cid = str(uuid.uuid4())
    qs = urlencode({"token": token, "device": "web-pc", "type": "0", "cid": cid})
    uri = f"{WS_URL}?{qs}"

    upserted = 0
    messages = 0
    last_game_id: str | None = None
    started = time.time()
    sslctx = ssl.create_default_context()
    subscribe_cmd = f"/contest/{period.symbol}/{period.period}/ticker/subscribe"
    order_cmd = f"/contest/{period.label}/newOrder/subscribe"
    pool_cmd = f"/contest/{period.label}/amountPool/subscribe"
    kline_cmd = f"/kline/{period.symbol.replace('/', '-')}/ticker/subscribe"

    def _timed_out() -> bool:
        return duration_sec > 0 and (time.time() - started) >= duration_sec

    try:
        while not _timed_out():
            try:
                async with websockets.connect(
                    uri,
                    ssl=sslctx,
                    additional_headers={
                        "Origin": "https://bc.game",
                        "User-Agent": DEFAULT_HEADERS["User-Agent"],
                    },
                    ping_interval=None,
                    max_size=8 * 1024 * 1024,
                    open_timeout=30,
                ) as ws:
                    # Drain optional welcome/connect frame
                    try:
                        welcome = await asyncio.wait_for(ws.recv(), timeout=5)
                        _decode_ws(welcome)
                    except asyncio.TimeoutError:
                        pass

                    for cmd in (subscribe_cmd, order_cmd, pool_cmd, kline_cmd):
                        await ws.send(_encode_ws({"cmd": cmd, "token": token}))
                    if on_event:
                        on_event(
                            {
                                "type": "subscribed",
                                "cmd": subscribe_cmd,
                                "label": period.label,
                                "period": period.period,
                                "settled": db.count_rounds(period.symbol, settled_only=True),
                            }
                        )

                    while not _timed_out():
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=6)
                        except asyncio.TimeoutError:
                            await ws.send(_encode_ws({"cmd": "ping", "token": token}))
                            continue

                        data = _decode_ws(raw)
                        if not data:
                            continue
                        messages += 1
                        cmd = str(data.get("cmd") or "")
                        resp = data.get("resp")

                        if not isinstance(resp, dict):
                            continue

                        if "amountPool" in cmd:
                            from .html_scraper import _pool_fields_from_resp

                            _, _, up_pool, down_pool = _pool_fields_from_resp(resp)
                            rid = str(resp.get("id") or resp.get("gameId") or last_game_id or "").strip()
                            if rid and (up_pool is not None or down_pool is not None):
                                db.upsert_pool_snapshot(
                                    round_id=rid,
                                    symbol=period.symbol,
                                    label=period.label,
                                    up_pct=None,
                                    down_pct=None,
                                    up_pool=up_pool,
                                    down_pool=down_pool,
                                    countdown_sec=None,
                                    lock=False,
                                    source="api_ws_amountPool",
                                )
                            continue

                        # Only contest game tickers carry round history / settlement.
                        if cmd.endswith("/ticker") and "contest" in cmd and "/kline/" not in cmd:
                            hist = resp.get("previousRoundResult") or []
                            if isinstance(hist, list) and hist:
                                n = db.upsert_history_batch(
                                    hist,
                                    symbol=period.symbol,
                                    label=period.label,
                                    period_sec=period.period,
                                    collected_from="previousRoundResult",
                                )
                                upserted += n

                            win_i = 0
                            if resp.get("id"):
                                # Persist live game only once it has a UP/DOWN result.
                                win = resp.get("winSide")
                                try:
                                    win_i = int(win) if win is not None else 0
                                except (TypeError, ValueError):
                                    win_i = 0
                                if win_i in (1, 2):
                                    if db.upsert_round(
                                        resp,
                                        symbol=period.symbol,
                                        label=period.label,
                                        period_sec=period.period,
                                        collected_from="game_ticker",
                                        settled_only=True,
                                    ):
                                        upserted += 1
                                last_game_id = str(resp.get("id"))

                            if on_event and (
                                messages % 8 == 0
                                or win_i in (1, 2)
                                or (isinstance(hist, list) and bool(hist))
                            ):
                                on_event(
                                    {
                                        "type": "game",
                                        "cmd": cmd,
                                        "game_id": resp.get("id"),
                                        "status": resp.get("status"),
                                        "win_side": resp.get("winSide"),
                                        "history_len": len(hist) if isinstance(hist, list) else 0,
                                        "db_rounds": db.count_rounds(
                                            period.symbol, settled_only=True
                                        ),
                                    }
                                )
                        elif (
                            STORE_TICKS
                            and cmd.startswith("/kline/")
                            and resp.get("t")
                            and resp.get("p") is not None
                        ):
                            try:
                                db.upsert_ticks(
                                    period.symbol, [(int(resp["t"]), float(resp["p"]))]
                                )
                            except Exception:
                                pass

                        if messages % MAINT_EVERY_MESSAGES == 0:
                            try:
                                pruned_ticks = db.prune_ticks(keep_last=TICK_KEEP_LAST)
                                pruned_rounds = db.prune_rounds(keep_last=ROUND_KEEP_LAST)
                                if on_event and (pruned_ticks or pruned_rounds):
                                    on_event(
                                        {
                                            "type": "maintenance",
                                            "pruned_ticks": pruned_ticks,
                                            "pruned_rounds": pruned_rounds,
                                            "db_rounds": db.count_rounds(
                                                period.symbol, settled_only=True
                                            ),
                                            "db_ticks": db.count_ticks(period.symbol),
                                        }
                                    )
                            except Exception:
                                logger.exception("maintenance prune failed")
            except websockets.exceptions.ConnectionClosed as exc:
                logger.warning("websocket closed (%s); reconnecting…", exc)
                if on_event:
                    on_event({"type": "reconnect", "error": str(exc)})
                await asyncio.sleep(2.0)
                # Reuse cached token; only force refresh if nearly expired.
                try:
                    token = await asyncio.to_thread(
                        client.create_temporary_session, force=False
                    )
                    qs = urlencode(
                        {
                            "token": token,
                            "device": "web-pc",
                            "type": "0",
                            "cid": str(uuid.uuid4()),
                        }
                    )
                    uri = f"{WS_URL}?{qs}"
                except RateLimitedError as rl:
                    if on_event:
                        on_event({"type": "rate_limited", "error": str(rl)})
                    # Raise so the outer continuous loop can await without blocking.
                    raise
                except Exception:
                    logger.exception("token refresh failed")
                    await asyncio.sleep(5)
                continue
            except Exception as exc:
                logger.warning("websocket error (%s); reconnecting…", exc)
                if on_event:
                    on_event({"type": "reconnect", "error": str(exc)})
                await asyncio.sleep(2.0)
                continue
    except Exception as exc:
        db.end_run(run_id, upserted, notes=f"error: {exc}")
        raise

    db.end_run(run_id, upserted, notes=f"messages={messages}; last_game={last_game_id}")
    summary = {
        "run_id": run_id,
        "symbol": period.symbol,
        "label": period.label,
        "period_sec": period.period,
        "duration_sec": duration_sec,
        "messages": messages,
        "rounds_upserted": upserted,
        "rounds_total": db.count_rounds(period.symbol, settled_only=True),
        "last_game_id": last_game_id,
    }
    if on_event:
        on_event({"type": "done", **summary})
    return summary


def scrape_live_history_sync(
    db: Database,
    *,
    symbol: str = "BTC/USD",
    label: str | None = None,
    duration_sec: int = 120,
    on_event: ProgressCallback | None = None,
) -> dict[str, Any]:
    return asyncio.run(
        scrape_live_history(
            db,
            symbol=symbol,
            label=label,
            duration_sec=duration_sec,
            on_event=on_event,
        )
    )
