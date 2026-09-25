"""BC.Game Up/Down history scraper + analytics API."""
from __future__ import annotations

import asyncio
import csv
import io
import json
import logging
import os
import threading
import time
from pathlib import Path
from typing import Any

from fastapi import BackgroundTasks, FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .db import Database
from .html_scraper import scrape_html_history
from .scraper import DetradeClient, RateLimitedError, scrape_live_history
from .simulate import simulate_bankroll
from .odds_parse import settle_one_bet, ENTRY_FEE

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
DB_PATH = DATA_DIR / "updown_history.db"
STATIC_DIR = ROOT / "static"
SIM_STATE_PATH = DATA_DIR / "live_sim.json"
SCRAPE_MODE = os.getenv("SCRAPE_MODE", "html").strip().lower()  # html | api

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("updown")

app = FastAPI(title="BC Up/Down History", version="1.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)
db = Database(DB_PATH)

_scrape_lock = asyncio.Lock()
_scrape_task: asyncio.Task | None = None
_scrape_state: dict[str, Any] = {
    "running": False,
    "stop_requested": False,
    "last_result": None,
    "last_error": None,
    "events": [],
    "request": None,
    "live_odds": None,
}

_sim_lock = threading.Lock()
_sim_session: dict[str, Any] | None = None


def _sim_to_disk(session: dict[str, Any] | None) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if not session:
        if SIM_STATE_PATH.exists():
            try:
                SIM_STATE_PATH.unlink()
            except OSError:
                pass
        return
    payload = dict(session)
    payload["seen_ids"] = sorted(str(x) for x in session.get("seen_ids") or [])
    try:
        SIM_STATE_PATH.write_text(json.dumps(payload), encoding="utf-8")
    except OSError as exc:
        logger.warning("Could not persist live sim: %s", exc)


def _sim_from_disk() -> dict[str, Any] | None:
    if not SIM_STATE_PATH.is_file():
        return None
    try:
        raw = json.loads(SIM_STATE_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("Could not load live sim: %s", exc)
        return None
    if not isinstance(raw, dict):
        return None
    seen = raw.get("seen_ids") or []
    raw["seen_ids"] = set(str(x) for x in seen)
    raw["equity"] = list(raw.get("equity") or [])
    return raw


def _playable_prediction_rows(symbol: str, limit: int = 5000) -> list[dict[str, Any]]:
    """Settled rounds that have a walk-forward prediction AND a locked $1 payout."""
    data = db.analytics_summary(symbol=symbol, limit=limit)
    review = (data.get("prediction_review") or {}).get("rows") or []
    snaps = {
        str(s["round_id"]): s
        for s in db.list_pool_snapshots(symbol=symbol, locked_only=True, limit=limit)
    }
    playable: list[dict[str, Any]] = []
    for r in review:
        pred = r.get("predicted")
        actual = r.get("actual")
        rid = str(r.get("id") or "")
        snap = snaps.get(rid)
        if pred not in ("UP", "DOWN") or actual not in ("UP", "DOWN") or not snap:
            continue
        if snap.get("up_pct") is None or snap.get("down_pct") is None:
            continue
        playable.append(
            {
                "id": rid,
                "round_id": rid,
                "predicted": pred,
                "actual": actual,
                "result": actual,
                "up_pct": snap.get("up_pct"),
                "down_pct": snap.get("down_pct"),
                "up_pool": snap.get("up_pool"),
                "down_pool": snap.get("down_pool"),
                "fee_rate": snap.get("fee_rate"),
            }
        )
    return playable


def _sim_public(session: dict[str, Any] | None) -> dict[str, Any]:
    if not session:
        return {"running": False}
    deposit = float(session["deposit"])
    balance = float(session["balance"])
    equity = session["equity"]
    wins = session["wins"]
    losses = session["losses"]
    bets = wins + losses
    peak = float(session["peak"])
    return {
        "running": bool(session["running"]),
        "busted": bool(session["busted"]),
        "deposit": deposit,
        "stake": float(session["stake"]),
        "fee_rate": float(session["fee_rate"]),
        "symbol": session["symbol"],
        "balance": round(balance, 4),
        "profit": round(balance - deposit, 4),
        "roi": round((balance - deposit) / deposit, 4) if deposit else None,
        "bets": bets,
        "wins": wins,
        "losses": losses,
        "win_rate": round(wins / bets, 4) if bets else None,
        "peak_balance": round(peak, 4),
        "max_drawdown": round(session["max_drawdown"], 4),
        "started_at": session["started_at"],
        "waiting_for_round": bool(session["running"] and not session["busted"]),
        "equity": equity,
        "last_bet": equity[-1] if equity else None,
    }


class ScrapeRequest(BaseModel):
    symbol: str = "BTC/USD"
    label: str | None = Field(default="btc_5")
    # 0 = continuous until stop
    duration_sec: int = Field(default=0, ge=0, le=86400)
    mode: str | None = Field(default=None, description="html or api; default SCRAPE_MODE")


@app.on_event("startup")
async def _startup() -> None:
    global _sim_session
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    purged = db.purge_unsettled()
    logger.info(
        "DB ready at %s (%s settled, purged %s unsettled)",
        DB_PATH,
        db.count_rounds(settled_only=True),
        purged,
    )
    restored = _sim_from_disk()
    if restored:
        with _sim_lock:
            _sim_session = restored
        logger.info(
            "Restored live sim (running=%s balance=%s bets=%s)",
            restored.get("running"),
            restored.get("balance"),
            len(restored.get("equity") or []),
        )
    # Auto-start continuous collector for BTC/USD btc_5
    asyncio.create_task(_autostart_collector())


async def _autostart_collector() -> None:
    await asyncio.sleep(1)
    if _scrape_state["running"]:
        return
    body = ScrapeRequest(symbol="BTC/USD", label="btc_5", duration_sec=0)
    await _run_scrape(body)


@app.get("/api/health")
def health() -> dict[str, Any]:
    return {
        "ok": True,
        "db": str(DB_PATH),
        "rounds": db.count_rounds(settled_only=True),
        "scrape_running": _scrape_state["running"],
        "scrape_mode": SCRAPE_MODE,
        "request": _scrape_state.get("request"),
    }


@app.get("/api/periods")
def periods(symbol: str = "BTC/USD") -> dict[str, Any]:
    client = DetradeClient()
    try:
        client.ensure_auth()
    except RateLimitedError as exc:
        return {"symbol": symbol, "periods": [], "error": str(exc)}
    items = [
        {
            "id": p.period_id,
            "symbol": p.symbol,
            "label": p.label,
            "period": p.period,
            "min_amount": p.min_amount,
            "max_amount": p.max_amount,
        }
        for p in client.list_periods(symbol)
    ]
    return {"symbol": symbol, "periods": items}


@app.get("/api/rounds")
def rounds(
    limit: int = Query(200, ge=1, le=5000),
    symbol: str | None = "BTC/USD",
) -> dict[str, Any]:
    items = db.recent_rounds(limit=limit, symbol=symbol, settled_only=True)
    return {
        "items": items,
        "total": db.count_rounds(symbol, settled_only=True),
    }


@app.get("/api/analytics")
def analytics(
    symbol: str | None = "BTC/USD",
    limit: int = Query(500, ge=10, le=5000),
) -> dict[str, Any]:
    return db.analytics_summary(symbol=symbol, limit=limit)


@app.get("/api/prediction")
def prediction(
    symbol: str | None = "BTC/USD",
    limit: int = Query(500, ge=10, le=5000),
) -> dict[str, Any]:
    data = db.analytics_summary(symbol=symbol, limit=limit)
    return data.get("prediction") or {}


@app.get("/api/db/stats")
def db_stats() -> dict[str, Any]:
    return db.stats()


@app.post("/api/db/optimize")
def db_optimize(
    prune_ticks_keep: int = Query(5_000, ge=0, le=1_000_000),
    prune_rounds_keep: int = Query(50_000, ge=1_000, le=5_000_000),
) -> dict[str, Any]:
    pruned_ticks = db.prune_ticks(keep_last=prune_ticks_keep) if prune_ticks_keep else 0
    pruned_rounds = db.prune_rounds(keep_last=prune_rounds_keep)
    info = db.optimize()
    return {
        "ok": True,
        "pruned_ticks": pruned_ticks,
        "pruned_rounds": pruned_rounds,
        "stats": db.stats(),
        "optimize": info,
    }


@app.get("/api/pattern")
def pattern(
    symbol: str | None = "BTC/USD",
    limit: int = Query(120, ge=10, le=500),
) -> dict[str, Any]:
    data = db.analytics_summary(symbol=symbol, limit=limit)
    return {
        "pattern": data["pattern"],
        "chips": data["recent_chips"],
        "total_settled": data["total_settled"],
        "up_count": data["up_count"],
        "down_count": data["down_count"],
        "current_streak": data["current_streak"],
        "current_streak_side": data["current_streak_side"],
        "transitions": data["transitions"],
    }


@app.get("/api/stats/remote")
def remote_stats() -> dict[str, Any]:
    client = DetradeClient()
    try:
        client.ensure_auth()
    except RateLimitedError as exc:
        return {"error": str(exc)}
    return client.fetch_statistics()


@app.get("/api/scrape/status")
def scrape_status() -> dict[str, Any]:
    return {
        "running": _scrape_state["running"],
        "stop_requested": _scrape_state["stop_requested"],
        "last_result": _scrape_state["last_result"],
        "last_error": _scrape_state["last_error"],
        "events": _scrape_state["events"][-40:],
        "rounds": db.count_rounds(settled_only=True),
        "request": _scrape_state.get("request"),
        "live_odds": _scrape_state.get("live_odds"),
        "odds_locked_count": len(db.list_pool_snapshots(locked_only=True, limit=100_000)),
    }


async def _run_scrape(body: ScrapeRequest) -> None:
    global _scrape_task
    if _scrape_lock.locked():
        return
    async with _scrape_lock:
        _scrape_state["running"] = True
        _scrape_state["stop_requested"] = False
        _scrape_state["last_error"] = None
        _scrape_state["events"] = []
        mode = (body.mode or SCRAPE_MODE or "html").strip().lower()
        _scrape_state["request"] = {**body.model_dump(), "mode": mode}

        def on_event(evt: dict[str, Any]) -> None:
            _scrape_state["events"].append(evt)
            if len(_scrape_state["events"]) > 120:
                _scrape_state["events"] = _scrape_state["events"][-120:]
            if evt.get("type") == "odds":
                src = str(evt.get("source") or "")
                # Live UI shows DOM Up/Down Wins % only — never let WS/pool overwrite it.
                if src != "dom":
                    return
                _scrape_state["live_odds"] = evt

        try:
            if mode == "html":
                # Single long-lived browser poller (stop via stop_requested checked in sleep).
                while not _scrape_state["stop_requested"]:
                    try:
                        # Run until stopped: duration_sec=0 means continuous inside, but we use
                        # finite chunks so Stop can interrupt between browser restarts.
                        duration = body.duration_sec if body.duration_sec > 0 else 3600
                        result = await scrape_html_history(
                            db,
                            symbol=body.symbol,
                            label=body.label or "btc_5",
                            duration_sec=duration,
                            on_event=on_event,
                            should_stop=lambda: bool(_scrape_state["stop_requested"]),
                        )
                        _scrape_state["last_result"] = result
                        _scrape_state["last_error"] = None
                    except Exception as exc:
                        logger.exception("html scrape failed")
                        _scrape_state["last_error"] = str(exc)
                        on_event({"type": "error", "error": str(exc)})
                        for _ in range(15):
                            if _scrape_state["stop_requested"]:
                                break
                            await asyncio.sleep(1)
                        continue
                    if body.duration_sec and body.duration_sec > 0:
                        break
                    if _scrape_state["stop_requested"]:
                        break
            else:
                # Legacy DeTrade REST+WS collector.
                if body.duration_sec and body.duration_sec > 0:
                    result = await scrape_live_history(
                        db,
                        symbol=body.symbol,
                        label=body.label,
                        duration_sec=body.duration_sec,
                        on_event=on_event,
                    )
                    _scrape_state["last_result"] = result
                else:
                    while not _scrape_state["stop_requested"]:
                        try:
                            result = await scrape_live_history(
                                db,
                                symbol=body.symbol,
                                label=body.label,
                                duration_sec=180,
                                on_event=on_event,
                            )
                            _scrape_state["last_result"] = result
                            _scrape_state["last_error"] = None
                        except RateLimitedError as rl:
                            msg = str(rl)
                            logger.warning("rate limited: %s", msg)
                            _scrape_state["last_error"] = msg
                            on_event({"type": "rate_limited", "error": msg})
                            for _ in range(90):
                                if _scrape_state["stop_requested"]:
                                    break
                                await asyncio.sleep(1)
                            continue
                        except Exception as exc:
                            logger.exception("scrape chunk failed")
                            _scrape_state["last_error"] = str(exc)
                            on_event({"type": "error", "error": str(exc)})
                            await asyncio.sleep(15)
                            continue
                        if _scrape_state["stop_requested"]:
                            break
                        await asyncio.sleep(3)
        except Exception as exc:
            logger.exception("scrape failed")
            _scrape_state["last_error"] = str(exc)
        finally:
            _scrape_state["running"] = False
            _scrape_state["stop_requested"] = False


@app.get("/api/prediction/review")
def prediction_review(
    symbol: str | None = "BTC/USD",
    limit: int = Query(500, ge=10, le=5000),
) -> dict[str, Any]:
    data = db.analytics_summary(symbol=symbol, limit=limit)
    return data.get("prediction_review") or {}


@app.get("/api/export/rounds.csv")
def export_rounds_csv(
    symbol: str | None = "BTC/USD",
    limit: int = Query(5000, ge=10, le=100_000),
) -> StreamingResponse:
    items = db.recent_rounds(limit=limit, symbol=symbol, settled_only=True)
    # chronological
    items = list(reversed(items))
    buf = io.StringIO()
    writer = csv.DictWriter(
        buf,
        fieldnames=[
            "id",
            "symbol",
            "label",
            "period_sec",
            "result",
            "win_side",
            "start_price",
            "end_price",
            "move",
            "price_start_time",
            "price_end_time",
            "scraped_at",
        ],
    )
    writer.writeheader()
    for r in items:
        sp, ep = r.get("start_price"), r.get("end_price")
        move = None
        if sp not in (None, 0) and ep not in (None, 0):
            move = float(ep) - float(sp)
        writer.writerow(
            {
                "id": r.get("id"),
                "symbol": r.get("symbol"),
                "label": r.get("label"),
                "period_sec": r.get("period_sec"),
                "result": r.get("result"),
                "win_side": r.get("win_side"),
                "start_price": sp,
                "end_price": ep,
                "move": move,
                "price_start_time": r.get("price_start_time"),
                "price_end_time": r.get("price_end_time"),
                "scraped_at": r.get("scraped_at"),
            }
        )
    buf.seek(0)
    filename = f"updown_rounds_{(symbol or 'all').replace('/', '-')}.csv"
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/api/export/predictions.csv")
def export_predictions_csv(
    symbol: str | None = "BTC/USD",
    limit: int = Query(5000, ge=10, le=100_000),
) -> StreamingResponse:
    data = db.analytics_summary(symbol=symbol, limit=limit)
    rows = (data.get("prediction_review") or {}).get("rows") or []
    buf = io.StringIO()
    writer = csv.DictWriter(
        buf,
        fieldnames=[
            "n",
            "id",
            "actual",
            "predicted",
            "outcome",
            "hit",
            "start_price",
            "end_price",
            "move",
        ],
    )
    writer.writeheader()
    for r in rows:
        writer.writerow(
            {
                "n": r.get("n"),
                "id": r.get("id"),
                "actual": r.get("actual"),
                "predicted": r.get("predicted") or "NO_TRADE",
                "outcome": r.get("outcome"),
                "hit": r.get("hit"),
                "start_price": r.get("start_price"),
                "end_price": r.get("end_price"),
                "move": r.get("move"),
            }
        )
    buf.seek(0)
    filename = f"updown_predictions_{(symbol or 'all').replace('/', '-')}.csv"
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.post("/api/scrape")
async def scrape(body: ScrapeRequest, background: BackgroundTasks) -> dict[str, Any]:
    if _scrape_state["running"]:
        return {"ok": False, "message": "Collector already running", "status": scrape_status()}
    background.add_task(_run_scrape, body)
    return {"ok": True, "message": "Collector started", "request": body.model_dump()}


@app.post("/api/scrape/stop")
async def scrape_stop() -> dict[str, Any]:
    _scrape_state["stop_requested"] = True
    return {"ok": True, "message": "Stop requested (finishes current WS chunk)"}


@app.get("/api/odds")
def list_odds(
    symbol: str | None = "BTC/USD",
    locked_only: bool = True,
    limit: int = Query(5000, ge=1, le=100_000),
) -> dict[str, Any]:
    rows = db.list_pool_snapshots(symbol=symbol, locked_only=locked_only, limit=limit)
    return {
        "count": len(rows),
        "locked_only": locked_only,
        "live": _scrape_state.get("live_odds"),
        "items": rows,
    }


class SimulateRequest(BaseModel):
    symbol: str = "BTC/USD"
    deposit: float = Field(default=100.0, gt=0, le=1_000_000)
    stake: float = Field(default=1.0, gt=0, le=1_000_000)
    fee_rate: float = Field(
        default=0.0,
        ge=0,
        le=0.5,
        description="Unused — win pays scraped potential return; loss loses full stake",
    )
    limit: int = Field(default=5000, ge=10, le=100_000)


@app.post("/api/simulate/bankroll")
def simulate_bankroll_api(body: SimulateRequest) -> dict[str, Any]:
    """One-shot historical backtest (prediction correctness × locked payouts)."""
    playable = _playable_prediction_rows(body.symbol, body.limit)
    result = simulate_bankroll(
        playable,
        deposit=body.deposit,
        stake=body.stake,
        fee_rate=body.fee_rate,
    )
    result["playable_rounds"] = len(playable)
    result["locked_snapshots"] = len(
        db.list_pool_snapshots(symbol=body.symbol, locked_only=True, limit=body.limit)
    )
    return result


@app.post("/api/simulate/live/start")
def simulate_live_start(body: SimulateRequest) -> dict[str, Any]:
    """
    Start a real-time sim: ignore past rounds, bet each NEW settled prediction
    with locked $1 payout until stop or bust.
    """
    global _sim_session
    existing = _playable_prediction_rows(body.symbol, body.limit)
    seen = {str(r["round_id"]) for r in existing}
    with _sim_lock:
        _sim_session = {
            "running": True,
            "busted": False,
            "symbol": body.symbol,
            "deposit": float(body.deposit),
            "stake": float(body.stake),
            "fee_rate": float(body.fee_rate),
            "balance": float(body.deposit),
            "peak": float(body.deposit),
            "max_drawdown": 0.0,
            "wins": 0,
            "losses": 0,
            "equity": [],
            "seen_ids": seen,
            "started_at": int(time.time() * 1000),
            "limit": body.limit,
        }
        pub = _sim_public(_sim_session)
    _sim_to_disk(_sim_session)
    pub["ok"] = True
    pub["message"] = (
        f"Live sim started at ${body.deposit:.2f}. "
        f"Ignoring {len(seen)} past playable rounds — waiting for the next settled bet."
    )
    return pub


@app.post("/api/simulate/live/stop")
def simulate_live_stop() -> dict[str, Any]:
    global _sim_session
    with _sim_lock:
        if _sim_session:
            _sim_session["running"] = False
        pub = _sim_public(_sim_session)
        _sim_to_disk(_sim_session)
    pub["ok"] = True
    pub["message"] = "Live sim stopped"
    return pub


@app.get("/api/simulate/live/status")
def simulate_live_status() -> dict[str, Any]:
    with _sim_lock:
        return _sim_public(_sim_session)


@app.post("/api/simulate/live/tick")
def simulate_live_tick() -> dict[str, Any]:
    """Apply any newly settled prediction+payout rounds since last tick."""
    global _sim_session
    with _sim_lock:
        if not _sim_session or not _sim_session["running"] or _sim_session["busted"]:
            pub = _sim_public(_sim_session)
            pub["ok"] = True
            pub["new_bets"] = []
            return pub

        session = _sim_session
        started_at = session["started_at"]
        symbol = session["symbol"]
        stake = float(session["stake"])
        fee_default = float(session["fee_rate"])
        seen_snapshot = set(session["seen_ids"])
        limit = int(session.get("limit") or 5000)

    playable = _playable_prediction_rows(symbol, limit)
    newcomers = [r for r in playable if str(r["round_id"]) not in seen_snapshot]
    new_bets: list[dict[str, Any]] = []

    with _sim_lock:
        # Abort if Start replaced this session while we computed playable rows.
        if (
            not _sim_session
            or not _sim_session["running"]
            or _sim_session.get("started_at") != started_at
        ):
            pub = _sim_public(_sim_session)
            pub["ok"] = True
            pub["new_bets"] = []
            if _sim_session and _sim_session.get("started_at") != started_at:
                pub["message"] = "Session replaced — ignored stale tick"
            return pub
        session = _sim_session
        for row in newcomers:
            rid = str(row["round_id"])
            if rid in session["seen_ids"]:
                continue
            session["seen_ids"].add(rid)
            if session["busted"] or float(session["balance"]) < stake:
                session["busted"] = True
                session["running"] = False
                break
            fee = row.get("fee_rate")
            fee_f = float(fee) if fee is not None else fee_default
            settled = settle_one_bet(
                balance=float(session["balance"]),
                stake=stake,
                predicted=row["predicted"],
                result=row["result"],
                up_pct=float(row["up_pct"]),
                down_pct=float(row["down_pct"]),
                fee_rate=fee_f,
                up_pool=row.get("up_pool"),
                down_pool=row.get("down_pool"),
                round_id=rid,
                bet_n=len(session["equity"]) + 1,
            )
            session["balance"] = settled["balance"]
            peak = max(float(session["peak"]), float(session["balance"]))
            session["peak"] = peak
            dd = (peak - float(session["balance"])) / peak if peak > 0 else 0.0
            session["max_drawdown"] = max(float(session["max_drawdown"]), dd)
            if settled["row"]["outcome"] == "WIN":
                session["wins"] += 1
            else:
                session["losses"] += 1
            session["equity"].append(settled["row"])
            new_bets.append(settled["row"])
            if float(session["balance"]) < stake:
                session["busted"] = True
                session["running"] = False
                break
        pub = _sim_public(session)
        _sim_to_disk(session)
    pub["ok"] = True
    pub["new_bets"] = new_bets
    if new_bets:
        pub["message"] = f"Applied {len(new_bets)} new bet(s)"
    elif pub.get("busted"):
        pub["message"] = "Busted — balance below stake"
    elif pub.get("running"):
        pub["message"] = "Waiting for next settled round…"
    else:
        pub["message"] = "Stopped"
    return pub


@app.get("/")
def index() -> FileResponse:
    # Avoid sticky browser cache of inline JS (limit, today stats, etc.).
    return FileResponse(
        STATIC_DIR / "index.html",
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate",
            "Pragma": "no-cache",
        },
    )


app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
