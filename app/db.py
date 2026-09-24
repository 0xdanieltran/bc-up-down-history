"""SQLite persistence for BC.Game / DeTrade Up-Down round history."""
from __future__ import annotations

import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any

WIN_SIDE_MAP = {1: "UP", 2: "DOWN"}
STATUS_MAP = {
    0: "UNKNOWN",
    1001: "STARTED",
    1002: "START_PAY_OUT",
    1003: "CUTOFF_TRADE",
    1004: "PAY_OUT",
    1005: "FINISHED",
    1006: "READY_TO_START",
    1007: "CANCEL",
    1008: "SETTLING",
}

_SCHEMA = """
CREATE TABLE IF NOT EXISTS rounds (
    id TEXT PRIMARY KEY,
    symbol TEXT NOT NULL,
    label TEXT NOT NULL,
    period_sec INTEGER,
    status INTEGER,
    status_name TEXT,
    win_side INTEGER,
    result TEXT,
    start_price REAL,
    end_price REAL,
    price_start_time INTEGER,
    price_end_time INTEGER,
    trade_cutoff_time INTEGER,
    fee_rate REAL,
    up_pool_amount REAL,
    down_pool_amount REAL,
    raw_json TEXT,
    scraped_at INTEGER NOT NULL,
    collected_from TEXT
);

CREATE INDEX IF NOT EXISTS idx_rounds_symbol_id ON rounds(symbol, id);
CREATE INDEX IF NOT EXISTS idx_rounds_result ON rounds(result);

CREATE TABLE IF NOT EXISTS scrape_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at INTEGER NOT NULL,
    ended_at INTEGER,
    symbol TEXT,
    label TEXT,
    rounds_upserted INTEGER DEFAULT 0,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS ticks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    t INTEGER NOT NULL,
    price REAL NOT NULL,
    UNIQUE(symbol, t)
);

CREATE INDEX IF NOT EXISTS idx_ticks_symbol_t ON ticks(symbol, t DESC);
"""


def _num(v: Any) -> float | None:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _int(v: Any) -> int | None:
    if v is None or v == "":
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _better_price(new: float | None, old: float | None) -> float | None:
    if new is None:
        return old
    if old is None or old == 0:
        return new
    if new == 0:
        return old
    return new


def _transition_probs(results: list[str]) -> dict[str, dict[str, float | int]]:
    counts = {"UP": {"UP": 0, "DOWN": 0}, "DOWN": {"UP": 0, "DOWN": 0}}
    for a, b in zip(results, results[1:]):
        if a in counts and b in counts[a]:
            counts[a][b] += 1
    out: dict[str, dict[str, float | int]] = {}
    for side, nxt in counts.items():
        total = int(nxt["UP"] + nxt["DOWN"])
        out[side] = {
            "UP": (nxt["UP"] / total) if total else 0.5,
            "DOWN": (nxt["DOWN"] / total) if total else 0.5,
            "samples": total,
        }
    return out


def _prediction_review(results: list[str], seq: list[dict[str, Any]]) -> dict[str, Any]:
    """Walk-forward review: predicted vs actual for each round after warmup."""
    rows: list[dict[str, Any]] = []
    hits = 0
    misses = 0
    skips = 0
    for i in range(5, len(results)):
        hist = results[:i]
        pred = _predict_from_history_simple(hist)
        actual = results[i]
        item = seq[i] if i < len(seq) else {}
        if pred is None:
            outcome = "SKIP"
            skips += 1
            hit = None
        else:
            hit = pred == actual
            outcome = "HIT" if hit else "MISS"
            if hit:
                hits += 1
            else:
                misses += 1
        rows.append(
            {
                "n": i + 1,
                "id": item.get("id"),
                "actual": actual,
                "predicted": pred,
                "outcome": outcome,
                "hit": hit,
                "start_price": item.get("start_price"),
                "end_price": item.get("end_price"),
                "move": item.get("move"),
            }
        )
    trials = hits + misses
    return {
        "rows": rows,
        "chips": [
            {
                "n": r["n"],
                "actual": r["actual"],
                "predicted": r["predicted"],
                "outcome": r["outcome"],
            }
            for r in rows
        ],
        "hits": hits,
        "misses": misses,
        "skips": skips,
        "trials": trials,
        "hit_rate": round(hits / trials, 4) if trials else None,
    }


def _predict_from_history(results: list[str]) -> dict[str, Any]:
    """
    Pattern-based next-round prediction (not a guaranteed edge).
    Blends: Markov transition | overall base rate | recent window | mild streak fade.
    """
    if len(results) < 5:
        return {
            "side": None,
            "confidence": 0.0,
            "p_up": 0.5,
            "p_down": 0.5,
            "label": "WAIT",
            "reasons": ["Need at least 5 settled rounds before predicting."],
            "based_on_last": None,
            "model_hit_rate": None,
            "model_trials": 0,
        }

    last = results[-1]
    trans = _transition_probs(results)
    base_up = sum(1 for r in results if r == "UP") / len(results)
    recent = results[-20:]
    recent_up = sum(1 for r in recent if r == "UP") / len(recent)

    streak = 1
    for val in reversed(results[:-1]):
        if val == last:
            streak += 1
        else:
            break

    markov_up = float(trans[last]["UP"])
    # Mild mean-reversion if streak is long (>=3): nudge toward flip
    streak_nudge = 0.0
    if streak >= 3:
        streak_nudge = min(0.08, 0.025 * (streak - 2))
        if last == "UP":
            markov_up = max(0.05, markov_up - streak_nudge)
        else:
            markov_up = min(0.95, markov_up + streak_nudge)

    # Weighted blend
    w_markov, w_base, w_recent = 0.55, 0.20, 0.25
    # If few transition samples after last side, lean more on base/recent
    samples = int(trans[last]["samples"])
    if samples < 8:
        w_markov, w_base, w_recent = 0.35, 0.30, 0.35

    p_up = w_markov * markov_up + w_base * base_up + w_recent * recent_up
    p_up = max(0.05, min(0.95, p_up))
    p_down = 1.0 - p_up

    side = "UP" if p_up >= p_down else "DOWN"
    edge = abs(p_up - 0.5)
    # Confidence scales with edge + sample size
    confidence = min(0.92, 0.45 + edge * 1.4 + min(0.15, samples / 80))
    if edge < 0.03:
        label = "NO TRADE"
        side_out = None
    elif confidence >= 0.72 and edge >= 0.08:
        label = f"LEAN {side}"
        side_out = side
    else:
        label = f"WEAK {side}"
        side_out = side

    reasons = [
        f"Last result was {last} (streak {streak}).",
        f"After {last}: P(UP)={float(trans[last]['UP']):.0%} from {samples} transitions.",
        f"Overall UP rate {base_up:.0%}; last {len(recent)} rounds UP {recent_up:.0%}.",
    ]
    if streak >= 3:
        reasons.append(f"Streak fade nudge applied (~{streak_nudge:.0%}) toward a flip.")

    # Walk-forward hit rate of the same blended rule
    hits = 0
    trials = 0
    for i in range(5, len(results)):
        hist = results[:i]
        pred = _predict_from_history_simple(hist)
        if pred is None:
            continue
        trials += 1
        if pred == results[i]:
            hits += 1

    return {
        "side": side_out,
        "label": label,
        "confidence": round(confidence, 3),
        "p_up": round(p_up, 4),
        "p_down": round(p_down, 4),
        "edge": round(edge, 4),
        "based_on_last": last,
        "streak": streak,
        "transition_samples": samples,
        "reasons": reasons,
        "model_hit_rate": round(hits / trials, 4) if trials else None,
        "model_trials": trials,
        "disclaimer": "Pattern heuristic only - not financial advice; no guaranteed edge.",
    }


def _predict_from_history_simple(results: list[str]) -> str | None:
    """Lightweight predictor for walk-forward scoring (no nested backtest)."""
    if len(results) < 5:
        return None
    last = results[-1]
    trans = _transition_probs(results)
    base_up = sum(1 for r in results if r == "UP") / len(results)
    recent = results[-20:]
    recent_up = sum(1 for r in recent if r == "UP") / len(recent)
    samples = int(trans[last]["samples"])
    markov_up = float(trans[last]["UP"])
    streak = 1
    for val in reversed(results[:-1]):
        if val == last:
            streak += 1
        else:
            break
    if streak >= 3:
        nudge = min(0.08, 0.025 * (streak - 2))
        markov_up = max(0.05, markov_up - nudge) if last == "UP" else min(0.95, markov_up + nudge)
    w_markov, w_base, w_recent = (0.35, 0.30, 0.35) if samples < 8 else (0.55, 0.20, 0.25)
    p_up = w_markov * markov_up + w_base * base_up + w_recent * recent_up
    if abs(p_up - 0.5) < 0.03:
        return None
    return "UP" if p_up >= 0.5 else "DOWN"


class Database:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(self.path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        # Performance-oriented defaults for continuous local collection.
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA synchronous=NORMAL;")
        self._conn.execute("PRAGMA temp_store=MEMORY;")
        self._conn.execute("PRAGMA cache_size=-64000;")  # ~64MB
        self._conn.execute("PRAGMA mmap_size=268435456;")  # 256MB
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    @staticmethod
    def _slim_raw(round_data: dict[str, Any]) -> str:
        """Drop bulky nested history already stored as separate round rows."""
        slim = {k: v for k, v in round_data.items() if k != "previousRoundResult"}
        return json.dumps(slim, separators=(",", ":"), default=str)

    def start_run(self, symbol: str, label: str) -> int:
        with self._lock:
            cur = self._conn.execute(
                "INSERT INTO scrape_runs(started_at, symbol, label) VALUES (?, ?, ?)",
                (int(time.time() * 1000), symbol, label),
            )
            self._conn.commit()
            return int(cur.lastrowid)

    def end_run(self, run_id: int, upserted: int, notes: str = "") -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE scrape_runs SET ended_at=?, rounds_upserted=?, notes=? WHERE id=?",
                (int(time.time() * 1000), upserted, notes, run_id),
            )
            self._conn.commit()

    def upsert_round(
        self,
        round_data: dict[str, Any],
        *,
        symbol: str,
        label: str,
        period_sec: int | None = None,
        collected_from: str = "ws",
        settled_only: bool = False,
    ) -> bool:
        """Insert or update a round. Returns True if a new row was inserted."""
        rid = str(round_data.get("id") or "").strip()
        if not rid:
            return False

        win_side_i = _int(round_data.get("winSide"))
        result = WIN_SIDE_MAP.get(win_side_i) if win_side_i in WIN_SIDE_MAP else None

        if settled_only and result is None:
            return False

        start_price = _num(round_data.get("startPrice"))
        end_price = _num(round_data.get("endPrice"))

        # Skip empty order-like junk.
        if (
            result is None
            and (start_price is None or start_price == 0)
            and (end_price is None or end_price == 0)
            and _int(round_data.get("status")) in (None, 0)
        ):
            return False

        status_i = _int(round_data.get("status"))
        status_name = STATUS_MAP.get(status_i) if status_i is not None else None

        up_pool = _num(round_data.get("upPoolAmount"))
        down_pool = _num(round_data.get("downPoolAmount"))
        if up_pool is None:
            up_pool = (_num(round_data.get("first5sUpPoolAmount")) or 0) + (
                _num(round_data.get("second5sUpPoolAmount")) or 0
            )
            up_pool = up_pool or None
        if down_pool is None:
            down_pool = (_num(round_data.get("first5sDownPoolAmount")) or 0) + (
                _num(round_data.get("second5sDownPoolAmount")) or 0
            )
            down_pool = down_pool or None

        now = int(time.time() * 1000)
        with self._lock:
            existing = self._conn.execute(
                "SELECT * FROM rounds WHERE id=?", (rid,)
            ).fetchone()
            if existing:
                start_price = _better_price(start_price, existing["start_price"])
                end_price = _better_price(end_price, existing["end_price"])
                if result is None:
                    result = existing["result"]
                    win_side_i = existing["win_side"] if win_side_i in (None, 0) else win_side_i
                if status_i is None:
                    status_i = existing["status"]
                    status_name = existing["status_name"]

            self._conn.execute(
                """
                INSERT INTO rounds (
                    id, symbol, label, period_sec, status, status_name, win_side, result,
                    start_price, end_price, price_start_time, price_end_time, trade_cutoff_time,
                    fee_rate, up_pool_amount, down_pool_amount, raw_json, scraped_at, collected_from
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    status=excluded.status,
                    status_name=excluded.status_name,
                    win_side=excluded.win_side,
                    result=excluded.result,
                    start_price=excluded.start_price,
                    end_price=excluded.end_price,
                    price_start_time=COALESCE(excluded.price_start_time, rounds.price_start_time),
                    price_end_time=COALESCE(excluded.price_end_time, rounds.price_end_time),
                    trade_cutoff_time=COALESCE(excluded.trade_cutoff_time, rounds.trade_cutoff_time),
                    fee_rate=COALESCE(excluded.fee_rate, rounds.fee_rate),
                    up_pool_amount=COALESCE(excluded.up_pool_amount, rounds.up_pool_amount),
                    down_pool_amount=COALESCE(excluded.down_pool_amount, rounds.down_pool_amount),
                    raw_json=excluded.raw_json,
                    scraped_at=excluded.scraped_at,
                    collected_from=excluded.collected_from
                """,
                (
                    rid,
                    symbol,
                    label,
                    period_sec,
                    status_i,
                    status_name,
                    win_side_i,
                    result,
                    start_price,
                    end_price,
                    _int(round_data.get("priceStartTime")),
                    _int(round_data.get("priceEndTime")),
                    _int(round_data.get("tradeCutoffTime")),
                    _num(round_data.get("feeRate")),
                    up_pool,
                    down_pool,
                    self._slim_raw(round_data),
                    now,
                    collected_from,
                ),
            )
            self._conn.commit()
            return existing is None

    def upsert_history_batch(
        self,
        items: list[dict[str, Any]],
        *,
        symbol: str,
        label: str,
        period_sec: int | None,
        collected_from: str = "previousRoundResult",
    ) -> int:
        inserted = 0
        for item in items:
            if self.upsert_round(
                item,
                symbol=symbol,
                label=label,
                period_sec=period_sec,
                collected_from=collected_from,
                settled_only=True,
            ):
                inserted += 1
        return inserted

    def upsert_ticks(self, symbol: str, ticks: list[tuple[int, float]]) -> int:
        if not ticks:
            return 0
        with self._lock:
            before = self._conn.total_changes
            self._conn.executemany(
                "INSERT OR IGNORE INTO ticks(symbol, t, price) VALUES (?, ?, ?)",
                [(symbol, t, p) for t, p in ticks],
            )
            self._conn.commit()
            return self._conn.total_changes - before

    def count_ticks(self, symbol: str | None = None) -> int:
        with self._lock:
            if symbol:
                row = self._conn.execute(
                    "SELECT COUNT(*) AS c FROM ticks WHERE symbol=?", (symbol,)
                ).fetchone()
            else:
                row = self._conn.execute("SELECT COUNT(*) AS c FROM ticks").fetchone()
            return int(row["c"])

    def prune_ticks(self, keep_last: int = 5_000, older_than_ms: int | None = None) -> int:
        """Keep DB lean: drop old/excess price ticks (not needed for pattern analytics)."""
        deleted = 0
        with self._lock:
            if older_than_ms is not None:
                cutoff = int(time.time() * 1000) - int(older_than_ms)
                cur = self._conn.execute("DELETE FROM ticks WHERE t < ?", (cutoff,))
                deleted += cur.rowcount
            # Cap total rows per symbol by keeping newest keep_last
            symbols = [
                r[0]
                for r in self._conn.execute("SELECT DISTINCT symbol FROM ticks").fetchall()
            ]
            for sym in symbols:
                cur = self._conn.execute(
                    """
                    DELETE FROM ticks
                    WHERE symbol=? AND id NOT IN (
                        SELECT id FROM ticks
                        WHERE symbol=?
                        ORDER BY t DESC
                        LIMIT ?
                    )
                    """,
                    (sym, sym, keep_last),
                )
                deleted += cur.rowcount
            self._conn.commit()
        return deleted

    def prune_rounds(self, keep_last: int = 50_000, symbol: str | None = None) -> int:
        """Keep newest settled rounds by snowflake id (pattern analysis rarely needs more)."""
        with self._lock:
            if symbol:
                cur = self._conn.execute(
                    """
                    DELETE FROM rounds
                    WHERE symbol=? AND id NOT IN (
                        SELECT id FROM rounds
                        WHERE symbol=? AND result IS NOT NULL
                        ORDER BY CAST(id AS INTEGER) DESC
                        LIMIT ?
                    )
                    """,
                    (symbol, symbol, keep_last),
                )
            else:
                cur = self._conn.execute(
                    """
                    DELETE FROM rounds
                    WHERE id NOT IN (
                        SELECT id FROM rounds
                        WHERE result IS NOT NULL
                        ORDER BY CAST(id AS INTEGER) DESC
                        LIMIT ?
                    )
                    """,
                    (keep_last,),
                )
            self._conn.commit()
            return cur.rowcount

    def optimize(self) -> dict[str, Any]:
        """ANALYZE + WAL checkpoint + incremental vacuum."""
        with self._lock:
            self._conn.execute("ANALYZE;")
            self._conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
            self._conn.execute("PRAGMA optimize;")
            # Reclaim free pages without full VACUUM lock when possible
            try:
                self._conn.execute("PRAGMA incremental_vacuum(128);")
            except sqlite3.Error:
                pass
            page_count = self._conn.execute("PRAGMA page_count").fetchone()[0]
            page_size = self._conn.execute("PRAGMA page_size").fetchone()[0]
            freelist = self._conn.execute("PRAGMA freelist_count").fetchone()[0]
        return {
            "page_count": page_count,
            "page_size": page_size,
            "freelist_count": freelist,
            "approx_bytes": int(page_count) * int(page_size),
            "rounds": self.count_rounds(settled_only=True),
            "ticks": self.count_ticks(),
        }

    def stats(self) -> dict[str, Any]:
        path = self.path
        size = path.stat().st_size if path.exists() else 0
        with self._lock:
            page_count = self._conn.execute("PRAGMA page_count").fetchone()[0]
            page_size = self._conn.execute("PRAGMA page_size").fetchone()[0]
            freelist = self._conn.execute("PRAGMA freelist_count").fetchone()[0]
        return {
            "path": str(path),
            "size_bytes": size,
            "size_mb": round(size / (1024 * 1024), 3),
            "page_count": page_count,
            "freelist_count": freelist,
            "approx_pages_bytes": int(page_count) * int(page_size),
            "rounds": self.count_rounds(),
            "rounds_settled": self.count_rounds(settled_only=True),
            "ticks": self.count_ticks(),
        }

    def count_rounds(self, symbol: str | None = None, settled_only: bool = False) -> int:
        where = "WHERE 1=1"
        params: list[Any] = []
        if symbol:
            where += " AND symbol=?"
            params.append(symbol)
        if settled_only:
            where += " AND result IS NOT NULL"
        with self._lock:
            row = self._conn.execute(
                f"SELECT COUNT(*) AS c FROM rounds {where}", params
            ).fetchone()
            return int(row["c"])

    def purge_unsettled(self) -> int:
        with self._lock:
            cur = self._conn.execute("DELETE FROM rounds WHERE result IS NULL")
            self._conn.commit()
            return cur.rowcount

    def recent_rounds(
        self,
        limit: int = 200,
        symbol: str | None = None,
        settled_only: bool = True,
    ) -> list[dict[str, Any]]:
        where = "WHERE 1=1"
        params: list[Any] = []
        if symbol:
            where += " AND symbol=?"
            params.append(symbol)
        if settled_only:
            where += " AND result IS NOT NULL"
        params.append(limit)
        with self._lock:
            rows = self._conn.execute(
                f"""
                SELECT * FROM rounds
                {where}
                ORDER BY CAST(id AS INTEGER) DESC
                LIMIT ?
                """,
                params,
            ).fetchall()
            return [dict(r) for r in rows]

    def settled_sequence(self, symbol: str | None = None, limit: int = 500) -> list[dict[str, Any]]:
        """Oldest → newest settled rounds for pattern analysis."""
        where = "WHERE result IS NOT NULL"
        params: list[Any] = []
        if symbol:
            where += " AND symbol=?"
            params.append(symbol)
        params.append(limit)
        with self._lock:
            rows = self._conn.execute(
                f"""
                SELECT id, result, win_side, start_price, end_price,
                       price_start_time, price_end_time, scraped_at, period_sec, label
                FROM rounds
                {where}
                ORDER BY CAST(id AS INTEGER) DESC
                LIMIT ?
                """,
                params,
            ).fetchall()
        # reverse to chronological
        items = [dict(r) for r in reversed(rows)]
        for i, item in enumerate(items):
            sp = item.get("start_price")
            ep = item.get("end_price")
            item["move"] = (float(ep) - float(sp)) if sp not in (None, 0) and ep not in (None, 0) else None
            item["n"] = i + 1
        return items

    def analytics_summary(self, symbol: str | None = None, limit: int = 500) -> dict[str, Any]:
        seq = self.settled_sequence(symbol=symbol, limit=limit)
        results = [r["result"] for r in seq if r.get("result")]
        total = len(results)
        ups = sum(1 for r in results if r == "UP")
        downs = sum(1 for r in results if r == "DOWN")

        current_streak = 0
        streak_side = None
        if results:
            streak_side = results[-1]
            for val in reversed(results):
                if val == streak_side:
                    current_streak += 1
                else:
                    break

        longest = 0
        longest_side = None
        run = 0
        prev = None
        for val in results:
            if val == prev:
                run += 1
            else:
                run = 1
                prev = val
            if run > longest:
                longest = run
                longest_side = val

        timeline = []
        up_cum = 0
        down_cum = 0
        for row in seq:
            if row["result"] == "UP":
                up_cum += 1
            else:
                down_cum += 1
            timeline.append(
                {
                    "id": row["id"],
                    "n": row["n"],
                    "ts": row.get("price_end_time") or row.get("scraped_at"),
                    "result": row["result"],
                    "up_cum": up_cum,
                    "down_cum": down_cum,
                    "start_price": row.get("start_price"),
                    "end_price": row.get("end_price"),
                    "move": row.get("move"),
                    "signal": 1 if row["result"] == "UP" else -1,
                }
            )

        # streak length histogram
        streak_hist: dict[str, dict[str, int]] = {"UP": {}, "DOWN": {}}
        run = 0
        prev = None
        for val in results:
            if val == prev:
                run += 1
            else:
                if prev and run:
                    streak_hist[prev][str(run)] = streak_hist[prev].get(str(run), 0) + 1
                run = 1
                prev = val
        if prev and run:
            streak_hist[prev][str(run)] = streak_hist[prev].get(str(run), 0) + 1

        # transitions: UU, UD, DU, DD
        transitions = {"UU": 0, "UD": 0, "DU": 0, "DD": 0}
        for a, b in zip(results, results[1:]):
            key = ("U" if a == "UP" else "D") + ("U" if b == "UP" else "D")
            transitions[key] += 1

        # rolling UP rate (window 20)
        rolling = []
        window = 20
        for i in range(len(results)):
            chunk = results[max(0, i - window + 1) : i + 1]
            rolling.append(
                {
                    "n": i + 1,
                    "up_rate": sum(1 for x in chunk if x == "UP") / len(chunk),
                    "window": len(chunk),
                }
            )

        # pattern string for glance view (newest last)
        pattern = "".join("U" if r == "UP" else "D" for r in results)
        chips = [
            {
                "id": r["id"],
                "result": r["result"],
                "start_price": r.get("start_price"),
                "end_price": r.get("end_price"),
                "move": r.get("move"),
            }
            for r in seq
        ]

        prediction = _predict_from_history(results)
        review = _prediction_review(results, seq)

        return {
            "total_settled": total,
            "up_count": ups,
            "down_count": downs,
            "up_ratio": (ups / total) if total else 0.0,
            "down_ratio": (downs / total) if total else 0.0,
            "current_streak": current_streak,
            "current_streak_side": streak_side,
            "longest_streak": longest,
            "longest_streak_side": longest_side,
            "timeline": timeline,
            "transitions": transitions,
            "rolling_up_rate": rolling,
            "streak_hist": streak_hist,
            "pattern": pattern,
            "chips": chips,
            "recent_chips": chips[-80:],
            "prediction": prediction,
            "prediction_review": review,
        }
