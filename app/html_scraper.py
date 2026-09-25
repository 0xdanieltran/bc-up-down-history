"""Scrape BC.Game Up/Down history from Chromium (DeTrade WS + DOM fingerprints)."""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import time
import zlib
from pathlib import Path
from typing import Any, Callable

from .db import Database
from .odds_parse import DOM_ODDS_JS, ENTRY_FEE, pools_to_return_pct

logger = logging.getLogger(__name__)

PAGE_URL = os.getenv("HTML_SCRAPE_URL", "https://bc.game/trading/up-down")
# Poll every 1s so we catch center timer at ~3s / ~2s.
POLL_SEC = int(os.getenv("HTML_POLL_SEC", "2"))
HEADLESS = os.getenv("HTML_HEADLESS", "0").strip().lower() in {"1", "true", "yes"}
PROFILE_DIR = Path(
    os.getenv(
        "HTML_BROWSER_PROFILE",
        str(Path(__file__).resolve().parent.parent / "data" / "browser_profile"),
    )
)
# Unpacked Chrome extension (sibling bc-up-down-extension by default).
# Set HTML_EXTENSION_DIR= to disable. Extensions require headed Chromium.
_DEFAULT_EXT = Path(__file__).resolve().parent.parent.parent / "bc-up-down-extension"
_EXT_ENV = os.getenv("HTML_EXTENSION_DIR", str(_DEFAULT_EXT)).strip()
EXTENSION_DIR = Path(_EXT_ENV) if _EXT_ENV else None
USER_WAIT_SEC = int(os.getenv("HTML_USER_WAIT_SEC", "600"))
# Lock when center countdown shows 2 or 3 seconds remaining.
ODDS_LOCK_MIN_SEC = float(os.getenv("ODDS_LOCK_MIN_SEC", "2"))
ODDS_LOCK_MAX_SEC = float(os.getenv("ODDS_LOCK_MAX_SEC", "3"))
ODDS_TARGET_SEC = float(os.getenv("ODDS_TARGET_SEC", "2.5"))
ODDS_FEE_DEFAULT = ENTRY_FEE
_DOM_ODDS_JS = DOM_ODDS_JS

ProgressCallback = Callable[[dict[str, Any]], None]
_PRICE_RE = re.compile(r"[\d]+(?:\.\d+)?")


def _extension_load_args() -> list[str]:
    """Args so Playwright Chromium loads an unpacked MV3 extension."""
    if EXTENSION_DIR is None:
        return []
    ext = EXTENSION_DIR.resolve()
    if not (ext / "manifest.json").is_file():
        logger.warning("HTML_EXTENSION_DIR set but no manifest.json at %s", ext)
        return []
    # Absolute path; Playwright Chromium ignores Load unpacked UI.
    path = str(ext)
    return [
        f"--disable-extensions-except={path}",
        f"--load-extension={path}",
    ]


def _in_odds_lock_window(countdown: float | None) -> bool:
    """True when center timer is 2–3 seconds remaining (inclusive)."""
    if countdown is None:
        return False
    try:
        cd = float(countdown)
    except (TypeError, ValueError):
        return False
    return ODDS_LOCK_MIN_SEC <= cd <= ODDS_LOCK_MAX_SEC


def _pools_to_payouts(
    up_pool: float | None,
    down_pool: float | None,
    fee_rate: float = ODDS_FEE_DEFAULT,
) -> tuple[float | None, float | None]:
    del fee_rate
    return pools_to_return_pct(up_pool, down_pool)


def _pool_fields_from_resp(resp: dict[str, Any]) -> tuple[float | None, float | None, float | None, float | None]:
    """Extract pool amounts only. Return % comes from the DOM (Up/Down Wins), not pool math."""
    up_pool = None
    down_pool = None
    for uk in ("upPoolAmount", "upAmount", "upPool", "up"):
        if uk in resp and resp[uk] is not None:
            try:
                up_pool = float(resp[uk])
                break
            except (TypeError, ValueError):
                pass
    for dk in ("downPoolAmount", "downAmount", "downPool", "down"):
        if dk in resp and resp[dk] is not None:
            try:
                down_pool = float(resp[dk])
                break
            except (TypeError, ValueError):
                pass
    if up_pool is None:
        a = resp.get("first5sUpPoolAmount")
        b = resp.get("second5sUpPoolAmount")
        try:
            if a is not None or b is not None:
                up_pool = float(a or 0) + float(b or 0)
        except (TypeError, ValueError):
            pass
    if down_pool is None:
        a = resp.get("first5sDownPoolAmount")
        b = resp.get("second5sDownPoolAmount")
        try:
            if a is not None or b is not None:
                down_pool = float(a or 0) + float(b or 0)
        except (TypeError, ValueError):
            pass

    # Explicit odds fields from API only (never derive from pool sizes).
    up_pct = down_pct = None
    for uk in ("upOdds", "upPayout", "upPayRate", "upPercent", "upPct"):
        if resp.get(uk) is not None:
            try:
                v = float(resp[uk])
                up_pct = v * 100.0 if v <= 20 else v
                break
            except (TypeError, ValueError):
                pass
    for dk in ("downOdds", "downPayout", "downPayRate", "downPercent", "downPct"):
        if resp.get(dk) is not None:
            try:
                v = float(resp[dk])
                down_pct = v * 100.0 if v <= 20 else v
                break
            except (TypeError, ValueError):
                pass

    return up_pct, down_pct, up_pool, down_pool


def _decode_ws_payload(raw: Any) -> dict[str, Any] | None:
    try:
        if isinstance(raw, str):
            return json.loads(raw)
        if isinstance(raw, (bytes, bytearray)):
            try:
                data = zlib.decompress(bytes(raw))
            except zlib.error:
                data = bytes(raw)
            return json.loads(data.decode("utf-8"))
    except Exception:
        return None
    return None


def _parse_prices(tooltip_text: str) -> tuple[float | None, float | None]:
    text = " ".join((tooltip_text or "").split())
    start = end = None
    m_start = re.search(r"Start\s*Rate\s*([\d.]+)", text, re.I)
    m_end = re.search(r"End\s*rate\s*([\d.]+)", text, re.I)
    if m_start:
        start = float(m_start.group(1))
    if m_end:
        end = float(m_end.group(1))
    if start is None or end is None:
        nums = [float(x) for x in _PRICE_RE.findall(text)]
        if len(nums) >= 2:
            start = start if start is not None else nums[0]
            end = end if end is not None else nums[1]
    return start, end


def _side_from_class(class_name: str) -> str | None:
    cls = class_name or ""
    has_up = "text-up" in cls or "bg-up" in cls
    has_down = "text-down" in cls or "bg-down" in cls
    if has_down and not has_up:
        return "DOWN"
    if has_up and not has_down:
        return "UP"
    if has_down:
        return "DOWN"
    if has_up:
        return "UP"
    return None


def _fingerprint(side: str, start: float | None, end: float | None) -> str:
    """Unique enough id for a settled chip (prices identify the round)."""
    if start is not None and end is not None:
        raw = f"html|{side}|{start:.8f}|{end:.8f}"
    else:
        # Weak fallback — include ms so we never collapse streaks silently.
        raw = f"html|{side}|noprice|{int(time.time() * 1000)}"
    return "h" + hashlib.sha1(raw.encode("utf-8")).hexdigest()[:15]


def new_results_from_snapshot(prev: list[str], curr: list[str]) -> list[str]:
    if not curr or not prev or prev == curr:
        return []
    for i in range(len(prev)):
        suffix = prev[i:]
        if curr[: len(suffix)] == suffix:
            return curr[len(suffix) :]
    return curr[-1:]


async def scrape_html_history(
    db: Database,
    *,
    symbol: str = "BTC/USD",
    label: str = "btc_5",
    duration_sec: int = 0,
    poll_sec: int = POLL_SEC,
    on_event: ProgressCallback | None = None,
    should_stop: Callable[[], bool] | None = None,
) -> dict[str, Any]:
    """
    Collect settled rounds by:
      1) Tapping the page DeTrade WebSocket (real game ids) when available
      2) DOM poll every ~2s: fingerprint the newest chip via Start/End prices
         so same-side streaks (UUUUUU→UUUUUU) are still detected
    """
    try:
        from playwright.async_api import async_playwright
    except ImportError as exc:
        raise RuntimeError(
            "playwright is not installed. Run: pip install playwright && playwright install chromium"
        ) from exc

    period_sec = 5
    if label and "_" in label:
        try:
            period_sec = int(label.rsplit("_", 1)[-1])
        except ValueError:
            period_sec = 5

    run_id = db.start_run(symbol, label)
    upserted = 0
    polls = 0
    ws_frames = 0
    odds_locked = 0
    started = time.time()
    PROFILE_DIR.mkdir(parents=True, exist_ok=True)
    use_headless = HEADLESS
    prev_sides: list[str] = []
    last_fp: str | None = None
    seen_ids: set[str] = set()
    live_game_id: str | None = None
    live_countdown: float | None = None
    last_good_up: float | None = None
    last_good_down: float | None = None
    last_good_at: float = 0.0
    last_dom_cd: float | None = None
    loop = asyncio.get_running_loop()

    def _timed_out() -> bool:
        return duration_sec > 0 and (time.time() - started) >= duration_sec

    def _stop() -> bool:
        return bool(should_stop and should_stop())

    def _save_odds(
        *,
        round_id: str | None,
        up_pct: float | None,
        down_pct: float | None,
        up_pool: float | None = None,
        down_pool: float | None = None,
        countdown_sec: float | None = None,
        source: str,
        lock: bool | None = None,
    ) -> dict[str, Any] | None:
        nonlocal odds_locked
        rid = (round_id or live_game_id or "").strip()
        # Live UI: push DOM reading whenever present.
        if on_event and source == "dom" and (up_pct is not None or down_pct is not None or countdown_sec is not None):
            on_event(
                {
                    "type": "odds",
                    "round_id": rid or None,
                    "up_pct": up_pct,
                    "down_pct": down_pct,
                    "up_payout": up_pct,
                    "down_payout": down_pct,
                    "up_pool": up_pool,
                    "down_pool": down_pool,
                    "countdown_sec": countdown_sec,
                    "locked": bool(lock),
                    "source": source,
                    "target_sec": ODDS_TARGET_SEC,
                    "unit": "return_percent",
                    "skipped": not bool(rid),
                }
            )
        if not rid:
            return None
        # Persist Up/Down Wins % only when locking at timer 2–3s (DOM).
        if source == "dom":
            if lock is None:
                lock = _in_odds_lock_window(countdown_sec)
            if not lock:
                return None
            try:
                if (
                    up_pct is None
                    or down_pct is None
                    or float(up_pct) < 110
                    or float(down_pct) < 110
                ):
                    return None
            except (TypeError, ValueError):
                return None
        else:
            lock = False

        result = db.upsert_pool_snapshot(
            round_id=rid,
            symbol=symbol,
            label=label,
            up_pct=up_pct,
            down_pct=down_pct,
            up_pool=up_pool,
            down_pool=down_pool,
            countdown_sec=countdown_sec,
            lock=bool(lock),
            source=source,
        )
        if result.get("locked") and not result.get("skipped"):
            odds_locked += 1
        if on_event and source == "dom" and result.get("locked"):
            row = result.get("row") or {}
            on_event(
                {
                    "type": "odds",
                    "round_id": rid,
                    "up_pct": up_pct if up_pct is not None else row.get("up_pct"),
                    "down_pct": down_pct if down_pct is not None else row.get("down_pct"),
                    "up_payout": up_pct if up_pct is not None else row.get("up_pct"),
                    "down_payout": down_pct if down_pct is not None else row.get("down_pct"),
                    "up_pool": up_pool if up_pool is not None else row.get("up_pool"),
                    "down_pool": down_pool if down_pool is not None else row.get("down_pool"),
                    "countdown_sec": countdown_sec,
                    "locked": True,
                    "source": source,
                    "target_sec": ODDS_TARGET_SEC,
                    "unit": "return_percent",
                    "skipped": bool(result.get("skipped")),
                }
            )
        return result

    def _emit_round(rid: str, side: str, start: float | None, end: float | None, source: str) -> bool:
        nonlocal upserted
        if not rid or rid in seen_ids:
            return False
        win_side = 1 if side == "UP" else 2
        inserted = db.upsert_round(
            {
                "id": rid,
                "winSide": win_side,
                "startPrice": start,
                "endPrice": end,
                "status": 1004,
            },
            symbol=symbol,
            label=label,
            period_sec=period_sec,
            collected_from=source,
            settled_only=True,
        )
        seen_ids.add(rid)
        if inserted:
            upserted += 1
            if on_event:
                on_event(
                    {
                        "type": "round",
                        "game_id": rid,
                        "result": side,
                        "source": source,
                        "start_price": start,
                        "end_price": end,
                        "db_rounds": db.count_rounds(symbol, settled_only=True),
                        "odds": db.get_pool_snapshot(rid),
                    }
                )
        return inserted

    def _handle_ws_data(data: dict[str, Any]) -> None:
        nonlocal ws_frames, upserted, live_game_id, live_countdown
        ws_frames += 1
        cmd = str(data.get("cmd") or "")
        resp = data.get("resp")
        if not isinstance(resp, dict):
            return

        # Live pool updates (subscribe amountPool).
        if "amountPool" in cmd or "amount_pool" in cmd.lower():
            up_pct, down_pct, up_pool, down_pool = _pool_fields_from_resp(resp)
            rid = str(resp.get("id") or resp.get("gameId") or live_game_id or "").strip()
            _save_odds(
                round_id=rid or None,
                up_pct=None,
                down_pct=None,
                up_pool=up_pool,
                down_pool=down_pool,
                countdown_sec=live_countdown,
                source="ws_amountPool",
            )
            return

        if not (cmd.endswith("/ticker") and "contest" in cmd and "/kline/" not in cmd):
            return

        hist = resp.get("previousRoundResult") or []
        if isinstance(hist, list) and hist:
            n = db.upsert_history_batch(
                hist,
                symbol=symbol,
                label=label,
                period_sec=period_sec,
                collected_from="html_ws_previousRoundResult",
            )
            if n:
                upserted += n
                for item in hist:
                    rid = str(item.get("id") or "")
                    if rid:
                        seen_ids.add(rid)
                if on_event:
                    on_event(
                        {
                            "type": "ws_history",
                            "count": len(hist),
                            "new": n,
                            "db_rounds": db.count_rounds(symbol, settled_only=True),
                        }
                    )

        rid = str(resp.get("id") or "").strip()
        if rid:
            live_game_id = rid

        # Countdown / cutoff hints from ticker when present.
        for key in ("countdown", "countDown", "remainSec", "remainSeconds", "leftSec"):
            if resp.get(key) is not None:
                try:
                    live_countdown = float(resp[key])
                    break
                except (TypeError, ValueError):
                    pass
        cutoff = resp.get("tradeCutoffTime")
        now_ms = int(time.time() * 1000)
        try:
            if cutoff is not None:
                c = int(cutoff)
                if c < 1_000_000_000_000:
                    c *= 1000
                rem = (c - now_ms) / 1000.0
                if 0 <= rem <= 30:
                    live_countdown = rem
        except (TypeError, ValueError):
            pass

        _, _, up_pool, down_pool = _pool_fields_from_resp(resp)
        if up_pool is not None or down_pool is not None:
            _save_odds(
                round_id=rid or live_game_id,
                up_pct=None,
                down_pct=None,
                up_pool=up_pool,
                down_pool=down_pool,
                countdown_sec=live_countdown,
                source="ws_ticker",
            )

        win = resp.get("winSide")
        try:
            win_i = int(win) if win is not None else 0
        except (TypeError, ValueError):
            win_i = 0
        if rid and win_i in (1, 2) and rid not in seen_ids:
            side = "UP" if win_i == 1 else "DOWN"
            start = resp.get("startPrice")
            end = resp.get("endPrice")
            try:
                start_f = float(start) if start is not None else None
            except (TypeError, ValueError):
                start_f = None
            try:
                end_f = float(end) if end is not None else None
            except (TypeError, ValueError):
                end_f = None
            _emit_round(rid, side, start_f, end_f, "html_ws_game")

    async def _history_buttons(page: Any) -> Any:
        buttons = page.locator(
            "button.detrade-button.text-up, button.detrade-button.text-down, "
            "button.detrade-button[class*='text-up'], button.detrade-button[class*='text-down'], "
            "button.detrade-button[class*='bg-up'], button.detrade-button[class*='bg-down']"
        )
        if await buttons.count() == 0:
            buttons = page.locator(
                "button[class*='text-up'], button[class*='text-down'], "
                "button[class*='bg-up/'], button[class*='bg-down/']"
            )
        return buttons

    async def _read_sides(page: Any) -> list[str]:
        buttons = await _history_buttons(page)
        count = await buttons.count()
        sides: list[str] = []
        for i in range(count):
            class_name = (await buttons.nth(i).get_attribute("class")) or ""
            side = _side_from_class(class_name)
            if side:
                sides.append(side)
        return sides

    async def _newest_fingerprint(page: Any, sides: list[str]) -> tuple[str | None, float | None, float | None]:
        """Hover the rightmost (newest) chip and read Start/End prices."""
        if not sides:
            return None, None, None
        buttons = await _history_buttons(page)
        count = await buttons.count()
        if count == 0:
            return None, None, None
        idx = count - 1
        side = sides[-1]
        start = end = None
        try:
            await buttons.nth(idx).hover(timeout=1500)
            await page.wait_for_timeout(200)
            tip = page.locator("text=Start Rate").first
            if await tip.count() > 0:
                tip_root = page.locator(
                    "div.safe-bottom-area, article:has-text('Start Rate')"
                ).last
                tip_text = await tip_root.inner_text(timeout=1200)
                start, end = _parse_prices(tip_text)
        except Exception:
            pass
        finally:
            try:
                await page.mouse.move(8, 8)
            except Exception:
                pass
        if start is None or end is None:
            return None, None, None
        return _fingerprint(side, start, end), start, end

    async with async_playwright() as p:
        ext_args = _extension_load_args()
        # Extensions only work in headed Chromium.
        if ext_args and use_headless:
            logger.warning("Extension load requested — forcing headed mode (not headless)")
            use_headless = False
        launch_args = ["--disable-blink-features=AutomationControlled", *ext_args]
        if ext_args:
            logger.info("Loading unpacked extension from %s", EXTENSION_DIR)
            if on_event:
                on_event({"type": "extension", "path": str(EXTENSION_DIR.resolve())})
        context = await p.chromium.launch_persistent_context(
            user_data_dir=str(PROFILE_DIR),
            headless=use_headless,
            viewport={"width": 1400, "height": 900},
            args=launch_args,
        )
        page = context.pages[0] if context.pages else await context.new_page()

        def on_websocket(ws: Any) -> None:
            url = getattr(ws, "url", "") or ""
            if "websocket.detrade.com" not in url:
                return
            logger.info("Attached to DeTrade websocket")
            if on_event:
                on_event({"type": "ws_attached", "url": url.split("?")[0]})

            def on_frame(payload: Any) -> None:
                data = _decode_ws_payload(payload)
                if not data:
                    return
                try:
                    loop.call_soon_threadsafe(_handle_ws_data, data)
                except RuntimeError:
                    _handle_ws_data(data)

            ws.on("framereceived", on_frame)

        page.on("websocket", on_websocket)

        try:
            await page.goto(PAGE_URL, wait_until="domcontentloaded", timeout=90_000)
            await page.wait_for_timeout(2500)

            msg = (
                "Browser open — login / close popups if needed. "
                "Collecting via DeTrade socket + newest-chip price fingerprints."
            )
            logger.info("%s", msg)
            if on_event:
                on_event({"type": "waiting_for_user", "error": msg, "url": page.url})

            wait_deadline = time.time() + USER_WAIT_SEC
            while time.time() < wait_deadline and not _timed_out() and not _stop():
                sides = await _read_sides(page)
                url = page.url
                if sides and "login" not in url.lower() and "signin" not in url.lower():
                    logger.info("History chips detected (%s)", len(sides))
                    if on_event:
                        on_event({"type": "ready", "buttons": len(sides), "url": url})
                    break
                await asyncio.sleep(1)

            while not _timed_out() and not _stop():
                polls += 1
                try:
                    sides = await _read_sides(page)
                    pattern = "".join("U" if s == "UP" else "D" for s in sides)

                    # Every ~2s: read center timer; if 2–3s, lock Up/Down Wins % for this round.
                    try:
                        dom_odds = await page.evaluate(_DOM_ODDS_JS)
                    except Exception:
                        dom_odds = None
                    if isinstance(dom_odds, dict):
                        dom_cd = None
                        try:
                            if dom_odds.get("countdown") is not None:
                                dom_cd = float(dom_odds["countdown"])
                                live_countdown = dom_cd
                                last_dom_cd = dom_cd
                        except (TypeError, ValueError):
                            pass
                        up_pct = dom_odds.get("up_pct")
                        down_pct = dom_odds.get("down_pct")
                        up_ret = dom_odds.get("up_return")
                        down_ret = dom_odds.get("down_return")
                        try:
                            inv = float(dom_odds.get("investment") or 1.0)
                            if inv <= 0:
                                inv = 1.0
                        except (TypeError, ValueError):
                            inv = 1.0
                        if up_pct is None and up_ret is not None:
                            try:
                                up_pct = round(float(up_ret) / inv * 100.0)
                            except (TypeError, ValueError):
                                pass
                        if down_pct is None and down_ret is not None:
                            try:
                                down_pct = round(float(down_ret) / inv * 100.0)
                            except (TypeError, ValueError):
                                pass

                        in_lock = _in_odds_lock_window(dom_cd) or _in_odds_lock_window(
                            live_countdown
                        )
                        lock_cd = dom_cd
                        if not _in_odds_lock_window(dom_cd) and _in_odds_lock_window(
                            live_countdown
                        ):
                            lock_cd = float(live_countdown)
                        try:
                            if (
                                up_pct is not None
                                and down_pct is not None
                                and float(up_pct) >= 110
                                and float(down_pct) >= 110
                            ):
                                last_good_up = float(up_pct)
                                last_good_down = float(down_pct)
                                last_good_at = time.time()
                        except (TypeError, ValueError):
                            pass
                        # Live UI: always show latest timer / % from page.
                        if on_event and (
                            up_pct is not None or down_pct is not None or dom_cd is not None
                        ):
                            on_event(
                                {
                                    "type": "odds",
                                    "round_id": live_game_id,
                                    "up_pct": up_pct,
                                    "down_pct": down_pct,
                                    "up_payout": up_pct,
                                    "down_payout": down_pct,
                                    "countdown_sec": dom_cd,
                                    "locked": False,
                                    "source": "dom",
                                    "target_sec": ODDS_TARGET_SEC,
                                    "unit": "return_percent",
                                    "skipped": not in_lock,
                                }
                            )
                        # Persist ONLY when timer is 2–3s (use last good % if this tick missed parse).
                        lock_up = up_pct
                        lock_down = down_pct
                        if in_lock and (lock_up is None or lock_down is None):
                            if last_good_up is not None and last_good_down is not None and (
                                time.time() - last_good_at
                            ) <= 4.0:
                                lock_up = last_good_up
                                lock_down = last_good_down
                        if in_lock and lock_up is not None and lock_down is not None and live_game_id:
                            _save_odds(
                                round_id=live_game_id,
                                up_pct=float(lock_up),
                                down_pct=float(lock_down),
                                countdown_sec=lock_cd if lock_cd is not None else dom_cd,
                                source="dom",
                                lock=True,
                            )
                        if on_event and (polls % 5 == 0 or in_lock or dom_cd is not None):
                            on_event(
                                {
                                    "type": "odds_debug",
                                    "countdown": dom_cd,
                                    "in_lock_window": in_lock,
                                    "raw": dom_odds.get("raw"),
                                    "debug": dom_odds.get("debug"),
                                    "up_pct": up_pct,
                                    "down_pct": down_pct,
                                    "up_return": up_ret,
                                    "down_return": down_ret,
                                    "live_game_id": live_game_id,
                                }
                            )

                    # Prefer WS. Only fingerprint-hover when socket is quiet.
                    fp = start = end = None
                    if ws_frames == 0 or (polls % 5 == 0 and last_fp is None):
                        fp, start, end = await _newest_fingerprint(page, sides)
                    elif sides:
                        # Keep DOM pattern in sync without hovering.
                        pass

                    if on_event:
                        on_event(
                            {
                                "type": "html_poll",
                                "buttons": len(sides),
                                "pattern": pattern,
                                "newest_fp": fp,
                                "poll": polls,
                                "poll_sec": poll_sec,
                                "ws_frames": ws_frames,
                                "countdown_sec": live_countdown,
                                "live_game_id": live_game_id,
                                "odds_locked": odds_locked,
                                "db_rounds": db.count_rounds(symbol, settled_only=True),
                            }
                        )

                    if ws_frames > 0:
                        # Socket owns correctness; just track visible pattern for status.
                        prev_sides = list(sides) if sides else prev_sides
                    elif not sides:
                        logger.warning("No chips on poll %s", polls)
                    elif fp and fp != last_fp:
                        # Newest chip identity changed → at least one new settled round.
                        # Prefer WS ids; DOM fingerprint fills gaps (esp. same-side streaks).
                        side = sides[-1]
                        if last_fp is None:
                            logger.info("DOM baseline pattern=%s fp=%s", pattern, fp)
                            if on_event:
                                on_event(
                                    {
                                        "type": "html_baseline",
                                        "pattern": pattern,
                                        "fp": fp,
                                        "count": len(sides),
                                    }
                                )
                        else:
                            # How many new chips by side-pattern diff (may be 0 on streak).
                            fresh = new_results_from_snapshot(prev_sides, sides)
                            if not fresh:
                                fresh = [side]  # same-side streak: fingerprint proves 1 new
                            # Only persist the newest via fingerprint (unique). Older missed
                            # rounds in `fresh[:-1]` lack prices — skip rather than invent.
                            _emit_round(fp, side, start, end, "html_dom_fp")
                            logger.info(
                                "poll %s pattern=%s new=%s fp=%s prices=%s→%s",
                                polls,
                                pattern,
                                side[0],
                                fp[:8],
                                start,
                                end,
                            )
                            if on_event:
                                on_event(
                                    {
                                        "type": "html_snapshot",
                                        "pattern": pattern,
                                        "new": 1,
                                        "new_pattern": "U" if side == "UP" else "D",
                                        "fp": fp,
                                        "upserted": upserted,
                                        "db_rounds": db.count_rounds(symbol, settled_only=True),
                                    }
                                )
                        last_fp = fp
                        prev_sides = list(sides)
                    else:
                        prev_sides = list(sides) if sides else prev_sides

                except Exception as exc:
                    msg = str(exc)
                    logger.exception("html poll failed")
                    if on_event:
                        on_event({"type": "error", "error": msg})
                    # Browser/page gone — exit so the outer loop can relaunch Chromium.
                    closed = (
                        "has been closed" in msg.lower()
                        or "target closed" in msg.lower()
                        or "browser has been closed" in msg.lower()
                    )
                    if closed:
                        if on_event:
                            on_event({"type": "browser_closed", "error": msg})
                        break

                # Near cutoff, poll every 1s so we don't miss the 2–3s lock window.
                cd_now = None
                for cand in (last_dom_cd, live_countdown):
                    if cand is None:
                        continue
                    try:
                        cd_now = float(cand)
                        break
                    except (TypeError, ValueError):
                        pass
                sleep_for = 1 if (cd_now is not None and cd_now <= 6) else max(int(poll_sec), 1)
                for _ in range(sleep_for):
                    if _timed_out() or _stop():
                        break
                    await asyncio.sleep(1)
        finally:
            try:
                await context.close()
            except Exception:
                pass

    db.end_run(run_id, upserted, notes=f"html polls={polls} ws_frames={ws_frames} odds_locked={odds_locked}")
    return {
        "run_id": run_id,
        "symbol": symbol,
        "label": label,
        "upserted": upserted,
        "polls": polls,
        "ws_frames": ws_frames,
        "odds_locked": odds_locked,
        "mode": "html",
        "db_rounds": db.count_rounds(symbol, settled_only=True),
    }
