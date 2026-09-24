"""Scrape BC.Game Up/Down history from the rendered HTML (no DeTrade temp-login API)."""
from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import re
import time
from pathlib import Path
from typing import Any, Callable

from .db import Database

logger = logging.getLogger(__name__)

PAGE_URL = os.getenv("HTML_SCRAPE_URL", "https://bc.game/trading/up-down")
POLL_SEC = int(os.getenv("HTML_POLL_SEC", "27"))
HEADLESS = os.getenv("HTML_HEADLESS", "0").strip().lower() in {"1", "true", "yes"}
PROFILE_DIR = Path(
    os.getenv(
        "HTML_BROWSER_PROFILE",
        str(Path(__file__).resolve().parent.parent / "data" / "browser_profile"),
    )
)
# How long to leave the page alone while you login / dismiss gates (seconds).
USER_WAIT_SEC = int(os.getenv("HTML_USER_WAIT_SEC", "600"))

ProgressCallback = Callable[[dict[str, Any]], None]

_PRICE_RE = re.compile(r"[\d]+(?:\.\d+)?")


def _round_id(result: str, start: float | None, end: float | None, seq: str) -> str:
    raw = f"html|{result}|{start}|{end}|{seq}"
    return "h" + hashlib.sha1(raw.encode("utf-8")).hexdigest()[:15]


def _parse_prices(tooltip_text: str) -> tuple[float | None, float | None]:
    """Extract Start Rate / End rate from tooltip text."""
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
    Open the Up/Down page in Chromium and poll the history chip strip.

    Buttons use classes text-up / text-down. Hover tooltips expose Start/End rates.
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
    started = time.time()
    PROFILE_DIR.mkdir(parents=True, exist_ok=True)
    # Default is headed so you can click/login. Set HTML_HEADLESS=1 later.
    use_headless = HEADLESS

    def _timed_out() -> bool:
        return duration_sec > 0 and (time.time() - started) >= duration_sec

    def _stop() -> bool:
        return bool(should_stop and should_stop())

    async def _history_button_count(page: Any) -> tuple[Any, int]:
        buttons = page.locator(
            "button.detrade-button.text-up, button.detrade-button.text-down, "
            "button.detrade-button[class*='text-up'], button.detrade-button[class*='text-down'], "
            "button.detrade-button[class*='bg-up'], button.detrade-button[class*='bg-down']"
        )
        count = await buttons.count()
        if count == 0:
            buttons = page.locator(
                "button[class*='text-up'], button[class*='text-down'], "
                "button[class*='bg-up/'], button[class*='bg-down/']"
            )
            count = await buttons.count()
        return buttons, count

    async with async_playwright() as p:
        context = await p.chromium.launch_persistent_context(
            user_data_dir=str(PROFILE_DIR),
            headless=use_headless,
            viewport={"width": 1400, "height": 900},
            args=["--disable-blink-features=AutomationControlled"],
        )
        page = context.pages[0] if context.pages else await context.new_page()
        try:
            await page.goto(PAGE_URL, wait_until="domcontentloaded", timeout=90_000)
            await page.wait_for_timeout(3000)

            msg = (
                "Browser open — you can login, close popups, and open Up/Down. "
                "I will NOT reload or move the mouse until history chips appear."
            )
            logger.info("%s", msg)
            if on_event:
                on_event({"type": "waiting_for_user", "error": msg, "url": page.url})

            # Passive wait: do not navigate or hover — leave the page fully to the user.
            wait_deadline = time.time() + USER_WAIT_SEC
            while time.time() < wait_deadline and not _timed_out() and not _stop():
                _, count = await _history_button_count(page)
                url = page.url
                if on_event and int(time.time()) % 5 == 0:
                    on_event(
                        {
                            "type": "waiting_for_user",
                            "buttons": count,
                            "url": url,
                            "hint": "Act in the Chromium window; scraping starts when chips appear.",
                        }
                    )
                if count > 0 and "login" not in url.lower() and "signin" not in url.lower():
                    logger.info("History chips detected (%s) — starting scrape loop", count)
                    if on_event:
                        on_event({"type": "ready", "buttons": count, "url": url})
                    break
                await asyncio.sleep(2)

            while not _timed_out() and not _stop():
                polls += 1
                try:
                    buttons, count = await _history_button_count(page)

                    if on_event:
                        on_event(
                            {
                                "type": "html_poll",
                                "buttons": count,
                                "url": page.url,
                                "poll": polls,
                                "headless": use_headless,
                            }
                        )

                    if count == 0:
                        for _ in range(max(poll_sec, 1)):
                            if _timed_out() or _stop():
                                break
                            await asyncio.sleep(1)
                        continue

                    # DOM order left→right; treat as oldest→newest (strip is justify-end).
                    seen_seq: list[str] = []
                    for i in range(count):
                        if _stop():
                            break
                        btn = buttons.nth(i)
                        class_name = (await btn.get_attribute("class")) or ""
                        side = _side_from_class(class_name)
                        if side is None:
                            continue

                        start_price = end_price = None
                        # Only hover for prices every few polls to avoid fighting your mouse.
                        if polls == 1 or polls % 3 == 0:
                            try:
                                await btn.hover(timeout=2000)
                                await page.wait_for_timeout(180)
                                tip = page.locator("text=Start Rate").first
                                if await tip.count() > 0:
                                    tip_root = page.locator(
                                        "div.safe-bottom-area, article:has-text('Start Rate')"
                                    ).last
                                    tip_text = await tip_root.inner_text(timeout=1500)
                                    start_price, end_price = _parse_prices(tip_text)
                            except Exception:
                                pass

                        seq_key = f"{i}:{side}:{start_price}:{end_price}"
                        seen_seq.append(side)
                        rid = _round_id(side, start_price, end_price, seq_key)
                        if start_price is not None and end_price is not None:
                            rid = _round_id(
                                side, start_price, end_price, f"{start_price}:{end_price}"
                            )

                        win_side = 1 if side == "UP" else 2
                        inserted = db.upsert_round(
                            {
                                "id": rid,
                                "winSide": win_side,
                                "startPrice": start_price,
                                "endPrice": end_price,
                                "status": 1004,
                            },
                            symbol=symbol,
                            label=label,
                            period_sec=period_sec,
                            collected_from="html",
                            settled_only=True,
                        )
                        if inserted:
                            upserted += 1
                            if on_event:
                                on_event(
                                    {
                                        "type": "round",
                                        "game_id": rid,
                                        "result": side,
                                        "start_price": start_price,
                                        "end_price": end_price,
                                        "db_rounds": db.count_rounds(symbol, settled_only=True),
                                    }
                                )

                    try:
                        await page.mouse.move(10, 10)
                    except Exception:
                        pass

                    if on_event:
                        on_event(
                            {
                                "type": "html_snapshot",
                                "pattern": "".join("U" if s == "UP" else "D" for s in seen_seq),
                                "count": len(seen_seq),
                                "upserted": upserted,
                                "db_rounds": db.count_rounds(symbol, settled_only=True),
                            }
                        )
                except Exception as exc:
                    logger.exception("html poll failed")
                    if on_event:
                        on_event({"type": "error", "error": str(exc)})

                for _ in range(max(poll_sec, 1)):
                    if _timed_out() or _stop():
                        break
                    await asyncio.sleep(1)
        finally:
            await context.close()

    db.end_run(run_id, upserted, notes=f"html polls={polls}")
    return {
        "run_id": run_id,
        "symbol": symbol,
        "label": label,
        "upserted": upserted,
        "polls": polls,
        "mode": "html",
        "db_rounds": db.count_rounds(symbol, settled_only=True),
    }
