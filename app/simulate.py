"""Bankroll simulation: prediction correctness × BC return % (e.g. 196%)."""
from __future__ import annotations

from typing import Any

from .odds_parse import ENTRY_FEE, normalize_return_pct, settle_one_bet


def simulate_bankroll(
    rows: list[dict[str, Any]],
    *,
    deposit: float,
    stake: float,
    fee_rate: float = ENTRY_FEE,
) -> dict[str, Any]:
    deposit = float(deposit)
    stake = float(stake)
    if deposit <= 0 or stake <= 0:
        return {"ok": False, "error": "deposit and stake must be > 0"}

    balance = deposit
    peak = deposit
    max_drawdown = 0.0
    equity: list[dict[str, Any]] = []
    bets = 0
    wins = 0
    losses = 0
    skips = 0
    busted = False
    bust_at: int | None = None

    for row in rows:
        predicted = row.get("predicted")
        result = row.get("result") or row.get("actual")
        up_pct = row.get("up_pct")
        down_pct = row.get("down_pct")
        if predicted not in ("UP", "DOWN") or result not in ("UP", "DOWN"):
            skips += 1
            continue
        if normalize_return_pct(up_pct) is None and row.get("up_pool") is None:
            skips += 1
            continue
        if balance < stake:
            busted = True
            bust_at = bets
            break
        fee = row.get("fee_rate")
        fee_f = float(fee) if fee is not None else float(fee_rate)
        try:
            settled = settle_one_bet(
                balance=balance,
                stake=stake,
                predicted=predicted,
                result=result,
                up_pct=float(up_pct) if up_pct is not None else 0.0,
                down_pct=float(down_pct) if down_pct is not None else 0.0,
                fee_rate=fee_f,
                up_pool=row.get("up_pool"),
                down_pool=row.get("down_pool"),
                round_id=row.get("round_id") or row.get("id"),
                bet_n=bets + 1,
            )
        except ValueError:
            skips += 1
            continue
        balance = float(settled["balance"])
        if settled["row"]["outcome"] == "WIN":
            wins += 1
        else:
            losses += 1
        bets += 1
        peak = max(peak, balance)
        dd = (peak - balance) / peak if peak > 0 else 0.0
        max_drawdown = max(max_drawdown, dd)
        equity.append(settled["row"])
        if balance < stake:
            busted = True
            bust_at = bets
            break

    return {
        "ok": True,
        "deposit": deposit,
        "stake": stake,
        "fee_rate": fee_rate,
        "strategy": "prediction_correctness",
        "final_balance": round(balance, 4),
        "profit": round(balance - deposit, 4),
        "roi": round((balance - deposit) / deposit, 4) if deposit else None,
        "bets": bets,
        "wins": wins,
        "losses": losses,
        "skips": skips,
        "win_rate": round(wins / bets, 4) if bets else None,
        "busted": busted,
        "bust_at_bet": bust_at,
        "rounds_survived": bets,
        "max_drawdown": round(max_drawdown, 4),
        "peak_balance": round(peak, 4),
        "samples_available": len(rows),
        "equity": equity,
        "odds_unit": "return_percent",
    }
