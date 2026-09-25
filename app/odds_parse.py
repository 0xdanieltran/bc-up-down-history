"""Parse BC Up/Down return % / potential return from the page and settle P&L."""
from __future__ import annotations

from typing import Any

# Exact BC markup:
#   <div class="cutdown-bounce-in">8</div><div>Sec</div>
#   <span>Up Wins</span><strong class="text-50 text-up">197%</strong>
#   <h4>Potential Return</h4><p>$1.96</p>
DOM_ODDS_JS = r"""
() => {
  const out = {
    countdown: null,
    up_pct: null,
    down_pct: null,
    up_return: null,
    down_return: null,
    investment: null,
    raw: [],
    debug: []
  };

  const textOf = (el) => (el.innerText || el.textContent || '').replace(/\s+/g, ' ').trim();
  const visible = (el) => {
    const r = el.getBoundingClientRect();
    if (r.width < 1 || r.height < 1) return false;
    const st = window.getComputedStyle(el);
    if (st.display === 'none' || st.visibility === 'hidden' || Number(st.opacity) === 0) return false;
    return true;
  };

  // --- Center timer: number div + sibling "Sec" (class cutdown-bounce-in) ---
  const bounce = document.querySelector('.cutdown-bounce-in, [class*="cutdown-bounce"]');
  if (bounce) {
    const n = parseInt(textOf(bounce), 10);
    if (!Number.isNaN(n) && n >= 0 && n <= 30) {
      out.countdown = n;
      out.raw.push('cd:bounce=' + n);
    }
  }
  if (out.countdown == null) {
    const wraps = document.querySelectorAll(
      '.abs-center, [class*="abs-center"], [class*="flex-center"]'
    );
    for (const wrap of wraps) {
      if (!visible(wrap)) continue;
      let secEl = null;
      let numEl = null;
      for (const c of wrap.querySelectorAll('div, span')) {
        if (c.children.length) continue;
        const t = textOf(c);
        if (t === 'Sec' || t === 'sec') secEl = c;
        else if (/^\d{1,2}$/.test(t)) numEl = c;
      }
      if (secEl && numEl) {
        const n = parseInt(textOf(numEl), 10);
        if (n >= 0 && n <= 30) {
          out.countdown = n;
          out.raw.push('cd:wrap=' + n);
          break;
        }
      }
    }
  }
  if (out.countdown == null) {
    for (const el of document.querySelectorAll('div, span')) {
      if (!visible(el) || el.children.length) continue;
      if (textOf(el) !== 'Sec' && textOf(el) !== 'sec') continue;
      const parent = el.parentElement;
      if (!parent) continue;
      for (const sib of parent.children) {
        if (sib === el || sib.children.length > 0) continue;
        const t = textOf(sib);
        if (!/^\d{1,2}$/.test(t)) continue;
        const n = parseInt(t, 10);
        if (n < 0 || n > 30) continue;
        out.countdown = n;
        out.raw.push('cd:sec-sib=' + n);
        break;
      }
      if (out.countdown != null) break;
    }
  }

  // --- Up Wins / Down Wins: badge + strong.text-50 with NNN% ---
  const readSide = (labelRe, side) => {
    for (const badge of document.querySelectorAll('span, div, p')) {
      if (!visible(badge)) continue;
      const bt = textOf(badge);
      if (!labelRe.test(bt) || bt.length > 24) continue;
      // Stay inside this side's <section> so we don't mix Up/Down Potential Return.
      const section = badge.closest('section') || badge.parentElement;
      if (!section) continue;
      let pctEl = section.querySelector('strong.text-50, strong[class*="text-50"]');
      if (!pctEl) {
        for (const s of section.querySelectorAll('strong, b')) {
          if (/^\d{2,3}(?:\.\d+)?\s*%$/.test(textOf(s))) { pctEl = s; break; }
        }
      }
      if (pctEl) {
        const pm = textOf(pctEl).match(/^(\d{2,3}(?:\.\d+)?)\s*%$/);
        if (pm) {
          const v = parseFloat(pm[1]);
          if (v >= 100 && v <= 900) {
            if (side === 'UP') out.up_pct = v;
            else out.down_pct = v;
            out.raw.push(side + ':' + v + '%');
          }
        }
      }
      const heads = section.querySelectorAll('h4, h3');
      for (const h of heads) {
        const ht = textOf(h);
        if (/^potential\s*return$/i.test(ht)) {
          let p = h.nextElementSibling;
          for (let k = 0; k < 3 && p; k++) {
            const m = textOf(p).match(/\$\s*(\d+(?:\.\d+)?)/);
            if (m) {
              const dollars = parseFloat(m[1]);
              if (dollars >= 0.5 && dollars <= 100) {
                if (side === 'UP') out.up_return = dollars;
                else out.down_return = dollars;
                out.raw.push(side + ':$' + dollars);
              }
              break;
            }
            p = p.nextElementSibling;
          }
        }
        if (/^your\s*investment$/i.test(ht) && out.investment == null) {
          let p = h.nextElementSibling;
          for (let k = 0; k < 3 && p; k++) {
            const m = textOf(p).match(/\$\s*(\d+(?:\.\d+)?)/);
            if (m) {
              out.investment = parseFloat(m[1]);
              break;
            }
            p = p.nextElementSibling;
          }
        }
      }
      return;
    }
  };

  readSide(/^up\s*wins$/i, 'UP');
  readSide(/^down\s*wins$/i, 'DOWN');

  // Fallback: text-up / text-down strong percentages
  if (out.up_pct == null || out.down_pct == null) {
    for (const el of document.querySelectorAll('strong, b')) {
      if (!visible(el)) continue;
      const t = textOf(el);
      const pm = t.match(/^(\d{2,3}(?:\.\d+)?)\s*%$/);
      if (!pm) continue;
      const v = parseFloat(pm[1]);
      if (v < 100 || v > 900) continue;
      const cls = (el.className || '') + ' ' + ((el.parentElement && el.parentElement.className) || '');
      if (out.up_pct == null && /text-up|\bup\b/i.test(cls)) {
        out.up_pct = v; out.raw.push('UP:cls=' + v);
      }
      if (out.down_pct == null && /text-down|\bdown\b/i.test(cls)) {
        out.down_pct = v; out.raw.push('DOWN:cls=' + v);
      }
    }
  }

  const inv = out.investment && out.investment > 0 ? out.investment : 1.0;
  if (out.up_pct == null && out.up_return != null) {
    out.up_pct = Math.round((out.up_return / inv) * 100);
  }
  if (out.down_pct == null && out.down_return != null) {
    out.down_pct = Math.round((out.down_return / inv) * 100);
  }

  out.debug = [
    'cd=' + out.countdown,
    'up%=' + out.up_pct,
    'down%=' + out.down_pct,
    'up$=' + out.up_return,
    'down$=' + out.down_return,
    'inv$=' + out.investment
  ];
  return out;
}
"""


def normalize_return_pct(raw: float | None) -> float | None:
    """Normalize to BC display %, e.g. 196. Accepts 196 or 1.96."""
    if raw is None:
        return None
    try:
        v = float(raw)
    except (TypeError, ValueError):
        return None
    if v <= 0:
        return None
    if 1.05 <= v <= 20:
        return v * 100.0
    if v >= 50:
        return v
    return None


def potential_return_dollars(
    stake: float,
    return_pct: float | None = None,
    *,
    scraped_return_for_1: float | None = None,
) -> float:
    """
    Dollars received if the bet wins (no separate house fee).
    Prefer scraped Potential return (scaled to stake), else stake * (pct/100).
    """
    stake = float(stake)
    if scraped_return_for_1 is not None and scraped_return_for_1 > 0:
        return float(scraped_return_for_1) * stake
    pct = normalize_return_pct(return_pct)
    if pct is None:
        raise ValueError("missing return pct")
    return stake * (pct / 100.0)


def pools_to_return_pct(
    up_pool: float | None, down_pool: float | None
) -> tuple[float | None, float | None]:
    try:
        up = float(up_pool) if up_pool is not None else None
        down = float(down_pool) if down_pool is not None else None
    except (TypeError, ValueError):
        return None, None
    if up is None or down is None or up <= 0 or down <= 0:
        return None, None
    total = up + down
    return 100.0 * total / up, 100.0 * total / down


def settle_one_bet(
    *,
    balance: float,
    stake: float,
    predicted: str,
    result: str,
    up_pct: float,
    down_pct: float,
    fee_rate: float = 0.0,
    up_pool: float | None = None,
    down_pool: float | None = None,
    up_return: float | None = None,
    down_return: float | None = None,
    round_id: str | None = None,
    bet_n: int = 1,
) -> dict[str, Any]:
    """WIN → potential return from locked Up/Down Wins %; LOSS → lose full stake."""
    del fee_rate
    del up_pool
    del down_pool
    up_r = normalize_return_pct(up_pct)
    down_r = normalize_return_pct(down_pct)
    if up_r is None or down_r is None:
        raise ValueError("missing locked Up/Down Wins %")

    side_pct = up_r if predicted == "UP" else down_r
    scraped = up_return if predicted == "UP" else down_return
    ret = potential_return_dollars(stake, side_pct, scraped_return_for_1=scraped)
    hit = predicted == result
    if hit:
        new_bal = balance - stake + ret
        outcome = "WIN"
        pnl = ret - stake
    else:
        new_bal = balance - stake
        outcome = "LOSS"
        pnl = -stake
    return {
        "balance": round(new_bal, 4),
        "row": {
            "n": bet_n,
            "round_id": round_id,
            "balance": round(new_bal, 4),
            "bet": predicted,
            "outcome": outcome,
            "up_pct": round(up_r, 2),
            "down_pct": round(down_r, 2),
            "result": result,
            "mult": round(side_pct / 100.0, 4),
            "potential_return": round(ret, 4),
            "pnl": round(pnl, 4),
            "pred_outcome": "HIT" if hit else "MISS",
        },
    }


ENTRY_FEE = 0.0
