"""
Analyzes a Polymarket wallet's trade history and produces a TraderProfile.

Scoring:
  win_rate        ≥ 0.60 required (survive copy slippage ~0.02)
  min_trades      ≥ 20   required (statistical significance)
  confidence      0–1    (win_rate quality × sample size depth)
  copy_weight     0.5–2.0 (scales our per-trade copy size)
  sport_focus     detected from market titles

A trader's copy_weight scales COPY_SIZE_USD:
  weight 1.0 → $25 per trade (base)
  weight 1.5 → $37.50
  weight 2.0 → $50 (cap)
"""
import asyncio
import logging
import math
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Optional

import aiohttp

logger = logging.getLogger(__name__)

DATA_API = "https://data-api.polymarket.com"

# ── Qualification thresholds ──────────────────────────────────────────────────
MIN_WIN_RATE   = 0.60   # must win at least 60% of resolved trades
MIN_TRADES     = 20     # need enough history to be meaningful
MIN_PROFIT     = 100    # must have made at least $100 total
MAX_TRADES_CAP = 200    # confidence saturates after this many trades


@dataclass
class TraderProfile:
    address:        str
    username:       str
    win_rate:       float       # 0.0–1.0
    total_trades:   int
    wins:           int
    losses:         int
    total_profit:   float
    avg_entry:      float       # average price paid across all trades
    sport_focus:    str         # "tennis" | "football" | "basketball" | "mixed" | "other"
    sport_breakdown: dict       # {"tennis": 0.72, "football": 0.18, ...}
    confidence:     float       # 0.0–1.0  (quality of the edge evidence)
    copy_weight:    float       # 0.5–2.0  (multiply base copy size by this)
    status:         str         # "approved" | "watching" | "rejected" | "paused"
    reject_reason:  str         # why rejected, if applicable
    last_seen:      str         # ISO timestamp of most recent trade
    added_at:       str         # when we first added this trader

    def to_dict(self) -> dict:
        return asdict(self)


class TraderAnalyzer:
    def __init__(self):
        self._session: Optional[aiohttp.ClientSession] = None

    async def analyze(self, address: str, username: str = "") -> TraderProfile:
        """
        Fetch the trader's full history and return a scored TraderProfile.
        """
        trades = await self._fetch_trades(address)
        return self._score(address, username or address[:10] + "…", trades)

    async def _fetch_trades(self, address: str, max_pages: int = 6) -> list[dict]:
        """Fetch up to max_pages * 500 activity items."""
        all_items = []
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as session:
            for page in range(max_pages):
                offset = page * 500
                params = {"user": address, "limit": "500", "offset": str(offset)}
                try:
                    async with session.get(f"{DATA_API}/activity", params=params) as resp:
                        if resp.status != 200:
                            break
                        data = await resp.json()
                        if not isinstance(data, list) or not data:
                            break
                        all_items.extend(data)
                        if len(data) < 500:
                            break  # last page
                except Exception as e:
                    logger.error(f"Fetch error for {address[:10]}: {e}")
                    break
                await asyncio.sleep(0.3)   # be gentle with the API
        return all_items

    def _score(self, address: str, username: str, items: list[dict]) -> TraderProfile:
        now_str = datetime.now(timezone.utc).isoformat()

        # ── Split into buys and redemptions ───────────────────────────────────
        buys    = [i for i in items if i.get("side") == "BUY"
                   and (i.get("type") or "").upper() in ("TRADE", "")]
        redeems = [i for i in items if (i.get("type") or "").upper() == "REDEEM"]

        # ── Identify unique positions (by tokenId) ────────────────────────────
        # Each tokenId = one outcome in one market
        # First BUY per tokenId = position entry
        seen_tokens:  dict[str, dict] = {}
        for b in buys:
            tid = str(b.get("tokenId") or b.get("asset_id") or "")
            if tid and tid not in seen_tokens:
                seen_tokens[tid] = b

        # ── Determine wins/losses ─────────────────────────────────────────────
        # A tokenId that appears in REDEEMs = winning position
        redeemed_tokens: set[str] = set()
        for r in redeems:
            tid = str(r.get("tokenId") or r.get("asset_id") or "")
            if tid:
                redeemed_tokens.add(tid)

        # We can only score positions that have resolved.
        # We infer "lost" when a token has NOT been redeemed AND the market
        # has likely resolved (the target wallet has been active since).
        # Conservative: only count explicit wins from redeems.
        # For losses we need to cross-reference positions API — but for now
        # use a heuristic: total_trades from profile vs visible wins.
        wins   = len(redeemed_tokens & set(seen_tokens.keys()))
        # Note: losses are hard to detect on-chain so we use total_trades
        # from the profile API as the denominator when available.
        total_positions = len(seen_tokens)
        losses = max(0, total_positions - wins)

        total_trades = wins + losses
        win_rate = wins / total_trades if total_trades > 0 else 0.0

        # ── Average entry price ───────────────────────────────────────────────
        prices = [float(b.get("price") or 0) for b in seen_tokens.values() if b.get("price")]
        avg_entry = sum(prices) / len(prices) if prices else 0.0

        # ── Sport / category detection ────────────────────────────────────────
        sport_counts: dict[str, int] = {}
        for b in seen_tokens.values():
            title = (b.get("title") or b.get("question") or "").lower()
            sport = _detect_sport(title)
            sport_counts[sport] = sport_counts.get(sport, 0) + 1

        total_cat = sum(sport_counts.values()) or 1
        sport_breakdown = {k: round(v / total_cat, 2) for k, v in
                           sorted(sport_counts.items(), key=lambda x: -x[1])}
        top_sport = max(sport_counts, key=sport_counts.get) if sport_counts else "other"
        sport_focus = top_sport if sport_counts.get(top_sport, 0) / total_cat >= 0.5 else "mixed"

        # ── Profit estimate ───────────────────────────────────────────────────
        # Rough: each win returns $1/share, each entry paid avg_entry/share
        # We don't have exact share counts, so estimate from usdcSize
        total_invested = sum(float(b.get("usdcSize") or 0) for b in seen_tokens.values())
        win_invested   = sum(
            float(seen_tokens[tid].get("usdcSize") or 0)
            for tid in redeemed_tokens & set(seen_tokens.keys())
        )
        est_win_return = (win_invested / avg_entry) if avg_entry > 0 else 0
        total_profit   = round(est_win_return - total_invested, 2) if total_invested else 0.0

        # ── Confidence score (0–1) ────────────────────────────────────────────
        # Win rate quality: how far above coin-flip? Scaled 0.5→1.0 to 0→1.0
        wr_quality = max(0.0, (win_rate - 0.50) / 0.50)
        # Sample depth: saturates at MAX_TRADES_CAP
        sample_depth = min(1.0, total_trades / MAX_TRADES_CAP)
        # Blend: need BOTH quality and quantity
        confidence = round(math.sqrt(wr_quality * sample_depth), 3)

        # ── Copy weight (0.5–2.0) ─────────────────────────────────────────────
        # 60% WR, 20 trades → low confidence → 0.5x
        # 75% WR, 100 trades → high confidence → ~1.5x
        # 85% WR, 200 trades → very high → 2.0x
        copy_weight = round(max(0.5, min(2.0, 0.5 + confidence * 1.5)), 2)

        # ── Qualification ─────────────────────────────────────────────────────
        status = "approved"
        reject_reason = ""
        if total_trades < MIN_TRADES:
            status = "watching"
            reject_reason = f"only {total_trades} trades (need {MIN_TRADES})"
        elif win_rate < MIN_WIN_RATE:
            status = "rejected"
            reject_reason = f"win rate {win_rate:.1%} < {MIN_WIN_RATE:.0%} threshold"
        elif total_profit < MIN_PROFIT:
            status = "rejected"
            reject_reason = f"estimated profit ${total_profit:.0f} < ${MIN_PROFIT} threshold"

        # ── Last seen ─────────────────────────────────────────────────────────
        timestamps = [i.get("timestamp") or "" for i in items if i.get("timestamp")]
        last_seen  = max(timestamps) if timestamps else ""

        return TraderProfile(
            address=address,
            username=username,
            win_rate=round(win_rate, 4),
            total_trades=total_trades,
            wins=wins,
            losses=losses,
            total_profit=total_profit,
            avg_entry=round(avg_entry, 3),
            sport_focus=sport_focus,
            sport_breakdown=sport_breakdown,
            confidence=confidence,
            copy_weight=copy_weight,
            status=status,
            reject_reason=reject_reason,
            last_seen=last_seen,
            added_at=now_str,
        )


def _detect_sport(title: str) -> str:
    title = title.lower()
    if any(k in title for k in ("tennis", "atp", "wta", "roland garros", "wimbledon",
                                 "us open", "australian open", "french open", "slam")):
        return "tennis"
    if any(k in title for k in ("nba", "basketball", "lakers", "celtics", "nets", "bulls")):
        return "basketball"
    if any(k in title for k in ("nfl", "football", "super bowl", "chiefs", "patriots",
                                 "premier league", "la liga", "bundesliga", "serie a",
                                 "champions league", "fifa", "world cup", "soccer")):
        return "football"
    if any(k in title for k in ("mlb", "baseball", "yankees", "dodgers", "world series")):
        return "baseball"
    if any(k in title for k in ("nhl", "hockey", "stanley cup")):
        return "hockey"
    if any(k in title for k in ("mma", "ufc", "boxing", "fight")):
        return "mma"
    if any(k in title for k in ("crypto", "btc", "bitcoin", "eth", "sol")):
        return "crypto"
    if any(k in title for k in ("election", "president", "senate", "congress", "vote")):
        return "politics"
    return "other"
