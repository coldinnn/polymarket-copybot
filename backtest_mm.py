"""
Market Maker Backtest

Replays minute-level price history from a resolved Polymarket market
and simulates the market_maker.py strategy to estimate profitability.

Fill logic (more realistic than paper mode):
  A BID fills when price drops THROUGH our bid level between two ticks.
  An ASK fills when price rises THROUGH our ask level between two ticks.

This simulates resting GTC limit orders that get hit by takers —
a fill requires the price to actually cross our level, not just touch it.

Usage:
  python backtest_mm.py                      # runs default backtest
  python backtest_mm.py --spread 0.02        # ±2¢ spread
  python backtest_mm.py --quit-at 0.80       # stop if price > 0.80 (avoid resolution risk)

Key outputs:
  - Total P&L
  - Fill count (bid/ask)
  - Adverse selection ratio
  - Round-trip completion rate
  - Comparison across spread sizes
"""

import asyncio
import json
import sys
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

import aiohttp

GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_HOST = "https://clob.polymarket.com"


# ── Fetch historical data ─────────────────────────────────────────────────────

async def fetch_resolved_ou_markets(limit: int = 20) -> list[dict]:
    """Get recently resolved O/U soccer markets with high volume."""
    async with aiohttp.ClientSession() as s:
        async with s.get(
            f"{GAMMA_API}/markets",
            params={"active": "false", "closed": "true", "limit": "500",
                    "order": "volume24hr", "ascending": "false"},
            timeout=aiohttp.ClientTimeout(total=15),
        ) as r:
            markets = await r.json()

    ou_markets = []
    for m in markets:
        q = m.get("question", "")
        if "O/U" not in q and "total" not in q.lower():
            continue
        vol = float(m.get("volume24hr") or 0)
        if vol < 50_000:
            continue
        tokens_raw = m.get("clobTokenIds", "[]")
        prices_raw = m.get("outcomePrices", "[]")
        try:
            tokens = json.loads(tokens_raw) if isinstance(tokens_raw, str) else tokens_raw
            prices = json.loads(prices_raw) if isinstance(prices_raw, str) else prices_raw
        except Exception:
            continue
        if not tokens or not prices:
            continue
        ou_markets.append({
            "question":    q[:70],
            "vol24h":      vol,
            "token_over":  tokens[0],
            "token_under": tokens[1] if len(tokens) > 1 else "",
            "final_over":  float(prices[0]) if prices else 0.5,
            "conditionId": m.get("conditionId", ""),
        })

    ou_markets.sort(key=lambda x: -x["vol24h"])
    return ou_markets[:limit]


async def fetch_price_history(token_id: str) -> list[dict]:
    """
    Fetch minute-level OHLC-style price snapshots from the CLOB.
    Returns list of {t: unix_timestamp, p: price} dicts, sorted by time.
    """
    async with aiohttp.ClientSession() as s:
        async with s.get(
            f"{CLOB_HOST}/prices-history",
            params={"market": token_id, "interval": "1d", "fidelity": "1"},
            timeout=aiohttp.ClientTimeout(total=10),
        ) as r:
            if r.status != 200:
                return []
            data = await r.json()
    return sorted(data.get("history", []), key=lambda x: x["t"])


# ── Backtest engine ───────────────────────────────────────────────────────────

@dataclass
class Fill:
    tick:        int
    side:        str      # "bid" | "ask"
    price:       float
    shares:      int
    ts:          str
    price_after: float    # price at next tick (for adverse selection calc)


@dataclass
class BacktestResult:
    market:          str
    vol24h:          float
    final_price:     float       # 0 or 1 after resolution
    spread:          float       # ±half_spread
    quote_usd:       float
    stop_above:      float
    fills:           list[Fill]  = field(default_factory=list)
    ticks:           int         = 0

    # ── Computed ──────────────────────────────────────────────────────────────

    @property
    def bid_fills(self): return [f for f in self.fills if f.side == "bid"]
    @property
    def ask_fills(self): return [f for f in self.fills if f.side == "ask"]

    @property
    def pnl(self) -> float:
        """
        P&L from all fills, with open inventory marked to final_price.
        BID fill: we paid price, got 1 share.  Later worth final_price.
        ASK fill: we received price, gave 1 share. We saved final_price.
        """
        cash = 0.0
        inventory = 0
        for f in self.fills:
            if f.side == "bid":
                cash -= f.price * f.shares
                inventory += f.shares
            else:
                cash += f.price * f.shares
                inventory -= f.shares
        # Mark open inventory to final_price
        cash += inventory * self.final_price
        return round(cash, 4)

    @property
    def spread_income(self) -> float:
        """P&L if every bid fill was perfectly paired with an ask fill."""
        return round(self.spread * 2 * min(len(self.bid_fills), len(self.ask_fills)) *
                     (self.quote_usd / 0.5), 4)  # rough estimate

    @property
    def adverse_selection_pct(self) -> float:
        """
        % of bid fills where price continued DOWN after fill (we bought into a falling knife).
        High adverse selection = our quotes are getting picked off by informed traders.
        """
        bid_adverse = sum(1 for f in self.bid_fills if f.price_after < f.price - 0.005)
        return round(bid_adverse / max(len(self.bid_fills), 1) * 100, 1)

    @property
    def roundtrip_pct(self) -> float:
        """% of bids that were followed by an ask fill within 30 ticks."""
        if not self.bid_fills:
            return 0.0
        paired = 0
        ask_ticks = {f.tick for f in self.ask_fills}
        for b in self.bid_fills:
            if any(b.tick < a <= b.tick + 30 for a in ask_ticks):
                paired += 1
        return round(paired / len(self.bid_fills) * 100, 1)

    def summary(self) -> str:
        lines = [
            f"Market:       {self.market}",
            f"Volume 24h:   ${self.vol24h:,.0f}",
            f"Spread:       ±{self.spread:.2f}  ({self.spread*200:.0f}¢ total)",
            f"Quote size:   ${self.quote_usd:.0f}/side",
            f"Stop above:   {self.stop_above:.2f}  (don't quote near resolution)",
            f"Ticks:        {self.ticks} (minutes of data)",
            f"",
            f"Fills:        {len(self.bid_fills)} bid  {len(self.ask_fills)} ask",
            f"Adverse sel:  {self.adverse_selection_pct:.0f}%  (bid fills where price kept falling)",
            f"Round-trips:  {self.roundtrip_pct:.0f}%  (bids paired with ask within 30min)",
            f"",
            f"P&L (realistic): ${self.pnl:+.2f}  (inventory marked to final price {self.final_price})",
            f"Final outcome:   {'OVER wins' if self.final_price > 0.5 else 'UNDER wins'}",
        ]
        return "\n".join(lines)


def run_backtest(
    history:     list[dict],
    final_price: float,
    market:      str,
    vol24h:      float,
    half_spread: float = 0.02,
    quote_usd:   float = 10.0,
    stop_above:  float = 0.85,   # stop quoting when price exceeds this (too close to resolution)
    stop_below:  float = 0.10,   # same for other side
    max_inv_usd: float = 50.0,
    requote_ticks: int = 5,      # refresh quotes every N ticks
) -> BacktestResult:
    """
    Simulate the MM strategy on a sequence of (timestamp, price) pairs.
    """
    result = BacktestResult(
        market=market, vol24h=vol24h, final_price=final_price,
        spread=half_spread, quote_usd=quote_usd, stop_above=stop_above,
    )

    inventory = 0        # positive = long (hold OVER shares)
    cash      = 0.0

    quote_bid   = None
    quote_ask   = None
    last_quote_tick = -requote_ticks - 1

    for i, tick in enumerate(history[:-1]):
        result.ticks += 1
        price_now  = tick["p"]
        price_next = history[i + 1]["p"]
        ts = datetime.fromtimestamp(tick["t"]).strftime("%H:%M")

        # Stop quoting near resolution
        if price_now > stop_above or price_now < stop_below:
            quote_bid = quote_ask = None
            continue

        # Inventory skew
        inv_usd = inventory * price_now
        skew    = max(-0.005, min(0.005, -inv_usd / max_inv_usd * half_spread))

        # Requote every N ticks
        if i - last_quote_tick >= requote_ticks:
            quote_bid = round(price_now - half_spread + skew, 2)
            quote_ask = round(price_now + half_spread + skew, 2)
            last_quote_tick = i

        if quote_bid is None:
            continue

        # Check for fills: price moved THROUGH our level
        # BID fills if price falls from above bid to at or below bid
        bid_shares = max(1, int(quote_usd / max(quote_bid, 0.01)))
        ask_shares = max(1, int(quote_usd / max(quote_ask, 0.01)))

        # BID fill: price NOW was above bid, price NEXT is at or below bid
        if price_now > quote_bid >= price_next and inv_usd < max_inv_usd:
            shares = bid_shares
            cash     -= quote_bid * shares
            inventory += shares
            result.fills.append(Fill(
                tick=i, side="bid", price=quote_bid, shares=shares,
                ts=ts, price_after=price_next,
            ))

        # ASK fill: price NOW was below ask, price NEXT is at or above ask
        if price_now < quote_ask <= price_next and inv_usd > -max_inv_usd:
            shares = ask_shares
            cash     += quote_ask * shares
            inventory -= shares
            result.fills.append(Fill(
                tick=i, side="ask", price=quote_ask, shares=shares,
                ts=ts, price_after=price_next,
            ))

    # Mark open inventory to resolution price
    result.ticks = len(history)
    return result


# ── Sensitivity sweep ─────────────────────────────────────────────────────────

def sweep_spreads(
    history:     list[dict],
    final_price: float,
    market:      str,
    vol24h:      float,
    spreads:     list[float] = [0.01, 0.02, 0.03, 0.05],
    stop_above:  float       = 0.85,
    stop_below:  float       = 0.10,
) -> list[BacktestResult]:
    return [
        run_backtest(history, final_price, market, vol24h,
                     half_spread=s, stop_above=stop_above, stop_below=stop_below)
        for s in spreads
    ]


# ── Main ──────────────────────────────────────────────────────────────────────

async def main():
    args = sys.argv[1:]

    spread = 0.02
    stop_above = 0.85
    for i, a in enumerate(args):
        if a == "--spread" and i + 1 < len(args):
            spread = float(args[i + 1])
        if a == "--quit-at" and i + 1 < len(args):
            stop_above = float(args[i + 1])

    print("Fetching resolved O/U markets...\n")
    markets = await fetch_resolved_ou_markets(limit=10)
    if not markets:
        print("No markets found.")
        return

    print(f"Found {len(markets)} markets. Running backtest on top 3 by volume:\n")
    print(f"{'Market':<55}  {'Volume':>12}  {'Final':>6}")
    print("─" * 80)
    for m in markets[:3]:
        print(f"  {m['question']:<53}  ${m['vol24h']:>10,.0f}  {'OVER' if m['final_over'] > 0.5 else 'UNDER':>5}")
    print()

    # ── Run backtest on top market ────────────────────────────────────────────
    top = markets[0]
    print(f"Loading price history for: {top['question']}")
    history = await fetch_price_history(top["token_over"])
    if not history:
        print("No price history available.")
        return
    print(f"  {len(history)} price ticks loaded\n")

    # ── Sweep spread sizes ────────────────────────────────────────────────────
    print("=" * 65)
    print("SPREAD SENSITIVITY (same market, different quote widths)")
    print("=" * 65)
    results = sweep_spreads(
        history, top["final_over"], top["question"], top["vol24h"],
        spreads=[0.01, 0.02, 0.03, 0.05],
        stop_above=stop_above,
    )

    print(f"\n{'Spread':>8}  {'Bids':>5}  {'Asks':>5}  {'Adv%':>6}  {'RT%':>5}  {'P&L':>10}")
    print("-" * 50)
    for r in results:
        print(
            f"  ±{r.spread:.2f}    {len(r.bid_fills):>5}  {len(r.ask_fills):>5}"
            f"  {r.adverse_selection_pct:>5.0f}%  {r.roundtrip_pct:>4.0f}%  ${r.pnl:>+9.2f}"
        )

    # ── Detailed result for chosen spread ─────────────────────────────────────
    chosen = next(r for r in results if abs(r.spread - spread) < 0.001) or results[1]
    print(f"\n{'=' * 65}")
    print(f"DETAILED: ±{spread:.2f} spread")
    print(f"{'=' * 65}")
    print(chosen.summary())

    # ── Show fill timeline ─────────────────────────────────────────────────────
    if chosen.fills:
        print(f"\nFill timeline (first 20):")
        print(f"{'Time':>6}  {'Side':>4}  {'Price':>6}  {'Shares':>6}  {'Next':>6}  {'Adv?':>5}")
        print("-" * 50)
        for f in chosen.fills[:20]:
            adv = "YES" if (f.side == "bid" and f.price_after < f.price - 0.005) else ""
            print(f"  {f.ts}  {f.side:>4}  {f.price:.3f}  {f.shares:>6}  {f.price_after:.3f}  {adv:>5}")

    # ── Run on all top markets ─────────────────────────────────────────────────
    print(f"\n{'=' * 65}")
    print(f"MULTI-MARKET SUMMARY (±{spread:.2f} spread, stop>{stop_above:.2f})")
    print(f"{'=' * 65}")
    print(f"{'Market':<45}  {'Vol24h':>10}  {'Fills':>6}  {'P&L':>10}")
    print("-" * 80)
    for m in markets[:5]:
        hist = await fetch_price_history(m["token_over"])
        if not hist:
            continue
        r = run_backtest(hist, m["final_over"], m["question"], m["vol24h"],
                         half_spread=spread, stop_above=stop_above)
        print(f"  {m['question']:<43}  ${m['vol24h']:>9,.0f}  {len(r.fills):>6}  ${r.pnl:>+9.2f}")
    print()
    print("NOTE: P&L uses final resolved price. OVER=0 means market resolved UNDER.")
    print("      High adverse selection = fills happening right before big moves.")


if __name__ == "__main__":
    asyncio.run(main())
