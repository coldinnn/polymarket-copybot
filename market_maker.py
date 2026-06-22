"""
Polymarket Market Maker — Step 1: Order Placement

Strategy
--------
Post a resting BID slightly below mid-price and a resting ASK slightly above.
Earn the spread on round-trips. Requote every REFRESH_SECS or whenever mid
drifts more than REQUOTE_THRESHOLD away from our current quotes.

Paper mode (default, MM_PAPER=true)
  Simulates fills by comparing mid-price polls against quoted prices.
  A bid fills when market mid drops to or below bid_price.
  An ask fills when market mid rises to or above ask_price.

Live mode (MM_PAPER=false)
  Places real GTC limit orders via CLOB API.
  Polls open orders to detect fills.
  Cancels stale quotes before reposting.

Required env vars
-----------------
  MM_TOKEN_ID    : token ID of the binary outcome to quote (YES or NO leg)
  POLY_PRIVATE_KEY: wallet private key for live mode

Optional env vars
-----------------
  MM_PAPER          : "true" (default) | "false" — paper vs live
  MM_SPREAD         : half-spread in cents, default 0.01 (quote 1¢ each side)
  MM_QUOTE_USD      : $ per side, default 10.0
  MM_MAX_INVENTORY_USD : max net position before pausing one side, default 50.0
  MM_REFRESH_SECS   : requote interval in seconds, default 5
  MM_REQUOTE_THRESH : requote when mid moves this many cents, default 0.02
  MM_MIN_MID        : don't quote if mid < this (default 0.10)
  MM_MAX_MID        : don't quote if mid > this (default 0.90)
"""

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import aiohttp

# ── Config ────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

CLOB_HOST    = "https://clob.polymarket.com"
DATA_API     = "https://data-api.polymarket.com"
CHAIN_ID     = 137

PAPER_MODE        = os.getenv("MM_PAPER", "true").lower() != "false"
TOKEN_ID          = os.getenv("MM_TOKEN_ID", "")
HALF_SPREAD       = float(os.getenv("MM_SPREAD", "0.01"))      # cents each side
QUOTE_USD         = float(os.getenv("MM_QUOTE_USD", "10"))
MAX_INV_USD       = float(os.getenv("MM_MAX_INVENTORY_USD", "50"))
REFRESH_SECS      = float(os.getenv("MM_REFRESH_SECS", "5"))
REQUOTE_THRESH    = float(os.getenv("MM_REQUOTE_THRESH", "0.02"))
MIN_MID           = float(os.getenv("MM_MIN_MID", "0.10"))
MAX_MID           = float(os.getenv("MM_MAX_MID", "0.90"))


# ── Data models ───────────────────────────────────────────────────────────────

@dataclass
class Quote:
    """A pair of resting bid/ask prices we're currently quoting."""
    bid_price:   float
    ask_price:   float
    bid_shares:  int
    ask_shares:  int
    mid_at_post: float
    posted_at:   float = field(default_factory=time.time)
    bid_order_id: str = ""
    ask_order_id: str = ""


@dataclass
class Fill:
    side:       str      # "bid" | "ask"
    price:      float
    shares:     int
    cost_usd:   float    # positive = we spent money (bid fill), negative = we received (ask fill)
    filled_at:  str


@dataclass
class MMStats:
    fills:          list[Fill] = field(default_factory=list)
    net_shares:     int   = 0      # positive = long inventory, negative = short
    net_usd_spent:  float = 0.0    # total cash in/out
    spread_earned:  float = 0.0    # estimated spread P&L
    quotes_posted:  int   = 0
    requotes:       int   = 0
    started_at:     str   = ""

    def pnl(self, current_mid: float) -> float:
        """
        Realized spread P&L + unrealized mark-to-mid on open inventory.
        """
        realized   = self.spread_earned
        unrealized = self.net_shares * current_mid - self.net_usd_spent
        return round(realized + unrealized, 4)

    def summary(self, current_mid: float) -> str:
        fills_bid = [f for f in self.fills if f.side == "bid"]
        fills_ask = [f for f in self.fills if f.side == "ask"]
        return (
            f"Fills bid={len(fills_bid)} ask={len(fills_ask)}  "
            f"Inventory={self.net_shares:+d} shares  "
            f"P&L={self.pnl(current_mid):+.4f} USDC  "
            f"Quotes={self.quotes_posted} Requotes={self.requotes}"
        )


# ── Market Maker ──────────────────────────────────────────────────────────────

CLOB_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


class MarketMaker:
    def __init__(self, token_id: str, label: str = ""):
        self.token_id   = token_id
        self.label      = label or f"…{token_id[-8:]}"
        self._client    = None
        self._running   = False
        self.quote:     Optional[Quote]  = None
        self.stats      = MMStats(started_at=datetime.now(timezone.utc).strftime("%H:%M:%S"))
        self._live_mid: Optional[float]  = None   # latest mid from WebSocket

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def initialize(self):
        if PAPER_MODE:
            logger.info("MarketMaker: PAPER mode (set MM_PAPER=false for live)")
            return
        try:
            from py_clob_client_v2 import ClobClient
            pk = os.getenv("POLY_PRIVATE_KEY", "")
            if not pk:
                raise RuntimeError("POLY_PRIVATE_KEY not set")
            temp   = ClobClient(CLOB_HOST, chain_id=CHAIN_ID, key=pk)
            creds  = temp.create_or_derive_api_key()
            self._client = ClobClient(
                CLOB_HOST, chain_id=CHAIN_ID, key=pk,
                creds=creds, signature_type=0,
            )
            logger.info("MarketMaker: LIVE mode, wallet authenticated")
        except Exception as e:
            logger.error(f"CLOB init failed, falling back to paper: {e}")

    async def run(self):
        """Main loop: quote → check fills → requote → repeat."""
        if not self.token_id:
            logger.error("No MM_TOKEN_ID set. Run: python market_finder.py to find markets.")
            return

        self._running = True
        logger.info(
            f"Starting market maker on token {self.token_id[:20]}...\n"
            f"  Spread: ±{HALF_SPREAD:.2f} ({HALF_SPREAD*2*100:.1f}¢ total)\n"
            f"  Quote size: ${QUOTE_USD:.2f}/side\n"
            f"  Refresh: {REFRESH_SECS}s | Requote threshold: {REQUOTE_THRESH:.2f}"
        )

        try:
            await asyncio.gather(
                self._ws_feed(),       # real-time orderbook via WebSocket
                self._quote_loop(),    # place/cancel/repost quotes
                self._stats_loop(),    # periodic P&L summary
            )
        except KeyboardInterrupt:
            pass
        finally:
            await self._cancel_live_quotes()

    def stop(self):
        self._running = False

    # ── Quote loop ────────────────────────────────────────────────────────────

    async def _quote_loop(self):
        while self._running:
            try:
                mid = await self._get_mid()
                if mid is None:
                    logger.warning("No mid price — retrying in 5s")
                    await asyncio.sleep(5)
                    continue

                if mid < MIN_MID or mid > MAX_MID:
                    logger.info(f"Mid {mid:.3f} outside [{MIN_MID}, {MAX_MID}] — paused")
                    await self._cancel_live_quotes()
                    self.quote = None
                    await asyncio.sleep(REFRESH_SECS)
                    continue

                # Check for fills on existing quotes
                if self.quote is not None:
                    await self._check_fills(mid)

                # Decide whether to requote
                should_requote = (
                    self.quote is None
                    or abs(mid - self.quote.mid_at_post) >= REQUOTE_THRESH
                    or (time.time() - self.quote.posted_at) >= REFRESH_SECS
                )

                if should_requote:
                    if self.quote is not None:
                        self.stats.requotes += 1
                        await self._cancel_live_quotes()
                    await self._post_quotes(mid)

            except Exception as e:
                logger.error(f"Quote loop error: {e}", exc_info=True)

            await asyncio.sleep(REFRESH_SECS)

    async def _check_fills(self, current_mid: float):
        """
        Paper mode: fill if mid has moved through our quote price.
        Live mode: check open orders API.
        """
        if self.quote is None:
            return

        paper = PAPER_MODE or self._client is None

        if paper:
            # BID fills when market mid drops to or below bid_price
            # (someone hit our resting bid)
            if current_mid <= self.quote.bid_price:
                self._record_fill("bid", self.quote.bid_price, self.quote.bid_shares)

            # ASK fills when market mid rises to or above ask_price
            # (someone lifted our resting ask)
            if current_mid >= self.quote.ask_price:
                self._record_fill("ask", self.quote.ask_price, self.quote.ask_shares)
        else:
            # Live: check if our orders are still open
            await self._check_live_fills()

    def _record_fill(self, side: str, price: float, shares: int):
        if side == "bid":
            cost = round(price * shares, 4)        # we paid cost
            self.stats.net_shares    += shares
            self.stats.net_usd_spent += cost
            self.stats.spread_earned -= price * shares  # deduct cost leg
        else:
            revenue = round(price * shares, 4)     # we received revenue
            self.stats.net_shares    -= shares
            self.stats.net_usd_spent -= revenue
            self.stats.spread_earned += price * shares  # add revenue leg

        # If we now have matching inventory, realize the round-trip spread
        # (simplified: just track the spread_earned accumulation above)

        fill = Fill(
            side=side,
            price=price,
            shares=shares,
            cost_usd=price * shares if side == "bid" else -price * shares,
            filled_at=datetime.now(timezone.utc).strftime("%H:%M:%S"),
        )
        self.stats.fills.append(fill)
        tag = "BID FILL ▼" if side == "bid" else "ASK FILL ▲"
        logger.info(
            f"{tag}  price={price:.3f}  shares={shares}  "
            f"inventory={self.stats.net_shares:+d}"
        )

    async def _post_quotes(self, mid: float):
        """Calculate bid/ask prices and post both sides."""
        # Inventory skew: if we're too long, widen bid / tighten ask to lean short
        inv_usd   = self.stats.net_shares * mid
        skew      = max(-0.005, min(0.005, -inv_usd / MAX_INV_USD * HALF_SPREAD))

        bid_price = round(mid - HALF_SPREAD + skew, 2)
        ask_price = round(mid + HALF_SPREAD + skew, 2)

        # Clamp to valid range
        bid_price = max(0.01, min(0.98, bid_price))
        ask_price = max(0.02, min(0.99, ask_price))

        if ask_price <= bid_price:
            logger.warning(f"Degenerate spread: bid={bid_price} ask={ask_price}, skipping")
            return

        # Pause a side if inventory is too large
        bid_shares = int(QUOTE_USD / bid_price) if inv_usd <  MAX_INV_USD else 0
        ask_shares = int(QUOTE_USD / ask_price) if inv_usd > -MAX_INV_USD else 0

        paper = PAPER_MODE or self._client is None

        bid_oid = ""
        ask_oid = ""

        if not paper:
            bid_oid, ask_oid = await self._place_live_quotes(bid_price, bid_shares, ask_price, ask_shares)

        self.quote = Quote(
            bid_price=bid_price,
            ask_price=ask_price,
            bid_shares=bid_shares,
            ask_shares=ask_shares,
            mid_at_post=mid,
            bid_order_id=bid_oid,
            ask_order_id=ask_oid,
        )
        self.stats.quotes_posted += 1

        tag = "[PAPER]" if paper else "[LIVE]"
        logger.info(
            f"QUOTE {tag}  mid={mid:.3f}  "
            f"bid={bid_price:.3f}×{bid_shares}  "
            f"ask={ask_price:.3f}×{ask_shares}  "
            f"skew={skew:+.4f}  inv={inv_usd:+.2f}$"
        )

    # ── Live order management ─────────────────────────────────────────────────

    async def _place_live_quotes(
        self,
        bid_price: float, bid_shares: int,
        ask_price: float, ask_shares: int,
    ) -> tuple[str, str]:
        """Place GTC limit bid and ask. Returns (bid_order_id, ask_order_id)."""
        from py_clob_client_v2 import OrderArgs, OrderType, PartialCreateOrderOptions
        from py_clob_client_v2.order_builder.constants import BUY, SELL

        bid_oid = ""
        ask_oid = ""

        if bid_shares > 0:
            try:
                result = await asyncio.to_thread(
                    self._client.create_and_post_order,
                    OrderArgs(token_id=self.token_id, price=bid_price, size=bid_shares, side=BUY),
                    PartialCreateOrderOptions(tick_size="0.01", neg_risk=False),
                    OrderType.GTC,
                )
                bid_oid = (result or {}).get("orderID", "")
                logger.debug(f"Live BID placed: {bid_oid} @ {bid_price}")
            except Exception as e:
                logger.error(f"Live bid placement error: {e}")

        if ask_shares > 0:
            try:
                result = await asyncio.to_thread(
                    self._client.create_and_post_order,
                    OrderArgs(token_id=self.token_id, price=ask_price, size=ask_shares, side=SELL),
                    PartialCreateOrderOptions(tick_size="0.01", neg_risk=False),
                    OrderType.GTC,
                )
                ask_oid = (result or {}).get("orderID", "")
                logger.debug(f"Live ASK placed: {ask_oid} @ {ask_price}")
            except Exception as e:
                logger.error(f"Live ask placement error: {e}")

        return bid_oid, ask_oid

    async def _cancel_live_quotes(self):
        """Cancel our resting orders before requoting."""
        if PAPER_MODE or self._client is None or self.quote is None:
            return
        try:
            # Try targeted cancel first, fall back to cancel_all
            for oid in [self.quote.bid_order_id, self.quote.ask_order_id]:
                if oid:
                    try:
                        await asyncio.to_thread(self._client.cancel, {"orderID": oid})
                    except Exception:
                        pass
        except Exception as e:
            logger.error(f"Cancel error: {e}")

    async def _check_live_fills(self):
        """Poll open orders to detect which of our quotes got filled."""
        if self._client is None or self.quote is None:
            return
        try:
            open_orders = await asyncio.to_thread(self._client.get_orders)
            open_ids    = {o.get("id") or o.get("orderID") for o in (open_orders or [])}

            if self.quote.bid_order_id and self.quote.bid_order_id not in open_ids:
                self._record_fill("bid", self.quote.bid_price, self.quote.bid_shares)
                self.quote.bid_order_id = ""

            if self.quote.ask_order_id and self.quote.ask_order_id not in open_ids:
                self._record_fill("ask", self.quote.ask_price, self.quote.ask_shares)
                self.quote.ask_order_id = ""
        except Exception as e:
            logger.debug(f"Open order check error: {e}")

    # ── Stats loop ────────────────────────────────────────────────────────────

    async def _stats_loop(self):
        """Print a P&L summary every 30 seconds."""
        while self._running:
            await asyncio.sleep(30)
            try:
                mid = await self._get_mid()
                if mid is not None:
                    logger.info(f"── STATS ── {self.stats.summary(mid)}")
            except Exception:
                pass

    # ── WebSocket real-time orderbook ─────────────────────────────────────────

    async def _ws_feed(self):
        """
        Subscribe to the Polymarket CLOB order book WebSocket.
        Computes mid from best bid/ask and stores in self._live_mid.
        Falls back gracefully if websockets isn't installed.
        """
        try:
            import websockets  # type: ignore
        except ImportError:
            logger.info("websockets not installed — using REST polling for mid price")
            return

        sub_msg = json.dumps({
            "assets_ids": [self.token_id],
            "type": "market",
        })

        while self._running:
            try:
                async with websockets.connect(
                    CLOB_WS,
                    ping_interval=20,
                    open_timeout=10,
                ) as ws:
                    await ws.send(sub_msg)
                    logger.info("WebSocket connected — live orderbook feed active")
                    async for raw in ws:
                        if not self._running:
                            break
                        try:
                            msgs = json.loads(raw)
                            if not isinstance(msgs, list):
                                msgs = [msgs]
                            for msg in msgs:
                                bids = msg.get("bids", [])
                                asks = msg.get("asks", [])
                                if bids and asks:
                                    # Polymarket WS: bids sorted low→high (best = last),
                                    # asks sorted high→low (best = last).
                                    best_bid = float(bids[-1]["price"])
                                    best_ask = float(asks[-1]["price"])
                                    self._live_mid = round((best_bid + best_ask) / 2, 4)
                        except Exception:
                            pass
            except Exception as e:
                if self._running:
                    logger.debug(f"WebSocket disconnected ({e}), reconnecting in 3s")
                    await asyncio.sleep(3)
                else:
                    break

    # ── Helpers ───────────────────────────────────────────────────────────────

    async def _get_mid(self) -> Optional[float]:
        """
        Always use REST /midpoint as the authoritative source.
        WebSocket mid is stored in _live_mid for reference only — the WS orderbook
        sorts bids low→high and asks high→low, so bids[0]/asks[0] are the *worst*
        quotes, not the best.  REST is fast enough (< 200ms) for our 5-second loop.
        """
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{CLOB_HOST}/midpoint",
                    params={"token_id": self.token_id},
                    timeout=aiohttp.ClientTimeout(total=5),
                ) as resp:
                    if resp.status != 200:
                        return None
                    data = await resp.json()
                    mid  = data.get("mid")
                    return float(mid) if mid else None
        except Exception:
            return None


# ── Market Finder ─────────────────────────────────────────────────────────────

GAMMA_API = "https://gamma-api.polymarket.com"


async def find_markets(
    min_mid: float = 0.15,
    max_mid: float = 0.85,
    limit:   int   = 20,
) -> list[dict]:
    """
    Fetch active Polymarket binary markets via Gamma API and score them.

    Good markets for MM:
    - Mid price near 0.5 (more two-sided flow)
    - High 24h volume (more fill opportunities)
    - Binary (not neg-risk/combo)
    """
    import json as _json

    raw_markets: list[dict] = []
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{GAMMA_API}/markets",
                params={"active": "true", "closed": "false", "limit": "200",
                        "order": "volume24hr", "ascending": "false"},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status == 200:
                    raw_markets = await resp.json()
    except Exception as e:
        logger.error(f"Gamma API fetch error: {e}")

    scored = []
    for m in raw_markets:
        # Parse outcomePrices (may be a JSON-encoded string)
        prices_raw = m.get("outcomePrices", [])
        if isinstance(prices_raw, str):
            try:
                prices = _json.loads(prices_raw)
            except Exception:
                continue
        else:
            prices = prices_raw

        if not prices or len(prices) < 2:
            continue

        try:
            yes_price = float(prices[0])
        except Exception:
            continue

        if yes_price < min_mid or yes_price > max_mid:
            continue

        # Get token IDs
        tokens_raw = m.get("clobTokenIds", [])
        if isinstance(tokens_raw, str):
            try:
                tokens = _json.loads(tokens_raw)
            except Exception:
                continue
        else:
            tokens = tokens_raw

        if not tokens:
            continue

        yes_token_id = tokens[0] if tokens else ""
        no_token_id  = tokens[1] if len(tokens) > 1 else ""

        # Score: higher vol24h * how close to 0.5
        vol24h    = float(m.get("volume24hr") or 0)
        mid_score = 1.0 - abs(yes_price - 0.5) * 2   # 1.0 at 0.5, 0.0 at 0.0/1.0

        scored.append({
            "question":    m.get("question", "")[:80],
            "token_id":    yes_token_id,       # YES leg (default to make markets on)
            "no_token_id": no_token_id,
            "condition_id": m.get("conditionId", ""),
            "mid":         round(yes_price, 3),
            "mid_score":   round(mid_score, 3),
            "vol24h":      round(vol24h, 0),
        })

        if len(scored) >= limit * 3:
            break

    # Sort by vol24h * mid_score composite
    scored.sort(key=lambda x: -(x["vol24h"] * x["mid_score"]))
    scored = scored[:limit]
    return scored


async def run_finder():
    """Print top markets suitable for market making."""
    print("\n  Scanning Polymarket for good MM markets (mid 0.15-0.85) ...\n")
    markets = await find_markets()
    if not markets:
        print("No markets found.")
        return
    print(f"{'Mid':>6}  {'Vol24h':>10}  {'Question':<65}  Token (YES)")
    print("─" * 110)
    for i, m in enumerate(markets[:15], 1):
        vol_str = f"${m['vol24h']:,.0f}"
        print(f"  {m['mid']:.3f}  {vol_str:>10}  {m['question']:<65}  {m['token_id'][:22]}...")
    print(f"\n  To start market making:")
    print(f"    MM_TOKEN_ID={markets[0]['token_id']} python market_maker.py")


# ── Entry point ───────────────────────────────────────────────────────────────

async def main():
    import sys

    if "--find" in sys.argv or not TOKEN_ID:
        await run_finder()
        return

    mm = MarketMaker(TOKEN_ID)
    mm.initialize()

    try:
        await mm.run()
    except KeyboardInterrupt:
        mm.stop()
        mid = await mm._get_mid() or 0.5
        logger.info(f"\n── FINAL STATS ──\n{mm.stats.summary(mid)}")
        fills_bid = [f for f in mm.stats.fills if f.side == "bid"]
        fills_ask = [f for f in mm.stats.fills if f.side == "ask"]
        print(f"\nBid fills:  {len(fills_bid)}")
        print(f"Ask fills:  {len(fills_ask)}")
        print(f"Net shares: {mm.stats.net_shares:+d}")
        print(f"P&L:        {mm.stats.pnl(mid):+.4f} USDC")


if __name__ == "__main__":
    asyncio.run(main())
