"""
Places copy orders when WalletMonitor emits a CopySignal.

COPY_PAPER=true  → simulate orders, no real CLOB calls (default for safety)
COPY_PAPER=false → live mode, real orders placed

Position lifecycle:
  open → won  (market resolved, our outcome token is redeemable at $1.00)
  open → lost (market resolved, our outcome token is worthless)

Redemption is handled by the existing redeemer.py logic in the main bot.
Here we just track P&L by querying the positions API periodically.
"""
import asyncio
import logging
import os
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Optional

import aiohttp

from monitor import WalletMonitor, CopySignal

logger = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────
CLOB_HOST     = "https://clob.polymarket.com"
DATA_API      = "https://data-api.polymarket.com"
CHAIN_ID      = 137

COPY_SIZE_USD = float(os.getenv("COPY_SIZE_USD", "25"))   # $ per copy trade
MAX_ENTRY     = float(os.getenv("COPY_MAX_ENTRY", "0.92"))  # skip if market moved above this
MIN_ENTRY     = float(os.getenv("COPY_MIN_ENTRY", "0.05"))  # skip if suspiciously cheap
PAPER_MODE    = os.getenv("COPY_PAPER", "true").lower() != "false"

RESOLVE_INTERVAL = 300  # check for resolved positions every 5 min


@dataclass
class CopyPosition:
    token_id:          str
    condition_id:      str
    title:             str
    outcome:           str
    entry_price:       float
    size_usd:          float
    shares:            int
    target_price:      float   # what the target wallet paid
    target_size_usd:   float
    entered_at:        str
    paper:             bool
    status:            str = "open"   # open | won | lost
    pnl:               float = 0.0
    exit_price:        float = 0.0
    resolved_at:       str = ""

    def to_dict(self) -> dict:
        return asdict(self)


class CopyTrader:
    def __init__(self, monitor: WalletMonitor):
        self.monitor   = monitor
        self._client   = None    # ClobClient, None in paper mode
        self._lock     = asyncio.Lock()
        self._running  = False
        self.positions: list[CopyPosition] = []
        self.history:   list[CopyPosition] = []
        self._scan_log: list[dict]         = []
        self._bankroll  = float(os.getenv("COPY_BANKROLL", "500"))
        self._deployed  = 0.0

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def initialize(self):
        if PAPER_MODE:
            logger.info("CopyTrader running in PAPER mode (set COPY_PAPER=false for live)")
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
            logger.info("CopyTrader CLOB client ready (LIVE)")
        except Exception as e:
            logger.error(f"CLOB init failed, falling back to paper: {e}")

    async def start(self):
        self._running = True
        await asyncio.gather(
            asyncio.create_task(self._process_signals(), name="copy_signals"),
            asyncio.create_task(self._resolve_loop(),    name="copy_resolve"),
        )

    def stop(self):
        self._running = False

    # ── Signal processing ─────────────────────────────────────────────────────

    async def _process_signals(self):
        while self._running:
            try:
                signal: CopySignal = await asyncio.wait_for(
                    self.monitor.signal_queue.get(), timeout=5.0
                )
                await self._copy(signal)
            except asyncio.TimeoutError:
                pass
            except Exception as e:
                logger.error(f"Signal processing error: {e}")

    async def _copy(self, signal: CopySignal):
        async with self._lock:
            # Already have this token
            if any(p.token_id == signal.token_id for p in self.positions):
                return

            # Fetch current mid — don't buy if target already moved the market
            mid = await self._get_mid(signal.token_id)
            if mid is None:
                self._log(f"SKIP {signal.title[:40]}: could not fetch mid")
                return

            if mid > MAX_ENTRY:
                self._log(f"SKIP {signal.title[:40]}: mid {mid:.2f} > max {MAX_ENTRY}")
                return
            if mid < MIN_ENTRY:
                self._log(f"SKIP {signal.title[:40]}: mid {mid:.2f} < min {MIN_ENTRY}")
                return

            # Lift ask slightly to guarantee FOK fill
            price  = round(min(mid + 0.01, 0.99), 2)
            shares = int(COPY_SIZE_USD / price)
            if shares < 1:
                self._log(f"SKIP {signal.title[:40]}: shares=0 at price {price}")
                return

            actual_cost = round(price * shares, 2)

            # ── Place order ────────────────────────────────────────────────────
            paper = PAPER_MODE or self._client is None
            if not paper:
                placed = await self._place_live_order(signal.token_id, price, shares)
                if not placed:
                    self._log(f"ORDER FAILED {signal.title[:40]}")
                    return

            pos = CopyPosition(
                token_id=signal.token_id,
                condition_id=signal.condition_id,
                title=signal.title,
                outcome=signal.outcome,
                entry_price=price,
                size_usd=actual_cost,
                shares=shares,
                target_price=signal.price,
                target_size_usd=signal.target_size_usd,
                entered_at=datetime.utcnow().strftime("%H:%M:%S"),
                paper=paper,
            )
            self.positions.append(pos)
            self.monitor.mark_copied(signal.token_id)
            self._deployed += actual_cost

            tag = "PAPER" if paper else "LIVE"
            self._log(
                f"COPY [{tag}] {pos.title[:45]}  {pos.outcome}"
                f"  ${actual_cost:.2f} @ {price:.2f}"
                f"  (target @ {signal.price:.2f}, ${signal.target_size_usd:,.0f})"
            )

    async def _place_live_order(self, token_id: str, price: float, shares: int) -> bool:
        try:
            from py_clob_client_v2 import OrderArgs, OrderType, PartialCreateOrderOptions
            from py_clob_client_v2.order_builder.constants import BUY
            result = await asyncio.to_thread(
                self._client.create_and_post_order,
                OrderArgs(token_id=token_id, price=price, size=shares, side=BUY),
                PartialCreateOrderOptions(tick_size="0.01", neg_risk=False),
                OrderType.FOK,
            )
            if not result or result.get("status") in ("error", "unmatched"):
                logger.warning(f"Live order not filled: {result}")
                return False
            return True
        except Exception as e:
            logger.error(f"Live order error: {e}")
            return False

    # ── Resolve loop ──────────────────────────────────────────────────────────

    async def _resolve_loop(self):
        while self._running:
            await asyncio.sleep(RESOLVE_INTERVAL)
            try:
                await self._check_resolved()
            except Exception as e:
                logger.error(f"Resolve loop error: {e}")

    async def _check_resolved(self):
        """
        Check target wallet's recent activity for REDEEM events.
        A REDEEM by the target means their market resolved — look up whether
        our position in that market won or lost.
        """
        if not self.positions:
            return

        # Fetch target wallet recent activity to find REDEEMs
        url    = f"{DATA_API}/activity"
        params = {"user": self.monitor.__class__.__module__, "limit": "100"}

        # Get target wallet activity to detect resolves
        from monitor import TARGET_WALLET
        params = {"user": TARGET_WALLET, "limit": "100"}
        redeemed_conditions: set[str] = set()

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        for item in (data if isinstance(data, list) else []):
                            if (item.get("type") or "").upper() in ("REDEEM",):
                                cid = str(item.get("market") or item.get("conditionId") or "")
                                if cid:
                                    redeemed_conditions.add(cid)
        except Exception as e:
            logger.error(f"Resolve fetch error: {e}")
            return

        if not redeemed_conditions:
            return

        # For each open position whose conditionId was redeemed by target,
        # check if OUR shares are redeemable (won) or worthless (lost)
        async with self._lock:
            still_open = []
            for pos in self.positions:
                if pos.condition_id not in redeemed_conditions:
                    still_open.append(pos)
                    continue

                # Market resolved — check our position
                won = await self._is_redeemable(pos.token_id)
                if won is None:
                    # Can't determine yet, keep open
                    still_open.append(pos)
                    continue

                pos.resolved_at = datetime.utcnow().strftime("%H:%M:%S")
                if won:
                    pos.status     = "won"
                    pos.exit_price = 1.0
                    pos.pnl        = round(pos.shares * 1.0 - pos.size_usd, 2)
                    self._log(f"WIN  {pos.title[:45]}  +${pos.pnl:.2f}")
                else:
                    pos.status     = "lost"
                    pos.exit_price = 0.0
                    pos.pnl        = -pos.size_usd
                    self._log(f"LOSS  {pos.title[:45]}  -${pos.size_usd:.2f}")

                self.history.append(pos)

            self.positions = still_open

    async def _is_redeemable(self, token_id: str) -> Optional[bool]:
        """Check if this token_id appears as redeemable in our positions."""
        pk = os.getenv("POLY_PRIVATE_KEY", "")
        wallet = os.getenv("POLY_WALLET", "")
        if not wallet and self._client:
            try:
                wallet = self._client._signer.address
            except Exception:
                pass
        if not wallet:
            return None

        url    = f"{DATA_API}/positions"
        params = {"user": wallet, "limit": "500"}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params) as resp:
                    if resp.status != 200:
                        return None
                    data = await resp.json()
                    for p in (data if isinstance(data, list) else []):
                        tid = str(p.get("asset_id") or p.get("tokenId") or "")
                        if tid == token_id:
                            return bool(p.get("redeemable"))
                    # Token not in portfolio at all → worthless (lost)
                    return False
        except Exception:
            return None

    # ── Stats / helpers ───────────────────────────────────────────────────────

    def stats(self) -> dict:
        closed = self.history
        wins   = [p for p in closed if p.status == "won"]
        losses = [p for p in closed if p.status == "lost"]
        total_pnl    = sum(p.pnl for p in closed)
        win_rate     = (len(wins) / len(closed) * 100) if closed else 0.0
        total_trades = len(closed)
        return {
            "bankroll":     round(self._bankroll + total_pnl, 2),
            "started":      self._bankroll,
            "pnl":          round(total_pnl, 2),
            "pnl_pct":      round(total_pnl / self._bankroll * 100, 2) if self._bankroll else 0,
            "win_rate":     round(win_rate, 1),
            "wins":         len(wins),
            "losses":       len(losses),
            "open":         len(self.positions),
            "total_trades": total_trades,
            "copy_size":    COPY_SIZE_USD,
            "paper":        PAPER_MODE,
        }

    def _log(self, msg: str):
        entry = {"time": datetime.utcnow().strftime("%H:%M:%S"), "msg": msg}
        self._scan_log.append(entry)
        if len(self._scan_log) > 100:
            self._scan_log = self._scan_log[-100:]
        logger.info(msg)

    async def _get_mid(self, token_id: str) -> Optional[float]:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{CLOB_HOST}/midpoint", params={"token_id": token_id},
                    timeout=aiohttp.ClientTimeout(total=5),
                ) as resp:
                    if resp.status != 200:
                        return None
                    data = await resp.json()
                    mid  = data.get("mid")
                    return float(mid) if mid else None
        except Exception:
            return None
