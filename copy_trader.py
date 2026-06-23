"""
Places copy orders based on CopySignals from WalletMonitor.

Copy size = COPY_SIZE_USD * signal.copy_weight (capped at COPY_SIZE_MAX).
Higher-confidence traders get proportionally larger positions.

COPY_PAPER=true  → simulate orders (default)
COPY_PAPER=false → live CLOB orders
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

CLOB_HOST      = "https://clob.polymarket.com"
DATA_API       = "https://data-api.polymarket.com"
CHAIN_ID       = 137

COPY_SIZE_USD  = float(os.getenv("COPY_SIZE_USD",  "25"))
COPY_SIZE_MAX  = float(os.getenv("COPY_SIZE_MAX",  "50"))
MAX_ENTRY      = float(os.getenv("COPY_MAX_ENTRY", "0.92"))
MIN_ENTRY      = float(os.getenv("COPY_MIN_ENTRY", "0.05"))
PAPER_MODE     = os.getenv("COPY_PAPER", "true").lower() != "false"
RESOLVE_INTERVAL = 300
MAX_EXPOSURE_PCT = float(os.getenv("COPY_MAX_EXPOSURE_PCT", "1.0"))  # cap open exposure at this multiple of bankroll


@dataclass
class CopyPosition:
    token_id:        str
    condition_id:    str
    title:           str
    outcome:         str
    entry_price:     float
    size_usd:        float
    shares:          int
    target_price:    float
    target_size_usd: float
    source_wallet:   str
    source_username: str
    copy_weight:     float
    entered_at:      str
    paper:           bool
    status:          str   = "open"   # open | won | lost
    pnl:             float = 0.0
    exit_price:      float = 0.0
    resolved_at:     str   = ""

    def to_dict(self) -> dict:
        return asdict(self)


class CopyTrader:
    def __init__(self, monitor: WalletMonitor):
        self.monitor   = monitor
        self._client   = None
        self._lock     = asyncio.Lock()
        self._running  = False
        self.positions: list[CopyPosition] = []
        self.history:   list[CopyPosition] = []
        self._scan_log: list[dict]         = []
        self._bankroll  = float(os.getenv("COPY_BANKROLL", "500"))

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def initialize(self):
        if PAPER_MODE:
            logger.info("CopyTrader: PAPER mode (set COPY_PAPER=false for live)")
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
            logger.info("CopyTrader: LIVE mode")
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
            if any(p.token_id == signal.token_id for p in self.positions):
                return

            mid = await self._get_mid(signal.token_id)
            if mid is None:
                self._log(f"SKIP {signal.title[:40]}: no mid price")
                return
            if mid > MAX_ENTRY:
                self._log(f"SKIP {signal.title[:40]}: mid {mid:.2f} > {MAX_ENTRY} max")
                return
            if mid < MIN_ENTRY:
                self._log(f"SKIP {signal.title[:40]}: mid {mid:.2f} < {MIN_ENTRY} min")
                return

            # Weighted copy size — higher confidence traders get larger copies
            raw_size = min(COPY_SIZE_USD * signal.copy_weight, COPY_SIZE_MAX)
            price    = round(min(mid + 0.01, 0.99), 2)
            shares   = int(raw_size / price)
            if shares < 1:
                return
            actual_cost = round(price * shares, 2)

            # Exposure cap — refuse new copies once open exposure would exceed
            # MAX_EXPOSURE_PCT of bankroll. Without this, a burst of signals from
            # high-frequency wallets (e.g. 5-minute crypto markets) can pile up
            # dozens of positions far past available capital before any of them
            # resolve and free up room.
            current_exposure = sum(p.size_usd for p in self.positions)
            max_exposure = self._bankroll * MAX_EXPOSURE_PCT
            if current_exposure + actual_cost > max_exposure:
                self._log(
                    f"SKIP {signal.title[:40]}: exposure cap hit "
                    f"(${current_exposure:.0f} open + ${actual_cost:.0f} > ${max_exposure:.0f} max)"
                )
                return

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
                source_wallet=signal.source_wallet,
                source_username=signal.source_username,
                copy_weight=signal.copy_weight,
                entered_at=datetime.utcnow().strftime("%H:%M:%S"),
                paper=paper,
            )
            self.positions.append(pos)
            self.monitor.mark_copied(signal.token_id)

            tag = "PAPER" if paper else "LIVE"
            self._log(
                f"COPY [{tag}] [{signal.source_username}] {pos.title[:40]}"
                f"  {pos.outcome}  ${actual_cost:.2f} @ {price:.2f}"
                f"  (target @ {signal.price:.2f}  weight={signal.copy_weight:.1f})"
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
        if not self.positions:
            return
        from monitor import DATA_API as _DATA_API
        # Collect condition IDs that have been redeemed by any source wallet
        source_wallets = list({p.source_wallet for p in self.positions})
        redeemed_conditions: set[str] = set()

        for wallet in source_wallets:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        f"{_DATA_API}/activity",
                        params={"user": wallet, "limit": "100"},
                    ) as resp:
                        if resp.status != 200:
                            continue
                        data = await resp.json()
                        for item in (data if isinstance(data, list) else []):
                            if (item.get("type") or "").upper() == "REDEEM":
                                cid = str(item.get("conditionId") or "")
                                if cid:
                                    redeemed_conditions.add(cid)
            except Exception as e:
                logger.error(f"Resolve fetch error for {wallet[:10]}: {e}")

        if not redeemed_conditions:
            return

        async with self._lock:
            still_open = []
            for pos in self.positions:
                if pos.condition_id not in redeemed_conditions:
                    still_open.append(pos)
                    continue
                won = await self._is_redeemable(pos.token_id)
                if won is None:
                    still_open.append(pos)
                    continue
                pos.resolved_at = datetime.utcnow().strftime("%H:%M:%S")
                if won:
                    pos.status     = "won"
                    pos.exit_price = 1.0
                    pos.pnl        = round(pos.shares * 1.0 - pos.size_usd, 2)
                    self._log(f"WIN  [{pos.source_username}] {pos.title[:40]}  +${pos.pnl:.2f}")
                else:
                    pos.status     = "lost"
                    pos.exit_price = 0.0
                    pos.pnl        = -pos.size_usd
                    self._log(f"LOSS [{pos.source_username}] {pos.title[:40]}  -${pos.size_usd:.2f}")
                self.history.append(pos)
            self.positions = still_open

    async def _is_redeemable(self, token_id: str) -> Optional[bool]:
        wallet = os.getenv("POLY_WALLET", "")
        if not wallet and self._client:
            try:
                wallet = self._client._signer.address
            except Exception:
                pass
        if not wallet:
            return None
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{DATA_API}/positions",
                    params={"user": wallet, "limit": "500"},
                ) as resp:
                    if resp.status != 200:
                        return None
                    data = await resp.json()
                    for p in (data if isinstance(data, list) else []):
                        tid = str(p.get("asset_id") or p.get("tokenId") or "")
                        if tid == token_id:
                            return bool(p.get("redeemable"))
                    return False
        except Exception:
            return None

    # ── Stats ─────────────────────────────────────────────────────────────────

    def stats(self) -> dict:
        closed   = self.history
        wins     = [p for p in closed if p.status == "won"]
        losses   = [p for p in closed if p.status == "lost"]
        total_pnl = sum(p.pnl for p in closed)
        win_rate  = (len(wins) / len(closed) * 100) if closed else 0.0
        return {
            "bankroll":     round(self._bankroll + total_pnl, 2),
            "started":      self._bankroll,
            "pnl":          round(total_pnl, 2),
            "pnl_pct":      round(total_pnl / self._bankroll * 100, 2) if self._bankroll else 0,
            "win_rate":     round(win_rate, 1),
            "wins":         len(wins),
            "losses":       len(losses),
            "open":         len(self.positions),
            "total_trades": len(closed),
            "copy_size":    COPY_SIZE_USD,
            "paper":        PAPER_MODE,
        }

    # ── Per-trader breakdown ──────────────────────────────────────────────────

    def trader_stats(self) -> list[dict]:
        """Win/loss/PnL breakdown per source trader."""
        wallets: dict[str, dict] = {}
        for p in self.history + self.positions:
            w = p.source_wallet
            if w not in wallets:
                wallets[w] = {
                    "username": p.source_username,
                    "wallet":   w,
                    "wins": 0, "losses": 0, "open": 0, "pnl": 0.0,
                }
            if p.status == "won":
                wallets[w]["wins"]   += 1
                wallets[w]["pnl"]    += p.pnl
            elif p.status == "lost":
                wallets[w]["losses"] += 1
                wallets[w]["pnl"]    += p.pnl
            else:
                wallets[w]["open"]   += 1
        return sorted(wallets.values(), key=lambda x: -x["pnl"])

    # ── Helpers ───────────────────────────────────────────────────────────────

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
                    f"{CLOB_HOST}/midpoint",
                    params={"token_id": token_id},
                    timeout=aiohttp.ClientTimeout(total=5),
                ) as resp:
                    if resp.status != 200:
                        return None
                    data = await resp.json()
                    mid  = data.get("mid")
                    return float(mid) if mid else None
        except Exception:
            return None
