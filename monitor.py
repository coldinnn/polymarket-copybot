"""
Polls the target wallet's Polymarket activity every 3 seconds.
Emits a CopySignal the first time a new outcome token is bought.
"""
import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

import aiohttp

logger = logging.getLogger(__name__)

TARGET_WALLET = "0xf8831548531d56ad6a4331493243c447a827cd1f"
POLL_INTERVAL = 3   # seconds between polls
DATA_API      = "https://data-api.polymarket.com"


@dataclass
class CopySignal:
    token_id:        str
    condition_id:    str          # market condition ID (for CLOB options fetch)
    title:           str
    outcome:         str
    price:           float        # price target wallet paid on first detected trade
    target_size_usd: float        # how much they deployed (for context only)
    detected_at:     str = field(default_factory=lambda: datetime.utcnow().strftime("%H:%M:%S"))


class WalletMonitor:
    def __init__(self):
        self._seen_ids:      set[str]               = set()
        self._copied_tokens: set[str]               = set()
        self._signal_queue:  asyncio.Queue          = asyncio.Queue()
        self.latest_feed:    list[dict]             = []   # last 20 raw activity items
        self._running = False

    # ── Public API ────────────────────────────────────────────────────────────

    @property
    def signal_queue(self) -> asyncio.Queue:
        return self._signal_queue

    def mark_copied(self, token_id: str):
        """Call after placing a copy order so we don't copy the same token twice."""
        self._copied_tokens.add(token_id)

    async def start(self):
        self._running = True
        logger.info(f"Monitoring {TARGET_WALLET[:10]}…")
        while self._running:
            try:
                await self._poll()
            except Exception as e:
                logger.error(f"Monitor poll error: {e}")
            await asyncio.sleep(POLL_INTERVAL)

    def stop(self):
        self._running = False

    # ── Internal ──────────────────────────────────────────────────────────────

    async def _poll(self):
        url    = f"{DATA_API}/activity"
        params = {"user": TARGET_WALLET, "limit": "50"}

        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=8)) as session:
            async with session.get(url, params=params) as resp:
                if resp.status != 200:
                    logger.warning(f"Activity API returned {resp.status}")
                    return
                data = await resp.json()

        if not isinstance(data, list):
            return

        # Keep latest 20 items for the dashboard feed (all activity types)
        self.latest_feed = data[:20]

        for item in data:
            txn_id = str(item.get("id") or item.get("transactionHash") or "")
            if not txn_id or txn_id in self._seen_ids:
                continue
            self._seen_ids.add(txn_id)

            # Only act on BUY trades
            if item.get("side") != "BUY":
                continue
            item_type = (item.get("type") or "").upper()
            if item_type not in ("TRADE", ""):
                continue

            token_id = str(item.get("tokenId") or item.get("asset_id") or "")
            if not token_id or token_id in self._copied_tokens:
                continue

            signal = CopySignal(
                token_id=token_id,
                condition_id=str(item.get("market") or ""),
                title=str(item.get("title") or "Unknown market"),
                outcome=str(item.get("outcome") or ""),
                price=float(item.get("price") or 0),
                target_size_usd=float(item.get("usdcSize") or 0),
            )

            logger.info(
                f"NEW POSITION DETECTED  {signal.title[:50]}"
                f"  {signal.outcome}  @ {signal.price:.2f}"
                f"  (target deployed ${signal.target_size_usd:,.0f})"
            )
            await self._signal_queue.put(signal)
