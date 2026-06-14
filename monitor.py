"""
Monitors multiple approved Polymarket wallets for new BUY positions.

The approved wallet list is driven by LeaderboardScanner — updated live
as new traders are approved or paused. Each wallet gets its own async
polling loop at POLL_INTERVAL seconds.

A CopySignal fires the first time a new tokenId is seen across ALL wallets
(we don't copy the same token twice even if two traders both buy it).
"""
import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

import aiohttp

logger = logging.getLogger(__name__)

DATA_API      = "https://data-api.polymarket.com"
POLL_INTERVAL = 3   # seconds between polls per wallet


@dataclass
class CopySignal:
    token_id:        str
    condition_id:    str
    title:           str
    outcome:         str
    price:           float
    target_size_usd: float
    source_wallet:   str        # which trader triggered this
    source_username: str
    copy_weight:     float      # trader's copy_weight from their profile
    detected_at:     str = field(default_factory=lambda: datetime.utcnow().strftime("%H:%M:%S"))


class WalletMonitor:
    def __init__(self):
        self._seen_ids:      set[str]      = set()     # txn IDs already processed
        self._copied_tokens: set[str]      = set()     # tokenIds already copied
        self._signal_queue:  asyncio.Queue = asyncio.Queue()
        self._wallet_feeds:  dict[str, list] = {}      # address → latest feed items
        self._wallet_tasks:  dict[str, asyncio.Task]  = {}
        self._running        = False
        # Scanner reference injected after init
        self._scanner        = None

    # ── Public API ────────────────────────────────────────────────────────────

    @property
    def signal_queue(self) -> asyncio.Queue:
        return self._signal_queue

    @property
    def active_wallets(self) -> list[str]:
        return list(self._wallet_tasks.keys())

    def get_feed(self, address: str) -> list:
        return self._wallet_feeds.get(address, [])

    def latest_feed_all(self) -> list[dict]:
        """Merged feed from all wallets, most recent first."""
        merged = []
        for addr, items in self._wallet_feeds.items():
            for item in items:
                item = dict(item)
                item["_wallet"] = addr
                merged.append(item)
        merged.sort(key=lambda x: x.get("timestamp") or "", reverse=True)
        return merged[:30]

    def mark_copied(self, token_id: str):
        self._copied_tokens.add(token_id)

    def set_scanner(self, scanner):
        """Inject LeaderboardScanner so we can pull approved wallets."""
        self._scanner = scanner

    async def start(self):
        self._running = True
        # Manage wallet tasks — start new ones, cancel removed ones
        while self._running:
            await self._sync_wallets()
            await asyncio.sleep(30)   # re-sync every 30s

    def stop(self):
        self._running = False
        for t in self._wallet_tasks.values():
            t.cancel()

    # ── Wallet management ─────────────────────────────────────────────────────

    async def _sync_wallets(self):
        """Add tasks for newly approved wallets, cancel tasks for paused ones."""
        if self._scanner is None:
            return
        approved = set(self._scanner.approved_wallets)
        current  = set(self._wallet_tasks.keys())

        for addr in approved - current:
            logger.info(f"Monitor: adding wallet {addr[:10]}…")
            task = asyncio.create_task(
                self._poll_wallet(addr), name=f"poll_{addr[:8]}"
            )
            self._wallet_tasks[addr] = task

        for addr in current - approved:
            logger.info(f"Monitor: removing wallet {addr[:10]}…")
            self._wallet_tasks[addr].cancel()
            del self._wallet_tasks[addr]

    # ── Per-wallet polling ────────────────────────────────────────────────────

    async def _poll_wallet(self, address: str):
        logger.info(f"Polling {address[:10]}…")
        while self._running:
            try:
                await self._poll_once(address)
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.error(f"Poll error ({address[:10]}): {e}")
            await asyncio.sleep(POLL_INTERVAL)

    async def _poll_once(self, address: str):
        url    = f"{DATA_API}/activity"
        params = {"user": address, "limit": "50"}

        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=8)) as session:
            async with session.get(url, params=params) as resp:
                if resp.status != 200:
                    return
                data = await resp.json()

        if not isinstance(data, list):
            return

        self._wallet_feeds[address] = data[:20]

        for item in data:
            txn_id = str(item.get("id") or item.get("transactionHash") or "")
            if not txn_id or txn_id in self._seen_ids:
                continue
            self._seen_ids.add(txn_id)

            if item.get("side") != "BUY":
                continue
            if (item.get("type") or "").upper() != "TRADE":
                continue

            token_id = str(item.get("asset") or "")
            if not token_id or token_id in self._copied_tokens:
                continue

            # Get trader profile for copy_weight
            profile = self._scanner.get_profile(address) if self._scanner else None
            copy_weight   = profile.copy_weight if profile else 1.0
            source_user   = profile.username    if profile else address[:10] + "…"

            signal = CopySignal(
                token_id=token_id,
                condition_id=str(item.get("conditionId") or ""),
                title=str(item.get("title") or "Unknown market"),
                outcome=str(item.get("outcome") or ""),
                price=float(item.get("price") or 0),
                target_size_usd=float(item.get("usdcSize") or 0),
                source_wallet=address,
                source_username=source_user,
                copy_weight=copy_weight,
            )

            logger.info(
                f"SIGNAL [{source_user}] {signal.title[:45]}"
                f"  {signal.outcome} @ {signal.price:.2f}"
                f"  weight={copy_weight:.1f}"
            )
            await self._signal_queue.put(signal)
