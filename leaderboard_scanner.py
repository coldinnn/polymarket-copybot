"""
Scans the Polymarket leaderboard periodically and auto-qualifies traders.

Flow (runs every SCAN_INTERVAL seconds):
  1. Fetch top N traders from leaderboard API
  2. Skip wallets already in our registry
  3. For each new wallet, run TraderAnalyzer
  4. Add approved/watching traders to the registry
  5. Re-score existing traders every RESCORE_INTERVAL to catch degradation

The registry is the source of truth for which wallets WalletMonitor watches.
"""
import asyncio
import json
import logging
import os
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import aiohttp

from trader_analyzer import TraderAnalyzer, TraderProfile

logger = logging.getLogger(__name__)

DATA_API          = "https://data-api.polymarket.com"
GLOBAL_TRADES_API = "https://data-api.polymarket.com/trades"

RESCORE_INTERVAL  = 86400   # re-score existing traders every 24 hours
DISCOVERY_POLL_INTERVAL = 30    # poll the global trades feed every 30s
MIN_TRADES_TO_ANALYZE   = 4     # wallet must appear this many times in the observation window
MIN_TRADE_SIZE_USD      = 5.0   # ignore dust trades when counting activity
REGISTRY_PATH     = Path(os.getenv("REGISTRY_PATH", "/tmp/trader_registry.json"))

# Seed wallets — top leaderboard traders, always analyzed on startup.
# Analyzer auto-detects sport focus and qualifies/rejects by win rate.
SEED_WALLETS = {
    "0xf8831548531d56ad6a4331493243c447a827cd1f": "Inaccuratestake",       # #2 tennis
    "0x99aea8f9a64d0142b6b66a4b9d02a2211d45386f": "LEEEROYJENKINS",         # #1 overall
    "0x26437896ed9dfeb2f69765edcafe8fdceaab39ae": "Latina",                 # #3 overall
    "0xb91aeb5accc33a5f9a8615b8ed6b2d352e913987": "afghj2421",              # #4 overall
    "0x0346afae2603313d2bbee96b628536c8cbe352a5": "GoalLineGhost",           # #6 sports
    "0x4761ecf3578e388a9b16c43f874efe32ee855ae8": "Grenderen",              # #10 overall
    "0x84cfffc3f16dcc353094de30d4a45226eccd2f63": "mooseborzoi",            # #11 overall
    "0xfe787d2da716d60e8acff57fb87eb13cd4d10319": "ferrariChampions2026",   # #13 sports
    "0xad9c94a65d1f053b8bb31815865a0d4c64b69889": "jalenbrunson-official",  # #14 sports
    "0x5268527977f700f9bf9b6d5cd843859e4e70135d": "HomeRunHazard",          # #18 sports
    "0xbee54d90051720e27921dc6874f02d646ffca636": "downtownfee",            # #8 overall
    "0xf0318c32136c2db7fec88b84869aee6a1106c80c": "BreakTheBank",           # #17 overall
}


class LeaderboardScanner:
    def __init__(self):
        self._analyzer  = TraderAnalyzer()
        self._registry: dict[str, TraderProfile] = {}   # address → profile
        self._scan_log: list[dict] = []
        self._running   = False
        self._seen_window: dict[str, dict] = {}          # address → {count, username, total_usd}
        self._seen_trade_ids: set = set()                # dedup transactionHash across polls
        self._load_registry()

    # ── Public API ────────────────────────────────────────────────────────────

    @property
    def registry(self) -> dict[str, TraderProfile]:
        return self._registry

    @property
    def approved_wallets(self) -> list[str]:
        """Addresses we should actively copy (approved status)."""
        return [addr for addr, p in self._registry.items()
                if p.status == "approved"]

    @property
    def all_profiles(self) -> list[TraderProfile]:
        return sorted(self._registry.values(),
                      key=lambda p: (-p.confidence, -p.total_profit))

    def get_profile(self, address: str) -> Optional[TraderProfile]:
        return self._registry.get(address.lower())

    async def start(self):
        self._running = True
        # Analyze all seed wallets on startup
        await self._seed()
        await asyncio.gather(
            self._discovery_loop(),
            self._rescore_loop(),
        )

    async def _rescore_loop(self):
        while self._running:
            await asyncio.sleep(RESCORE_INTERVAL)
            try:
                await self._rescore_all()
            except Exception as e:
                logger.error(f"Rescore error: {e}")

    def stop(self):
        self._running = False

    # ── Re-score all ─────────────────────────────────────────────────────────

    async def _rescore_all(self):
        """Re-analyze every tracked wallet every 24h to catch degradation."""
        self._log(f"Re-scoring {len(self._registry)} tracked wallets…")
        for address in list(self._registry.keys()):
            existing = self._registry[address]
            try:
                await asyncio.sleep(0.5)
                profile = await self._analyzer.analyze(address, existing.username)
                if existing.status == "approved" and profile.win_rate < existing.win_rate - 0.10:
                    profile.status = "paused"
                    profile.reject_reason = (
                        f"win rate dropped {existing.win_rate:.1%} → {profile.win_rate:.1%}"
                    )
                    self._log(f"⚠ PAUSED {existing.username}: {profile.reject_reason}")
                elif existing.status == "paused" and profile.win_rate >= 0.60:
                    profile.status = "approved"
                    self._log(f"↑ REINSTATED {existing.username}: {profile.win_rate:.1%} WR")
                self._registry[address] = profile
            except Exception as e:
                logger.error(f"Rescore failed for {existing.username}: {e}")
        self._save_registry()
        self._log(f"Rescore done. {len(self.approved_wallets)} approved.")

    # ── Seeding ───────────────────────────────────────────────────────────────

    async def _seed(self):
        for address, username in SEED_WALLETS.items():
            addr_lower = address.lower()
            if addr_lower not in self._registry:
                self._log(f"Analyzing seed wallet: {username}")
                try:
                    profile = await self._analyzer.analyze(address, username)
                    self._registry[addr_lower] = profile
                    self._save_registry()
                    self._log(
                        f"Seed {username}: {profile.win_rate:.1%} WR, "
                        f"{profile.total_trades} trades, {profile.sport_focus}, "
                        f"status={profile.status}"
                    )
                except Exception as e:
                    logger.error(f"Seed analysis failed for {username}: {e}")

    # ── Discovery (replaces the dead leaderboard scan — no public leaderboard API) ──
    #
    # Polymarket exposes no public "top traders" endpoint, so instead we mine the
    # GLOBAL trades feed (every trade on the platform, not filtered by wallet) for
    # wallets that show up repeatedly. A wallet seen >= MIN_TRADES_TO_ANALYZE times
    # in the rolling observation window is "active enough to be worth analyzing" —
    # then it goes through the same TraderAnalyzer qualification as seed wallets.
    # This means the registry self-refreshes with currently-active traders instead
    # of going stale on a fixed hardcoded list.

    async def _discovery_loop(self):
        from trader_analyzer import _detect_sport
        while self._running:
            try:
                await self._poll_global_trades(_detect_sport)
            except Exception as e:
                logger.error(f"Discovery poll error: {e}")
            await asyncio.sleep(DISCOVERY_POLL_INTERVAL)

    async def _poll_global_trades(self, detect_sport):
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
            async with session.get(GLOBAL_TRADES_API, params={"limit": "200"}) as resp:
                if resp.status != 200:
                    return
                trades = await resp.json()

        new_observations = 0
        for t in trades:
            txn = t.get("transactionHash", "")
            if not txn or txn in self._seen_trade_ids:
                continue
            self._seen_trade_ids.add(txn)

            usdc = float(t.get("size", 0)) * float(t.get("price", 0))
            if usdc < MIN_TRADE_SIZE_USD:
                continue

            address = (t.get("proxyWallet") or "").lower()
            if not address or address in self._registry:
                continue

            sport = detect_sport(t.get("title", ""))
            entry = self._seen_window.setdefault(address, {
                "count": 0, "username": t.get("name") or t.get("pseudonym") or address[:10] + "…",
                "sports_seen": set(),
            })
            entry["count"] += 1
            entry["sports_seen"].add(sport)
            new_observations += 1

        # Trim _seen_trade_ids so it doesn't grow forever
        if len(self._seen_trade_ids) > 20000:
            self._seen_trade_ids = set(list(self._seen_trade_ids)[-10000:])

        # Promote any candidate that's crossed the activity threshold
        candidates = [addr for addr, e in self._seen_window.items()
                      if e["count"] >= MIN_TRADES_TO_ANALYZE]
        if candidates:
            await self._promote_candidates(candidates)

    async def _promote_candidates(self, addresses: list[str]):
        promoted = 0
        for address in addresses:
            entry = self._seen_window.pop(address, None)
            if not entry or address in self._registry:
                continue
            username = entry["username"]
            try:
                await asyncio.sleep(0.5)   # rate limit vs data-api
                profile = await self._analyzer.analyze(address, username)
                self._registry[address] = profile
                promoted += 1
                self._log(
                    f"{'✓ APPROVED' if profile.status=='approved' else '~ watching' if profile.status=='watching' else '✗ rejected'}"
                    f" [discovered] {username}: {profile.win_rate:.1%} WR, "
                    f"{profile.total_trades} trades, {profile.sport_focus}"
                    + (f" — {profile.reject_reason}" if profile.reject_reason else "")
                )
            except Exception as e:
                logger.error(f"Discovery analysis failed for {address[:10]}: {e}")
        if promoted:
            self._save_registry()
            self._log(f"Discovery: {promoted} new wallets analyzed, {len(self.approved_wallets)} approved total")

    # ── Registry persistence ──────────────────────────────────────────────────

    def _save_registry(self):
        try:
            REGISTRY_PATH.parent.mkdir(parents=True, exist_ok=True)
            data = {addr: asdict(p) for addr, p in self._registry.items()}
            REGISTRY_PATH.write_text(json.dumps(data, indent=2))
        except Exception as e:
            logger.error(f"Failed to save registry: {e}")

    def _load_registry(self):
        try:
            if REGISTRY_PATH.exists():
                raw = json.loads(REGISTRY_PATH.read_text())
                for addr, d in raw.items():
                    try:
                        self._registry[addr] = TraderProfile(**d)
                    except Exception:
                        pass
                logger.info(f"Loaded {len(self._registry)} traders from registry")
        except Exception as e:
            logger.error(f"Failed to load registry: {e}")

    def _log(self, msg: str):
        entry = {"time": datetime.utcnow().strftime("%H:%M:%S"), "msg": msg}
        self._scan_log.append(entry)
        if len(self._scan_log) > 200:
            self._scan_log = self._scan_log[-200:]
        logger.info(f"[Scanner] {msg}")
