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
LEADERBOARD_API   = "https://data-api.polymarket.com/leaderboard"

SCAN_INTERVAL     = 3600    # scan leaderboard every 1 hour
RESCORE_INTERVAL  = 86400   # re-score existing traders every 24 hours
TOP_N             = 50      # analyze top 50 leaderboard traders
REGISTRY_PATH     = Path(os.getenv("REGISTRY_PATH", "/tmp/trader_registry.json"))

# Seed wallets — always tracked regardless of leaderboard position
SEED_WALLETS = {
    "0xf8831548531d56ad6a4331493243c447a827cd1f": "Inaccuratestake",
}


class LeaderboardScanner:
    def __init__(self):
        self._analyzer  = TraderAnalyzer()
        self._registry: dict[str, TraderProfile] = {}   # address → profile
        self._scan_log: list[dict] = []
        self._running   = False
        self._last_rescore: dict[str, float] = {}       # address → epoch
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
        # Seed wallets first
        await self._seed()
        # Then periodic scan
        while self._running:
            try:
                await self._scan()
            except Exception as e:
                logger.error(f"Leaderboard scan error: {e}")
            await asyncio.sleep(SCAN_INTERVAL)

    def stop(self):
        self._running = False

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

    # ── Leaderboard scan ──────────────────────────────────────────────────────

    async def _scan(self):
        self._log("Scanning leaderboard…")
        top_traders = await self._fetch_leaderboard()
        if not top_traders:
            self._log("Leaderboard fetch returned no data")
            return

        new_count = 0
        for trader in top_traders[:TOP_N]:
            address  = (trader.get("address") or trader.get("user") or "").lower()
            username = trader.get("name") or trader.get("username") or address[:10] + "…"
            if not address:
                continue
            if address in self._registry:
                # Maybe re-score if stale
                await self._maybe_rescore(address)
                continue
            # New wallet — analyze it
            try:
                await asyncio.sleep(0.5)   # rate limit
                profile = await self._analyzer.analyze(address, username)
                self._registry[address] = profile
                new_count += 1
                self._log(
                    f"{'✓ APPROVED' if profile.status=='approved' else '~ watching' if profile.status=='watching' else '✗ rejected'}"
                    f" {username}: {profile.win_rate:.1%} WR, "
                    f"{profile.total_trades} trades, {profile.sport_focus}"
                    + (f" — {profile.reject_reason}" if profile.reject_reason else "")
                )
            except Exception as e:
                logger.error(f"Analysis failed for {address[:10]}: {e}")

        self._save_registry()
        approved = len(self.approved_wallets)
        self._log(f"Scan complete: {new_count} new wallets, {approved} approved total")

    async def _maybe_rescore(self, address: str):
        import time
        last = self._last_rescore.get(address, 0)
        if time.time() - last < RESCORE_INTERVAL:
            return
        self._last_rescore[address] = __import__("time").time()
        existing = self._registry[address]
        try:
            profile = await self._analyzer.analyze(address, existing.username)
            # Check for degradation
            if (existing.status == "approved"
                    and profile.win_rate < existing.win_rate - 0.10):
                profile.status = "paused"
                profile.reject_reason = (
                    f"win rate dropped from {existing.win_rate:.1%} "
                    f"to {profile.win_rate:.1%}"
                )
                self._log(f"⚠ PAUSED {existing.username}: {profile.reject_reason}")
            elif existing.status == "paused" and profile.win_rate >= 0.60:
                profile.status = "approved"
                self._log(f"↑ REINSTATED {existing.username}: {profile.win_rate:.1%} WR")
            self._registry[address] = profile
            self._save_registry()
        except Exception as e:
            logger.error(f"Rescore failed for {address[:10]}: {e}")

    # ── Leaderboard fetch ─────────────────────────────────────────────────────

    async def _fetch_leaderboard(self) -> list[dict]:
        """
        Try multiple known leaderboard endpoints.
        Polymarket exposes leaderboard data at several paths.
        """
        endpoints = [
            f"{DATA_API}/leaderboard",
            f"{DATA_API}/leaderboard?interval=monthly&limit={TOP_N}",
            f"{DATA_API}/profiles?sortBy=profit&limit={TOP_N}",
        ]
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as session:
            for url in endpoints:
                try:
                    async with session.get(url) as resp:
                        if resp.status != 200:
                            continue
                        data = await resp.json()
                        if isinstance(data, list) and data:
                            return data
                        if isinstance(data, dict):
                            for key in ("data", "traders", "leaderboard", "users"):
                                if key in data and isinstance(data[key], list):
                                    return data[key]
                except Exception as e:
                    logger.debug(f"Leaderboard endpoint {url} failed: {e}")
        return []

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
