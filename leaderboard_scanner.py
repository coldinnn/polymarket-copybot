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
        # Analyze all seed wallets on startup
        await self._seed()
        # Periodic re-score loop (no leaderboard API available publicly)
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
