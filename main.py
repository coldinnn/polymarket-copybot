"""
Polymarket Multi-Trader Copy Bot — FastAPI dashboard + orchestration.

Automatically scans the Polymarket leaderboard every hour, qualifies
traders by win rate + sample size + sport focus, then copies their
positions in real-time (or paper-trades them).

Env vars:
  POLY_PRIVATE_KEY   — your Polygon EOA private key
  POLY_WALLET        — your deposit wallet address
  COPY_SIZE_USD      — base $ per copy trade (default: 25)
  COPY_SIZE_MAX      — max $ per trade even at weight 2.0 (default: 50)
  COPY_BANKROLL      — starting bankroll for P&L tracking (default: 500)
  COPY_PAPER         — "true" (default) or "false" for live trading
  COPY_MAX_ENTRY     — skip if mid > this after target moves it (default: 0.92)
"""
import asyncio
import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse

from monitor import WalletMonitor
from copy_trader import CopyTrader, PAPER_MODE
from leaderboard_scanner import LeaderboardScanner
from market_maker import MarketMaker

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ── Global state ───────────────────────────────────────────────────────────────
_scanner = LeaderboardScanner()
_monitor = WalletMonitor()
_trader  = CopyTrader(_monitor)
_monitor.set_scanner(_scanner)

# Market maker — only active when MM_TOKEN_ID is set
_mm_token = os.getenv("MM_TOKEN_ID", "")
_mm: MarketMaker | None = MarketMaker(_mm_token) if _mm_token else None


@asynccontextmanager
async def lifespan(app: FastAPI):
    _trader.initialize()
    tasks = [
        asyncio.create_task(_scanner.start(), name="scanner"),
        asyncio.create_task(_monitor.start(), name="monitor"),
        asyncio.create_task(_trader.start(),  name="trader"),
    ]
    if _mm:
        _mm.initialize()
        tasks.append(asyncio.create_task(_mm.run(), name="market_maker"))
        logger.info(f"Market maker started on token {_mm_token[:20]}...")
    yield
    _scanner.stop()
    _monitor.stop()
    _trader.stop()
    if _mm:
        _mm.stop()
    for t in tasks:
        t.cancel()


app = FastAPI(title="Polymarket Copy Trader", lifespan=lifespan)

# ── API ────────────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/copy/stats")
def copy_stats():
    return JSONResponse(_trader.stats())

@app.get("/copy/positions")
def copy_positions():
    return JSONResponse([p.to_dict() for p in _trader.positions])

@app.get("/copy/history")
def copy_history():
    return JSONResponse([p.to_dict() for p in reversed(_trader.history)])

@app.get("/copy/log")
def copy_log():
    return JSONResponse(list(reversed(_trader._scan_log)))

@app.get("/copy/trader-stats")
def copy_trader_stats():
    return JSONResponse(_trader.trader_stats())

@app.get("/copy/feed")
def copy_feed():
    return JSONResponse(_monitor.latest_feed_all())

# ── Market Maker API ───────────────────────────────────────────────────────────

@app.get("/mm/status")
async def mm_status():
    if not _mm:
        return JSONResponse({"active": False, "reason": "MM_TOKEN_ID not set"})
    mid = await _mm._get_mid()
    return JSONResponse({
        "active":       True,
        "paper":        os.getenv("MM_PAPER", "true").lower() != "false",
        "token_id":     _mm_token[:30] + "...",
        "live_mid":     _mm._live_mid,
        "quote":        {
            "bid":       _mm.quote.bid_price  if _mm.quote else None,
            "ask":       _mm.quote.ask_price  if _mm.quote else None,
            "bid_shares": _mm.quote.bid_shares if _mm.quote else None,
            "ask_shares": _mm.quote.ask_shares if _mm.quote else None,
        } if _mm.quote else None,
        "stats": {
            "quotes_posted": _mm.stats.quotes_posted,
            "requotes":      _mm.stats.requotes,
            "bid_fills":     len([f for f in _mm.stats.fills if f.side == "bid"]),
            "ask_fills":     len([f for f in _mm.stats.fills if f.side == "ask"]),
            "net_shares":    _mm.stats.net_shares,
            "pnl":           _mm.stats.pnl(mid or 0.5),
        },
        "fill_log": [
            {"side": f.side, "price": f.price, "shares": f.shares, "at": f.filled_at}
            for f in _mm.stats.fills[-20:]
        ],
    })

@app.get("/traders")
def traders():
    return JSONResponse([p.to_dict() for p in _scanner.all_profiles])

@app.get("/scanner/log")
def scanner_log():
    return JSONResponse(list(reversed(_scanner._scan_log)))

# ── Dashboard ──────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard():
    mode_badge = (
        '<span style="background:#f59e0b;color:#000;padding:2px 8px;border-radius:4px;font-size:11px;font-weight:700">PAPER</span>'
        if PAPER_MODE else
        '<span style="background:#ef4444;color:#fff;padding:2px 8px;border-radius:4px;font-size:11px;font-weight:700">LIVE</span>'
    )
    return HTMLResponse(f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Polymarket Copy Trader</title>
<style>
  :root{{--bg:#0d1117;--card:#161b22;--border:#30363d;--text:#e6edf3;
        --muted:#8b949e;--green:#3fb950;--red:#f85149;--yellow:#d29922;--blue:#58a6ff;--purple:#bc8cff}}
  *{{box-sizing:border-box;margin:0;padding:0}}
  body{{background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;font-size:14px;padding:20px}}
  .page-header{{display:flex;align-items:center;gap:12px;margin-bottom:20px;flex-wrap:wrap}}
  .page-header h1{{font-size:20px;font-weight:700}}
  .tabs{{display:flex;gap:4px;margin-bottom:20px;border-bottom:1px solid var(--border);padding-bottom:0}}
  .tab{{padding:8px 16px;cursor:pointer;border-radius:6px 6px 0 0;font-size:13px;font-weight:500;color:var(--muted);border:1px solid transparent;border-bottom:none;margin-bottom:-1px}}
  .tab.active{{color:var(--text);background:var(--card);border-color:var(--border)}}
  .tab-content{{display:none}} .tab-content.active{{display:block}}
  .stats-row{{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:16px}}
  .stat{{background:var(--card);border:1px solid var(--border);border-radius:8px;padding:14px;text-align:center}}
  .stat-val{{font-size:22px;font-weight:700;margin-bottom:2px}}
  .stat-label{{font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px}}
  .grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(300px,1fr));gap:16px;margin-bottom:16px}}
  .card{{background:var(--card);border:1px solid var(--border);border-radius:8px;padding:16px}}
  h2{{font-size:15px;font-weight:600;margin-bottom:12px}}
  .green{{color:var(--green)}} .red{{color:var(--red)}} .yellow{{color:var(--yellow)}} .blue{{color:var(--blue)}} .purple{{color:var(--purple)}} .muted{{color:var(--muted)}}
  table{{width:100%;border-collapse:collapse;font-size:13px}}
  th{{color:var(--muted);font-weight:500;padding:6px 8px;text-align:left;border-bottom:1px solid var(--border);font-size:11px;text-transform:uppercase}}
  td{{padding:7px 8px;border-bottom:1px solid #21262d;vertical-align:middle}}
  tr:last-child td{{border-bottom:none}}
  .badge{{display:inline-block;padding:2px 7px;border-radius:4px;font-size:11px;font-weight:600}}
  .badge-green{{background:#1a3a2a;color:var(--green)}}
  .badge-red{{background:#3a1a1a;color:var(--red)}}
  .badge-blue{{background:#1a2a3a;color:var(--blue)}}
  .badge-yellow{{background:#3a2e1a;color:var(--yellow)}}
  .badge-purple{{background:#2a1a3a;color:var(--purple)}}
  .badge-muted{{background:#21262d;color:var(--muted)}}
  .bar-bg{{background:#21262d;border-radius:3px;height:6px;width:80px;display:inline-block;vertical-align:middle;margin-left:4px}}
  .bar-fill{{height:6px;border-radius:3px;background:var(--green)}}
  .log-entry{{padding:4px 0;border-bottom:1px solid #21262d;font-size:12px;font-family:monospace}}
  .log-t{{color:#444d56;margin-right:8px}}
  .log-copy{{color:var(--blue)}} .log-win{{color:var(--green)}} .log-loss{{color:var(--red)}} .log-skip{{color:var(--muted)}} .log-scan{{color:var(--yellow)}}
  .feed-item{{padding:5px 0;border-bottom:1px solid #21262d;font-size:12px}}
  #lastUpdate{{font-size:11px;color:var(--muted)}}
  @media(max-width:640px){{.stats-row{{grid-template-columns:repeat(2,1fr)}}}}
</style>
</head>
<body>
<div class="page-header">
  <h1>📋 Copy Trader</h1>
  {mode_badge}
  <div id="lastUpdate"></div>
</div>

<!-- Stats -->
<div class="stats-row" id="statsGrid"></div>

<!-- Tabs -->
<div class="tabs">
  <div class="tab active" onclick="switchTab('positions')">📂 Positions</div>
  <div class="tab" onclick="switchTab('traders')">👥 Traders</div>
  <div class="tab" onclick="switchTab('history')">📜 History</div>
  <div class="tab" onclick="switchTab('feed')">🔍 Feed</div>
  <div class="tab" onclick="switchTab('log')">🖥 Log</div>
  <div class="tab" onclick="switchTab('mm')">📈 Market Maker</div>
</div>

<!-- Positions tab -->
<div id="tab-positions" class="tab-content active">
  <div class="card">
    <h2>Open Positions <span id="openCount" class="muted" style="font-size:12px;font-weight:400"></span></h2>
    <table><thead><tr><th>Market</th><th>Outcome</th><th>Trader</th><th>Entry</th><th>Size</th><th>Target</th></tr></thead>
    <tbody id="openBody"><tr><td colspan="6" class="muted">No open positions</td></tr></tbody></table>
  </div>
</div>

<!-- Traders tab -->
<div id="tab-traders" class="tab-content">
  <div class="card" style="margin-bottom:16px">
    <h2>Monitored Traders <span id="traderCount" class="muted" style="font-size:12px;font-weight:400"></span></h2>
    <table>
      <thead><tr>
        <th>Trader</th><th>Sport</th><th>Win Rate</th><th>Trades</th>
        <th>Confidence</th><th>Weight</th><th>Our PnL</th><th>Status</th>
      </tr></thead>
      <tbody id="tradersBody"><tr><td colspan="8" class="muted">Loading…</td></tr></tbody>
    </table>
  </div>
  <div class="card">
    <h2>Scanner Log</h2>
    <div id="scannerLog" style="max-height:260px;overflow-y:auto"></div>
  </div>
</div>

<!-- History tab -->
<div id="tab-history" class="tab-content">
  <div class="card">
    <h2>Trade History <span id="histCount" class="muted" style="font-size:12px;font-weight:400"></span></h2>
    <table><thead><tr><th>Market</th><th>Outcome</th><th>Trader</th><th>Entry</th><th>Size</th><th>Result</th><th>P&amp;L</th></tr></thead>
    <tbody id="histBody"><tr><td colspan="7" class="muted">No completed trades yet</td></tr></tbody></table>
  </div>
</div>

<!-- Feed tab -->
<div id="tab-feed" class="tab-content">
  <div class="card">
    <h2>Live Activity Feed <span class="muted" style="font-size:12px;font-weight:400">all monitored wallets · every 3s</span></h2>
    <div id="feedBody"></div>
  </div>
</div>

<!-- Log tab -->
<div id="tab-log" class="tab-content">
  <div class="card">
    <h2>Copy Log</h2>
    <div id="logBody" style="max-height:400px;overflow-y:auto"></div>
  </div>
</div>

<!-- Market Maker tab -->
<div id="tab-mm" class="tab-content">
  <div class="stats-row" id="mmStats" style="grid-template-columns:repeat(5,1fr)"></div>
  <div class="grid" style="grid-template-columns:1fr 1fr">
    <div class="card">
      <h2>Current Quote</h2>
      <div id="mmQuote"><span class="muted">Loading…</span></div>
    </div>
    <div class="card">
      <h2>Market</h2>
      <div id="mmMarket"><span class="muted">Loading…</span></div>
    </div>
  </div>
  <div class="card">
    <h2>Fill Log <span id="mmFillCount" class="muted" style="font-size:12px;font-weight:400"></span></h2>
    <table>
      <thead><tr><th>Time</th><th>Side</th><th>Price</th><th>Shares</th></tr></thead>
      <tbody id="mmFillBody"><tr><td colspan="4" class="muted">No fills yet — waiting for mid to cross 0.48 or 0.52</td></tr></tbody>
    </table>
  </div>
</div>

<script>
const $ = id => document.getElementById(id);
let activeTab = 'positions';

function switchTab(name) {{
  activeTab = name;
  document.querySelectorAll('.tab').forEach((t,i) => {{
    const tabs = ['positions','traders','history','feed','log','mm'];
    t.classList.toggle('active', tabs[i] === name);
  }});
  document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
  $('tab-' + name)?.classList.add('active');
}}

async function loadAll() {{
  const safe = url => fetch(url).then(r=>r.json()).catch(()=>null);
  const [stats, positions, history, log, traderStats, traders, scanLog, feed, mm] = await Promise.all([
    safe('/copy/stats'),
    safe('/copy/positions'),
    safe('/copy/history'),
    safe('/copy/log'),
    safe('/copy/trader-stats'),
    safe('/traders'),
    safe('/scanner/log'),
    safe('/copy/feed'),
    safe('/mm/status'),
  ]);
  try {{ if(stats)     renderStats(stats);                    }} catch(e){{console.error('stats',e)}}
  try {{ if(positions) renderOpen(positions);                 }} catch(e){{console.error('open',e)}}
  try {{ if(history)   renderHistory(history);                }} catch(e){{console.error('hist',e)}}
  try {{ if(log)       renderLog(log);                        }} catch(e){{console.error('log',e)}}
  try {{ if(mm)        renderMM(mm);                          }} catch(e){{console.error('mm',e)}}
  try {{ if(traders)   renderTraders(traders, traderStats||[]); }} catch(e){{console.error('traders',e)}}
  try {{ if(scanLog)   renderScannerLog(scanLog);             }} catch(e){{console.error('scanlog',e)}}
  try {{ if(feed)      renderFeed(feed);                      }} catch(e){{console.error('feed',e)}}
  $('lastUpdate').textContent = 'Updated ' + new Date().toLocaleTimeString();
}}

function renderStats(s) {{
  const pnlColor = s.pnl >= 0 ? 'green' : 'red';
  const pnlSign  = s.pnl >= 0 ? '+' : '';
  const tag = s.paper ? ' <span style="font-size:11px;color:var(--yellow)">(paper)</span>' : '';
  $('statsGrid').innerHTML = `
    <div class="stat"><div class="stat-val">${{s.bankroll?.toFixed(2)||'—'}}</div><div class="stat-label">Bankroll${{tag}}</div></div>
    <div class="stat"><div class="stat-val ${{pnlColor}}">${{pnlSign}}${{s.pnl?.toFixed(2)||'—'}}</div><div class="stat-label">P&L (${{s.pnl_pct>=0?'+':''}}${{s.pnl_pct?.toFixed(1)||0}}%)</div></div>
    <div class="stat"><div class="stat-val">${{s.win_rate?.toFixed(1)||'—'}}%</div><div class="stat-label">${{s.wins||0}}W / ${{s.losses||0}}L</div></div>
    <div class="stat"><div class="stat-val">${{s.total_trades||0}}</div><div class="stat-label">Settled · ${{s.open||0}} open</div></div>
  `;
}}

function renderOpen(positions) {{
  $('openCount').textContent = positions.length + ' open';
  if(!positions.length) {{
    $('openBody').innerHTML = '<tr><td colspan="6" class="muted">No open positions</td></tr>';
    return;
  }}
  $('openBody').innerHTML = positions.map(p => `
    <tr>
      <td style="max-width:160px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${{p.title}}">${{p.title}}</td>
      <td>${{p.outcome||'—'}}</td>
      <td class="blue">${{p.source_username||'—'}}</td>
      <td class="green">${{p.entry_price?.toFixed(2)}}¢</td>
      <td>$${{p.size_usd?.toFixed(2)}}</td>
      <td class="muted">${{p.target_price?.toFixed(2)}}¢</td>
    </tr>
  `).join('');
}}

function renderTraders(traders, traderStats) {{
  const tsMap = {{}};
  (traderStats||[]).forEach(t => {{ tsMap[t.wallet] = t; }});

  $('traderCount').textContent = traders.length + ' tracked';
  if(!traders.length) {{
    $('tradersBody').innerHTML = '<tr><td colspan="8" class="muted">Scanning leaderboard…</td></tr>';
    return;
  }}

  const sportEmoji = {{tennis:'🎾',football:'⚽',basketball:'🏀',baseball:'⚾',hockey:'🏒',mma:'🥊',crypto:'₿',politics:'🗳️',mixed:'🎯',other:'❓'}};

  $('tradersBody').innerHTML = traders.map(p => {{
    const statusBadge = {{
      approved: '<span class="badge badge-green">✓ approved</span>',
      watching: '<span class="badge badge-yellow">~ watching</span>',
      rejected: '<span class="badge badge-red">✗ rejected</span>',
      paused:   '<span class="badge badge-muted">⏸ paused</span>',
    }}[p.status] || p.status;

    const confPct = Math.round((p.confidence||0)*100);
    const confBar = `<div class="bar-bg"><div class="bar-fill" style="width:${{confPct}}%"></div></div>`;
    const wrColor = p.win_rate >= 0.75 ? 'green' : p.win_rate >= 0.60 ? 'yellow' : 'red';

    const ts      = tsMap[p.address] || {{}};
    const ourPnl  = ts.pnl ?? null;
    const pnlStr  = ourPnl !== null
      ? `<span class="${{ourPnl>=0?'green':'red'}}">${{ourPnl>=0?'+':''}}$${{ourPnl.toFixed(2)}}</span>`
      : '<span class="muted">—</span>';

    return `<tr>
      <td><strong>${{p.username}}</strong><br><span class="muted" style="font-size:11px;font-family:monospace">${{p.address.substring(0,10)}}…</span></td>
      <td>${{sportEmoji[p.sport_focus]||'❓'}} ${{p.sport_focus}}</td>
      <td class="${{wrColor}}">${{(p.win_rate*100).toFixed(1)}}%</td>
      <td>${{p.wins}}W / ${{p.losses}}L</td>
      <td>${{confPct}}% ${{confBar}}</td>
      <td class="purple">${{p.copy_weight?.toFixed(1)}}×</td>
      <td>${{pnlStr}}</td>
      <td>${{statusBadge}}</td>
    </tr>`;
  }}).join('');
}}

function renderHistory(history) {{
  $('histCount').textContent = history.length + ' trades';
  if(!history.length) {{
    $('histBody').innerHTML = '<tr><td colspan="7" class="muted">No completed trades</td></tr>';
    return;
  }}
  $('histBody').innerHTML = history.map(p => {{
    const badge = p.status==='won'
      ? '<span class="badge badge-green">WIN</span>'
      : '<span class="badge badge-red">LOSS</span>';
    const pSign = p.pnl>=0?'+':'';
    return `<tr>
      <td style="max-width:150px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${{p.title}}">${{p.title}}</td>
      <td>${{p.outcome||'—'}}</td>
      <td class="blue">${{p.source_username||'—'}}</td>
      <td>${{p.entry_price?.toFixed(2)}}¢</td>
      <td>$${{p.size_usd?.toFixed(2)}}</td>
      <td>${{badge}}</td>
      <td class="${{p.pnl>=0?'green':'red'}}">${{pSign}}$${{Math.abs(p.pnl).toFixed(2)}}</td>
    </tr>`;
  }}).join('');
}}

function renderLog(entries) {{
  if(!entries.length) {{
    $('logBody').innerHTML = '<div class="log-entry muted">No activity yet</div>';
    return;
  }}
  $('logBody').innerHTML = entries.slice(0,80).map(e => {{
    const msg = e.msg||'';
    let cls = 'log-skip';
    if(msg.startsWith('COPY')) cls='log-copy';
    if(msg.startsWith('WIN'))  cls='log-win';
    if(msg.startsWith('LOSS')) cls='log-loss';
    if(msg.startsWith('SIGNAL')) cls='log-copy';
    return `<div class="log-entry"><span class="log-t">${{e.time}}</span><span class="${{cls}}">${{msg}}</span></div>`;
  }}).join('');
}}

function renderScannerLog(entries) {{
  if(!entries.length) {{
    $('scannerLog').innerHTML = '<div class="log-entry muted">No scan activity yet</div>';
    return;
  }}
  $('scannerLog').innerHTML = entries.slice(0,60).map(e => {{
    const msg = e.msg||'';
    let cls = 'log-scan';
    if(msg.includes('APPROVED')) cls='log-win';
    if(msg.includes('rejected')) cls='log-skip';
    if(msg.includes('PAUSED'))   cls='log-loss';
    return `<div class="log-entry"><span class="log-t">${{e.time}}</span><span class="${{cls}}">${{msg}}</span></div>`;
  }}).join('');
}}

function renderFeed(items) {{
  if(!items.length) {{
    $('feedBody').innerHTML = '<div class="feed-item muted">No activity yet</div>';
    return;
  }}
  $('feedBody').innerHTML = items.slice(0,25).map(item => {{
    const side  = (item.side||'').toUpperCase();
    const type  = (item.type||'').toUpperCase();
    const price = parseFloat(item.price||0);
    const usdc  = parseFloat(item.usdcSize||0);
    const wallet= (item._wallet||'').substring(0,10)+'…';

    let sideEl = type==='REDEEM'
      ? '<span style="color:var(--yellow);font-weight:600">REDEEM</span>'
      : side==='BUY'
        ? '<span style="color:var(--green);font-weight:600">BUY</span>'
        : '<span style="color:var(--red);font-weight:600">SELL</span>';

    const ts    = item.timestamp ? new Date(item.timestamp).toLocaleTimeString() : '';
    const title = (item.title||'').substring(0,40);
    const out   = item.outcome||'';
    const ps    = price ? ` @ ${{price.toFixed(2)}}` : '';
    const ss    = usdc  ? ` · $${{usdc>=1000?(usdc/1000).toFixed(0)+'K':usdc.toFixed(0)}}` : '';

    return `<div class="feed-item">
      <span class="muted" style="font-size:11px;margin-right:6px">${{ts}}</span>
      <span class="blue" style="font-size:11px;margin-right:4px">${{wallet}}</span>
      ${{sideEl}}
      <span style="margin:0 4px">${{title}}</span>
      <span class="muted">${{out}}</span>
      <span class="yellow">${{ps}}${{ss}}</span>
    </div>`;
  }}).join('');
}}

function renderMM(d) {{
  if (!d || !d.active) {{
    $('mmStats').innerHTML = '<div class="stat"><div class="stat-val muted">—</div><div class="stat-label">MM not active</div></div>';
    return;
  }}
  const s      = d.stats || {{}};
  const q      = d.quote || {{}};
  const mid    = d.live_mid ?? '—';
  const pnl    = s.pnl ?? 0;
  const pnlCol = pnl >= 0 ? 'green' : 'red';
  const pnlSign= pnl >= 0 ? '+' : '';
  const modeBadge = d.paper
    ? '<span class="badge badge-yellow">PAPER</span>'
    : '<span class="badge badge-red">LIVE</span>';

  // Stats row
  $('mmStats').innerHTML = `
    <div class="stat">
      <div class="stat-val ${{pnlCol}}">${{pnlSign}}$${{pnl.toFixed(4)}}</div>
      <div class="stat-label">P&L (USDC)</div>
    </div>
    <div class="stat">
      <div class="stat-val">${{s.bid_fills ?? 0}} / ${{s.ask_fills ?? 0}}</div>
      <div class="stat-label">Bid / Ask Fills</div>
    </div>
    <div class="stat">
      <div class="stat-val ${{s.net_shares > 0 ? 'green' : s.net_shares < 0 ? 'red' : ''}}">${{s.net_shares >= 0 ? '+' : ''}}${{s.net_shares ?? 0}}</div>
      <div class="stat-label">Net Inventory (shares)</div>
    </div>
    <div class="stat">
      <div class="stat-val">${{s.quotes_posted ?? 0}}</div>
      <div class="stat-label">Quotes Posted</div>
    </div>
    <div class="stat">
      <div class="stat-val blue">${{typeof mid === 'number' ? mid.toFixed(3) : mid}}</div>
      <div class="stat-label">Live Mid</div>
    </div>
  `;

  // Quote card
  if (q.bid) {{
    const spread = q.ask && q.bid ? ((q.ask - q.bid) * 100).toFixed(1) : '—';
    $('mmQuote').innerHTML = `
      <div style="display:flex;gap:24px;align-items:center;margin-bottom:12px">
        <div style="text-align:center">
          <div style="font-size:28px;font-weight:700;color:var(--green)">${{q.bid?.toFixed(3)}}</div>
          <div class="muted" style="font-size:11px">BID × ${{q.bid_shares}} shares</div>
        </div>
        <div style="text-align:center;color:var(--muted)">
          <div style="font-size:14px">——  ${{spread}}¢ spread  ——</div>
          <div style="font-size:11px;margin-top:4px">mid ${{typeof mid === 'number' ? mid.toFixed(3) : mid}}</div>
        </div>
        <div style="text-align:center">
          <div style="font-size:28px;font-weight:700;color:var(--red)">${{q.ask?.toFixed(3)}}</div>
          <div class="muted" style="font-size:11px">ASK × ${{q.ask_shares}} shares</div>
        </div>
      </div>
      <div style="font-size:12px;color:var(--muted)">Fills when mid crosses these levels ${{modeBadge}}</div>
    `;
  }} else {{
    $('mmQuote').innerHTML = '<span class="muted">No active quote</span>';
  }}

  // Market info card
  $('mmMarket').innerHTML = `
    <div style="font-size:12px;margin-bottom:8px">
      <span class="muted">Token: </span><span style="font-family:monospace;font-size:11px">${{d.token_id}}</span>
    </div>
    <div style="font-size:12px;margin-bottom:4px">
      <span class="muted">Mode: </span>${{modeBadge}}
    </div>
    <div style="font-size:12px;margin-bottom:4px">
      <span class="muted">Requotes: </span>${{s.requotes ?? 0}}
    </div>
    <div style="font-size:12px">
      <span class="muted">Status: </span>
      <span class="green">● quoting</span>
    </div>
  `;

  // Fill log
  const fills = d.fill_log || [];
  $('mmFillCount').textContent = fills.length + ' fills total';
  if (!fills.length) {{
    $('mmFillBody').innerHTML = '<tr><td colspan="4" class="muted">No fills yet — price must cross 0.48 (bid) or 0.52 (ask)</td></tr>';
    return;
  }}
  $('mmFillBody').innerHTML = [...fills].reverse().map(f => {{
    const cls = f.side === 'bid' ? 'green' : 'red';
    const arrow = f.side === 'bid' ? '▼ BID' : '▲ ASK';
    return `<tr>
      <td class="muted">${{f.at}}</td>
      <td><span class="${{cls}}" style="font-weight:600">${{arrow}}</span></td>
      <td>${{f.price?.toFixed(3)}}</td>
      <td>${{f.shares}}</td>
    </tr>`;
  }}).join('');
}}

loadAll();
setInterval(loadAll, 5000);
</script>
</body>
</html>""")


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8001"))
    print(f"  Copy Trader → http://localhost:{port}")
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
