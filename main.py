"""
Polymarket Copy Trader — FastAPI dashboard + orchestration.

Mirrors wallet 0xf883… (Inaccuratestake) — #1 on Polymarket leaderboard,
Roland Garros specialist, $3.9M+ profit this month.

Env vars:
  POLY_PRIVATE_KEY  — your Polygon EOA private key
  POLY_WALLET       — your deposit wallet address
  COPY_SIZE_USD     — $ per copy trade (default: 25)
  COPY_BANKROLL     — starting bankroll for P&L tracking (default: 500)
  COPY_PAPER        — "true" (default) or "false" for live trading
  COPY_MAX_ENTRY    — skip if market mid > this (default: 0.92)
"""
import asyncio
import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse

from monitor import WalletMonitor, TARGET_WALLET
from copy_trader import CopyTrader, PAPER_MODE

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ── Global state ───────────────────────────────────────────────────────────────
_monitor = WalletMonitor()
_trader  = CopyTrader(_monitor)

# ── Lifespan ───────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    _trader.initialize()
    tasks = [
        asyncio.create_task(_monitor.start(), name="monitor"),
        asyncio.create_task(_trader.start(),  name="trader"),
    ]
    yield
    _monitor.stop()
    _trader.stop()
    for t in tasks:
        t.cancel()

app = FastAPI(title="Polymarket Copy Trader", lifespan=lifespan)

# ── API endpoints ──────────────────────────────────────────────────────────────

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

@app.get("/copy/feed")
def copy_feed():
    return JSONResponse(_monitor.latest_feed)

# ── Dashboard ─────────────────────────────────────────────────────────────────

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
<title>Copy Trader — Inaccuratestake</title>
<style>
  :root{{--bg:#0d1117;--card:#161b22;--border:#30363d;--text:#e6edf3;
        --muted:#8b949e;--green:#3fb950;--red:#f85149;--yellow:#d29922;--blue:#58a6ff}}
  *{{box-sizing:border-box;margin:0;padding:0}}
  body{{background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;font-size:14px;padding:20px}}
  h2{{font-size:16px;font-weight:600;margin-bottom:12px;color:var(--text)}}
  .page-header{{display:flex;align-items:center;gap:12px;margin-bottom:24px;flex-wrap:wrap}}
  .page-header h1{{font-size:20px;font-weight:700}}
  .target-tag{{background:var(--card);border:1px solid var(--border);border-radius:6px;padding:4px 10px;font-size:12px;color:var(--muted);font-family:monospace}}
  .grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(300px,1fr));gap:16px;margin-bottom:16px}}
  .card{{background:var(--card);border:1px solid var(--border);border-radius:8px;padding:16px}}
  .stats-row{{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:16px}}
  .stat{{background:var(--card);border:1px solid var(--border);border-radius:8px;padding:14px;text-align:center}}
  .stat-val{{font-size:22px;font-weight:700;margin-bottom:2px}}
  .stat-label{{font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px}}
  .green{{color:var(--green)}} .red{{color:var(--red)}} .yellow{{color:var(--yellow)}} .blue{{color:var(--blue)}}
  .muted{{color:var(--muted)}}
  table{{width:100%;border-collapse:collapse;font-size:13px}}
  th{{color:var(--muted);font-weight:500;padding:6px 8px;text-align:left;border-bottom:1px solid var(--border);font-size:11px;text-transform:uppercase}}
  td{{padding:7px 8px;border-bottom:1px solid #21262d;vertical-align:middle}}
  tr:last-child td{{border-bottom:none}}
  .badge{{display:inline-block;padding:2px 7px;border-radius:4px;font-size:11px;font-weight:600}}
  .badge-green{{background:#1a3a2a;color:var(--green)}}
  .badge-red{{background:#3a1a1a;color:var(--red)}}
  .badge-blue{{background:#1a2a3a;color:var(--blue)}}
  .badge-yellow{{background:#3a2e1a;color:var(--yellow)}}
  .log-entry{{padding:4px 0;border-bottom:1px solid #21262d;font-size:12px;font-family:monospace;color:var(--muted)}}
  .log-entry .t{{color:#444d56;margin-right:8px}}
  .log-copy{{color:var(--blue)}} .log-win{{color:var(--green)}} .log-loss{{color:var(--red)}} .log-skip{{color:var(--muted)}}
  .feed-item{{padding:6px 0;border-bottom:1px solid #21262d;font-size:12px}}
  .feed-side-buy{{color:var(--green);font-weight:600}} .feed-side-sell{{color:var(--red);font-weight:600}}
  .feed-redeem{{color:var(--yellow);font-weight:600}}
  .divider{{height:1px;background:var(--border);margin:16px 0}}
  #lastUpdate{{font-size:11px;color:var(--muted);margin-bottom:16px}}
  @media(max-width:640px){{.stats-row{{grid-template-columns:repeat(2,1fr)}}}}
</style>
</head>
<body>
<div class="page-header">
  <h1>📋 Copy Trader</h1>
  {mode_badge}
  <div class="target-tag">copying {TARGET_WALLET[:10]}… (Inaccuratestake)</div>
  <div id="lastUpdate"></div>
</div>

<!-- Stats -->
<div class="stats-row" id="statsGrid">
  <div class="stat"><div class="stat-val">—</div><div class="stat-label">Bankroll</div></div>
  <div class="stat"><div class="stat-val">—</div><div class="stat-label">P&amp;L</div></div>
  <div class="stat"><div class="stat-val">—</div><div class="stat-label">Win Rate</div></div>
  <div class="stat"><div class="stat-val">—</div><div class="stat-label">Trades</div></div>
</div>

<div class="grid">
  <!-- Open positions -->
  <div class="card">
    <h2>📂 Open Positions <span id="openCount" class="muted" style="font-size:13px;font-weight:400"></span></h2>
    <table><thead><tr><th>Market</th><th>Outcome</th><th>Entry</th><th>Size</th><th>Target @</th></tr></thead>
    <tbody id="openBody"><tr><td colspan="5" class="muted">No open positions</td></tr></tbody></table>
  </div>

  <!-- Target wallet live feed -->
  <div class="card">
    <h2>🔍 Target Wallet Feed <span class="muted" style="font-size:12px;font-weight:400">live · every 3s</span></h2>
    <div id="feedBody"></div>
  </div>
</div>

<!-- Trade history -->
<div class="card" style="margin-bottom:16px">
  <h2>📜 Trade History <span id="histCount" class="muted" style="font-size:13px;font-weight:400"></span></h2>
  <table><thead><tr><th>Market</th><th>Outcome</th><th>Entry</th><th>Size</th><th>Target @</th><th>Result</th><th>P&amp;L</th></tr></thead>
  <tbody id="histBody"><tr><td colspan="7" class="muted">No completed trades yet</td></tr></tbody></table>
</div>

<!-- Scan log -->
<div class="card">
  <h2>🖥 Scan Log</h2>
  <div id="logBody" style="max-height:260px;overflow-y:auto"></div>
</div>

<script>
const $ = id => document.getElementById(id);

function fmt(n, prefix='$') {{
  if(n===null||n===undefined||n==='') return '—';
  const v = parseFloat(n);
  return (prefix||'') + (isNaN(v)?'—':v.toFixed(2));
}}

async function loadAll() {{
  try {{
    const [stats, positions, history, log, feed] = await Promise.all([
      fetch('/copy/stats').then(r=>r.json()),
      fetch('/copy/positions').then(r=>r.json()),
      fetch('/copy/history').then(r=>r.json()),
      fetch('/copy/log').then(r=>r.json()),
      fetch('/copy/feed').then(r=>r.json()),
    ]);
    renderStats(stats);
    renderOpen(positions);
    renderHistory(history);
    renderLog(log);
    renderFeed(feed);
    $('lastUpdate').textContent = 'Updated ' + new Date().toLocaleTimeString();
  }} catch(e) {{ console.error(e); }}
}}

function renderStats(s) {{
  const pnlColor = s.pnl >= 0 ? 'green' : 'red';
  const pnlSign  = s.pnl >= 0 ? '+' : '';
  const paperTag = s.paper ? ' <span style="font-size:12px;color:var(--yellow)">(paper)</span>' : '';
  $('statsGrid').innerHTML = `
    <div class="stat"><div class="stat-val">${{s.bankroll?.toFixed(2)||'—'}}</div><div class="stat-label">Bankroll${{paperTag}}</div></div>
    <div class="stat"><div class="stat-val ${{pnlColor}}">${{pnlSign}}${{s.pnl?.toFixed(2)||'—'}}</div><div class="stat-label">P&L (${{s.pnl_pct>=0?'+':''}}${{s.pnl_pct?.toFixed(1)||0}}%)</div></div>
    <div class="stat"><div class="stat-val">${{s.win_rate?.toFixed(1)||'—'}}%</div><div class="stat-label">${{s.wins||0}}W / ${{s.losses||0}}L</div></div>
    <div class="stat"><div class="stat-val">${{s.total_trades||0}}</div><div class="stat-label">Settled · ${{s.open||0}} open</div></div>
  `;
}}

function renderOpen(positions) {{
  $('openCount').textContent = positions.length + ' open';
  if(!positions.length) {{
    $('openBody').innerHTML = '<tr><td colspan="5" class="muted">No open positions</td></tr>';
    return;
  }}
  $('openBody').innerHTML = positions.map(p => `
    <tr>
      <td style="max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${{p.title}}">${{p.title}}</td>
      <td>${{p.outcome||'—'}}</td>
      <td class="green">${{p.entry_price?.toFixed(2)||'—'}}¢</td>
      <td>$${{p.size_usd?.toFixed(2)||'—'}}</td>
      <td class="muted">${{p.target_price?.toFixed(2)||'—'}}¢ ($${{(p.target_size_usd/1000).toFixed(0)}}K)</td>
    </tr>
  `).join('');
}}

function renderHistory(history) {{
  $('histCount').textContent = history.length + ' trades';
  if(!history.length) {{
    $('histBody').innerHTML = '<tr><td colspan="7" class="muted">No completed trades yet</td></tr>';
    return;
  }}
  $('histBody').innerHTML = history.map(p => {{
    const isWin   = p.status === 'won';
    const badge   = isWin
      ? '<span class="badge badge-green">WIN</span>'
      : '<span class="badge badge-red">LOSS</span>';
    const pnlCol  = isWin ? 'green' : 'red';
    const pnlSign = p.pnl >= 0 ? '+' : '';
    return `
      <tr>
        <td style="max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${{p.title}}">${{p.title}}</td>
        <td>${{p.outcome||'—'}}</td>
        <td>${{p.entry_price?.toFixed(2)||'—'}}¢</td>
        <td>$${{p.size_usd?.toFixed(2)||'—'}}</td>
        <td class="muted">${{p.target_price?.toFixed(2)||'—'}}¢</td>
        <td>${{badge}}</td>
        <td class="${{pnlCol}}">${{pnlSign}}$${{Math.abs(p.pnl)?.toFixed(2)||'—'}}</td>
      </tr>
    `;
  }}).join('');
}}

function renderLog(entries) {{
  if(!entries.length) {{
    $('logBody').innerHTML = '<div class="log-entry muted">No activity yet</div>';
    return;
  }}
  $('logBody').innerHTML = entries.slice(0,50).map(e => {{
    const msg = e.msg || '';
    let cls = 'log-skip';
    if(msg.startsWith('COPY'))  cls = 'log-copy';
    if(msg.startsWith('WIN'))   cls = 'log-win';
    if(msg.startsWith('LOSS'))  cls = 'log-loss';
    return `<div class="log-entry"><span class="t">${{e.time}}</span><span class="${{cls}}">${{msg}}</span></div>`;
  }}).join('');
}}

function renderFeed(items) {{
  if(!items.length) {{
    $('feedBody').innerHTML = '<div class="feed-item muted">No activity yet</div>';
    return;
  }}
  $('feedBody').innerHTML = items.slice(0,15).map(item => {{
    const side = (item.side||'').toUpperCase();
    const type = (item.type||'').toUpperCase();
    const price = parseFloat(item.price||0);
    const usdc  = parseFloat(item.usdcSize||0);

    let sideEl = '';
    if(type === 'REDEEM') {{
      sideEl = `<span class="feed-redeem">REDEEM</span>`;
    }} else if(side === 'BUY') {{
      sideEl = `<span class="feed-side-buy">BUY</span>`;
    }} else {{
      sideEl = `<span class="feed-side-sell">SELL</span>`;
    }}

    const title   = (item.title||'').substring(0,45);
    const outcome = item.outcome || '';
    const priceStr = price ? ` @ ${{price.toFixed(2)}}` : '';
    const sizeStr  = usdc  ? ` · $${{usdc>=1000?(usdc/1000).toFixed(0)+'K':usdc.toFixed(0)}}` : '';
    const ts = item.timestamp ? new Date(item.timestamp).toLocaleTimeString() : '';

    return `<div class="feed-item">
      <span style="color:var(--muted);font-size:11px;margin-right:6px">${{ts}}</span>
      ${{sideEl}}
      <span style="color:var(--text);margin:0 4px">${{title}}</span>
      <span style="color:var(--muted)">${{outcome}}</span>
      <span style="color:var(--yellow)">${{priceStr}}${{sizeStr}}</span>
    </div>`;
  }}).join('');
}}

// Load immediately then every 5 seconds
loadAll();
setInterval(loadAll, 5000);
</script>
</body>
</html>""")


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8001"))
    print(f"  Copy Trader dashboard → http://localhost:{port}")
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
