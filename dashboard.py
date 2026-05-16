import asyncio
import json
from aiohttp import web
from database import get_logs, get_stats

DASHBOARD_PORT = 8080

DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Log Aggregator Dashboard</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: 'Segoe UI', sans-serif; background: #0f172a; color: #e2e8f0; }
        .header { background: #1e293b; padding: 20px 30px; border-bottom: 1px solid #334155; }
        .header h1 { font-size: 22px; color: #38bdf8; }
        .header p { font-size: 13px; color: #94a3b8; margin-top: 4px; }
        .container { display: flex; gap: 20px; padding: 20px 30px; height: calc(100vh - 90px); }
        .panel { background: #1e293b; border-radius: 8px; padding: 16px; border: 1px solid #334155; }
        .sidebar { width: 280px; display: flex; flex-direction: column; gap: 16px; }
        .logs-panel { flex: 1; display: flex; flex-direction: column; overflow: hidden; }
        .stat-card { text-align: center; padding: 12px; }
        .stat-card .value { font-size: 28px; font-weight: bold; color: #38bdf8; }
        .stat-card .label { font-size: 12px; color: #94a3b8; margin-top: 4px; }
        .stat-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; }
        h3 { font-size: 14px; color: #94a3b8; margin-bottom: 10px; text-transform: uppercase; letter-spacing: 1px; }
        .log-list { flex: 1; overflow-y: auto; font-family: 'Consolas', monospace; font-size: 13px; }
        .log-entry { padding: 8px 12px; border-bottom: 1px solid #1e293b; display: flex; gap: 12px; align-items: flex-start; }
        .log-entry:hover { background: #334155; }
        .badge { padding: 2px 8px; border-radius: 4px; font-size: 11px; font-weight: bold; min-width: 50px; text-align: center; }
        .badge.INFO { background: #164e63; color: #22d3ee; }
        .badge.ERROR { background: #7f1d1d; color: #fca5a5; }
        .badge.WARN { background: #78350f; color: #fbbf24; }
        .service-name { color: #a78bfa; min-width: 120px; }
        .log-msg { color: #cbd5e1; }
        .log-time { color: #64748b; font-size: 11px; min-width: 80px; }
        .status { display: flex; align-items: center; gap: 6px; font-size: 13px; }
        .dot { width: 8px; height: 8px; border-radius: 50%; background: #22c55e; }
        .dot.disconnected { background: #ef4444; }
        .filters { display: flex; gap: 8px; margin-bottom: 12px; }
        .filters select, .filters input { background: #0f172a; border: 1px solid #334155; color: #e2e8f0; padding: 6px 10px; border-radius: 4px; font-size: 13px; }
    </style>
</head>
<body>
    <div class="header">
        <h1>Log Aggregator Dashboard</h1>
        <div class="status"><div class="dot" id="wsDot"></div><span id="wsStatus">Connecting...</span></div>
    </div>
    <div class="container">
        <div class="sidebar">
            <div class="panel">
                <h3>Metrics</h3>
                <div class="stat-grid">
                    <div class="stat-card"><div class="value" id="totalLogs">0</div><div class="label">Total Logs</div></div>
                    <div class="stat-card"><div class="value" id="activeConns">0</div><div class="label">Active Conns</div></div>
                    <div class="stat-card"><div class="value" id="msgRecv">0</div><div class="label">Messages</div></div>
                    <div class="stat-card"><div class="value" id="rateLimited">0</div><div class="label">Rate Limited</div></div>
                </div>
            </div>
            <div class="panel">
                <h3>By Level</h3>
                <div id="byLevel"></div>
            </div>
            <div class="panel">
                <h3>By Service</h3>
                <div id="byService"></div>
            </div>
        </div>
        <div class="panel logs-panel">
            <h3>Live Logs</h3>
            <div class="filters">
                <select id="filterLevel"><option value="">All Levels</option><option>INFO</option><option>ERROR</option><option>WARN</option></select>
                <input type="text" id="filterService" placeholder="Filter service...">
            </div>
            <div class="log-list" id="logList"></div>
        </div>
    </div>
    <script>
        const logList = document.getElementById('logList');
        const filterLevel = document.getElementById('filterLevel');
        const filterService = document.getElementById('filterService');
        let allLogs = [];

        function renderLog(log) {
            const levelFilter = filterLevel.value;
            const serviceFilter = filterService.value.toLowerCase();
            if (levelFilter && log.level !== levelFilter) return '';
            if (serviceFilter && !log.service.toLowerCase().includes(serviceFilter)) return '';
            const ts = log.timestamp ? new Date(log.timestamp * 1000).toLocaleTimeString() : '';
            return `<div class="log-entry">
                <span class="log-time">${ts}</span>
                <span class="badge ${log.level}">${log.level}</span>
                <span class="service-name">${log.service}</span>
                <span class="log-msg">${log.message}</span>
            </div>`;
        }

        function renderAll() {
            logList.innerHTML = allLogs.map(renderLog).join('');
            logList.scrollTop = logList.scrollHeight;
        }

        filterLevel.addEventListener('change', renderAll);
        filterService.addEventListener('input', renderAll);

        // Load existing logs
        fetch('/api/logs?limit=200').then(r => r.json()).then(logs => {
            allLogs = logs.reverse();
            renderAll();
        });

        // Load stats
        function loadStats() {
            fetch('/api/stats').then(r => r.json()).then(s => {
                document.getElementById('byLevel').innerHTML = Object.entries(s.db.by_level || {})
                    .map(([k,v]) => `<div style="display:flex;justify-content:space-between;padding:4px 0"><span class="badge ${k}">${k}</span><span>${v}</span></div>`).join('');
                document.getElementById('byService').innerHTML = Object.entries(s.db.by_service || {})
                    .map(([k,v]) => `<div style="display:flex;justify-content:space-between;padding:4px 0"><span class="service-name">${k}</span><span>${v}</span></div>`).join('');
                document.getElementById('totalLogs').textContent = s.server.logs_stored || 0;
                document.getElementById('activeConns').textContent = s.server.connections_active || 0;
                document.getElementById('msgRecv').textContent = s.server.messages_received || 0;
                document.getElementById('rateLimited').textContent = s.server.rate_limited || 0;
            });
        }
        loadStats();
        setInterval(loadStats, 5000);

        // WebSocket for live logs
        const ws = new WebSocket(`ws://${location.host}/ws`);
        ws.onopen = () => {
            document.getElementById('wsDot').className = 'dot';
            document.getElementById('wsStatus').textContent = 'Live';
        };
        ws.onclose = () => {
            document.getElementById('wsDot').className = 'dot disconnected';
            document.getElementById('wsStatus').textContent = 'Disconnected';
        };
        ws.onmessage = (e) => {
            const log = JSON.parse(e.data);
            allLogs.push(log);
            if (allLogs.length > 500) allLogs.shift();
            const html = renderLog(log);
            if (html) {
                logList.insertAdjacentHTML('beforeend', html);
                logList.scrollTop = logList.scrollHeight;
            }
        };
    </script>
</body>
</html>"""


def handle_index(request):
    return web.Response(text=DASHBOARD_HTML, content_type="text/html")


async def handle_get_logs(request):
    service = request.query.get("service")
    level = request.query.get("level")
    limit = min(int(request.query.get("limit", 100)), 1000)
    logs = await get_logs(service=service, level=level, limit=limit)
    return web.json_response(logs)


async def handle_get_stats(request):
    from server import get_metrics
    db_stats = await get_stats()
    return web.json_response({"server": get_metrics(), "db": db_stats})


async def handle_websocket(request):
    from server import dashboard_queues

    ws = web.WebSocketResponse()
    await ws.prepare(request)

    queue = asyncio.Queue()
    dashboard_queues.append(queue)

    try:
        while not ws.closed:
            log = await queue.get()
            await ws.send_str(json.dumps(log))
    except (ConnectionResetError, asyncio.CancelledError):
        pass
    finally:
        dashboard_queues.remove(queue)

    return ws


async def start_dashboard():
    app = web.Application()
    app.router.add_get("/", handle_index)
    app.router.add_get("/api/logs", handle_get_logs)
    app.router.add_get("/api/stats", handle_get_stats)
    app.router.add_get("/ws", handle_websocket)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", DASHBOARD_PORT)
    await site.start()

    from server import logger
    logger.info("Dashboard running on http://0.0.0.0:%s", DASHBOARD_PORT)

    # Keep running forever
    while True:
        await asyncio.sleep(3600)
