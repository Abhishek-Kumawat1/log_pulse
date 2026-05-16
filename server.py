import asyncio
import ssl
import os
import sys
import time
import logging
from protocol import async_decode_message, async_send_message
from database import init_db, insert_log

# Ensure this module is importable as 'server' even when run as __main__
sys.modules.setdefault('server', sys.modules[__name__])

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

HOST = os.environ.get("HOST", "127.0.0.1")
TCP_PORT = int(os.environ.get("TCP_PORT", 5000))
HEARTBEAT_TIMEOUT = 15

VALID_TOKENS = {
    "producer-token": "producer",
    "consumer-token": "consumer",
}

# ── Logging ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("server")

# ── Metrics ──────────────────────────────────────────────
metrics = {
    "connections_total": 0,
    "connections_active": 0,
    "messages_received": 0,
    "logs_stored": 0,
    "auth_failures": 0,
    "rate_limited": 0,
}

# ── State ────────────────────────────────────────────────
clients = {}          # writer -> {type, service/subscriptions, last_seen}
subscriptions = {}    # service_name -> [writer, ...]
dashboard_queues = [] # list of asyncio.Queue for WebSocket dashboard clients

# ── Rate Limiter (token bucket per connection) ───────────
RATE_LIMIT = 20       # max messages per window
RATE_WINDOW = 10      # seconds
rate_buckets = {}     # writer -> [timestamps]


def is_rate_limited(writer):
    now = time.time()
    if writer not in rate_buckets:
        rate_buckets[writer] = []

    bucket = rate_buckets[writer]
    # Remove timestamps outside the window
    rate_buckets[writer] = [t for t in bucket if now - t < RATE_WINDOW]

    if len(rate_buckets[writer]) >= RATE_LIMIT:
        metrics["rate_limited"] += 1
        return True

    rate_buckets[writer].append(now)
    return False


# ── Client handlers ──────────────────────────────────────

def register_client(writer, message):
    client_type = message.get("client_type")

    if client_type == "producer":
        service = message.get("service")
        clients[writer] = {
            "type": "producer",
            "service": service,
            "last_seen": time.time(),
        }
        logger.info("Registered producer for service '%s'", service)

    elif client_type == "consumer":
        subs = message.get("subscriptions", [])
        clients[writer] = {
            "type": "consumer",
            "subscriptions": subs,
            "last_seen": time.time(),
        }
        for service in subs:
            subscriptions.setdefault(service, []).append(writer)
        logger.info("Registered consumer for services %s", subs)


async def handle_log(message):
    service = message.get("service")
    log_message = message.get("message", "")
    level = message.get("level", "INFO")
    timestamp = time.time()

    # Persist to SQLite
    await insert_log(service, log_message, level)
    metrics["logs_stored"] += 1

    # Attach timestamp for downstream consumers and dashboard
    message["timestamp"] = timestamp

    # Forward to subscribed consumers
    consumers = subscriptions.get(service, []).copy()
    for consumer_writer in consumers:
        try:
            await async_send_message(consumer_writer, message)
        except Exception:
            pass

    # Broadcast to dashboard WebSocket clients
    for queue in dashboard_queues:
        await queue.put(message)


async def handle_auth(writer, message, addr):
    token = message.get("token")
    if token in VALID_TOKENS:
        await async_send_message(writer, {"type": "auth_success"})
        logger.info("Auth success: %s as %s", addr, VALID_TOKENS[token])
        return "success"

    metrics["auth_failures"] += 1
    await async_send_message(writer, {"type": "auth_failed"})
    logger.warning("Auth failed: %s", addr)
    return "failed"


async def dispatch_message(writer, message_type, message):
    if message_type == "register":
        register_client(writer, message)
    elif message_type == "log":
        await handle_log(message)
    elif message_type == "heartbeat":
        if writer in clients:
            clients[writer]["last_seen"] = time.time()
        await async_send_message(writer, {"type": "heartbeat_ack"})


async def handle_client(reader, writer):
    addr = writer.get_extra_info("peername")
    metrics["connections_total"] += 1
    metrics["connections_active"] += 1
    logger.info("Connected: %s", addr)

    authenticated = False

    try:
        while True:
            message = await async_decode_message(reader)
            if not message:
                break

            metrics["messages_received"] += 1
            message_type = message.get("type")

            if message_type == "auth":
                result = await handle_auth(writer, message, addr)
                if result == "success":
                    authenticated = True
                    continue
                else:
                    break

            if not authenticated:
                await async_send_message(writer, {
                    "type": "error",
                    "message": "Authentication required",
                })
                continue

            if is_rate_limited(writer):
                await async_send_message(writer, {
                    "type": "error",
                    "message": "Rate limit exceeded. Slow down.",
                })
                logger.warning("Rate limited: %s", addr)
                continue

            await dispatch_message(writer, message_type, message)

    except (ConnectionResetError, BrokenPipeError):
        pass
    except Exception as e:
        logger.error("Error with %s: %s", addr, e)
    finally:
        metrics["connections_active"] -= 1
        cleanup_client(writer)
        writer.close()
        logger.info("Disconnected: %s", addr)


def cleanup_client(writer):
    clients.pop(writer, None)
    rate_buckets.pop(writer, None)

    for service, consumer_list in subscriptions.items():
        if writer in consumer_list:
            consumer_list.remove(writer)


# ── Heartbeat monitor ────────────────────────────────────

async def monitor_clients():
    while True:
        await asyncio.sleep(5)
        now = time.time()
        dead = [w for w, info in clients.items()
                if now - info["last_seen"] > HEARTBEAT_TIMEOUT]

        for writer in dead:
            logger.warning("Heartbeat timeout — removing dead client")
            cleanup_client(writer)
            try:
                writer.close()
            except Exception:
                pass


# ── Server startup ───────────────────────────────────────

def get_metrics():
    return dict(metrics)


async def start_tcp_server():
    ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS)
    ssl_ctx.load_cert_chain(
        certfile=os.path.join(BASE_DIR, "server.crt"),
        keyfile=os.path.join(BASE_DIR, "server.key"),
    )

    server = await asyncio.start_server(
        handle_client, HOST, TCP_PORT, ssl=ssl_ctx
    )
    logger.info("TCP server listening on %s:%s (SSL)", HOST, TCP_PORT)
    return server


async def main():
    await init_db()

    tcp_server = await start_tcp_server()
    monitor_task = asyncio.create_task(monitor_clients())

    # Import and start dashboard (avoids circular import)
    from dashboard import start_dashboard
    dashboard_task = asyncio.create_task(start_dashboard())

    logger.info("All systems running. Press Ctrl+C to stop.")

    try:
        await asyncio.gather(
            tcp_server.serve_forever(),
            monitor_task,
            dashboard_task,
        )
    except asyncio.CancelledError:
        raise
    finally:
        tcp_server.close()
        logger.info("Server shut down.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted. Shutting down.")
