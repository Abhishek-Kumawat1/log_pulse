import asyncio
import ssl
import os
import sys
import random
from protocol import async_decode_message, async_send_message

HOST = os.environ.get("SERVER_HOST", "127.0.0.1")
PORT = int(os.environ.get("SERVER_PORT", 5000))
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

SERVICES = ["billing-service", "auth-service", "notification-service"]

LOG_MESSAGES = {
    "billing-service": [
        ("Payment processed successfully", "INFO"),
        ("Invoice generated for order #{}".format, "INFO"),
        ("Payment gateway timeout", "ERROR"),
        ("Retry attempt for failed transaction", "WARN"),
        ("Refund initiated for order #{}".format, "INFO"),
        ("Database connection pool exhausted", "ERROR"),
        ("Slow query detected: 3.2s", "WARN"),
        ("Monthly billing cycle started", "INFO"),
        ("Credit card validation failed", "ERROR"),
        ("Rate limit approaching for payment API", "WARN"),
    ],
    "auth-service": [
        ("User login successful: user_{}".format, "INFO"),
        ("Failed login attempt from IP 192.168.1.{}".format, "WARN"),
        ("JWT token expired for session #{}".format, "INFO"),
        ("Brute force detection triggered", "ERROR"),
        ("Password reset requested for user_{}".format, "INFO"),
        ("OAuth callback failed: invalid state", "ERROR"),
        ("Session invalidated due to inactivity", "INFO"),
        ("2FA verification sent to user_{}".format, "INFO"),
        ("Account locked after 5 failed attempts", "ERROR"),
        ("New API key generated for client_{}".format, "INFO"),
    ],
    "notification-service": [
        ("Email sent to user_{}@example.com".format, "INFO"),
        ("SMS delivery failed: invalid number", "ERROR"),
        ("Push notification queued for batch #{}".format, "INFO"),
        ("Email template rendering error", "ERROR"),
        ("Notification rate limit hit for user_{}".format, "WARN"),
        ("Webhook delivery timeout to endpoint", "WARN"),
        ("Bulk email job started: {} recipients".format, "INFO"),
        ("SMS provider API returned 503", "ERROR"),
        ("Notification preference updated for user_{}".format, "INFO"),
        ("Dead letter queue size exceeds threshold", "WARN"),
    ],
}


def generate_log(service):
    messages = LOG_MESSAGES.get(service, [("Generic log event", "INFO")])
    msg_template, level = random.choice(messages)
    if callable(msg_template):
        message = msg_template(random.randint(1000, 9999))
    else:
        message = msg_template
    return message, level


async def heartbeat(writer):
    while True:
        try:
            await async_send_message(writer, {"type": "heartbeat"})
        except Exception:
            break
        await asyncio.sleep(5)


async def start_producer():
    service = sys.argv[1] if len(sys.argv) > 1 else None

    if service and service in SERVICES:
        await run_single_producer(service)
    else:
        # Run all services concurrently
        print(f"[STARTING] Launching producers for: {', '.join(SERVICES)}")
        tasks = [run_single_producer(s) for s in SERVICES]
        await asyncio.gather(*tasks)


async def run_single_producer(service):
    ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS)
    ssl_ctx.check_hostname = True
    ssl_ctx.verify_mode = ssl.CERT_REQUIRED
    ssl_ctx.load_verify_locations(os.path.join(BASE_DIR, "server.crt"))

    reader, writer = await asyncio.open_connection(
        HOST, PORT, ssl=ssl_ctx, server_hostname="localhost"
    )

    # Authenticate
    await async_send_message(writer, {"type": "auth", "token": "producer-token"})
    response = await async_decode_message(reader)
    if response.get("type") != "auth_success":
        print(f"[AUTH FAILED] {service}: Server rejected the token")
        writer.close()
        return
    print(f"[AUTH] {service}: Authenticated successfully")

    # Register
    await async_send_message(writer, {
        "type": "register",
        "client_type": "producer",
        "service": service,
    })

    # Start heartbeat
    hb_task = asyncio.create_task(heartbeat(writer))

    # Send logs
    interval = random.uniform(1.5, 3.0)
    print(f"[PRODUCING] Sending logs for '{service}' every {interval:.1f}s...")
    while True:
        message, level = generate_log(service)
        log = {
            "type": "log",
            "service": service,
            "message": message,
            "level": level,
        }
        try:
            await async_send_message(writer, log)
        except Exception:
            print(f"[DISCONNECTED] {service}: Lost connection to server")
            break

        await asyncio.sleep(interval)

    hb_task.cancel()
    writer.close()


if __name__ == "__main__":
    try:
        asyncio.run(start_producer())
    except KeyboardInterrupt:
        print("Producer stopped.")
