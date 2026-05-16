import asyncio
import ssl
import os
from protocol import async_decode_message, async_send_message

HOST = os.environ.get("SERVER_HOST", "127.0.0.1")
PORT = int(os.environ.get("SERVER_PORT", 5000))
BASE_DIR = os.path.dirname(os.path.abspath(__file__))


async def heartbeat(writer):
    while True:
        try:
            await async_send_message(writer, {"type": "heartbeat"})
        except Exception:
            break
        await asyncio.sleep(5)


async def start_consumer():
    ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS)
    ssl_ctx.check_hostname = True
    ssl_ctx.verify_mode = ssl.CERT_REQUIRED
    ssl_ctx.load_verify_locations(os.path.join(BASE_DIR, "server.crt"))

    reader, writer = await asyncio.open_connection(
        HOST, PORT, ssl=ssl_ctx, server_hostname="localhost"
    )

    # Authenticate
    await async_send_message(writer, {"type": "auth", "token": "consumer-token"})
    response = await async_decode_message(reader)
    if response.get("type") != "auth_success":
        print("[AUTH FAILED] Server rejected the token")
        writer.close()
        return
    print("[AUTH] Authenticated successfully")

    # Register
    await async_send_message(writer, {
        "type": "register",
        "client_type": "consumer",
        "subscriptions": ["billing-service", "auth-service", "notification-service"],
    })

    # Start heartbeat
    hb_task = asyncio.create_task(heartbeat(writer))

    # Receive logs
    print("[CONSUMING] Waiting for logs...")
    while True:
        message = await async_decode_message(reader)
        if not message:
            print("[DISCONNECTED] Server closed connection")
            break

        if message.get("type") == "heartbeat_ack":
            continue

        print(f"LOG: {message}")

    hb_task.cancel()
    writer.close()


if __name__ == "__main__":
    try:
        asyncio.run(start_consumer())
    except KeyboardInterrupt:
        print("Consumer stopped.")
