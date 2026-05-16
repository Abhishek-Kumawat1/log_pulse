import json
import struct

# --- Synchronous (kept for backward compat) ---

def encode_message(message_dict):
    message_str = json.dumps(message_dict).encode("utf-8")
    message_length = struct.pack("!I", len(message_str))
    return message_length + message_str

def decode_message(sock):
    raw_length = recvall(sock, 4)
    if not raw_length:
        return None
    length = struct.unpack("!I", raw_length)[0]
    message = recvall(sock, length)
    if not message:
        return None
    message_dict = json.loads(message.decode("utf-8"))
    return message_dict

def recvall(sock, n):
    data = b''
    while len(data)<n:
        packet = sock.recv(n - len(data))

        if not packet:
            return None

        data += packet
    
    return data

# --- Async (for asyncio StreamReader/StreamWriter) ---

async def async_decode_message(reader):
    raw_length = await async_recvall(reader, 4)
    if not raw_length:
        return None
    length = struct.unpack("!I", raw_length)[0]
    message = await async_recvall(reader, length)
    if not message:
        return None
    return json.loads(message.decode("utf-8"))

async def async_send_message(writer, message_dict):
    data = encode_message(message_dict)
    writer.write(data)
    await writer.drain()

async def async_recvall(reader, n):
    data = b''
    while len(data) < n:
        packet = await reader.read(n - len(data))
        if not packet:
            return None
        data += packet
    return data
