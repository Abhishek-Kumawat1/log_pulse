import asyncio
import json
import struct
import time
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from protocol import encode_message, decode_message


class TestProtocol(unittest.TestCase):
    """Tests for the wire protocol (encode/decode)."""

    def test_encode_message_returns_bytes(self):
        result = encode_message({"type": "heartbeat"})
        self.assertIsInstance(result, bytes)

    def test_encode_message_has_4_byte_length_prefix(self):
        msg = {"type": "test"}
        encoded = encode_message(msg)
        length = struct.unpack("!I", encoded[:4])[0]
        self.assertEqual(length, len(encoded) - 4)

    def test_encode_decode_roundtrip(self):
        """Encode a message, then decode it from a fake socket-like object."""
        original = {"type": "log", "service": "auth", "message": "hello", "level": "INFO"}
        encoded = encode_message(original)

        # Create a fake socket that returns the encoded bytes
        class FakeSocket:
            def __init__(self, data):
                self._data = data
                self._pos = 0

            def recv(self, n):
                chunk = self._data[self._pos:self._pos + n]
                self._pos += n
                return chunk

        sock = FakeSocket(encoded)
        decoded = decode_message(sock)
        self.assertEqual(decoded, original)

    def test_decode_empty_socket_returns_none(self):
        class EmptySocket:
            def recv(self, n):
                return b''

        result = decode_message(EmptySocket())
        self.assertIsNone(result)

    def test_encode_preserves_unicode(self):
        msg = {"type": "log", "message": "Error: \u2603 snowman"}
        encoded = encode_message(msg)
        body = encoded[4:]
        decoded = json.loads(body.decode("utf-8"))
        self.assertEqual(decoded["message"], "Error: \u2603 snowman")

    def test_encode_multiple_messages_are_independent(self):
        msg1 = encode_message({"id": 1})
        msg2 = encode_message({"id": 2})
        # Each message should decode independently
        len1 = struct.unpack("!I", msg1[:4])[0]
        len2 = struct.unpack("!I", msg2[:4])[0]
        self.assertEqual(json.loads(msg1[4:4+len1])["id"], 1)
        self.assertEqual(json.loads(msg2[4:4+len2])["id"], 2)


class TestAsyncProtocol(unittest.TestCase):
    """Tests for the async protocol functions."""

    def _run(self, coro):
        return asyncio.get_event_loop().run_until_complete(coro)

    def test_async_roundtrip(self):
        from protocol import async_decode_message, async_send_message

        async def roundtrip():
            original = {"type": "response", "data": "ok"}

            # Use asyncio streams over a socketpair
            import socket as sock_mod
            rsock, wsock = sock_mod.socketpair()
            reader, _ = await asyncio.open_connection(sock=rsock)
            _, writer = await asyncio.open_connection(sock=wsock)

            await async_send_message(writer, original)
            decoded = await async_decode_message(reader)

            writer.close()
            await asyncio.sleep(0.1)  # let transport close cleanly
            return decoded, original

        decoded, original = self._run(roundtrip())
        self.assertEqual(decoded, original)


class TestRateLimiter(unittest.TestCase):
    """Tests for the rate limiting logic."""

    def test_allows_under_limit(self):
        from server import is_rate_limited, rate_buckets, RATE_LIMIT
        test_key = "test_writer_allow"
        rate_buckets.pop(test_key, None)

        for _ in range(RATE_LIMIT - 1):
            self.assertFalse(is_rate_limited(test_key))

        rate_buckets.pop(test_key, None)

    def test_blocks_over_limit(self):
        from server import is_rate_limited, rate_buckets, RATE_LIMIT
        test_key = "test_writer_block"
        rate_buckets.pop(test_key, None)

        for _ in range(RATE_LIMIT):
            is_rate_limited(test_key)

        self.assertTrue(is_rate_limited(test_key))
        rate_buckets.pop(test_key, None)


class TestDatabase(unittest.TestCase):
    """Tests for SQLite persistence."""

    DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "test_logs.db")

    def setUp(self):
        import database
        self._original_path = database.DB_PATH
        database.DB_PATH = self.DB_PATH
        asyncio.get_event_loop().run_until_complete(database.init_db())

    def tearDown(self):
        import database
        database.DB_PATH = self._original_path
        if os.path.exists(self.DB_PATH):
            os.remove(self.DB_PATH)

    def _run(self, coro):
        return asyncio.get_event_loop().run_until_complete(coro)

    def test_insert_and_retrieve_log(self):
        from database import insert_log, get_logs

        self._run(insert_log("auth-service", "User logged in", "INFO"))
        logs = self._run(get_logs())

        self.assertEqual(len(logs), 1)
        self.assertEqual(logs[0]["service"], "auth-service")
        self.assertEqual(logs[0]["message"], "User logged in")
        self.assertEqual(logs[0]["level"], "INFO")

    def test_filter_by_level(self):
        from database import insert_log, get_logs

        self._run(insert_log("svc", "msg1", "INFO"))
        self._run(insert_log("svc", "msg2", "ERROR"))

        errors = self._run(get_logs(level="ERROR"))
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0]["level"], "ERROR")

    def test_filter_by_service(self):
        from database import insert_log, get_logs

        self._run(insert_log("billing", "b1", "INFO"))
        self._run(insert_log("auth", "a1", "INFO"))

        billing = self._run(get_logs(service="billing"))
        self.assertEqual(len(billing), 1)
        self.assertEqual(billing[0]["service"], "billing")

    def test_stats(self):
        from database import insert_log, get_stats

        self._run(insert_log("svc", "m1", "INFO"))
        self._run(insert_log("svc", "m2", "ERROR"))
        self._run(insert_log("svc", "m3", "INFO"))

        stats = self._run(get_stats())
        self.assertEqual(stats["total_logs"], 3)
        self.assertEqual(stats["by_level"]["INFO"], 2)
        self.assertEqual(stats["by_level"]["ERROR"], 1)

    def test_limit(self):
        from database import insert_log, get_logs

        for i in range(10):
            self._run(insert_log("svc", f"msg{i}", "INFO"))

        logs = self._run(get_logs(limit=3))
        self.assertEqual(len(logs), 3)


class TestProducerMessages(unittest.TestCase):
    """Tests that the producer generates varied messages."""

    def test_generate_log_returns_tuple(self):
        from producer import generate_log
        msg, level = generate_log("billing-service")
        self.assertIsInstance(msg, str)
        self.assertIn(level, ["INFO", "ERROR", "WARN"])

    def test_generate_log_has_variety(self):
        from producer import generate_log
        messages = set()
        for _ in range(50):
            msg, _ = generate_log("billing-service")
            messages.add(msg)
        # Should generate more than 1 unique message
        self.assertGreater(len(messages), 1)


if __name__ == "__main__":
    unittest.main()
