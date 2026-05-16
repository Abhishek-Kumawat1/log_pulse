import aiosqlite
import os
import time

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "logs.db")


async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                service TEXT NOT NULL,
                message TEXT NOT NULL,
                level TEXT NOT NULL,
                timestamp REAL NOT NULL
            )
        """)
        await db.commit()


async def insert_log(service, message, level):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO logs (service, message, level, timestamp) VALUES (?, ?, ?, ?)",
            (service, message, level, time.time())
        )
        await db.commit()


async def get_logs(service=None, level=None, limit=100):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        query = "SELECT * FROM logs WHERE 1=1"
        params = []

        if service:
            query += " AND service = ?"
            params.append(service)
        if level:
            query += " AND level = ?"
            params.append(level)

        query += " ORDER BY timestamp DESC LIMIT ?"
        params.append(limit)

        async with db.execute(query, params) as cursor:
            rows = await cursor.fetchall()
            return [
                {
                    "id": row["id"],
                    "service": row["service"],
                    "message": row["message"],
                    "level": row["level"],
                    "timestamp": row["timestamp"],
                }
                for row in rows
            ]


async def get_stats():
    async with aiosqlite.connect(DB_PATH) as db:
        stats = {}

        async with db.execute("SELECT COUNT(*) FROM logs") as cursor:
            stats["total_logs"] = (await cursor.fetchone())[0]

        async with db.execute(
            "SELECT level, COUNT(*) FROM logs GROUP BY level"
        ) as cursor:
            stats["by_level"] = {row[0]: row[1] for row in await cursor.fetchall()}

        async with db.execute(
            "SELECT service, COUNT(*) FROM logs GROUP BY service"
        ) as cursor:
            stats["by_service"] = {row[0]: row[1] for row in await cursor.fetchall()}

        return stats
