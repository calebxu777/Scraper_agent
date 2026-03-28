import asyncio, aiosqlite, os
os.environ["CRAWL_DB_PATH"] = "artifacts_api/scrape_10.db"

async def main():
    async with aiosqlite.connect(os.environ["CRAWL_DB_PATH"]) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT url, status, detail FROM pages_queue ORDER BY status, url") as cur:
            rows = await cur.fetchall()
        for r in rows:
            print(f"{r['status']:12} {r['url']}")

asyncio.run(main())
