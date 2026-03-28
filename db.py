import aiosqlite
import json
import os
from datetime import datetime, timezone

DEFAULT_DB_PATH = os.path.join(os.path.dirname(__file__), "crawl_state.db")

def get_connection():
    return aiosqlite.connect(os.getenv("CRAWL_DB_PATH", DEFAULT_DB_PATH))


def current_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


async def _ensure_column(conn, table_name: str, column_name: str, column_sql: str):
    async with conn.execute(f"PRAGMA table_info({table_name})") as cursor:
        rows = await cursor.fetchall()
    existing_columns = {row[1] for row in rows}
    if column_name not in existing_columns:
        await conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_sql}")

async def init_db():
    async with get_connection() as conn:
        # Queue for URLs
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS pages_queue (
                url TEXT PRIMARY KEY,
                page_type TEXT, -- 'CATEGORY', 'PRODUCT', 'UNKNOWN'
                status TEXT DEFAULT 'PENDING', -- 'PENDING', 'PROCESSING', 'COMPLETED', 'FAILED'
                retry_count INTEGER DEFAULT 0,
                last_updated TIMESTAMP,
                detail TEXT,
                last_error TEXT
            )
        ''')
        
        # Storage for extracted products
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS products (
                source_url TEXT PRIMARY KEY,
                product_name TEXT,
                brand TEXT,
                category_hierarchy_json TEXT,
                description TEXT,
                specifications_json TEXT,
                image_urls_json TEXT,
                alternative_products_json TEXT,
                product_data TEXT, -- JSON payload of the DentalProduct
                extracted_at TIMESTAMP,
                extraction_method TEXT,
                extraction_latency REAL,
                quality_status TEXT,
                quality_notes TEXT,
                record_status TEXT DEFAULT 'complete',
                queue_status TEXT DEFAULT 'COMPLETED',
                detail TEXT,
                last_error TEXT
            )
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS product_variations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_url TEXT NOT NULL,
                sku TEXT,
                size TEXT,
                package_count TEXT,
                price REAL,
                availability INTEGER
            )
        ''')
        await _ensure_column(conn, "pages_queue", "detail", "TEXT")
        await _ensure_column(conn, "pages_queue", "last_error", "TEXT")
        await _ensure_column(conn, "products", "product_name", "TEXT")
        await _ensure_column(conn, "products", "brand", "TEXT")
        await _ensure_column(conn, "products", "category_hierarchy_json", "TEXT")
        await _ensure_column(conn, "products", "description", "TEXT")
        await _ensure_column(conn, "products", "specifications_json", "TEXT")
        await _ensure_column(conn, "products", "image_urls_json", "TEXT")
        await _ensure_column(conn, "products", "alternative_products_json", "TEXT")
        await _ensure_column(conn, "products", "extraction_method", "TEXT")
        await _ensure_column(conn, "products", "extraction_latency", "REAL")
        await _ensure_column(conn, "products", "quality_status", "TEXT")
        await _ensure_column(conn, "products", "quality_notes", "TEXT")
        await _ensure_column(conn, "products", "record_status", "TEXT DEFAULT 'complete'")
        await _ensure_column(conn, "products", "queue_status", "TEXT DEFAULT 'COMPLETED'")
        await _ensure_column(conn, "products", "detail", "TEXT")
        await _ensure_column(conn, "products", "last_error", "TEXT")
        await conn.commit()

async def add_to_queue(url: str, page_type: str = 'UNKNOWN'):
    async with get_connection() as conn:
        # Idempotent insert: only add if it doesn't exist
        await conn.execute('''
            INSERT OR IGNORE INTO pages_queue (url, page_type, status, last_updated)
            VALUES (?, ?, 'PENDING', ?)
        ''', (url, page_type, current_timestamp()))
        if page_type != 'UNKNOWN':
            await conn.execute('''
                UPDATE pages_queue
                SET page_type = ?, last_updated = ?
                WHERE url = ? AND (page_type IS NULL OR page_type = 'UNKNOWN')
            ''', (page_type, current_timestamp(), url))
        await conn.commit()

async def get_next_pending(limit: int = 1):
    async with get_connection() as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute('''
            SELECT url, page_type, retry_count
            FROM pages_queue 
            WHERE status = 'PENDING' 
            ORDER BY retry_count ASC, last_updated ASC
            LIMIT ?
        ''', (limit,)) as cursor:
            rows = await cursor.fetchall()
        
        # Mark as processing
        if rows:
            urls = [r['url'] for r in rows]
            placeholders = ','.join(['?'] * len(urls))
            await conn.execute(f'''
                UPDATE pages_queue SET status = 'PROCESSING', last_updated = ? WHERE url IN ({placeholders})
            ''', (current_timestamp(), *urls))
            await conn.commit()
            
        return [dict(r) for r in rows]

async def update_status(url: str, status: str, increment_retry: bool = False, detail: str | None = None, error: str | None = None):
    async with get_connection() as conn:
        if increment_retry:
            await conn.execute('''
                UPDATE pages_queue
                SET status = ?, retry_count = retry_count + 1, last_updated = ?, detail = ?, last_error = ?
                WHERE url = ?
            ''', (status, current_timestamp(), detail, error, url))
        else:
            await conn.execute('''
                UPDATE pages_queue
                SET status = ?, last_updated = ?, detail = ?, last_error = ?
                WHERE url = ?
            ''', (status, current_timestamp(), detail, error, url))
        await conn.commit()

async def save_product(
    url: str,
    product_data_json: str,
    extraction_method: str | None = None,
    extraction_latency: float | None = None,
    quality_status: str | None = None,
    quality_notes_json: str | None = None,
    record_status: str = "complete",
    queue_status: str = "COMPLETED",
    detail: str | None = None,
    error: str | None = None,
):
    async with get_connection() as conn:
        payload = json.loads(product_data_json)
        product_name = payload.get("product_name")
        brand = payload.get("brand")
        category_hierarchy_json = json.dumps(payload.get("category_hierarchy", []), ensure_ascii=False)
        description_value = payload.get("description")
        specifications_json = json.dumps(payload.get("specifications", {}), ensure_ascii=False)
        image_urls_json = json.dumps(payload.get("image_urls", []), ensure_ascii=False)
        alternative_products_json = json.dumps(payload.get("alternative_products", []), ensure_ascii=False)

        await conn.execute('''
            INSERT OR REPLACE INTO products (
                source_url, product_name, brand, category_hierarchy_json, description,
                specifications_json, image_urls_json, alternative_products_json,
                product_data, extracted_at,
                extraction_method, extraction_latency, quality_status, quality_notes,
                record_status, queue_status, detail, last_error
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            url,
            product_name,
            brand,
            category_hierarchy_json,
            description_value,
            specifications_json,
            image_urls_json,
            alternative_products_json,
            product_data_json,
            current_timestamp(),
            extraction_method,
            extraction_latency,
            quality_status,
            quality_notes_json,
            record_status,
            queue_status,
            detail,
            error,
        ))

        await conn.execute("DELETE FROM product_variations WHERE source_url = ?", (url,))
        for variation in payload.get("variations", []) or []:
            availability = variation.get("availability")
            await conn.execute('''
                INSERT INTO product_variations (source_url, sku, size, package_count, price, availability)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                url,
                variation.get("sku"),
                variation.get("size"),
                variation.get("package_count"),
                variation.get("price"),
                None if availability is None else int(bool(availability)),
            ))

        # Keep queue status aligned with the saved record outcome.
        await conn.execute('''
            UPDATE pages_queue
            SET status = ?, last_updated = ?, detail = ?, last_error = ?
            WHERE url = ?
        ''', (queue_status, current_timestamp(), detail, error, url))
        await conn.commit()


async def requeue_processing_tasks() -> int:
    async with get_connection() as conn:
        cursor = await conn.execute('''
            UPDATE pages_queue
            SET status = 'PENDING', last_updated = ?, detail = ?
            WHERE status = 'PROCESSING'
        ''', (current_timestamp(), "re-queued after restart"))
        recovered = cursor.rowcount
        await conn.commit()
        return recovered


async def get_queue_counts() -> dict[str, int]:
    async with get_connection() as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute('''
            SELECT status, COUNT(*) AS count
            FROM pages_queue
            GROUP BY status
        ''') as cursor:
            rows = await cursor.fetchall()
        return {row["status"]: row["count"] for row in rows}


async def get_queue_rows(status: str | None = None, limit: int | None = None) -> list[dict]:
    async with get_connection() as conn:
        conn.row_factory = aiosqlite.Row
        query = '''
            SELECT url, page_type, status, retry_count, last_updated, detail, last_error
            FROM pages_queue
        '''
        params_list: list = []
        if status is not None:
            query += " WHERE status = ?"
            params_list.append(status)
        query += " ORDER BY last_updated ASC, url ASC"
        if limit is not None:
            query += " LIMIT ?"
            params_list.append(limit)
        async with conn.execute(query, tuple(params_list)) as cursor:
            rows = [dict(row) for row in await cursor.fetchall()]
        return rows


async def count_products(include_incomplete: bool = False) -> int:
    async with get_connection() as conn:
        query = "SELECT COUNT(*) FROM products"
        params: tuple = ()
        if not include_incomplete:
            query += " WHERE record_status = ?"
            params = ("complete",)
        async with conn.execute(query, params) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else 0


async def get_products(limit: int | None = None, include_incomplete: bool = False) -> list[dict]:
    async with get_connection() as conn:
        conn.row_factory = aiosqlite.Row
        query = '''
            SELECT
                source_url, product_data, extracted_at,
                extraction_method, extraction_latency, quality_status, quality_notes,
                record_status, queue_status, detail, last_error
            FROM products
        '''
        params_list: list = []
        if not include_incomplete:
            query += " WHERE record_status = ?"
            params_list.append("complete")
        query += " ORDER BY extracted_at ASC"
        if limit is not None:
            query += " LIMIT ?"
            params_list.append(limit)
        async with conn.execute(query, tuple(params_list)) as cursor:
            rows = [dict(row) for row in await cursor.fetchall()]
        return rows
