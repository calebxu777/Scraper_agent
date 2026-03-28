import sqlite3
import os
from datetime import datetime, timezone

DEFAULT_DB_PATH = os.path.join(os.path.dirname(__file__), "crawl_state.db")

def get_connection():
    return sqlite3.connect(os.getenv("CRAWL_DB_PATH", DEFAULT_DB_PATH))


def current_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _ensure_column(cursor: sqlite3.Cursor, table_name: str, column_name: str, column_sql: str):
    cursor.execute(f"PRAGMA table_info({table_name})")
    existing_columns = {row[1] for row in cursor.fetchall()}
    if column_name not in existing_columns:
        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_sql}")

def init_db():
    conn = get_connection()
    c = conn.cursor()
    # Queue for URLs
    c.execute('''
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
    c.execute('''
        CREATE TABLE IF NOT EXISTS products (
            source_url TEXT PRIMARY KEY,
            product_data TEXT, -- JSON payload of the DentalProduct
            extracted_at TIMESTAMP
        )
    ''')
    _ensure_column(c, "pages_queue", "detail", "TEXT")
    _ensure_column(c, "pages_queue", "last_error", "TEXT")
    conn.commit()
    conn.close()

def add_to_queue(url: str, page_type: str = 'UNKNOWN'):
    conn = get_connection()
    c = conn.cursor()
    # Idempotent insert: only add if it doesn't exist
    c.execute('''
        INSERT OR IGNORE INTO pages_queue (url, page_type, status, last_updated)
        VALUES (?, ?, 'PENDING', ?)
    ''', (url, page_type, current_timestamp()))
    if page_type != 'UNKNOWN':
        c.execute('''
            UPDATE pages_queue
            SET page_type = ?, last_updated = ?
            WHERE url = ? AND (page_type IS NULL OR page_type = 'UNKNOWN')
        ''', (page_type, current_timestamp(), url))
    conn.commit()
    conn.close()

def get_next_pending(limit: int = 1):
    conn = get_connection()
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute('''
        SELECT url, page_type, retry_count
        FROM pages_queue 
        WHERE status = 'PENDING' 
        ORDER BY retry_count ASC, last_updated ASC
        LIMIT ?
    ''', (limit,))
    rows = c.fetchall()
    
    # Mark as processing
    if rows:
        urls = [r['url'] for r in rows]
        placeholders = ','.join(['?'] * len(urls))
        # use execute with tuple properly
        c.execute(f'''
            UPDATE pages_queue SET status = 'PROCESSING', last_updated = ? WHERE url IN ({placeholders})
        ''', (current_timestamp(), *urls))
        conn.commit()
        
    conn.close()
    return [dict(r) for r in rows]

def update_status(url: str, status: str, increment_retry: bool = False, detail: str | None = None, error: str | None = None):
    conn = get_connection()
    c = conn.cursor()
    if increment_retry:
        c.execute('''
            UPDATE pages_queue
            SET status = ?, retry_count = retry_count + 1, last_updated = ?, detail = ?, last_error = ?
            WHERE url = ?
        ''', (status, current_timestamp(), detail, error, url))
    else:
        c.execute('''
            UPDATE pages_queue
            SET status = ?, last_updated = ?, detail = ?, last_error = ?
            WHERE url = ?
        ''', (status, current_timestamp(), detail, error, url))
    conn.commit()
    conn.close()

def save_product(url: str, product_data_json: str):
    conn = get_connection()
    c = conn.cursor()
    c.execute('''
        INSERT OR REPLACE INTO products (source_url, product_data, extracted_at)
        VALUES (?, ?, ?)
    ''', (url, product_data_json, current_timestamp()))
    conn.commit()
    # Also mark queue as completed if it exists
    c.execute('''
        UPDATE pages_queue
        SET status = 'COMPLETED', last_updated = ?, detail = ?, last_error = NULL
        WHERE url = ?
    ''', (current_timestamp(), "saved product record", url))
    conn.commit()
    conn.close()


def requeue_processing_tasks() -> int:
    conn = get_connection()
    c = conn.cursor()
    c.execute('''
        UPDATE pages_queue
        SET status = 'PENDING', last_updated = ?, detail = ?
        WHERE status = 'PROCESSING'
    ''', (current_timestamp(), "re-queued after restart"))
    recovered = c.rowcount
    conn.commit()
    conn.close()
    return recovered


def get_queue_counts() -> dict[str, int]:
    conn = get_connection()
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute('''
        SELECT status, COUNT(*) AS count
        FROM pages_queue
        GROUP BY status
    ''')
    rows = c.fetchall()
    conn.close()
    return {row["status"]: row["count"] for row in rows}


def count_products() -> int:
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM products")
    total = c.fetchone()[0]
    conn.close()
    return total


def get_products(limit: int | None = None) -> list[dict]:
    conn = get_connection()
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    query = "SELECT source_url, product_data, extracted_at FROM products ORDER BY extracted_at ASC"
    params: tuple = ()
    if limit is not None:
        query += " LIMIT ?"
        params = (limit,)
    c.execute(query, params)
    rows = [dict(row) for row in c.fetchall()]
    conn.close()
    return rows

if __name__ == "__main__":
    init_db()
    print("Database initialized.")
