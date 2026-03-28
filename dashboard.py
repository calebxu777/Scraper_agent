import json
import os
import sqlite3

import pandas as pd
import streamlit as st

st.set_page_config(page_title="Safco Scraper Dashboard", layout="wide")


def get_connection():
    return sqlite3.connect(os.getenv("CRAWL_DB_PATH", "crawl_state.db"))


@st.cache_data(ttl=5)
def load_queue_rows(limit: int = 200) -> pd.DataFrame:
    conn = get_connection()
    query = """
        SELECT url, page_type, status, retry_count, last_updated, detail, last_error
        FROM pages_queue
        ORDER BY last_updated DESC
        LIMIT ?
    """
    df = pd.read_sql_query(query, conn, params=(limit,))
    conn.close()
    return df


@st.cache_data(ttl=5)
def load_products(limit: int = 200) -> pd.DataFrame:
    conn = get_connection()
    query = """
        SELECT source_url, product_data, extracted_at
        FROM products
        ORDER BY extracted_at DESC
        LIMIT ?
    """
    df = pd.read_sql_query(query, conn, params=(limit,))
    conn.close()
    return df


def get_dashboard_counts():
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM products")
    product_count = c.fetchone()[0]
    
    c.execute("SELECT status, COUNT(*) as count FROM pages_queue GROUP BY status")
    counts = {r[0]: r[1] for r in c.fetchall()}
    conn.close()
    return product_count, counts


def parse_product_rows(products_df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, row in products_df.iterrows():
        payload = json.loads(row["product_data"])
        rows.append(
            {
                "source_url": row["source_url"],
                "extracted_at": row["extracted_at"],
                "product_name": payload.get("product_name"),
                "brand": payload.get("brand"),
                "variation_count": len(payload.get("variations", [])),
                "image_count": len(payload.get("image_urls", [])),
            }
        )
    return pd.DataFrame(rows)


def render_metric_cards():
    product_count, counts = get_dashboard_counts()
    total_urls = sum(counts.values())
    completed = counts.get("COMPLETED", 0)
    failed = counts.get("FAILED", 0)
    skipped = counts.get("SKIPPED", 0)
    pending = counts.get("PENDING", 0)
    processing = counts.get("PROCESSING", 0)
    done = completed + failed + skipped
    percent_complete = (done / total_urls * 100) if total_urls else 0.0

    metric_columns = st.columns(6)
    metric_columns[0].metric("Products", product_count)
    metric_columns[1].metric("Total URLs", total_urls)
    metric_columns[2].metric("Completed %", f"{percent_complete:.1f}%")
    metric_columns[3].metric("Pending", pending)
    metric_columns[4].metric("Processing", processing)
    metric_columns[5].metric("Failed", failed)

    st.progress(min(max(percent_complete / 100.0, 0.0), 1.0), text=f"Crawl completion: {percent_complete:.1f}%")


def main():
    st.title("Safco Scraper Dashboard")
    st.caption(f"DB: {os.getenv('CRAWL_DB_PATH', 'crawl_state.db')}")

    with st.sidebar:
        st.header("Controls")
        queue_limit = st.slider("Queue rows", min_value=25, max_value=500, value=200, step=25)
        product_limit = st.slider("Product rows", min_value=25, max_value=500, value=200, step=25)
        st.button("Refresh", use_container_width=True)

    render_metric_cards()

    queue_df = load_queue_rows(queue_limit)
    products_raw_df = load_products(product_limit)
    products_df = parse_product_rows(products_raw_df) if not products_raw_df.empty else pd.DataFrame()

    col_left, col_right = st.columns([1.1, 1.4])

    with col_left:
        st.subheader("Queue Status")
        if queue_df.empty:
            st.info("No queue rows yet.")
        else:
            status_counts = queue_df["status"].value_counts().rename_axis("status").reset_index(name="count")
            st.bar_chart(status_counts.set_index("status"))

        st.subheader("Recent Queue Rows")
        if queue_df.empty:
            st.info("No queue activity yet.")
        else:
            st.dataframe(queue_df, use_container_width=True, hide_index=True)

        st.subheader("Recent Failures / Skips")
        if queue_df.empty:
            st.info("No failures or skips yet.")
        else:
            flagged_df = queue_df[queue_df["status"].isin(["FAILED", "SKIPPED"])]
            if flagged_df.empty:
                st.success("No failed or skipped rows in the recent window.")
            else:
                st.dataframe(flagged_df, use_container_width=True, hide_index=True)

    with col_right:
        st.subheader("Extracted Products")
        if products_df.empty:
            st.info("No extracted products yet.")
        else:
            st.dataframe(products_df, use_container_width=True, hide_index=True)

            options = products_df["source_url"].tolist()
            selected_url = st.selectbox("Inspect product", options=options)
            selected_row = products_raw_df[products_raw_df["source_url"] == selected_url].iloc[0]
            payload = json.loads(selected_row["product_data"])

            st.markdown(f"**Product:** {payload.get('product_name', 'Unknown')}")
            st.markdown(f"**Brand:** {payload.get('brand') or 'N/A'}")
            st.markdown(f"**Source URL:** {selected_url}")
            st.markdown(f"**Extracted At:** {selected_row['extracted_at']}")

            summary_col_1, summary_col_2, summary_col_3 = st.columns(3)
            summary_col_1.metric("Variations", len(payload.get("variations", [])))
            summary_col_2.metric("Images", len(payload.get("image_urls", [])))
            summary_col_3.metric("Alternatives", len(payload.get("alternative_products", [])))

            st.subheader("Product JSON")
            st.json(payload)


if __name__ == "__main__":
    main()
