"""SQLite database module for storing and querying Google reviews."""

import hashlib
import sqlite3
from datetime import date, datetime
from pathlib import Path
from typing import Optional


DB_PATH = Path(__file__).parent.parent / "data" / "reviews.db"


def _get_connection(db_path: Path = DB_PATH) -> sqlite3.Connection:
    """Get a database connection, creating the data directory if needed."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(db_path: Path = DB_PATH) -> None:
    """Create tables if they don't exist."""
    conn = _get_connection(db_path)
    try:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS stores (
                place_id TEXT PRIMARY KEY,
                brand TEXT NOT NULL,
                store_name TEXT NOT NULL,
                address TEXT,
                current_rating REAL
            );

            CREATE TABLE IF NOT EXISTS reviews (
                review_id TEXT PRIMARY KEY,
                place_id TEXT NOT NULL,
                rating INTEGER NOT NULL,
                review_date DATE NOT NULL,
                reviewer_name TEXT,
                review_text TEXT,
                owner_response TEXT,
                fetched_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (place_id) REFERENCES stores(place_id)
            );

            CREATE INDEX IF NOT EXISTS idx_reviews_place_date
                ON reviews(place_id, review_date);

            CREATE INDEX IF NOT EXISTS idx_reviews_date
                ON reviews(review_date);

            CREATE INDEX IF NOT EXISTS idx_reviews_rating
                ON reviews(rating);
        """)
        conn.commit()
    finally:
        conn.close()


def generate_review_id(place_id: str, reviewer_name: str, review_date: str, review_text: str) -> str:
    """Generate a deterministic review ID for deduplication."""
    raw = f"{place_id}|{reviewer_name}|{review_date}|{(review_text or '')[:100]}"
    return hashlib.sha256(raw.encode()).hexdigest()[:32]


def upsert_stores(stores: list[dict], db_path: Path = DB_PATH) -> int:
    """Insert or update store records. Returns count of upserted stores."""
    conn = _get_connection(db_path)
    try:
        count = 0
        for store in stores:
            conn.execute("""
                INSERT INTO stores (place_id, brand, store_name, address)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(place_id) DO UPDATE SET
                    brand = excluded.brand,
                    store_name = excluded.store_name,
                    address = excluded.address
            """, (store["place_id"], store["brand"], store["store"], store.get("address", "")))
            count += 1
        conn.commit()
        return count
    finally:
        conn.close()


def upsert_reviews(reviews: list[dict], db_path: Path = DB_PATH) -> tuple[int, int]:
    """Insert reviews with deduplication. Returns (inserted, skipped) counts."""
    conn = _get_connection(db_path)
    try:
        inserted = 0
        skipped = 0
        for review in reviews:
            review_id = generate_review_id(
                review["place_id"],
                review.get("reviewer_name", ""),
                review.get("review_date", ""),
                review.get("review_text", "")
            )
            try:
                conn.execute("""
                    INSERT INTO reviews (review_id, place_id, rating, review_date,
                                        reviewer_name, review_text, owner_response)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    review_id,
                    review["place_id"],
                    review["rating"],
                    review["review_date"],
                    review.get("reviewer_name", ""),
                    review.get("review_text", ""),
                    review.get("owner_response")
                ))
                inserted += 1
            except sqlite3.IntegrityError:
                # Duplicate — update owner_response if it changed
                conn.execute("""
                    UPDATE reviews SET owner_response = ?
                    WHERE review_id = ? AND (owner_response IS NULL OR owner_response != ?)
                """, (review.get("owner_response"), review_id, review.get("owner_response")))
                skipped += 1
        conn.commit()
        return inserted, skipped
    finally:
        conn.close()


def update_store_rating(place_id: str, rating: float, db_path: Path = DB_PATH) -> None:
    """Update a store's current Google rating."""
    conn = _get_connection(db_path)
    try:
        conn.execute("UPDATE stores SET current_rating = ? WHERE place_id = ?", (rating, place_id))
        conn.commit()
    finally:
        conn.close()


def get_last_sync_date(place_id: str, db_path: Path = DB_PATH) -> Optional[date]:
    """Get the most recent review date for a store (for incremental fetch cutoff)."""
    conn = _get_connection(db_path)
    try:
        row = conn.execute(
            "SELECT MAX(review_date) as last_date FROM reviews WHERE place_id = ?",
            (place_id,)
        ).fetchone()
        if row and row["last_date"]:
            return datetime.strptime(row["last_date"], "%Y-%m-%d").date()
        return None
    finally:
        conn.close()


def get_reviews(
    place_id: Optional[str] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    min_rating: Optional[int] = None,
    max_rating: Optional[int] = None,
    db_path: Path = DB_PATH
) -> list[dict]:
    """Query reviews with optional filters. Returns list of review dicts."""
    conn = _get_connection(db_path)
    try:
        query = """
            SELECT r.*, s.brand, s.store_name
            FROM reviews r
            JOIN stores s ON r.place_id = s.place_id
            WHERE 1=1
        """
        params = []

        if place_id:
            query += " AND r.place_id = ?"
            params.append(place_id)
        if start_date:
            query += " AND r.review_date >= ?"
            params.append(start_date.isoformat())
        if end_date:
            query += " AND r.review_date <= ?"
            params.append(end_date.isoformat())
        if min_rating:
            query += " AND r.rating >= ?"
            params.append(min_rating)
        if max_rating:
            query += " AND r.rating <= ?"
            params.append(max_rating)

        query += " ORDER BY r.review_date DESC"
        rows = conn.execute(query, params).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def get_all_stores(db_path: Path = DB_PATH) -> list[dict]:
    """Get all registered stores."""
    conn = _get_connection(db_path)
    try:
        rows = conn.execute("SELECT * FROM stores ORDER BY brand, store_name").fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def get_review_count(place_id: Optional[str] = None, db_path: Path = DB_PATH) -> int:
    """Get total review count, optionally filtered by store."""
    conn = _get_connection(db_path)
    try:
        if place_id:
            row = conn.execute("SELECT COUNT(*) as cnt FROM reviews WHERE place_id = ?", (place_id,)).fetchone()
        else:
            row = conn.execute("SELECT COUNT(*) as cnt FROM reviews").fetchone()
        return row["cnt"]
    finally:
        conn.close()
