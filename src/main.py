"""CLI entrypoint for the GBP Reviews Automation Pipeline."""

import argparse
import json
import logging
import os
import sys
from datetime import date, timedelta
from pathlib import Path

from dotenv import load_dotenv

# Load .env before imports that need env vars
load_dotenv()

from . import database as db
from .outscraper_client import OutscraperClient
from .reports import (
    compute_monthly_report,
    compute_weekly_report,
    get_all_reviews_for_tab,
    get_needs_attention_reviews,
    pct_above_threshold,
)
from .sheets_writer import (
    build_store_row_map,
    get_sheet_client,
    open_spreadsheet,
    populate_all_reviews_tab,
    populate_needs_attention_tab,
    populate_weekly_report_tab,
    update_current_ratings,
    write_formulas,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("cannabis-reviews")

CONFIG_DIR = Path(__file__).parent.parent / "config"
STORES_FILE = CONFIG_DIR / "stores.json"


def load_stores() -> list[dict]:
    """Load store registry from config/stores.json."""
    with open(STORES_FILE, "r") as f:
        return json.load(f)


def cmd_init(args) -> None:
    """Initialize: create DB tables and load store registry."""
    logger.info("Initializing database...")
    db.init_db()

    stores = load_stores()
    count = db.upsert_stores(stores)
    logger.info(f"Loaded {count} stores into database")
    logger.info(f"Total reviews in DB: {db.get_review_count()}")


def cmd_initial_load(args) -> None:
    """One-time: fetch ALL historical reviews for all stores."""
    api_key = os.getenv("OUTSCRAPER_API_KEY")
    if not api_key:
        logger.error("OUTSCRAPER_API_KEY not set")
        sys.exit(1)

    # Initialize DB and stores first
    db.init_db()
    stores = load_stores()
    db.upsert_stores(stores)

    client = OutscraperClient(api_key)
    place_ids = [s["place_id"] for s in stores]

    logger.info(f"Starting initial load for {len(place_ids)} stores (this may take a while)...")

    # Fetch reviews from 2025-01-01 onward only
    cutoff = date(2025, 1, 1)
    results = client.fetch_reviews(place_ids, cutoff_date=cutoff, reviews_limit=0, batch_size=5)

    # Separate reviews from store ratings
    reviews = [r for r in results if r.get("_type") != "store_rating"]
    ratings = [r for r in results if r.get("_type") == "store_rating"]

    # Store reviews
    inserted, skipped = db.upsert_reviews(reviews)
    logger.info(f"Reviews: {inserted} inserted, {skipped} duplicates skipped")

    # Update store current ratings
    for r in ratings:
        db.update_store_rating(r["place_id"], r["current_rating"])
    logger.info(f"Updated current ratings for {len(ratings)} stores")

    logger.info(f"Total reviews now in DB: {db.get_review_count()}")


def cmd_sync(args) -> None:
    """Incremental sync: fetch only new reviews since last sync."""
    api_key = os.getenv("OUTSCRAPER_API_KEY")
    if not api_key:
        logger.error("OUTSCRAPER_API_KEY not set")
        sys.exit(1)

    db.init_db()
    stores = load_stores()
    db.upsert_stores(stores)

    client = OutscraperClient(api_key)
    total_inserted = 0
    total_skipped = 0

    for store in stores:
        pid = store["place_id"]
        last_date = db.get_last_sync_date(pid)

        if last_date:
            cutoff = last_date - timedelta(days=1)  # Overlap by 1 day for safety
            logger.info(f"Syncing {store['brand']} - {store['store']} (since {cutoff})")
        else:
            cutoff = None
            logger.info(f"Syncing {store['brand']} - {store['store']} (full fetch - no prior data)")

        results = client.fetch_reviews([pid], cutoff_date=cutoff, batch_size=1)

        reviews = [r for r in results if r.get("_type") != "store_rating"]
        ratings = [r for r in results if r.get("_type") == "store_rating"]

        if reviews:
            inserted, skipped = db.upsert_reviews(reviews)
            total_inserted += inserted
            total_skipped += skipped

        for r in ratings:
            db.update_store_rating(r["place_id"], r["current_rating"])

    logger.info(f"Sync complete: {total_inserted} new, {total_skipped} existing")
    logger.info(f"Total reviews in DB: {db.get_review_count()}")


def cmd_report_monthly(args) -> None:
    """Generate and push monthly report to Google Sheets."""
    if args.year and args.month:
        year = int(args.year)
        month = int(args.month)
    else:
        # Auto-detect: report for the previous month
        today = date.today()
        if today.month == 1:
            year, month = today.year - 1, 12
        else:
            year, month = today.year, today.month - 1

    logger.info(f"Generating monthly report for {year}-{month:02d}...")

    report = compute_monthly_report(year, month)
    above_pct = pct_above_threshold(4.5)
    logger.info(f"Stores above 4.5: {above_pct}%")

    # Write to Google Sheets
    creds_file = os.getenv("GOOGLE_SHEETS_CREDENTIALS_FILE", ".credentials/service-account.json")
    sheet_id = os.getenv("TARGET_SHEET_ID")

    if not sheet_id:
        logger.error("TARGET_SHEET_ID not set")
        sys.exit(1)

    client = get_sheet_client(creds_file)
    spreadsheet = open_spreadsheet(client, sheet_id)

    # Update Online Reviews tab (year-specific)
    tab_name = f"Online Reviews {year}"
    try:
        online_reviews = spreadsheet.worksheet(tab_name)
    except Exception:
        # Fallback to generic name
        online_reviews = spreadsheet.worksheet("Online Reviews")
    build_store_row_map(online_reviews)

    # Write current ratings (plain values from Outscraper)
    update_current_ratings(online_reviews, report)

    # Update All Reviews tab FIRST (formulas reference this data)
    all_reviews = get_all_reviews_for_tab()
    populate_all_reviews_tab(spreadsheet, all_reviews)

    # Update Needs Attention tab
    needs_attention = get_needs_attention_reviews()
    populate_needs_attention_tab(spreadsheet, needs_attention)

    # Write formulas (C, F-AC, AE, AG) — auto-calculate from All Reviews data
    write_formulas(online_reviews, year, month)

    logger.info("Monthly report complete — Google Sheet updated ✅")


def cmd_report_weekly(args) -> None:
    """Generate and push weekly report to Google Sheets."""
    if args.start and args.end:
        week_start = date.fromisoformat(args.start)
        week_end = date.fromisoformat(args.end)
    else:
        # Auto: report for the previous week (Mon-Sun)
        today = date.today()
        week_end = today - timedelta(days=today.weekday() + 1)  # Last Sunday
        week_start = week_end - timedelta(days=6)  # Previous Monday

    logger.info(f"Generating weekly report for {week_start} to {week_end}...")

    report = compute_weekly_report(week_start, week_end)

    # Write to Google Sheets
    creds_file = os.getenv("GOOGLE_SHEETS_CREDENTIALS_FILE", ".credentials/service-account.json")
    sheet_id = os.getenv("TARGET_SHEET_ID")

    if not sheet_id:
        logger.error("TARGET_SHEET_ID not set")
        sys.exit(1)

    client = get_sheet_client(creds_file)
    spreadsheet = open_spreadsheet(client, sheet_id)

    # Update All Reviews and Needs Attention tabs
    all_reviews = get_all_reviews_for_tab()
    populate_all_reviews_tab(spreadsheet, all_reviews)

    needs_attention = get_needs_attention_reviews()
    populate_needs_attention_tab(spreadsheet, needs_attention)

    # Write weekly metrics to the Weekly Report tab
    above_pct = pct_above_threshold(4.5)
    populate_weekly_report_tab(spreadsheet, report, above_pct)

    logger.info("Weekly report complete — Google Sheet updated ✅")


def cmd_test_api(args) -> None:
    """Test Outscraper API connection with 1 store."""
    api_key = os.getenv("OUTSCRAPER_API_KEY")
    if not api_key:
        logger.error("OUTSCRAPER_API_KEY not set")
        sys.exit(1)

    stores = load_stores()
    test_store = stores[0]  # First store (Inspired Cannabis Abbotsford)

    logger.info(f"Testing API with: {test_store['brand']} - {test_store['store']}")
    logger.info(f"Place ID: {test_store['place_id']}")

    client = OutscraperClient(api_key)
    result = client.test_connection(test_store["place_id"])

    if result["success"]:
        logger.info(f"✅ API test successful!")
        logger.info(f"   Place name: {result['place_name']}")
        logger.info(f"   Rating: {result['rating']}")
        logger.info(f"   Total reviews: {result['total_reviews']}")
        if result.get("sample_review"):
            sample = result["sample_review"]
            logger.info(f"   Sample review: {sample.get('review_rating')}★ - "
                        f"{(sample.get('review_text', '') or '')[:100]}")
    else:
        logger.error(f"❌ API test failed: {result.get('error')}")
        sys.exit(1)


def cmd_test_sync(args) -> None:
    """Test sync: fetch reviews for a few stores over a limited time window."""
    api_key = os.getenv("OUTSCRAPER_API_KEY")
    if not api_key:
        logger.error("OUTSCRAPER_API_KEY not set")
        sys.exit(1)

    db.init_db()
    stores = load_stores()
    db.upsert_stores(stores)

    # Pick N stores spread across brands
    num_stores = args.stores
    num_days = args.days

    # Select stores: 1 per brand, cycling through brands
    brands_seen = {}
    selected = []
    for s in stores:
        brand = s["brand"]
        if brand not in brands_seen:
            brands_seen[brand] = True
            selected.append(s)
        if len(selected) >= num_stores:
            break

    cutoff = date.today() - timedelta(days=num_days)
    place_ids = [s["place_id"] for s in selected]

    logger.info(f"Test sync: {len(selected)} stores, last {num_days} days (since {cutoff})")
    for s in selected:
        logger.info(f"  • {s['brand']} — {s['store']}")

    client = OutscraperClient(api_key)
    results = client.fetch_reviews(place_ids, cutoff_date=cutoff, batch_size=len(place_ids))

    reviews = [r for r in results if r.get("_type") != "store_rating"]
    ratings = [r for r in results if r.get("_type") == "store_rating"]

    if reviews:
        inserted, skipped = db.upsert_reviews(reviews)
        logger.info(f"Reviews: {inserted} inserted, {skipped} duplicates skipped")
    else:
        logger.info("No reviews found in the time window (this may be normal)")

    for r in ratings:
        db.update_store_rating(r["place_id"], r["current_rating"])
    logger.info(f"Updated current ratings for {len(ratings)} stores")
    logger.info(f"Total reviews now in DB: {db.get_review_count()}")


def cmd_status(args) -> None:
    """Show database status."""
    db.init_db()
    stores = db.get_all_stores()
    review_count = db.get_review_count()

    print(f"\n{'='*60}")
    print(f"  GBP Reviews Pipeline — Status")
    print(f"{'='*60}")
    print(f"  Stores registered:  {len(stores)}")
    print(f"  Total reviews:      {review_count}")

    if stores:
        brands = {}
        for s in stores:
            brand = s["brand"]
            brands[brand] = brands.get(brand, 0) + 1
        print(f"\n  Brands:")
        for brand, count in brands.items():
            print(f"    {brand}: {count} stores")

    print(f"{'='*60}\n")


def main():
    parser = argparse.ArgumentParser(
        description="GBP Reviews Automation Pipeline",
        prog="python -m src.main"
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # init
    subparsers.add_parser("init", help="Initialize database and load store registry")

    # initial-load
    subparsers.add_parser("initial-load", help="Fetch all historical reviews (one-time)")

    # sync
    subparsers.add_parser("sync", help="Incremental sync — fetch new reviews only")

    # report monthly
    monthly_parser = subparsers.add_parser("report-monthly", help="Generate monthly report")
    monthly_parser.add_argument("--year", type=str, help="Year (e.g., 2026). Omit for auto.")
    monthly_parser.add_argument("--month", type=str, help="Month (e.g., 2). Omit for auto.")

    # report weekly
    weekly_parser = subparsers.add_parser("report-weekly", help="Generate weekly report")
    weekly_parser.add_argument("--start", type=str, help="Start date YYYY-MM-DD. Omit for auto.")
    weekly_parser.add_argument("--end", type=str, help="End date YYYY-MM-DD. Omit for auto.")

    # test-api
    subparsers.add_parser("test-api", help="Test Outscraper API connection")

    # test-sync
    test_sync_parser = subparsers.add_parser("test-sync", help="Test sync with limited stores/days")
    test_sync_parser.add_argument("--stores", type=int, default=3, help="Number of stores to test (default: 3)")
    test_sync_parser.add_argument("--days", type=int, default=14, help="Days of history to fetch (default: 14)")

    # status
    subparsers.add_parser("status", help="Show database status")

    args = parser.parse_args()

    commands = {
        "init": cmd_init,
        "initial-load": cmd_initial_load,
        "sync": cmd_sync,
        "report-monthly": cmd_report_monthly,
        "report-weekly": cmd_report_weekly,
        "test-api": cmd_test_api,
        "test-sync": cmd_test_sync,
        "status": cmd_status,
    }

    if args.command in commands:
        commands[args.command](args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
