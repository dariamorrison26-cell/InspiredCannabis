"""Report generator — computes all metrics from the reviews database."""

from calendar import monthrange
from datetime import date, timedelta
from typing import Optional

from . import database as db


def current_rating(place_id: str) -> Optional[float]:
    """Get a store's current Google rating from the DB."""
    stores = db.get_all_stores()
    for store in stores:
        if store["place_id"] == place_id:
            return store.get("current_rating")
    return None


def period_metrics(place_id: str, start_date: date, end_date: date) -> dict:
    """
    Compute metrics for a store over a date range.

    Returns:
        {
            "review_count": int,
            "avg_rating": float,
            "five_star_count": int,
            "five_star_pct": float,
            "one_star_count": int,
            "one_star_pct": float,
        }
    """
    reviews = db.get_reviews(place_id=place_id, start_date=start_date, end_date=end_date)

    if not reviews:
        return {
            "review_count": 0,
            "avg_rating": 0.0,
            "five_star_count": 0,
            "five_star_pct": 0.0,
            "one_star_count": 0,
            "one_star_pct": 0.0,
        }

    total = len(reviews)
    avg = sum(r["rating"] for r in reviews) / total
    five_star = sum(1 for r in reviews if r["rating"] == 5)
    one_star = sum(1 for r in reviews if r["rating"] == 1)

    return {
        "review_count": total,
        "avg_rating": round(avg, 2),
        "five_star_count": five_star,
        "five_star_pct": round((five_star / total) * 100, 1) if total else 0.0,
        "one_star_count": one_star,
        "one_star_pct": round((one_star / total) * 100, 1) if total else 0.0,
    }


def ytd_metrics(place_id: str, year: int) -> dict:
    """Compute year-to-date metrics for a store."""
    start = date(year, 1, 1)
    end = date.today() if year == date.today().year else date(year, 12, 31)
    return period_metrics(place_id, start, end)


def monthly_metrics(place_id: str, year: int, month: int) -> dict:
    """Compute metrics for a specific month."""
    start = date(year, month, 1)
    _, last_day = monthrange(year, month)
    end = date(year, month, last_day)
    return period_metrics(place_id, start, end)


def mom_shift(place_id: str, year: int, month: int) -> Optional[float]:
    """
    Month-over-month rating shift.
    Returns: this_month_avg - prior_month_avg (or None if no data).
    """
    this_month = monthly_metrics(place_id, year, month)

    # Calculate prior month
    if month == 1:
        prior_year, prior_month = year - 1, 12
    else:
        prior_year, prior_month = year, month - 1

    prior = monthly_metrics(place_id, prior_year, prior_month)

    if this_month["review_count"] == 0 or prior["review_count"] == 0:
        return None

    return round(this_month["avg_rating"] - prior["avg_rating"], 2)


def pct_above_threshold(threshold: float = 4.5) -> float:
    """
    Company-wide percentage of stores with current rating >= threshold.
    """
    stores = db.get_all_stores()
    if not stores:
        return 0.0

    rated_stores = [s for s in stores if s.get("current_rating") is not None]
    if not rated_stores:
        return 0.0

    above = sum(1 for s in rated_stores if s["current_rating"] >= threshold)
    return round((above / len(rated_stores)) * 100, 1)


def compute_monthly_report(year: int, month: int) -> list[dict]:
    """
    Generate a full monthly report for all stores.

    Returns a list of dicts, one per store, with all metrics:
        {
            "place_id", "brand", "store_name",
            "current_rating", "prior_year_avg",
            "ytd_avg", "ytd_count", "ytd_five_star_count", "ytd_five_star_pct",
            "ytd_one_star_count", "ytd_one_star_pct",
            "month_count", "month_avg",
            "monthly_data": {1: {"count": int, "avg": float}, ...},
            "mom_shift",
        }
    """
    stores = db.get_all_stores()
    prior_year = year - 1
    report = []

    for store in stores:
        pid = store["place_id"]
        ytd = ytd_metrics(pid, year)
        monthly = monthly_metrics(pid, year, month)
        shift = mom_shift(pid, year, month)

        # Prior year average (e.g., full 2025)
        prior = period_metrics(pid, date(prior_year, 1, 1), date(prior_year, 12, 31))
        prior_avg = prior["avg_rating"] if prior["review_count"] > 0 else None

        # Compute all months from Jan to current month
        all_months = {}
        for m in range(1, month + 1):
            mm = monthly_metrics(pid, year, m)
            all_months[m] = {
                "count": mm["review_count"],
                "avg": mm["avg_rating"] if mm["review_count"] > 0 else 0,
            }

        report.append({
            "place_id": pid,
            "brand": store["brand"],
            "store_name": store["store_name"],
            "current_rating": store.get("current_rating"),
            "prior_year_avg": prior_avg,
            "ytd_avg": ytd["avg_rating"],
            "ytd_count": ytd["review_count"],
            "ytd_five_star_count": ytd["five_star_count"],
            "ytd_five_star_pct": ytd["five_star_pct"],
            "ytd_one_star_count": ytd["one_star_count"],
            "ytd_one_star_pct": ytd["one_star_pct"],
            "month_count": monthly["review_count"],
            "month_avg": monthly["avg_rating"],
            "monthly_data": all_months,
            "mom_shift": shift,
        })

    return report


def compute_weekly_report(week_start: date, week_end: date) -> list[dict]:
    """
    Generate a weekly report for all stores.

    Returns a list of dicts with weekly + MTD metrics per store.
    """
    stores = db.get_all_stores()
    report = []

    # MTD: from 1st of week_end's month to week_end
    mtd_start = date(week_end.year, week_end.month, 1)

    # Calculate weeks elapsed in month (for weekly average comparison)
    days_in_month_so_far = (week_end - mtd_start).days + 1
    weeks_elapsed = max(1, days_in_month_so_far / 7)

    for store in stores:
        pid = store["place_id"]
        weekly = period_metrics(pid, week_start, week_end)
        mtd = period_metrics(pid, mtd_start, week_end)

        # MTD average reviews per week
        mtd_weekly_avg = round(mtd["review_count"] / weeks_elapsed, 1) if mtd["review_count"] > 0 else 0.0

        report.append({
            "place_id": pid,
            "brand": store["brand"],
            "store_name": store["store_name"],
            "current_rating": store.get("current_rating"),
            "week_count": weekly["review_count"],
            "week_avg": weekly["avg_rating"],
            "week_five_star_count": weekly["five_star_count"],
            "week_five_star_pct": weekly["five_star_pct"],
            "week_one_star_count": weekly["one_star_count"],
            "week_one_star_pct": weekly["one_star_pct"],
            "mtd_avg": mtd["avg_rating"],
            "mtd_count": mtd["review_count"],
            "week_vs_mtd_avg": round(weekly["avg_rating"] - mtd["avg_rating"], 2) if weekly["review_count"] > 0 and mtd["review_count"] > 0 else 0.0,
            "mtd_weekly_avg_count": mtd_weekly_avg,
            "week_vs_mtd_count": round(weekly["review_count"] - mtd_weekly_avg, 1) if weekly["review_count"] > 0 else 0.0,
            "week_start": week_start.isoformat(),
            "week_end": week_end.isoformat(),
        })

    return report


def get_all_reviews_for_tab(start_date: Optional[date] = None) -> list[dict]:
    """
    Get all reviews formatted for the 'All Reviews' tab.
    Sorted by date descending.
    """
    reviews = db.get_reviews(start_date=start_date)
    return [{
        "date": r["review_date"],
        "brand": r["brand"],
        "store": r["store_name"],
        "rating": r["rating"],
        "review_text": r.get("review_text", ""),
        "response_status": "✅ Responded" if r.get("owner_response") else "⚠️ No Response",
        "owner_response": r.get("owner_response", ""),
    } for r in reviews]


def get_needs_attention_reviews(start_date: Optional[date] = None) -> list[dict]:
    """
    Get 1★ and 2★ reviews for the 'Needs Attention' tab.
    Sorted by date descending.
    """
    reviews = db.get_reviews(start_date=start_date, max_rating=2)
    return [{
        "date": r["review_date"],
        "brand": r["brand"],
        "store": r["store_name"],
        "rating": r["rating"],
        "review_text": r.get("review_text", ""),
        "response_status": "✅ Responded" if r.get("owner_response") else "⚠️ No Response",
        "owner_response": r.get("owner_response", ""),
    } for r in reviews]


def compute_monthly_report_tab() -> list[dict]:
    """
    Generate flat rows for the Monthly Report tab — one row per store per month.

    Auto-discovers the date range from the earliest review in the DB through
    the current month. Returns rows sorted most-recent-month first, then by
    brand and store name within each month.
    """
    import calendar as cal_mod

    stores = db.get_all_stores()
    if not stores:
        return []

    # Find the earliest review date across all stores
    all_reviews = db.get_reviews()
    if not all_reviews:
        return []

    earliest = min(r["review_date"] for r in all_reviews)
    if isinstance(earliest, str):
        earliest = date.fromisoformat(earliest)

    today = date.today()

    # Build list of (year, month) pairs from earliest to current
    year_months = []
    y, m = earliest.year, earliest.month
    while (y, m) <= (today.year, today.month):
        year_months.append((y, m))
        if m == 12:
            y, m = y + 1, 1
        else:
            m += 1

    # Generate rows: most recent month first
    rows = []
    for y, m in reversed(year_months):
        month_name = cal_mod.month_abbr[m]  # "Jan", "Feb", etc.

        for store in stores:
            pid = store["place_id"]
            metrics = monthly_metrics(pid, y, m)
            shift = mom_shift(pid, y, m)

            rows.append({
                "year": y,
                "month_num": m,
                "month_name": month_name,
                "brand": store["brand"],
                "store_name": store["store_name"],
                "current_rating": store.get("current_rating", ""),
                "review_count": metrics["review_count"],
                "avg_rating": metrics["avg_rating"] if metrics["review_count"] > 0 else "",
                "five_star_count": metrics["five_star_count"],
                "five_star_pct": metrics["five_star_pct"],
                "one_star_count": metrics["one_star_count"],
                "one_star_pct": metrics["one_star_pct"],
                "mom_shift_val": shift if shift is not None else "",
            })

    return rows
