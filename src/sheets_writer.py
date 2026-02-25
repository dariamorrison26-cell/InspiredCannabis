"""Google Sheets writer — surgical updates to the client's report spreadsheet."""

import logging
from typing import Optional

import gspread
from google.oauth2.service_account import Credentials

logger = logging.getLogger(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]


def get_sheet_client(credentials_file: str) -> gspread.Client:
    """Authenticate and return a gspread client."""
    creds = Credentials.from_service_account_file(credentials_file, scopes=SCOPES)
    return gspread.authorize(creds)


def open_spreadsheet(client: gspread.Client, sheet_id: str) -> gspread.Spreadsheet:
    """Open a spreadsheet by ID."""
    return client.open_by_key(sheet_id)


# =============================================================================
# Online Reviews Tab — Surgical Cell Updates
# =============================================================================

# Store row mapping: maps (brand, store_name) -> row number in the Online Reviews tab
# This must match the client's existing sheet layout EXACTLY.
# Will be populated during initial setup by reading the sheet structure.
STORE_ROW_MAP: dict[tuple[str, str], int] = {}


def build_store_row_map(worksheet: gspread.Worksheet) -> dict[tuple[str, str], int]:
    """
    Scan the Online Reviews tab to build a mapping of (brand, store) -> row number.
    Reads column A (store names) and identifies brand header rows.
    """
    global STORE_ROW_MAP

    all_values = worksheet.get_all_values()
    current_brand = ""
    row_map = {}

    for i, row in enumerate(all_values):
        row_num = i + 1  # 1-indexed

        # Skip header rows (first 2-3 rows typically)
        if row_num <= 3:
            continue

        cell_a = row[0].strip() if row else ""

        if not cell_a:
            continue

        # Check if this is a brand header row (e.g., "Inspired Cannabis")
        brand_names = [
            "Inspired Cannabis", "Imagine Cannabis", "Dutch Love",
            "Cannabis Supply Co.", "Muse Cannabis"
        ]
        if any(cell_a.lower() == b.lower() for b in brand_names):
            current_brand = cell_a
            continue

        # This is a store row
        if current_brand:
            row_map[(current_brand, cell_a)] = row_num

    STORE_ROW_MAP = row_map
    logger.info(f"Built store row map: {len(row_map)} stores mapped")
    return row_map


def update_current_ratings(worksheet: gspread.Worksheet, report_data: list[dict]) -> int:
    """Update Column B (Current Rating) for each store. Returns count of cells updated."""
    updated = 0
    batch_updates = []

    for store in report_data:
        key = (store["brand"], store["store_name"])
        row = STORE_ROW_MAP.get(key)
        if row and store.get("current_rating") is not None:
            batch_updates.append({
                "range": f"B{row}",
                "values": [[store["current_rating"]]]
            })
            updated += 1

    if batch_updates:
        worksheet.batch_update(batch_updates)
    logger.info(f"Updated {updated} current ratings")
    return updated


def update_ytd_metrics(worksheet: gspread.Worksheet, report_data: list[dict]) -> int:
    """Update YTD columns (D = avg, E = count). Returns count of stores updated."""
    updated = 0
    batch_updates = []

    for store in report_data:
        key = (store["brand"], store["store_name"])
        row = STORE_ROW_MAP.get(key)
        if row:
            batch_updates.append({
                "range": f"D{row}:E{row}",
                "values": [[store["ytd_avg"], store["ytd_count"]]]
            })
            updated += 1

    if batch_updates:
        worksheet.batch_update(batch_updates)
    logger.info(f"Updated {updated} YTD metrics")
    return updated


def update_star_distributions(worksheet: gspread.Worksheet, report_data: list[dict]) -> int:
    """Update star distribution columns (AD-AG). Returns count of stores updated."""
    updated = 0
    batch_updates = []

    for store in report_data:
        key = (store["brand"], store["store_name"])
        row = STORE_ROW_MAP.get(key)
        if row:
            batch_updates.append({
                "range": f"AD{row}:AG{row}",
                "values": [[
                    store["ytd_one_star_count"],
                    store["ytd_one_star_pct"],
                    store["ytd_five_star_count"],
                    store["ytd_five_star_pct"],
                ]]
            })
            updated += 1

    if batch_updates:
        worksheet.batch_update(batch_updates)
    logger.info(f"Updated {updated} star distributions")
    return updated


def update_monthly_data(
    worksheet: gspread.Worksheet,
    year: int,
    month: int,
    report_data: list[dict],
    month_col_map: Optional[dict[int, tuple[str, str]]] = None
) -> int:
    """
    Update monthly review count and average columns.

    The column mapping depends on the sheet layout. month_col_map maps
    month number (1-12) -> (count_col, avg_col) letter pairs.
    """
    if month_col_map is None:
        # Default mapping: Jan starts at F, each month uses 2 columns
        # F=Jan count, G=Jan avg, H=Feb count, I=Feb avg, etc.
        cols = "FGHIJKLMNOPQRSTUVWXYZAAABAC"
        month_col_map = {}
        for m in range(1, 13):
            idx = (m - 1) * 2
            if idx + 1 < len(cols):
                month_col_map[m] = (cols[idx], cols[idx + 1])

    col_pair = month_col_map.get(month)
    if not col_pair:
        logger.warning(f"No column mapping for month {month}")
        return 0

    count_col, avg_col = col_pair
    updated = 0
    batch_updates = []

    for store in report_data:
        key = (store["brand"], store["store_name"])
        row = STORE_ROW_MAP.get(key)
        if row:
            batch_updates.append({
                "range": f"{count_col}{row}:{avg_col}{row}",
                "values": [[store["month_count"], store["month_avg"]]]
            })
            updated += 1

    if batch_updates:
        worksheet.batch_update(batch_updates)
    logger.info(f"Updated {updated} monthly data for {year}-{month:02d}")
    return updated


# =============================================================================
# All Reviews Tab
# =============================================================================

def populate_all_reviews_tab(
    spreadsheet: gspread.Spreadsheet,
    reviews: list[dict],
    tab_name: str = "All Reviews"
) -> int:
    """
    Populate the All Reviews tab with review data.
    Preserves the Notes column (last column) if it exists.
    """
    try:
        worksheet = spreadsheet.worksheet(tab_name)
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=tab_name, rows=max(len(reviews) + 10, 100), cols=8)

    # Read existing Notes column (column H) to preserve manual entries
    existing_notes = {}
    try:
        existing_data = worksheet.get_all_values()
        for i, row in enumerate(existing_data[1:], start=2):  # Skip header
            if len(row) >= 8 and row[7].strip():  # Column H = Notes
                # Key by date+brand+store+rating for matching
                note_key = f"{row[0]}|{row[1]}|{row[2]}|{row[3]}"
                existing_notes[note_key] = row[7]
    except Exception:
        pass

    # Build the data rows
    headers = ["Date", "Brand", "Store", "Rating", "Review Text",
               "Response Status", "Owner Response", "Notes"]

    rows = [headers]
    for r in reviews:
        note_key = f"{r['date']}|{r['brand']}|{r['store']}|{r['rating']}"
        note = existing_notes.get(note_key, "")
        rows.append([
            r["date"],
            r["brand"],
            r["store"],
            r["rating"],
            r.get("review_text", ""),
            r["response_status"],
            r.get("owner_response", ""),
            note,  # Preserve existing notes
        ])

    # Clear and rewrite (preserving notes)
    worksheet.clear()
    worksheet.update(range_name="A1", values=rows)

    # Format header row
    worksheet.format("A1:H1", {
        "textFormat": {"bold": True},
        "backgroundColor": {"red": 0.1, "green": 0.45, "blue": 0.91}
    })

    logger.info(f"Populated '{tab_name}' with {len(reviews)} reviews")
    return len(reviews)


# =============================================================================
# Needs Attention Tab
# =============================================================================

def populate_needs_attention_tab(
    spreadsheet: gspread.Spreadsheet,
    reviews: list[dict],
    tab_name: str = "Needs Attention"
) -> int:
    """
    Populate the Needs Attention tab with 1★ and 2★ reviews.
    Same structure as All Reviews but filtered.
    """
    try:
        worksheet = spreadsheet.worksheet(tab_name)
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=tab_name, rows=max(len(reviews) + 10, 100), cols=8)

    # Read existing Notes column to preserve manual entries
    existing_notes = {}
    try:
        existing_data = worksheet.get_all_values()
        for i, row in enumerate(existing_data[1:], start=2):
            if len(row) >= 8 and row[7].strip():
                note_key = f"{row[0]}|{row[1]}|{row[2]}|{row[3]}"
                existing_notes[note_key] = row[7]
    except Exception:
        pass

    headers = ["Date", "Brand", "Store", "Rating", "Review Text",
               "Response Status", "Owner Response", "Notes"]

    rows = [headers]
    for r in reviews:
        note_key = f"{r['date']}|{r['brand']}|{r['store']}|{r['rating']}"
        note = existing_notes.get(note_key, "")
        rows.append([
            r["date"],
            r["brand"],
            r["store"],
            r["rating"],
            r.get("review_text", ""),
            r["response_status"],
            r.get("owner_response", ""),
            note,
        ])

    worksheet.clear()
    worksheet.update(range_name="A1", values=rows)

    # Format header with red accent for urgency
    worksheet.format("A1:H1", {
        "textFormat": {"bold": True},
        "backgroundColor": {"red": 0.91, "green": 0.2, "blue": 0.2}
    })

    logger.info(f"Populated '{tab_name}' with {len(reviews)} negative reviews")
    return len(reviews)
