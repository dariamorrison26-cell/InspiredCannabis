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

    # Brand keywords to match against. Maps keyword -> canonical brand name.
    # The Sheet may use abbreviations like "INSPIRED" or full names like "Inspired Cannabis".
    brand_keywords = {
        "inspired": "Inspired Cannabis",
        "imagine": "Imagine Cannabis",
        "dutch love": "Dutch Love",
        "cannabis supply": "Cannabis Supply Co.",
        "muse": "Muse Cannabis",
    }

    # Also detect summary/subtotal rows to skip them
    summary_keywords = ["total", "average", "summary", "subtotal"]

    for i, row in enumerate(all_values):
        row_num = i + 1  # 1-indexed

        # Skip header rows (first 3 rows typically)
        if row_num <= 3:
            continue

        cell_a = row[0].strip() if row else ""

        if not cell_a:
            continue

        cell_lower = cell_a.lower()

        # Check if this is a brand header row
        matched_brand = None
        for keyword, canonical in brand_keywords.items():
            if keyword in cell_lower:
                matched_brand = canonical
                break

        if matched_brand:
            current_brand = matched_brand
            logger.debug(f"  Brand header at row {row_num}: '{cell_a}' -> {current_brand}")
            continue

        # Skip summary/subtotal rows (typically contain only numbers, no store name format)
        if any(kw in cell_lower for kw in summary_keywords):
            continue

        # Check if this looks like a header row (has "Current rate" in adjacent columns)
        if len(row) > 1 and row[1].strip().lower() in ("current rate", "current rating"):
            continue

        # This is a store row
        if current_brand:
            row_map[(current_brand, cell_a)] = row_num

    STORE_ROW_MAP = row_map
    logger.info(f"Built store row map: {len(row_map)} stores mapped")
    return row_map


def update_current_ratings(worksheet: gspread.Worksheet, report_data: list[dict]) -> int:
    """Update Column B (Current Rating) for each store. Plain values from Outscraper metadata."""
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


def write_formulas(worksheet: gspread.Worksheet, year: int, current_month: int) -> int:
    """
    Write Google Sheet formulas to the Online Reviews tab.

    All formulas reference the 'All Reviews' tab where:
        Column A = Date, B = Brand, C = Store, D = Rating

    Uses date range criteria (">="&DATE(...), "<"&DATE(...)) because
    Google Sheets COUNTIFS/AVERAGEIFS don't support YEAR()/MONTH() on ranges.

    Columns written:
        C  = Prior year average (AVERAGEIFS)
        D  = YTD average (AVERAGEIFS for full year)
        E  = YTD # Reviews (COUNTIFS for full year)
        F-AC = Monthly count + avg per month
        AE = 1★ count, AG = 5★ count
    """
    prior_year = year - 1
    updated = 0
    batch_updates = []

    # All Reviews tab references
    ar_date = "'All Reviews'!A:A"
    ar_brand = "'All Reviews'!B:B"
    ar_store = "'All Reviews'!C:C"
    ar_rating = "'All Reviews'!D:D"

    # Month column mapping
    month_cols = {
        1:  ("F", "G"),   2:  ("H", "I"),   3:  ("J", "K"),
        4:  ("L", "M"),   5:  ("N", "O"),   6:  ("P", "Q"),
        7:  ("R", "S"),   8:  ("T", "U"),   9:  ("V", "W"),
        10: ("X", "Y"),   11: ("Z", "AA"),  12: ("AB", "AC"),
    }

    for (brand, store_name), row in STORE_ROW_MAP.items():
        store_ref = f"A{row}"  # Store name cell in Online Reviews tab
        brand_str = f'"{brand}"'  # Brand name string for formula filtering

        # ── Column C: Prior year average ──
        formula_c = (
            f'=IFERROR(ROUND(AVERAGEIFS({ar_rating}, {ar_store}, {store_ref}, '
            f'{ar_brand}, {brand_str}, '
            f'{ar_date}, ">="&DATE({prior_year},1,1), '
            f'{ar_date}, "<"&DATE({year},1,1)), 1), "")'
        )
        batch_updates.append({"range": f"C{row}", "values": [[formula_c]]})

        # ── Column D: YTD Average (full year) ──
        formula_d = (
            f'=IFERROR(ROUND(AVERAGEIFS({ar_rating}, {ar_store}, {store_ref}, '
            f'{ar_brand}, {brand_str}, '
            f'{ar_date}, ">="&DATE({year},1,1), '
            f'{ar_date}, "<"&DATE({year + 1},1,1)), 1), "")'
        )
        batch_updates.append({"range": f"D{row}", "values": [[formula_d]]})

        # ── Column E: YTD # Reviews (full year) ──
        formula_e = (
            f'=COUNTIFS({ar_store}, {store_ref}, '
            f'{ar_brand}, {brand_str}, '
            f'{ar_date}, ">="&DATE({year},1,1), '
            f'{ar_date}, "<"&DATE({year + 1},1,1))'
        )
        batch_updates.append({"range": f"E{row}", "values": [[formula_e]]})

        # ── Columns F-AC: Monthly # Reviews and Average Rate ──
        for m in range(1, 13):
            count_col, avg_col = month_cols[m]

            # Date range for this month
            if m == 12:
                next_year, next_month = year + 1, 1
            else:
                next_year, next_month = year, m + 1

            # Monthly review count
            formula_count = (
                f'=COUNTIFS({ar_store}, {store_ref}, '
                f'{ar_brand}, {brand_str}, '
                f'{ar_date}, ">="&DATE({year},{m},1), '
                f'{ar_date}, "<"&DATE({next_year},{next_month},1))'
            )
            # Monthly average rating (rounded to 1 decimal)
            formula_avg = (
                f'=IFERROR(ROUND(AVERAGEIFS({ar_rating}, {ar_store}, {store_ref}, '
                f'{ar_brand}, {brand_str}, '
                f'{ar_date}, ">="&DATE({year},{m},1), '
                f'{ar_date}, "<"&DATE({next_year},{next_month},1)), 1), 0)'
            )

            batch_updates.append({"range": f"{count_col}{row}", "values": [[formula_count]]})
            batch_updates.append({"range": f"{avg_col}{row}", "values": [[formula_avg]]})

        # ── Column AE: 1★ count (full year) ──
        formula_1star = (
            f'=COUNTIFS({ar_store}, {store_ref}, {ar_rating}, 1, '
            f'{ar_brand}, {brand_str}, '
            f'{ar_date}, ">="&DATE({year},1,1), '
            f'{ar_date}, "<"&DATE({year + 1},1,1))'
        )
        batch_updates.append({"range": f"AE{row}", "values": [[formula_1star]]})

        # ── Column AG: 5★ count (full year) ──
        formula_5star = (
            f'=COUNTIFS({ar_store}, {store_ref}, {ar_rating}, 5, '
            f'{ar_brand}, {brand_str}, '
            f'{ar_date}, ">="&DATE({year},1,1), '
            f'{ar_date}, "<"&DATE({year + 1},1,1))'
        )
        batch_updates.append({"range": f"AG{row}", "values": [[formula_5star]]})

        updated += 1

    if batch_updates:
        # Use USER_ENTERED so Google Sheets interprets formulas
        worksheet.batch_update(batch_updates, value_input_option="USER_ENTERED")

    logger.info(f"Wrote formulas for {updated} stores ({len(batch_updates)} cells)")
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
    worksheet.update(range_name="A1", values=rows, value_input_option="USER_ENTERED")

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
    worksheet.update(range_name="A1", values=rows, value_input_option="USER_ENTERED")

    # Format header with red accent for urgency
    worksheet.format("A1:H1", {
        "textFormat": {"bold": True},
        "backgroundColor": {"red": 0.91, "green": 0.2, "blue": 0.2}
    })

    logger.info(f"Populated '{tab_name}' with {len(reviews)} negative reviews")
    return len(reviews)
