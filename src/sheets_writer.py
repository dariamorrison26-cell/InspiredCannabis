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

    Columns written:
        B  = Current Rate (plain value, handled by update_current_ratings)
        C  = Prior year average (AVERAGEIFS)
        D  = YTD average (AVERAGEIFS for full year)
        E  = YTD # Reviews (COUNTIFS for full year)
        F-AC = Monthly count + avg per month (2 cols × 12 months)
        AD = MOM shift (current month avg - prior month avg)
        AE = 1★ YTD count, AF = 1★ YTD %
        AG = 5★ YTD count, AH = 5★ YTD %
        AI-BH = Monthly 5★ count, 5★ %, 1★ count, 1★ % (4 cols × 12 months)
    """
    prior_year = year - 1
    updated = 0
    batch_updates = []

    # All Reviews tab references
    ar_date = "'All Reviews'!A:A"
    ar_brand = "'All Reviews'!B:B"
    ar_store = "'All Reviews'!C:C"
    ar_rating = "'All Reviews'!D:D"

    # Month column mapping: (count_col, avg_col)
    month_cols = {
        1:  ("F", "G"),   2:  ("H", "I"),   3:  ("J", "K"),
        4:  ("L", "M"),   5:  ("N", "O"),   6:  ("P", "Q"),
        7:  ("R", "S"),   8:  ("T", "U"),   9:  ("V", "W"),
        10: ("X", "Y"),   11: ("Z", "AA"),  12: ("AB", "AC"),
    }

    # Monthly star breakdown columns: (5★ count, 5★ %, 1★ count, 1★ %)
    # AI through BH = 48 columns (4 per month × 12 months)
    star_month_cols = {
        1:  ("AI", "AJ", "AK", "AL"),
        2:  ("AM", "AN", "AO", "AP"),
        3:  ("AQ", "AR", "AS", "AT"),
        4:  ("AU", "AV", "AW", "AX"),
        5:  ("AY", "AZ", "BA", "BB"),
        6:  ("BC", "BD", "BE", "BF"),
        7:  ("BG", "BH", "BI", "BJ"),
        8:  ("BK", "BL", "BM", "BN"),
        9:  ("BO", "BP", "BQ", "BR"),
        10: ("BS", "BT", "BU", "BV"),
        11: ("BW", "BX", "BY", "BZ"),
        12: ("CA", "CB", "CC", "CD"),
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

        # ── Column AD: MOM Shift ──
        # Current month avg - prior month avg
        if current_month == 1:
            # Jan: compare to Dec of prior year (use prior year avg as fallback)
            curr_avg_col = month_cols[1][1]  # G (Jan avg)
            formula_mom = f'=IFERROR({curr_avg_col}{row}-C{row}, "")'
        else:
            curr_avg_col = month_cols[current_month][1]
            prior_avg_col = month_cols[current_month - 1][1]
            formula_mom = f'=IFERROR({curr_avg_col}{row}-{prior_avg_col}{row}, "")'
        batch_updates.append({"range": f"AD{row}", "values": [[formula_mom]]})

        # ── Column AE: 1★ YTD count ──
        formula_1star = (
            f'=COUNTIFS({ar_store}, {store_ref}, {ar_rating}, 1, '
            f'{ar_brand}, {brand_str}, '
            f'{ar_date}, ">="&DATE({year},1,1), '
            f'{ar_date}, "<"&DATE({year + 1},1,1))'
        )
        batch_updates.append({"range": f"AE{row}", "values": [[formula_1star]]})

        # ── Column AF: 1★ YTD % ──
        formula_1star_pct = f'=IFERROR(ROUND(AE{row}/E{row}*100, 1), 0)'
        batch_updates.append({"range": f"AF{row}", "values": [[formula_1star_pct]]})

        # ── Column AG: 5★ YTD count ──
        formula_5star = (
            f'=COUNTIFS({ar_store}, {store_ref}, {ar_rating}, 5, '
            f'{ar_brand}, {brand_str}, '
            f'{ar_date}, ">="&DATE({year},1,1), '
            f'{ar_date}, "<"&DATE({year + 1},1,1))'
        )
        batch_updates.append({"range": f"AG{row}", "values": [[formula_5star]]})

        # ── Column AH: 5★ YTD % ──
        formula_5star_pct = f'=IFERROR(ROUND(AG{row}/E{row}*100, 1), 0)'
        batch_updates.append({"range": f"AH{row}", "values": [[formula_5star_pct]]})

        # ── Columns AI-CD: Monthly 5★/1★ count + % per month ──
        for m in range(1, 13):
            five_cnt_col, five_pct_col, one_cnt_col, one_pct_col = star_month_cols[m]
            count_col = month_cols[m][0]  # Monthly total count column

            # Date range for this month
            if m == 12:
                next_year, next_month = year + 1, 1
            else:
                next_year, next_month = year, m + 1

            # Monthly 5★ count
            formula_5cnt = (
                f'=COUNTIFS({ar_store}, {store_ref}, {ar_rating}, 5, '
                f'{ar_brand}, {brand_str}, '
                f'{ar_date}, ">="&DATE({year},{m},1), '
                f'{ar_date}, "<"&DATE({next_year},{next_month},1))'
            )
            batch_updates.append({"range": f"{five_cnt_col}{row}", "values": [[formula_5cnt]]})

            # Monthly 5★ %
            formula_5pct = f'=IFERROR(ROUND({five_cnt_col}{row}/{count_col}{row}*100, 1), 0)'
            batch_updates.append({"range": f"{five_pct_col}{row}", "values": [[formula_5pct]]})

            # Monthly 1★ count
            formula_1cnt = (
                f'=COUNTIFS({ar_store}, {store_ref}, {ar_rating}, 1, '
                f'{ar_brand}, {brand_str}, '
                f'{ar_date}, ">="&DATE({year},{m},1), '
                f'{ar_date}, "<"&DATE({next_year},{next_month},1))'
            )
            batch_updates.append({"range": f"{one_cnt_col}{row}", "values": [[formula_1cnt]]})

            # Monthly 1★ %
            formula_1pct = f'=IFERROR(ROUND({one_cnt_col}{row}/{count_col}{row}*100, 1), 0)'
            batch_updates.append({"range": f"{one_pct_col}{row}", "values": [[formula_1pct]]})

        updated += 1

    # ── Write % above 4.5 to a summary cell ──
    # Find a safe row for the summary (row 2 is typically a sub-header)
    # Use cell B2 for the percentage value
    total_stores = len(STORE_ROW_MAP)
    if total_stores > 0:
        # Count stores where B column (current rate) >= 4.5
        store_rows = list(STORE_ROW_MAP.values())
        min_row = min(store_rows)
        max_row = max(store_rows)
        formula_pct = (
            f'=ROUND(COUNTIF(B{min_row}:B{max_row},">="&4.5)'
            f'/COUNTA(B{min_row}:B{max_row})*100, 1)&"% above 4.5"'
        )
        batch_updates.append({"range": "AD2", "values": [[formula_pct]]})

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


# =============================================================================
# Weekly Report Tab
# =============================================================================

def populate_weekly_report_tab(
    spreadsheet: gspread.Spreadsheet,
    report_data: list[dict],
    pct_above: float,
    tab_name: str = "Weekly Report"
) -> int:
    """
    Append weekly metrics to the Weekly Report tab.

    On each run this function:
      1. Creates the tab if it doesn't exist (with headers + filter)
      2. Reads existing rows to find which week labels are already present
      3. Skips the week if it already exists (idempotent / no duplicates)
      4. Inserts new rows RIGHT AFTER the header (row 2) so newest data is first
      5. Re-applies the auto-filter to cover the new data range

    This approach preserves all historical backfilled data.
    """
    from datetime import date as dt_date

    headers = [
        "Year", "Month", "Week", "Brand", "Store", "Current Rate",
        "# Reviews", "Avg Rating",
        "5★ Count", "5★ %", "1★ Count", "1★ %",
        "MTD Avg", "MTD # Reviews", "Wk vs MTD Δ",
        "MTD Avg/Wk", "Wk vs MTD Reviews Δ"
    ]
    num_cols = len(headers)  # 17
    col_letter = chr(ord('A') + num_cols - 1)  # 'Q'

    if not report_data:
        logger.warning("No weekly report data to write")
        return 0

    # ── Derive the week label from report_data ──
    week_start = report_data[0].get("week_start", "")
    week_end = report_data[0].get("week_end", "")

    try:
        ws = dt_date.fromisoformat(week_start)
        we = dt_date.fromisoformat(week_end)
        year_val = we.year
        month_val = we.strftime("%b")
        week_label = f"{ws.strftime('%b %d')} – {we.strftime('%b %d')}"
    except (ValueError, TypeError):
        year_val = ""
        month_val = ""
        week_label = f"{week_start} to {week_end}"

    # ── Get or create the worksheet ──
    is_new_tab = False
    try:
        worksheet = spreadsheet.worksheet(tab_name)
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=tab_name, rows=500, cols=num_cols)
        is_new_tab = True

    # ── Read existing data to check for duplicates ──
    existing_weeks = set()
    if not is_new_tab:
        try:
            existing_data = worksheet.get_all_values()
            # Column C (index 2) = Week label
            for row in existing_data[1:]:  # skip header
                if len(row) > 2 and row[2].strip():
                    existing_weeks.add(row[2].strip())
        except Exception:
            pass

    # Check if this week is already in the sheet
    if week_label in existing_weeks:
        logger.info(f"Weekly Report: week '{week_label}' already exists — skipping (no duplicates)")
        return 0

    # ── Build new rows for this week ──
    new_rows = []
    for store in report_data:
        new_rows.append([
            year_val,
            month_val,
            week_label,
            store["brand"],
            store["store_name"],
            store.get("current_rating", ""),
            store["week_count"],
            store["week_avg"],
            store["week_five_star_count"],
            f'{store["week_five_star_pct"]}%',
            store["week_one_star_count"],
            f'{store["week_one_star_pct"]}%',
            store["mtd_avg"],
            store["mtd_count"],
            store.get("week_vs_mtd_avg", 0.0),
            store.get("mtd_weekly_avg_count", 0.0),
            store.get("week_vs_mtd_count", 0.0),
        ])

    if is_new_tab:
        # Fresh tab: write headers + data
        all_data = [headers] + new_rows
        worksheet.update(range_name="A1", values=all_data, value_input_option="USER_ENTERED")

        # Format header
        worksheet.format(f"A1:{col_letter}1", {
            "textFormat": {"bold": True},
            "backgroundColor": {"red": 0.1, "green": 0.45, "blue": 0.91}
        })
    else:
        # Existing tab: insert rows right after header (row 2) so newest is on top
        # First, clear any existing basic filter (can't insert rows with filter active)
        try:
            worksheet.clear_basic_filter()
        except Exception:
            pass

        # Insert blank rows at position 2 (after header)
        worksheet.insert_rows(new_rows, row=2, value_input_option="USER_ENTERED")

    # ── Re-apply auto-filter on the full range ──
    try:
        worksheet.clear_basic_filter()
    except Exception:
        pass

    total_rows = worksheet.row_count
    # Find actual last row with data
    try:
        all_vals = worksheet.col_values(1)  # Column A
        actual_last = len(all_vals)
    except Exception:
        actual_last = total_rows
    worksheet.set_basic_filter(f"A1:{col_letter}{actual_last}")

    logger.info(f"Weekly Report: appended {len(new_rows)} rows for week '{week_label}' (total rows now: {actual_last})")
    return len(new_rows)


# =============================================================================
# Monthly Report Tab
# =============================================================================

def populate_monthly_report_tab(
    spreadsheet: gspread.Spreadsheet,
    report_rows: list[dict],
    tab_name: str = "Monthly Report"
) -> int:
    """
    Populate the Monthly Report tab with per-store monthly metrics.

    One row per store per month, with Year/Month/Brand/Store filter columns.
    Includes 5★/1★ counts and percentages plus MOM shift.
    """
    headers = [
        "Year", "Month", "Brand", "Store", "Current Rate",
        "# Reviews", "Avg Rating",
        "5★ Count", "5★ %", "1★ Count", "1★ %",
        "MOM Shift"
    ]
    num_cols = len(headers)  # 12

    # Delete and recreate for clean state (same pattern as weekly)
    try:
        worksheet = spreadsheet.worksheet(tab_name)
        spreadsheet.del_worksheet(worksheet)
    except gspread.WorksheetNotFound:
        pass

    worksheet = spreadsheet.add_worksheet(
        title=tab_name,
        rows=max(len(report_rows) + 10, 100),
        cols=num_cols
    )

    if not report_rows:
        logger.warning("No monthly report data to write")
        return 0

    # Build data rows
    data_rows = []
    for row in report_rows:
        data_rows.append([
            row["year"],
            row["month_name"],
            row["brand"],
            row["store_name"],
            row["current_rating"],
            row["review_count"],
            row["avg_rating"],
            row["five_star_count"],
            f'{row["five_star_pct"]}%' if row["five_star_pct"] else "0%",
            row["one_star_count"],
            f'{row["one_star_pct"]}%' if row["one_star_pct"] else "0%",
            row["mom_shift_val"],
        ])

    # Write headers + data
    all_rows = [headers] + data_rows
    worksheet.update(range_name="A1", values=all_rows, value_input_option="USER_ENTERED")

    # Format header row (blue background, bold text — matches weekly)
    col_letter = chr(ord('A') + num_cols - 1)  # 'L'
    header_range = f"A1:{col_letter}1"
    worksheet.format(header_range, {
        "textFormat": {"bold": True},
        "backgroundColor": {"red": 0.1, "green": 0.45, "blue": 0.91}
    })

    # Add auto-filter on full data range (enables Year/Month/Brand/Store filtering)
    total_rows = len(all_rows)
    worksheet.set_basic_filter(f"A1:{col_letter}{total_rows}")

    logger.info(f"Monthly Report: wrote {len(data_rows)} rows across {tab_name}")
    return len(data_rows)
