"""
Cannabis Reviews Dashboard — Streamlit App
==========================================
Interactive dashboard for Inspired Cannabis & brands.
Reads from the same SQLite DB as the Google Sheets pipeline.
"""

import base64
import sys
from calendar import monthrange
from pathlib import Path

# Add project root to path so we can import src modules
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from datetime import date, timedelta

from src import database as db
from src.reports import (
    period_metrics,
    ytd_metrics,
    monthly_metrics,
    mom_shift,
    pct_above_threshold,
)

# =============================================================================
# Config & Brand Colors
# =============================================================================

# Logo
LOGO_PATH = Path(__file__).parent / "assets" / "logo.png"

def get_logo_base64():
    """Read logo file and return as base64 string."""
    if LOGO_PATH.exists():
        with open(LOGO_PATH, "rb") as f:
            return base64.b64encode(f.read()).decode()
    return None

NAVY = "#1B3A5C"
ORANGE = "#E8792B"
WHITE = "#FFFFFF"
LIGHT_GREY = "#F4F6F9"
DARK_TEXT = "#1A1A2E"
SUCCESS = "#2E7D32"
ALERT = "#D32F2F"

BRAND_COLORS = {
    "Inspired Cannabis": "#1B3A5C",
    "Imagine Cannabis": "#6A4C93",
    "Dutch Love": "#2D936C",
    "Cannabis Supply Co.": "#E8792B",
    "Muse Cannabis": "#C44536",
}

st.set_page_config(
    page_title="Cannabis Reviews Dashboard",
    page_icon="🌿",
    layout="wide",
    initial_sidebar_state="expanded"
)

# =============================================================================
# Custom CSS
# =============================================================================

st.markdown(f"""
<style>
    /* Global */
    .stApp {{
        background-color: {LIGHT_GREY};
    }}

    /* Header bar */
    .dashboard-header {{
        background: linear-gradient(135deg, {NAVY} 0%, #2a4d73 100%);
        color: {WHITE};
        padding: 0.6rem 1.5rem;
        border-radius: 10px;
        margin-bottom: 0.8rem;
        display: flex;
        align-items: center;
        justify-content: space-between;
    }}
    .dashboard-header h1 {{
        margin: 0;
        font-size: 1.3rem;
        font-weight: 700;
        letter-spacing: 0.5px;
    }}
    .dashboard-header .subtitle {{
        font-size: 0.75rem;
        opacity: 0.8;
        margin-top: 2px;
    }}

    /* KPI Cards */
    .kpi-card {{
        background: {WHITE};
        border-radius: 10px;
        padding: 0.5rem 0.8rem;
        text-align: center;
        box-shadow: 0 2px 8px rgba(0,0,0,0.06);
        border-top: 3px solid {ORANGE};
        transition: transform 0.2s;
    }}
    .kpi-card:hover {{
        transform: translateY(-2px);
        box-shadow: 0 4px 16px rgba(0,0,0,0.1);
    }}
    .kpi-value {{
        font-size: 1.5rem;
        font-weight: 800;
        color: {NAVY};
        margin: 0.15rem 0;
        line-height: 1;
    }}
    .kpi-label {{
        font-size: 0.65rem;
        color: #666;
        text-transform: uppercase;
        letter-spacing: 1px;
        font-weight: 600;
    }}
    .kpi-delta {{
        font-size: 0.7rem;
        margin-top: 2px;
    }}
    .kpi-delta.positive {{ color: {SUCCESS}; }}
    .kpi-delta.negative {{ color: {ALERT}; }}

    /* Section headers */
    .section-header {{
        color: {NAVY};
        font-size: 1.15rem;
        font-weight: 700;
        margin: 1.5rem 0 0.8rem 0;
        padding-bottom: 0.4rem;
        border-bottom: 2px solid {ORANGE};
        display: inline-block;
    }}

    /* Sidebar */
    [data-testid="stSidebar"] {{
        background: linear-gradient(180deg, {NAVY} 0%, #152d4a 100%);
    }}
    [data-testid="stSidebar"] * {{
        color: {WHITE} !important;
    }}
    [data-testid="stSidebar"] .stSelectbox label,
    [data-testid="stSidebar"] .stMultiSelect label,
    [data-testid="stSidebar"] .stDateInput label {{
        color: rgba(255,255,255,0.85) !important;
        font-weight: 600;
    }}
    /* Date input text must be dark on white background */
    [data-testid="stSidebar"] .stDateInput input {{
        color: #1a1a2e !important;
        background-color: {WHITE} !important;
    }}

    /* Tabs */
    .stTabs [data-baseweb="tab-list"] {{
        gap: 8px;
    }}
    .stTabs [data-baseweb="tab"] {{
        background: {WHITE};
        border-radius: 8px 8px 0 0;
        padding: 8px 24px;
        font-weight: 600;
    }}
    .stTabs [aria-selected="true"] {{
        background: {NAVY} !important;
        color: {WHITE} !important;
    }}

    /* Attention badge */
    .attention-badge {{
        background: {ALERT};
        color: white;
        padding: 4px 12px;
        border-radius: 20px;
        font-size: 0.85rem;
        font-weight: 700;
        display: inline-block;
        margin-left: 8px;
    }}

    /* Table containers */
    .dataframe-container {{
        background: {WHITE};
        border-radius: 12px;
        padding: 1rem;
        box-shadow: 0 2px 8px rgba(0,0,0,0.06);
    }}

    /* Hide Streamlit branding */
    #MainMenu {{ visibility: hidden; }}
    footer {{ visibility: hidden; }}
    header {{ visibility: hidden; }}
</style>
""", unsafe_allow_html=True)


# =============================================================================
# Data Loading
# =============================================================================

@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_data():
    """Load all data from SQLite DB."""
    db.init_db()
    stores = db.get_all_stores()
    reviews = db.get_reviews()
    return stores, reviews


@st.cache_data(ttl=300)
def get_reviews_df():
    """Get reviews as a pandas DataFrame."""
    _, reviews = load_data()
    if not reviews:
        return pd.DataFrame(columns=[
            "review_date", "brand", "store_name", "rating",
            "review_text", "reviewer_name", "owner_response"
        ])

    df = pd.DataFrame(reviews)
    if "review_date" in df.columns:
        df["review_date"] = pd.to_datetime(df["review_date"], errors="coerce")
    return df


@st.cache_data(ttl=300)
def get_stores_df():
    """Get stores as a pandas DataFrame."""
    stores, _ = load_data()
    if not stores:
        return pd.DataFrame(columns=["place_id", "brand", "store_name", "current_rating"])
    return pd.DataFrame(stores)


# =============================================================================
# Sidebar Filters
# =============================================================================

def render_sidebar():
    """Render the sidebar with filters."""
    with st.sidebar:
        logo_b64 = get_logo_base64()
        if logo_b64:
            st.markdown(f"""
            <div style="text-align: center; padding: 1rem 0 1rem 0;">
                <img src="data:image/png;base64,{logo_b64}" style="width: 120px; height: 120px; border-radius: 50%;" />
                <div style="font-size: 0.9rem; font-weight: 700; letter-spacing: 1px; margin-top: 8px;">
                    REVIEW ANALYTICS
                </div>
            </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown(f"""
            <div style="text-align: center; padding: 1rem 0 1.5rem 0;">
                <div style="font-size: 2rem;">🌿</div>
                <div style="font-size: 1.1rem; font-weight: 700; letter-spacing: 1px;">
                    REVIEW ANALYTICS
                </div>
            </div>
            """, unsafe_allow_html=True)
        st.markdown("---")

        # Refresh button
        if st.button("🔄 Refresh Data", use_container_width=True, key="refresh_btn"):
            st.cache_data.clear()
            st.rerun()

        st.markdown("---")

        # Brand filter
        stores_df = get_stores_df()
        all_brands = sorted(stores_df["brand"].unique().tolist()) if not stores_df.empty else []
        selected_brands = st.multiselect(
            "🏪 Brand",
            options=all_brands,
            default=all_brands,
            key="brand_filter"
        )

        # Date range filter
        reviews_df = get_reviews_df()
        if not reviews_df.empty and "review_date" in reviews_df.columns:
            min_date = reviews_df["review_date"].min()
            max_date = reviews_df["review_date"].max()
            if pd.notna(min_date) and pd.notna(max_date):
                date_range = st.date_input(
                    "📅 Date Range",
                    value=(min_date.date(), max_date.date()),
                    min_value=min_date.date(),
                    max_value=max_date.date(),
                    key="date_filter"
                )
            else:
                date_range = None
        else:
            date_range = None

        # Rating filter
        min_rating = st.select_slider(
            "⭐ Minimum Rating",
            options=[1, 2, 3, 4, 5],
            value=1,
            key="rating_filter"
        )

        # Search
        search_term = st.text_input(
            "🔍 Search Reviews",
            placeholder="Type to search...",
            key="search_filter"
        )

        st.markdown("---")
        st.markdown(f"""
        <div style="text-align: center; font-size: 0.7rem; opacity: 0.5;">
            Last updated: {date.today().strftime('%b %d, %Y')}
        </div>
        """, unsafe_allow_html=True)

    return selected_brands, date_range, min_rating, search_term


def apply_filters(df, selected_brands, date_range, min_rating, search_term):
    """Apply sidebar filters to a DataFrame."""
    filtered = df.copy()

    if selected_brands:
        filtered = filtered[filtered["brand"].isin(selected_brands)]

    if date_range and len(date_range) == 2 and "review_date" in filtered.columns:
        start, end = date_range
        filtered = filtered[
            (filtered["review_date"] >= pd.Timestamp(start)) &
            (filtered["review_date"] <= pd.Timestamp(end))
        ]

    if "rating" in filtered.columns:
        filtered = filtered[filtered["rating"] >= min_rating]

    if search_term and "review_text" in filtered.columns:
        filtered = filtered[
            filtered["review_text"].fillna("").str.contains(search_term, case=False, na=False) |
            filtered["store_name"].fillna("").str.contains(search_term, case=False, na=False) |
            filtered["reviewer_name"].fillna("").str.contains(search_term, case=False, na=False)
        ]

    return filtered


# =============================================================================
# KPI Scorecard Component
# =============================================================================

def render_kpi_card(label, value, delta=None, delta_label="", prefix="", suffix=""):
    """Render a single KPI card."""
    delta_html = ""
    if delta is not None:
        delta_class = "positive" if delta >= 0 else "negative"
        delta_icon = "↑" if delta >= 0 else "↓"
        delta_html = f'<div class="kpi-delta {delta_class}">{delta_icon} {abs(delta):.1f} {delta_label}</div>'

    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-label">{label}</div>
        <div class="kpi-value">{prefix}{value}{suffix}</div>
        {delta_html}
    </div>
    """, unsafe_allow_html=True)


# =============================================================================
# Page 1: Executive Overview
# =============================================================================

def page_overview(reviews_df, stores_df, selected_brands):
    """Render the Executive Overview page."""

    # Filter stores by selected brands
    filtered_stores = stores_df[stores_df["brand"].isin(selected_brands)] if selected_brands else stores_df

    # ── KPI Row ──────────────────────────────────────────────────────────
    total_stores = len(filtered_stores)
    total_reviews = len(reviews_df)

    rated_stores = filtered_stores[filtered_stores["current_rating"].notna()]
    avg_rating = rated_stores["current_rating"].mean() if not rated_stores.empty else 0

    five_star = len(reviews_df[reviews_df["rating"] == 5]) if not reviews_df.empty else 0
    five_star_pct = (five_star / total_reviews * 100) if total_reviews > 0 else 0

    above_45 = len(rated_stores[rated_stores["current_rating"] >= 4.5]) if not rated_stores.empty else 0
    above_45_pct = (above_45 / len(rated_stores) * 100) if not rated_stores.empty else 0

    responded = len(reviews_df[reviews_df["owner_response"].notna() & (reviews_df["owner_response"] != "")]) if not reviews_df.empty else 0
    response_pct = (responded / total_reviews * 100) if total_reviews > 0 else 0

    cols = st.columns(6)
    with cols[0]:
        render_kpi_card("Total Stores", total_stores)
    with cols[1]:
        render_kpi_card("Avg Rating", f"{avg_rating:.2f}", suffix=" ★")
    with cols[2]:
        render_kpi_card("Total Reviews", total_reviews)
    with cols[3]:
        render_kpi_card("5-Star Reviews", f"{five_star_pct:.0f}", suffix="%")
    with cols[4]:
        render_kpi_card("Above 4.5 ★", f"{above_45_pct:.0f}", suffix="%")
    with cols[5]:
        render_kpi_card("Response Rate", f"{response_pct:.0f}", suffix="%")

    st.markdown("")

    # ── Charts Row 1 ──────────────────────────────────────────────────────
    chart_col1, chart_col2 = st.columns(2)

    with chart_col1:
        st.markdown('<div class="section-header">Average Rating by Brand</div>', unsafe_allow_html=True)
        if not filtered_stores.empty:
            brand_ratings = filtered_stores.groupby("brand")["current_rating"].mean().reset_index()
            brand_ratings = brand_ratings.sort_values("current_rating", ascending=True)

            fig = go.Figure()
            for _, row in brand_ratings.iterrows():
                color = BRAND_COLORS.get(row["brand"], NAVY)
                fig.add_trace(go.Bar(
                    x=[row["current_rating"]],
                    y=[row["brand"]],
                    orientation="h",
                    marker_color=color,
                    name=row["brand"],
                    text=[f"{row['current_rating']:.2f} ★"],
                    textposition="auto",
                    showlegend=False,
                ))

            fig.update_layout(
                height=220,
                margin=dict(l=0, r=20, t=10, b=0),
                xaxis=dict(range=[0, 5], title="Average Rating"),
                yaxis=dict(title=""),
                plot_bgcolor="white",
                paper_bgcolor="white",
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No store data available")

    with chart_col2:
        st.markdown('<div class="section-header">Monthly Review Volume</div>', unsafe_allow_html=True)
        if not reviews_df.empty and "review_date" in reviews_df.columns:
            monthly = reviews_df.copy()
            monthly["month"] = monthly["review_date"].dt.to_period("M").astype(str)
            monthly_counts = monthly.groupby(["month", "brand"]).size().reset_index(name="count")

            fig = px.line(
                monthly_counts,
                x="month",
                y="count",
                color="brand",
                color_discrete_map=BRAND_COLORS,
                markers=True,
            )
            fig.update_layout(
                height=220,
                margin=dict(l=0, r=20, t=10, b=0),
                xaxis_title="Month",
                yaxis_title="# Reviews",
                plot_bgcolor="white",
                paper_bgcolor="white",
                legend=dict(orientation="h", y=-0.3),
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No review data available")

    # ── Charts Row 2 ──────────────────────────────────────────────────────
    chart_col3, chart_col4 = st.columns(2)

    with chart_col3:
        st.markdown('<div class="section-header">Star Distribution</div>', unsafe_allow_html=True)
        if not reviews_df.empty:
            star_counts = reviews_df["rating"].value_counts().sort_index()
            star_labels = [f"{int(s)} ★" for s in star_counts.index]
            colors = ["#D32F2F", "#FF7043", "#FFB74D", "#AED581", "#4CAF50"]

            fig = go.Figure(data=[go.Pie(
                labels=star_labels,
                values=star_counts.values,
                hole=0.5,
                marker_colors=colors[:len(star_counts)],
                textinfo="percent+value",
                textfont=dict(size=12),
            )])
            fig.update_layout(
                height=220,
                margin=dict(l=20, r=20, t=10, b=10),
                paper_bgcolor="white",
                legend=dict(orientation="h", y=-0.15),
                annotations=[dict(
                    text=f"{total_reviews}<br>Total",
                    x=0.5, y=0.5,
                    font_size=16, font_color=NAVY,
                    showarrow=False
                )]
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No review data available")

    with chart_col4:
        st.markdown('<div class="section-header">Response Rate by Brand</div>', unsafe_allow_html=True)
        if not reviews_df.empty:
            response_data = []
            for brand in reviews_df["brand"].unique():
                brand_reviews = reviews_df[reviews_df["brand"] == brand]
                total = len(brand_reviews)
                responded_count = len(brand_reviews[
                    brand_reviews["owner_response"].notna() &
                    (brand_reviews["owner_response"] != "")
                ])
                rate = (responded_count / total * 100) if total > 0 else 0
                response_data.append({
                    "brand": brand,
                    "rate": rate,
                    "responded": responded_count,
                    "total": total
                })

            resp_df = pd.DataFrame(response_data).sort_values("rate", ascending=True)

            fig = go.Figure()
            for _, row in resp_df.iterrows():
                color = BRAND_COLORS.get(row["brand"], NAVY)
                fig.add_trace(go.Bar(
                    x=[row["rate"]],
                    y=[row["brand"]],
                    orientation="h",
                    marker_color=color,
                    text=[f"{row['rate']:.0f}% ({row['responded']}/{row['total']})"],
                    textposition="auto",
                    showlegend=False,
                ))

            fig.update_layout(
                height=220,
                margin=dict(l=0, r=20, t=10, b=0),
                xaxis=dict(range=[0, 100], title="Response Rate (%)"),
                yaxis=dict(title=""),
                plot_bgcolor="white",
                paper_bgcolor="white",
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No review data available")

    # ── Store Performance Table (mirrors Online Reviews Sheet) ─────────────
    st.markdown('<div class="section-header">Store Performance</div>', unsafe_allow_html=True)

    if not filtered_stores.empty:
        month_names = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                       "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

        perf_data = []
        for _, store in filtered_stores.iterrows():
            pid = store["place_id"]

            # Current rating (from store metadata)
            cur_rating = store.get("current_rating")

            # 2025 full-year metrics (summary only, no monthly breakdown)
            y2025 = ytd_metrics(pid, 2025)
            y2025_avg = y2025["avg_rating"] if y2025["review_count"] > 0 else None
            y2025_total = y2025["review_count"]

            # 2026 full-year metrics
            y2026 = ytd_metrics(pid, 2026)
            y2026_avg = y2026["avg_rating"] if y2026["review_count"] > 0 else None
            y2026_total = y2026["review_count"]

            row_data = {
                "Brand": store["brand"],
                "Store": store["store_name"],
                "Current Rate": cur_rating,
                "2025 Avg": y2025_avg,
                "2025 Total": y2025_total,
                "2026 Avg": y2026_avg,
                "2026 Total": y2026_total,
            }

            # 2026 monthly breakdown (counts only — no avg columns)
            for m in range(1, 13):
                mm = monthly_metrics(pid, 2026, m)
                row_data[f"{month_names[m-1]} 2026"] = mm["review_count"] if mm["review_count"] > 0 else 0
                # Monthly 5★/1★ breakdown
                row_data[f"{month_names[m-1]} 5★#"] = mm["five_star_count"]
                row_data[f"{month_names[m-1]} 5★%"] = mm["five_star_pct"]
                row_data[f"{month_names[m-1]} 1★#"] = mm["one_star_count"]
                row_data[f"{month_names[m-1]} 1★%"] = mm["one_star_pct"]

            # Star distributions (2026 — percentages only)
            row_data["1★ %"] = y2026["one_star_pct"]
            row_data["5★ %"] = y2026["five_star_pct"]

            # MOM shift (latest month with data)
            shift = mom_shift(pid, 2026, max(1, min(date.today().month, 12)))
            row_data["MOM Shift"] = shift if shift is not None else 0.0

            perf_data.append(row_data)

        perf_df = pd.DataFrame(perf_data)

        # ── Inline Filters Above Table ──────────────────────────────────
        fcol1, fcol2, fcol3, fcol4 = st.columns([2, 2, 1.5, 1.5])
        with fcol1:
            table_brands = sorted(perf_df["Brand"].unique().tolist())
            filter_brand = st.multiselect(
                "🏷️ Filter by Brand",
                options=table_brands,
                default=table_brands,
                key="perf_brand_filter"
            )
        with fcol2:
            filter_store = st.text_input(
                "🔍 Search Store",
                value="",
                placeholder="Type store name...",
                key="perf_store_search"
            )
        with fcol3:
            filter_min_rating = st.selectbox(
                "⭐ Min Rating",
                options=[0, 3.0, 3.5, 4.0, 4.5, 4.8],
                index=0,
                format_func=lambda x: "All" if x == 0 else f"≥ {x:.1f}",
                key="perf_min_rating"
            )
        with fcol4:
            sort_options = {
                "Brand": "Brand",
                "Store": "Store",
                "Current Rate ↓": "Current Rate",
                "2025 Avg ↓": "2025 Avg",
                "2026 Avg ↓": "2026 Avg",
                "2026 Total ↓": "2026 Total",
            }
            sort_choice = st.selectbox(
                "↕️ Sort by",
                options=list(sort_options.keys()),
                index=0,
                key="perf_sort"
            )

        # Apply inline filters
        display_df = perf_df.copy()
        if filter_brand:
            display_df = display_df[display_df["Brand"].isin(filter_brand)]
        if filter_store.strip():
            display_df = display_df[
                display_df["Store"].str.contains(filter_store.strip(), case=False, na=False)
            ]
        if filter_min_rating > 0:
            display_df = display_df[
                display_df["Current Rate"].fillna(0) >= filter_min_rating
            ]

        # Apply sort
        sort_col = sort_options[sort_choice]
        ascending = sort_choice in ("Brand", "Store")
        display_df = display_df.sort_values(
            sort_col, ascending=ascending, na_position="last"
        ).reset_index(drop=True)

        st.caption(f"Showing {len(display_df)} of {len(perf_df)} stores")

        # ── Column Visibility Toggles ──────────────────────────────────
        month_names_full = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                            "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

        # Define column groups
        col_groups = {
            "Brand":            (["Brand"], True),
            "Store":            (["Store"], True),
            "Current Rate":     (["Current Rate"], True),
            "MOM Shift":        (["MOM Shift"], True),
            "2025 Summary":     (["2025 Avg", "2025 Total"], True),
            "2026 Summary":     (["2026 Avg", "2026 Total"], True),
            "2026 Monthly":     ([f"{mn} 2026" for mn in month_names_full], True),
            "Star Distribution":(["1★ %", "5★ %"], True),
            "Monthly Stars":    ([f"{mn} {s}" for mn in month_names_full for s in ["5★#", "5★%", "1★#", "1★%"]], False),
        }

        with st.expander("⚙️ Show / Hide Columns", expanded=False):
            row1 = st.columns(len(col_groups))
            toggles = {}
            for i, (label, (_, default)) in enumerate(col_groups.items()):
                with row1[i]:
                    toggles[label] = st.checkbox(label, value=default, key=f"show_{label}")

        # Build visible columns based on toggles
        visible_cols = []
        for label, (cols, _) in col_groups.items():
            if toggles.get(label, True):
                visible_cols += cols

        display_df = display_df[[c for c in visible_cols if c in display_df.columns]]

        # Build column config
        col_config = {
            "Current Rate": st.column_config.NumberColumn(format="%.1f"),
            "MOM Shift": st.column_config.NumberColumn(format="%+.2f"),
            "2025 Avg": st.column_config.NumberColumn(format="%.1f"),
            "2025 Total": st.column_config.NumberColumn(format="%d"),
            "2026 Avg": st.column_config.NumberColumn(format="%.1f"),
            "2026 Total": st.column_config.NumberColumn(format="%d"),
            "1★ %": st.column_config.NumberColumn(format="%.1f%%"),
            "5★ %": st.column_config.NumberColumn(format="%.1f%%"),
        }
        # Add star columns config
        for mn in month_names_full:
            col_config[f"{mn} 5★%"] = st.column_config.NumberColumn(format="%.1f%%")
            col_config[f"{mn} 1★%"] = st.column_config.NumberColumn(format="%.1f%%")

        # ── Brand-based row coloring ──────────────────────────────────
        brand_colors = {
            "Inspired Cannabis":    "background-color: rgba(255, 228, 196, 0.35)",  # light peach
            "Imagine Cannabis":     "background-color: rgba(200, 255, 200, 0.35)",  # light green
            "Dutch Love":           "background-color: rgba(220, 200, 255, 0.35)",  # light purple
            "Cannabis Supply Co.":  "background-color: rgba(200, 230, 255, 0.35)",  # light blue
            "Muse Cannabis":        "background-color: rgba(255, 220, 240, 0.35)",  # light pink
        }

        # We need the Brand column in the data for styling even if hidden
        style_df = perf_df.loc[display_df.index].copy()
        brands_for_style = style_df["Brand"]

        def color_rows(row):
            brand = brands_for_style.get(row.name, "")
            style = brand_colors.get(brand, "")
            return [style] * len(row)

        styled = display_df.style.apply(color_rows, axis=1)

        st.dataframe(
            styled,
            use_container_width=True,
            hide_index=True,
            height=400,
            column_config=col_config,
        )

        # CSV Export for performance table
        csv = perf_df.to_csv(index=False)
        st.download_button(
            label="📥 Export Performance Data",
            data=csv,
            file_name=f"store_performance_{date.today().isoformat()}.csv",
            mime="text/csv",
            key="perf_export"
        )
    else:
        st.info("No stores match the selected filters")


# =============================================================================
# Page 2: All Reviews
# =============================================================================

def page_all_reviews(reviews_df):
    """Render the All Reviews page."""
    st.markdown(f"""
    <div class="dashboard-header">
        <div>
            <h1>📝 All Reviews</h1>
            <div class="subtitle">Complete review log with search and export • {len(reviews_df)} reviews</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    if reviews_df.empty:
        st.warning("No reviews match the current filters.")
        return

    # Format display DataFrame
    display_df = reviews_df.copy()
    display_df = display_df.sort_values("review_date", ascending=False)

    # Star rating display
    display_df["Rating"] = display_df["rating"].apply(lambda r: "⭐" * int(r) if pd.notna(r) else "")

    # Response status
    display_df["Status"] = display_df["owner_response"].apply(
        lambda r: "✅ Responded" if pd.notna(r) and str(r).strip() else "⚠️ No Response"
    )

    # Format date
    display_df["Date"] = display_df["review_date"].dt.strftime("%Y-%m-%d")

    # Select and rename columns for display
    display_cols = display_df[[
        "Date", "brand", "store_name", "Rating", "review_text", "Status", "owner_response"
    ]].rename(columns={
        "brand": "Brand",
        "store_name": "Store",
        "review_text": "Review Text",
        "owner_response": "Owner Response",
    })

    # Quick stats row
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        render_kpi_card("Total Reviews", len(display_df))
    with col2:
        avg = display_df["rating"].mean() if not display_df.empty else 0
        render_kpi_card("Avg Rating", f"{avg:.2f}", suffix=" ★")
    with col3:
        responded = len(display_df[display_df["Status"] == "✅ Responded"])
        render_kpi_card("Responded", responded)
    with col4:
        pending = len(display_df[display_df["Status"] == "⚠️ No Response"])
        render_kpi_card("Pending Response", pending)

    st.markdown("")

    # Table
    st.dataframe(
        display_cols,
        use_container_width=True,
        hide_index=True,
        height=400,
        column_config={
            "Review Text": st.column_config.TextColumn(width="large"),
            "Owner Response": st.column_config.TextColumn(width="medium"),
        }
    )

    # CSV Export
    csv = display_cols.to_csv(index=False)
    st.download_button(
        label="📥 Export to CSV",
        data=csv,
        file_name=f"all_reviews_{date.today().isoformat()}.csv",
        mime="text/csv",
    )


# =============================================================================
# Page 3: Needs Attention
# =============================================================================

def page_needs_attention(reviews_df):
    """Render the Needs Attention page (1-2 star reviews)."""
    negative_df = reviews_df[reviews_df["rating"] <= 2].copy() if not reviews_df.empty else pd.DataFrame()

    # Filter to unresponded only by default
    all_neg_count = len(negative_df)
    unresponded_df = negative_df[
        negative_df["owner_response"].isna() |
        (negative_df["owner_response"] == "")
    ].copy() if not negative_df.empty else pd.DataFrame()
    unresponded_count = len(unresponded_df)

    st.markdown(f"""
    <div class="dashboard-header" style="background: linear-gradient(135deg, #8B0000 0%, #D32F2F 100%);">
        <div>
            <h1>🚨 Needs Attention</h1>
            <div class="subtitle">Unresponded 1★ and 2★ reviews requiring action</div>
        </div>
        <div>
            <span class="attention-badge">{unresponded_count} unresponded</span>
        </div>
    </div>
    """, unsafe_allow_html=True)

    if negative_df.empty:
        st.success("🎉 No negative reviews to address! All filtered reviews are 3★ or above.")
        return

    # Toggle to show all vs unresponded only
    show_all = st.toggle("Show all negative reviews (including responded)", value=False, key="needs_attn_toggle")
    working_df = negative_df if show_all else unresponded_df

    # Quick stats
    col1, col2, col3 = st.columns(3)
    with col1:
        render_kpi_card("Total Negative", all_neg_count)
    with col2:
        responded = all_neg_count - unresponded_count
        rate = (responded / all_neg_count * 100) if all_neg_count > 0 else 0
        render_kpi_card("Response Rate", f"{rate:.0f}", suffix="%")
    with col3:
        st.markdown(f"""
        <a href="#unresponded-reviews" style="text-decoration: none; color: inherit; display: block;">
            <div class="kpi-card" style="cursor: pointer; transition: transform 0.2s, box-shadow 0.2s;" 
                 onmouseover="this.style.transform='scale(1.03)'; this.style.boxShadow='0 4px 16px rgba(211,47,47,0.3)';"
                 onmouseout="this.style.transform='scale(1)'; this.style.boxShadow='';">
                <div class="kpi-label">⚠️ UNRESPONDED</div>
                <div class="kpi-value" style="color: #D32F2F;">{unresponded_count}</div>
                <div style="font-size: 0.7rem; color: #999; margin-top: 4px;">Click to view ↓</div>
            </div>
        </a>
        """, unsafe_allow_html=True)

    st.markdown("")

    if working_df.empty:
        st.success("🎉 All negative reviews have been responded to!")
        return

    # Negative reviews by brand
    st.markdown('<div class="section-header">Negative Reviews by Brand</div>', unsafe_allow_html=True)
    brand_neg = working_df.groupby("brand").size().reset_index(name="count").sort_values("count", ascending=False)

    fig = px.bar(
        brand_neg,
        x="brand",
        y="count",
        color="brand",
        color_discrete_map=BRAND_COLORS,
        text="count",
    )
    fig.update_layout(
        height=250,
        margin=dict(l=0, r=20, t=10, b=0),
        xaxis_title="",
        yaxis_title="# Negative Reviews",
        plot_bgcolor="white",
        paper_bgcolor="white",
        showlegend=False,
    )
    st.plotly_chart(fig, use_container_width=True)

    # Negative reviews by location
    st.markdown('<div class="section-header">Negative Reviews by Location</div>', unsafe_allow_html=True)
    loc_neg = working_df.groupby(["store_name", "brand"]).size().reset_index(name="count").sort_values("count", ascending=True)

    if not loc_neg.empty:
        fig_loc = go.Figure()
        for _, row in loc_neg.iterrows():
            color = BRAND_COLORS.get(row["brand"], NAVY)
            fig_loc.add_trace(go.Bar(
                x=[row["count"]],
                y=[row["store_name"]],
                orientation="h",
                marker_color=color,
                name=row["brand"],
                text=[f"{row['count']}"],
                textposition="auto",
                showlegend=False,
                hovertemplate=f"{row['brand']} — {row['store_name']}<br>%{{x}} reviews<extra></extra>",
            ))

        chart_height = max(300, len(loc_neg) * 28)
        fig_loc.update_layout(
            height=chart_height,
            margin=dict(l=0, r=20, t=10, b=0),
            xaxis=dict(title="# Negative Reviews"),
            yaxis=dict(title="", tickfont=dict(size=11)),
            plot_bgcolor="white",
            paper_bgcolor="white",
        )
        st.plotly_chart(fig_loc, use_container_width=True)

    # Table with scroll anchor
    st.markdown('<div id="unresponded-reviews" class="section-header">Review Details</div>', unsafe_allow_html=True)

    display_df = working_df.sort_values("review_date", ascending=False).copy()
    display_df["Rating"] = display_df["rating"].apply(lambda r: "⭐" * int(r) if pd.notna(r) else "")
    display_df["Status"] = display_df["owner_response"].apply(
        lambda r: "✅ Responded" if pd.notna(r) and str(r).strip() else "⚠️ No Response"
    )
    display_df["Date"] = display_df["review_date"].dt.strftime("%Y-%m-%d")

    # Column-aligned filter dropdowns
    f1, f2, f3, f4 = st.columns([1.2, 1.5, 1.5, 1])
    with f1:
        display_df["_month"] = display_df["review_date"].dt.to_period("M").astype(str)
        month_options = ["All Months"] + sorted(display_df["_month"].unique().tolist(), reverse=True)
        sel_month = st.selectbox("Date", month_options, index=0, key="attn_date", label_visibility="collapsed")
    with f2:
        brand_options = ["All Brands"] + sorted(display_df["brand"].unique().tolist())
        sel_brand = st.selectbox("Brand", brand_options, index=0, key="attn_brand2", label_visibility="collapsed")
    with f3:
        store_options = ["All Stores"] + sorted(display_df["store_name"].unique().tolist())
        sel_store = st.selectbox("Store", store_options, index=0, key="attn_store2", label_visibility="collapsed")
    with f4:
        rating_options = ["All Ratings", "⭐", "⭐⭐"]
        sel_rating = st.selectbox("Rating", rating_options, index=0, key="attn_rating2", label_visibility="collapsed")

    # Apply column filters
    filtered = display_df.copy()
    if sel_month != "All Months":
        filtered = filtered[filtered["_month"] == sel_month]
    if sel_brand != "All Brands":
        filtered = filtered[filtered["brand"] == sel_brand]
    if sel_store != "All Stores":
        filtered = filtered[filtered["store_name"] == sel_store]
    if sel_rating != "All Ratings":
        filtered = filtered[filtered["Rating"] == sel_rating]

    display_cols = filtered[[
        "Date", "brand", "store_name", "Rating", "review_text", "Status", "owner_response"
    ]].rename(columns={
        "brand": "Brand",
        "store_name": "Store",
        "review_text": "Review Text",
        "owner_response": "Owner Response",
    })

    st.caption(f"Showing {len(display_cols)} of {len(display_df)} reviews")

    st.dataframe(
        display_cols,
        use_container_width=True,
        hide_index=True,
        height=400,
        column_config={
            "Review Text": st.column_config.TextColumn(width="large"),
            "Owner Response": st.column_config.TextColumn(width="medium"),
        }
    )

    # CSV Export
    csv = display_cols.to_csv(index=False)
    st.download_button(
        label="📥 Export Needs Attention",
        data=csv,
        file_name=f"needs_attention_{date.today().isoformat()}.csv",
        mime="text/csv",
        key="needs_attention_export"
    )


# =============================================================================
# Main App
# =============================================================================

def page_weekly_report(reviews_df, stores_df):
    """Render the Weekly Report page."""
    st.markdown('<div class="section-header">Weekly Report</div>', unsafe_allow_html=True)

    from src.reports import period_metrics, pct_above_threshold

    today = date.today()

    # ── Build available weeks (Mon-Sun) going back ~12 weeks ──
    d = today
    while d.weekday() != 0:        # align to Monday
        d -= timedelta(days=1)
    all_available_weeks = []
    for i in range(12):
        w_start = d - timedelta(weeks=i)
        w_end = w_start + timedelta(days=6)
        label = f"{w_start.strftime('%b %d')} – {w_end.strftime('%b %d')}"
        all_available_weeks.append((label, w_start, w_end))
    all_available_weeks.reverse()  # oldest first

    default_weeks = [w[0] for w in all_available_weeks[-5:]]  # last 5 weeks

    # ── Filter Row ──────────────────────────────────────────────────
    fc1, fc2, fc3, fc4 = st.columns([3, 2, 2, 1])

    with fc1:
        selected_week_labels = st.multiselect(
            "📅 Weeks",
            options=[w[0] for w in all_available_weeks],
            default=default_weeks,
            key="weekly_week_select"
        )

    with fc2:
        all_brands = sorted(stores_df["brand"].unique().tolist())
        selected_brands_w = st.multiselect(
            "🏷️ Brand",
            options=all_brands,
            default=all_brands,
            key="weekly_brand_filter"
        )

    with fc3:
        if selected_brands_w:
            available_stores = sorted(
                stores_df[stores_df["brand"].isin(selected_brands_w)]["store_name"].unique().tolist()
            )
        else:
            available_stores = sorted(stores_df["store_name"].unique().tolist())

        selected_stores = st.multiselect(
            "🏪 Store",
            options=available_stores,
            default=available_stores,
            key="weekly_store_filter"
        )

    with fc4:
        min_reviews = st.selectbox(
            "📝 Min Reviews",
            options=[0, 1, 2, 5, 10],
            index=0,
            format_func=lambda x: "All" if x == 0 else f"≥ {x}",
            key="weekly_min_reviews"
        )

    # ── Resolve selected weeks to date ranges ──
    week_lookup = {w[0]: (w[1], w[2]) for w in all_available_weeks}
    weeks = [week_lookup[label] for label in selected_week_labels if label in week_lookup]

    if not weeks:
        st.info("Select at least one week to view data")
        return

    range_start = min(w[0] for w in weeks)
    range_end = max(w[1] for w in weeks)

    # ── Filter stores ──
    filtered_stores = stores_df.copy()
    if selected_brands_w:
        filtered_stores = filtered_stores[filtered_stores["brand"].isin(selected_brands_w)]
    if selected_stores:
        filtered_stores = filtered_stores[filtered_stores["store_name"].isin(selected_stores)]

    # ── Build weekly data ──
    mtd_start = date(range_end.year, range_end.month, 1)
    all_weekly_data = []

    for w_start, w_end in weeks:
        week_label = f"{w_start.strftime('%b %d')} – {w_end.strftime('%b %d')}"

        for _, store in filtered_stores.iterrows():
            pid = store["place_id"]
            weekly = period_metrics(pid, w_start, w_end)
            mtd = period_metrics(pid, mtd_start, range_end)

            all_weekly_data.append({
                "Week": week_label,
                "Brand": store["brand"],
                "Store": store["store_name"],
                "Current Rate": store.get("current_rating"),
                "# Reviews": weekly["review_count"],
                "Avg Rating": weekly["avg_rating"],
                "5★ Count": weekly["five_star_count"],
                "5★ %": weekly["five_star_pct"],
                "1★ Count": weekly["one_star_count"],
                "1★ %": weekly["one_star_pct"],
                "MTD Avg": mtd["avg_rating"],
                "MTD Reviews": mtd["review_count"],
            })

    if not all_weekly_data:
        st.info("No data for the selected filters")
        return

    weekly_df = pd.DataFrame(all_weekly_data)

    # Apply min reviews filter
    if min_reviews > 0:
        weekly_df = weekly_df[weekly_df["# Reviews"] >= min_reviews]

    # ── KPI Cards ──
    above_pct = pct_above_threshold(4.5)
    total_reviews = weekly_df["# Reviews"].sum()
    avg_rating = weekly_df.loc[weekly_df["# Reviews"] > 0, "Avg Rating"].mean()
    stores_shown = weekly_df[["Brand", "Store"]].drop_duplicates().shape[0]

    kc1, kc2, kc3, kc4 = st.columns(4)
    with kc1:
        render_kpi_card("Total Reviews", int(total_reviews))
    with kc2:
        render_kpi_card("Avg Rating", f"{avg_rating:.2f}" if pd.notna(avg_rating) else "—")
    with kc3:
        render_kpi_card("% Above 4.5", f"{above_pct}%")
    with kc4:
        render_kpi_card("Stores", stores_shown)

    # ── Charts ──────────────────────────────────────────────────────
    chart_colors = {
        "primary": "#4FC3F7",
        "secondary": "#81C784",
        "negative": "#E57373",
        "accent": "#FFD54F",
        "text": "#E0E0E0",
        "bg": "rgba(0,0,0,0)",
        "grid": "rgba(255,255,255,0.08)",
    }
    chart_layout_w = dict(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#E0E0E0", family="Inter, sans-serif"),
        margin=dict(l=40, r=20, t=30, b=30),
        height=220,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    grid_c = "rgba(255,255,255,0.08)"

    # Aggregate weekly data for charts
    week_agg = weekly_df.groupby("Week", sort=False).agg({
        "# Reviews": "sum",
        "Avg Rating": lambda x: x[weekly_df.loc[x.index, "# Reviews"] > 0].mean() if (weekly_df.loc[x.index, "# Reviews"] > 0).any() else 0,
        "5★ Count": "sum",
        "1★ Count": "sum",
    }).reset_index()

    ch1, ch2 = st.columns(2)

    with ch1:
        # Chart 1: Weekly Review Volume
        fig1 = go.Figure()
        fig1.add_trace(go.Bar(
            x=week_agg["Week"],
            y=week_agg["# Reviews"],
            marker_color=chart_colors["primary"],
            marker_line_width=0,
            hovertemplate="<b>%{x}</b><br>Reviews: %{y}<extra></extra>",
        ))
        fig1.update_layout(**chart_layout_w, title=dict(text="📊 Weekly Review Volume", font=dict(size=14)))
        fig1.update_xaxes(gridcolor=grid_c)
        fig1.update_yaxes(gridcolor=grid_c, title_text="# Reviews")
        st.plotly_chart(fig1, use_container_width=True)

    with ch2:
        # Chart 2: Avg Rating Trend
        valid_weeks = week_agg[week_agg["Avg Rating"] > 0]
        fig2 = go.Figure()
        fig2.add_trace(go.Scatter(
            x=valid_weeks["Week"],
            y=valid_weeks["Avg Rating"],
            mode="lines+markers",
            line=dict(color=chart_colors["secondary"], width=3),
            marker=dict(size=8, color=chart_colors["secondary"], line=dict(width=2, color="white")),
            hovertemplate="<b>%{x}</b><br>Avg: %{y:.2f}<extra></extra>",
        ))
        # Add 4.5 goal line
        fig2.add_hline(y=4.5, line_dash="dot", line_color=chart_colors["accent"],
                       annotation_text="4.5 Goal", annotation_font_color=chart_colors["accent"])
        fig2.update_layout(**chart_layout_w, title=dict(text="📈 Avg Rating Trend", font=dict(size=14)))
        fig2.update_xaxes(gridcolor=grid_c)
        fig2.update_yaxes(gridcolor=grid_c, title_text="Avg Rating", range=[1, 5.2])
        st.plotly_chart(fig2, use_container_width=True)

    ch3, ch4 = st.columns(2)

    with ch3:
        # Chart 3: Brand Comparison
        brand_agg = weekly_df.groupby("Brand").agg({
            "# Reviews": "sum",
            "5★ Count": "sum",
            "1★ Count": "sum",
        }).reset_index()
        fig3 = go.Figure()
        fig3.add_trace(go.Bar(
            name="5★",
            x=brand_agg["Brand"],
            y=brand_agg["5★ Count"],
            marker_color=chart_colors["secondary"],
            hovertemplate="<b>%{x}</b><br>5★: %{y}<extra></extra>",
        ))
        fig3.add_trace(go.Bar(
            name="1★",
            x=brand_agg["Brand"],
            y=brand_agg["1★ Count"],
            marker_color=chart_colors["negative"],
            hovertemplate="<b>%{x}</b><br>1★: %{y}<extra></extra>",
        ))
        fig3.update_layout(**chart_layout_w, title=dict(text="🏷️ Brand: 5★ vs 1★", font=dict(size=14)), barmode="group")
        fig3.update_xaxes(gridcolor=grid_c)
        fig3.update_yaxes(gridcolor=grid_c, title_text="Count")
        st.plotly_chart(fig3, use_container_width=True)

    with ch4:
        # Chart 4: Star Distribution Over Weeks
        fig4 = go.Figure()
        fig4.add_trace(go.Bar(
            name="5★",
            x=week_agg["Week"],
            y=week_agg["5★ Count"],
            marker_color=chart_colors["secondary"],
            hovertemplate="<b>%{x}</b><br>5★: %{y}<extra></extra>",
        ))
        fig4.add_trace(go.Bar(
            name="1★",
            x=week_agg["Week"],
            y=week_agg["1★ Count"],
            marker_color=chart_colors["negative"],
            hovertemplate="<b>%{x}</b><br>1★: %{y}<extra></extra>",
        ))
        fig4.update_layout(**chart_layout_w, title=dict(text="⭐ Weekly Star Distribution", font=dict(size=14)), barmode="stack")
        fig4.update_xaxes(gridcolor=grid_c)
        fig4.update_yaxes(gridcolor=grid_c, title_text="Count")
        st.plotly_chart(fig4, use_container_width=True)

    # ── Results summary ──
    st.caption(f"Showing {len(weekly_df)} rows • {len(weeks)} weeks • {stores_shown} stores")

    # ── Data Table ──
    col_config = {
        "Current Rate": st.column_config.NumberColumn(format="%.1f"),
        "Avg Rating": st.column_config.NumberColumn(format="%.2f"),
        "5★ %": st.column_config.NumberColumn(format="%.1f%%"),
        "1★ %": st.column_config.NumberColumn(format="%.1f%%"),
        "MTD Avg": st.column_config.NumberColumn(format="%.2f"),
    }

    st.dataframe(
        weekly_df,
        use_container_width=True,
        hide_index=True,
        height=400,
        column_config=col_config,
    )

    # CSV Export
    csv = weekly_df.to_csv(index=False)
    st.download_button(
        label="📥 Export Weekly Data",
        data=csv,
        file_name=f"weekly_report_{range_start}_{range_end}.csv",
        mime="text/csv",
        key="weekly_export"
    )


# =============================================================================
# Page: Monthly Report
# =============================================================================

def page_monthly_report(reviews_df, stores_df):
    """Render the Monthly Report page — focused single-month view."""
    st.markdown('<div class="section-header">Monthly Report</div>', unsafe_allow_html=True)

    today = date.today()
    month_names = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                   "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

    # ── Filter Row ──────────────────────────────────────────────────
    fc1, fc2, fc3, fc4 = st.columns([1, 1, 2, 2])

    with fc1:
        selected_year = st.selectbox(
            "📆 Year",
            options=[2025, 2026],
            index=1,
            key="monthly_year"
        )

    with fc2:
        current_month_idx = today.month - 1 if selected_year == today.year else 0
        selected_month = st.selectbox(
            "📅 Month",
            options=list(range(1, 13)),
            index=current_month_idx,
            format_func=lambda m: month_names[m - 1],
            key="monthly_month"
        )

    with fc3:
        all_brands = sorted(stores_df["brand"].unique().tolist())
        selected_brands_m = st.multiselect(
            "🏷️ Brand",
            options=all_brands,
            default=all_brands,
            key="monthly_brand_filter"
        )

    with fc4:
        if selected_brands_m:
            available_stores = sorted(
                stores_df[stores_df["brand"].isin(selected_brands_m)]["store_name"].unique().tolist()
            )
        else:
            available_stores = sorted(stores_df["store_name"].unique().tolist())

        selected_stores_m = st.multiselect(
            "🏪 Store",
            options=available_stores,
            default=available_stores,
            key="monthly_store_filter"
        )

    # ── Filter stores ──
    filtered = stores_df.copy()
    if selected_brands_m:
        filtered = filtered[filtered["brand"].isin(selected_brands_m)]
    if selected_stores_m:
        filtered = filtered[filtered["store_name"].isin(selected_stores_m)]

    if filtered.empty:
        st.info("No stores match the selected filters")
        return

    # ── Build monthly data ──
    rows = []
    for _, store in filtered.iterrows():
        pid = store["place_id"]
        mm = monthly_metrics(pid, selected_year, selected_month)
        shift = mom_shift(pid, selected_year, selected_month)

        rows.append({
            "Brand": store["brand"],
            "Store": store["store_name"],
            "Current Rate": store.get("current_rating"),
            "# Reviews": mm["review_count"],
            "Avg Rating": mm["avg_rating"],
            "5★ Count": mm["five_star_count"],
            "5★ %": mm["five_star_pct"],
            "1★ Count": mm["one_star_count"],
            "1★ %": mm["one_star_pct"],
            "MOM Shift": shift if shift is not None else 0.0,
        })

    monthly_df = pd.DataFrame(rows)

    # ── KPI Cards ──
    total_reviews = monthly_df["# Reviews"].sum()
    stores_with_reviews = monthly_df[monthly_df["# Reviews"] > 0]
    avg_rating = stores_with_reviews["Avg Rating"].mean() if not stores_with_reviews.empty else None
    above_45 = (monthly_df["Current Rate"].dropna() >= 4.5).sum()
    above_45_pct = round(above_45 / len(monthly_df) * 100, 1) if len(monthly_df) > 0 else 0
    avg_mom = monthly_df["MOM Shift"].mean()

    month_label = f"{month_names[selected_month - 1]} {selected_year}"

    kc1, kc2, kc3, kc4 = st.columns(4)
    with kc1:
        render_kpi_card(f"Reviews in {month_label}", int(total_reviews))
    with kc2:
        render_kpi_card("Avg Rating", f"{avg_rating:.2f}" if pd.notna(avg_rating) else "—")
    with kc3:
        render_kpi_card("% Above 4.5", f"{above_45_pct}%")
    with kc4:
        delta_class = "positive" if avg_mom >= 0 else "negative"
        render_kpi_card("Avg MOM Shift", f"{avg_mom:+.2f}")

    # ── Charts (matching Weekly Report visual style) ────────────────
    chart_colors = {
        "primary": "#4FC3F7", "secondary": "#81C784", "negative": "#E57373",
        "accent": "#FFD54F", "text": "#E0E0E0", "bg": "rgba(0,0,0,0)",
        "grid": "rgba(255,255,255,0.08)",
    }
    chart_layout_m = dict(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#E0E0E0", family="Inter, sans-serif"),
        margin=dict(l=40, r=20, t=30, b=30), height=220,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    grid_cm = "rgba(255,255,255,0.08)"

    # ── Build multi-month aggregation from reviews_df for trend charts ──
    filtered_pids = filtered["place_id"].tolist()
    trend_reviews = reviews_df[reviews_df["place_id"].isin(filtered_pids)].copy()
    if "review_date" in trend_reviews.columns and not trend_reviews.empty:
        trend_reviews["Month"] = trend_reviews["review_date"].dt.to_period("M")
        all_months = sorted(trend_reviews["Month"].unique())
        recent_months = all_months[-6:] if len(all_months) > 6 else all_months
        trend_reviews = trend_reviews[trend_reviews["Month"].isin(recent_months)]

        month_agg = trend_reviews.groupby("Month").agg(
            total_reviews=("rating", "count"),
            avg_rating=("rating", "mean"),
            five_star=("rating", lambda x: (x == 5).sum()),
            one_star=("rating", lambda x: (x <= 1).sum()),
        ).reset_index()
        month_agg["Month_str"] = month_agg["Month"].astype(str)
    else:
        month_agg = pd.DataFrame()

    # Brand aggregation for current month
    brand_agg = monthly_df.groupby("Brand").agg({
        "# Reviews": "sum", "5★ Count": "sum", "1★ Count": "sum",
    }).reset_index()

    ch1, ch2 = st.columns(2)

    with ch1:
        # Chart 1: Monthly Review Volume (blue vertical bars)
        fig1 = go.Figure()
        if not month_agg.empty:
            fig1.add_trace(go.Bar(
                x=month_agg["Month_str"],
                y=month_agg["total_reviews"],
                marker_color=chart_colors["primary"],
                marker_line_width=0,
                hovertemplate="<b>%{x}</b><br>Reviews: %{y}<extra></extra>",
            ))
        fig1.update_layout(**chart_layout_m, title=dict(text="📊 Monthly Review Volume", font=dict(size=14)))
        fig1.update_xaxes(gridcolor=grid_cm)
        fig1.update_yaxes(gridcolor=grid_cm, title_text="# Reviews")
        st.plotly_chart(fig1, use_container_width=True)

    with ch2:
        # Chart 2: Avg Rating Trend (green line+markers with 4.5 goal)
        fig2 = go.Figure()
        if not month_agg.empty:
            valid = month_agg[month_agg["avg_rating"] > 0]
            fig2.add_trace(go.Scatter(
                x=valid["Month_str"],
                y=valid["avg_rating"],
                mode="lines+markers",
                line=dict(color=chart_colors["secondary"], width=3),
                marker=dict(size=8, color=chart_colors["secondary"], line=dict(width=2, color="white")),
                hovertemplate="<b>%{x}</b><br>Avg: %{y:.2f}<extra></extra>",
            ))
        fig2.add_hline(y=4.5, line_dash="dot", line_color=chart_colors["accent"],
                       annotation_text="4.5 Goal", annotation_font_color=chart_colors["accent"])
        fig2.update_layout(**chart_layout_m, title=dict(text="📈 Avg Rating Trend", font=dict(size=14)))
        fig2.update_xaxes(gridcolor=grid_cm)
        fig2.update_yaxes(gridcolor=grid_cm, title_text="Avg Rating", range=[1, 5.2])
        st.plotly_chart(fig2, use_container_width=True)

    ch3, ch4 = st.columns(2)

    with ch3:
        # Chart 3: Brand 5★ vs 1★ (grouped bar)
        fig3 = go.Figure()
        fig3.add_trace(go.Bar(
            name="5★", x=brand_agg["Brand"], y=brand_agg["5★ Count"],
            marker_color=chart_colors["secondary"],
            hovertemplate="<b>%{x}</b><br>5★: %{y}<extra></extra>",
        ))
        fig3.add_trace(go.Bar(
            name="1★", x=brand_agg["Brand"], y=brand_agg["1★ Count"],
            marker_color=chart_colors["negative"],
            hovertemplate="<b>%{x}</b><br>1★: %{y}<extra></extra>",
        ))
        fig3.update_layout(**chart_layout_m, title=dict(text="🏷️ Brand: 5★ vs 1★", font=dict(size=14)), barmode="group")
        fig3.update_xaxes(gridcolor=grid_cm)
        fig3.update_yaxes(gridcolor=grid_cm, title_text="Count")
        st.plotly_chart(fig3, use_container_width=True)

    with ch4:
        # Chart 4: Monthly Star Distribution (stacked bar)
        fig4 = go.Figure()
        if not month_agg.empty:
            fig4.add_trace(go.Bar(
                name="5★", x=month_agg["Month_str"], y=month_agg["five_star"],
                marker_color=chart_colors["secondary"],
                hovertemplate="<b>%{x}</b><br>5★: %{y}<extra></extra>",
            ))
            fig4.add_trace(go.Bar(
                name="1★", x=month_agg["Month_str"], y=month_agg["one_star"],
                marker_color=chart_colors["negative"],
                hovertemplate="<b>%{x}</b><br>1★: %{y}<extra></extra>",
            ))
        fig4.update_layout(**chart_layout_m, title=dict(text="⭐ Monthly Star Distribution", font=dict(size=14)), barmode="stack")
        fig4.update_xaxes(gridcolor=grid_cm)
        fig4.update_yaxes(gridcolor=grid_cm, title_text="Count")
        st.plotly_chart(fig4, use_container_width=True)

    # Chart 5: MOM Shift by Store (kept as requested)
    stores_with_shift = monthly_df[monthly_df["MOM Shift"] != 0].copy()
    if not stores_with_shift.empty:
        stores_with_shift = stores_with_shift.sort_values("MOM Shift", ascending=False)
        fig5 = go.Figure()
        fig5.add_trace(go.Bar(
            x=stores_with_shift["Store"],
            y=stores_with_shift["MOM Shift"],
            marker_color=stores_with_shift["MOM Shift"].apply(
                lambda x: chart_colors["secondary"] if x > 0 else chart_colors["negative"]
            ),
            hovertemplate="<b>%{x}</b><br>MOM Shift: %{y:+.2f}<extra></extra>",
        ))
        fig5.add_hline(y=0, line_color="rgba(255,255,255,0.3)", line_width=1)
        fig5.update_layout(**chart_layout_m, title=dict(text=f"📉 MOM Shift by Store ({month_label})", font=dict(size=14)))
        fig5.update_xaxes(gridcolor=grid_cm, tickangle=-45)
        fig5.update_yaxes(gridcolor=grid_cm, title_text="Rating Change")
        st.plotly_chart(fig5, use_container_width=True)

    st.caption(f"Showing {len(monthly_df)} stores for {month_label}")

    # ── Data Table with color-coded MOM Shift ──
    col_config = {
        "Current Rate": st.column_config.NumberColumn(format="%.1f"),
        "Avg Rating": st.column_config.NumberColumn(format="%.2f"),
        "5★ %": st.column_config.NumberColumn(format="%.1f%%"),
        "1★ %": st.column_config.NumberColumn(format="%.1f%%"),
        "MOM Shift": st.column_config.NumberColumn(format="%+.2f"),
    }

    # Color MOM Shift: green = positive, red = negative
    def style_mom(val):
        if pd.isna(val) or val == 0:
            return ""
        return f"color: {'#2E7D32' if val > 0 else '#D32F2F'}; font-weight: 700"

    styled = monthly_df.style.map(style_mom, subset=["MOM Shift"])

    st.dataframe(
        styled,
        use_container_width=True,
        hide_index=True,
        height=400,
        column_config=col_config,
    )

    # CSV Export
    csv = monthly_df.to_csv(index=False)
    st.download_button(
        label="📥 Export Monthly Data",
        data=csv,
        file_name=f"monthly_report_{month_label.replace(' ', '_')}.csv",
        mime="text/csv",
        key="monthly_export"
    )


def main():
    # Sidebar filters
    selected_brands, date_range, min_rating, search_term = render_sidebar()

    # Load data
    reviews_df = get_reviews_df()
    stores_df = get_stores_df()

    # Apply filters to reviews
    filtered_reviews = apply_filters(reviews_df, selected_brands, date_range, min_rating, search_term)

    # Dashboard header
    logo_b64 = get_logo_base64()
    logo_html = f'<img src="data:image/png;base64,{logo_b64}" style="width: 44px; height: 44px; border-radius: 50%; vertical-align: middle; margin-right: 12px;" />' if logo_b64 else "🌿"
    st.markdown(f"""
    <div class="dashboard-header">
        <div>
            <h1>{logo_html} Cannabis Reviews Dashboard</h1>
            <div class="subtitle">Multi-Brand Performance Analytics • {date.today().strftime('%B %Y')}</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Navigation tabs
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📊 Executive Overview",
        "📅 Weekly Report",
        "📈 Monthly Report",
        "📝 All Reviews",
        "🚨 Needs Attention"
    ])

    with tab1:
        page_overview(filtered_reviews, stores_df, selected_brands)

    with tab2:
        page_weekly_report(filtered_reviews, stores_df)

    with tab3:
        page_monthly_report(filtered_reviews, stores_df)

    with tab4:
        page_all_reviews(filtered_reviews)

    with tab5:
        page_needs_attention(reviews_df)


if __name__ == "__main__":
    main()
