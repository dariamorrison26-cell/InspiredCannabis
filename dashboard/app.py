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
    page_title="Reviews Dashboard",
    page_icon="🌿",
    layout="wide",
    initial_sidebar_state="expanded"
)

# =============================================================================
# Custom CSS
# =============================================================================

st.markdown(f"""
<style>
    /* Google Fonts */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800;900&display=swap');

    /* Global */
    .stApp {{
        background-color: {LIGHT_GREY};
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
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
        letter-spacing: 0.3px;
        display: flex;
        align-items: center;
        gap: 12px;
        font-family: 'Inter', sans-serif;
    }}
    .dashboard-header h1 img {{
        flex-shrink: 0;
    }}
    .dashboard-header .subtitle {{
        font-size: 0.72rem;
        opacity: 0.75;
        margin-top: 2px;
        font-weight: 400;
        letter-spacing: 0.2px;
    }}

    /* Sidebar logo alignment — push sidebar content down to align with header */
    [data-testid="stSidebar"] > div:first-child {{
        padding-top: 0rem;
    }}
    /* Remove Streamlit's internal sidebar top spacer */
    [data-testid="stSidebarContent"] {{
        padding-top: 0rem !important;
    }}
    [data-testid="stSidebarUserContent"] {{
        padding-top: 0rem !important;
    }}

    /* KPI Cards — Glassmorphism */
    .kpi-card {{
        background: rgba(255, 255, 255, 0.85);
        backdrop-filter: blur(12px);
        -webkit-backdrop-filter: blur(12px);
        border-radius: 14px;
        padding: 0.65rem 0.8rem 0.4rem;
        text-align: center;
        box-shadow: 0 4px 20px rgba(0,0,0,0.06), 0 1px 3px rgba(0,0,0,0.04);
        border: 1px solid rgba(255,255,255,0.6);
        border-top: 3px solid {ORANGE};
        transition: transform 0.25s ease, box-shadow 0.25s ease;
        position: relative;
        overflow: hidden;
        height: 120px;
        display: flex;
        flex-direction: column;
        justify-content: flex-start;
        align-items: center;
        margin: 0 2px;
    }}
    .kpi-card.compact {{
        height: 65px;
        padding: 0.4rem 0.8rem 0.3rem;
    }}
    .kpi-card.compact .kpi-value {{
        font-size: 1.3rem;
    }}
    .kpi-card:hover {{
        transform: translateY(-3px);
        box-shadow: 0 8px 30px rgba(0,0,0,0.1), 0 2px 6px rgba(0,0,0,0.06);
    }}
    .kpi-value {{
        font-size: 1.6rem;
        font-weight: 800;
        color: {NAVY};
        margin: 0.1rem 0;
        line-height: 1;
        letter-spacing: -0.5px;
        font-family: 'Inter', sans-serif;
        animation: countUp 0.6s ease-out;
    }}
    @keyframes countUp {{
        from {{ opacity: 0; transform: translateY(8px); }}
        to {{ opacity: 1; transform: translateY(0); }}
    }}
    .kpi-label {{
        font-size: 0.6rem;
        color: #888;
        text-transform: uppercase;
        letter-spacing: 1.2px;
        font-weight: 600;
        margin-top: 2px;
    }}
    .kpi-delta {{
        font-size: 0.68rem;
        margin-top: 2px;
        font-weight: 700;
        letter-spacing: -0.2px;
    }}
    .kpi-delta.positive {{ color: {SUCCESS}; }}
    .kpi-delta.negative {{ color: {ALERT}; }}
    .kpi-sparkline {{
        margin-top: 2px;
        font-size: 14px;
        letter-spacing: 1px;
        line-height: 1;
        opacity: 0.75;
        color: {NAVY};
    }}

    /* Section headers */
    .section-header {{
        color: {NAVY};
        font-size: 1.1rem;
        font-weight: 700;
        margin: 0 0 0.4rem 0;
        padding-bottom: 0.35rem;
        border-bottom: 2px solid {ORANGE};
        display: inline-block;
        letter-spacing: -0.2px;
        font-family: 'Inter', sans-serif;
    }}

    /* Plotly Chart Containers — Glassmorphism */
    [data-testid="stPlotlyChart"] {{
        background: rgba(255, 255, 255, 0.85);
        backdrop-filter: blur(12px);
        -webkit-backdrop-filter: blur(12px);
        border-radius: 14px;
        box-shadow: 0 4px 20px rgba(0,0,0,0.06), 0 1px 3px rgba(0,0,0,0.04);
        border: 1px solid rgba(255,255,255,0.6);
        transition: transform 0.25s ease, box-shadow 0.25s ease;
        overflow: hidden;
        padding: 0.3rem;
    }}
    [data-testid="stPlotlyChart"]:hover {{
        transform: translateY(-2px);
        box-shadow: 0 8px 30px rgba(0,0,0,0.1), 0 2px 6px rgba(0,0,0,0.06);
    }}
    /* Override Plotly crosshair cursor */
    [data-testid="stPlotlyChart"] .nsewdrag,
    [data-testid="stPlotlyChart"] .drag,
    [data-testid="stPlotlyChart"] .draglayer {{
        cursor: default !important;
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
    /* Sidebar button text must be visible (dark on white bg) */
    [data-testid="stSidebar"] button,
    [data-testid="stSidebar"] button * {{
        color: {NAVY} !important;
    }}
    [data-testid="stSidebar"] button {{
        background-color: {WHITE} !important;
        border: 1px solid rgba(255,255,255,0.3) !important;
        font-weight: 600 !important;
    }}
    [data-testid="stSidebar"] button:hover,
    [data-testid="stSidebar"] button:hover * {{
        background-color: {ORANGE} !important;
        color: {WHITE} !important;
        border-color: {ORANGE} !important;
    }}
    /* Sidebar multiselect — keep pill text dark on white bg */
    [data-testid="stSidebar"] [data-baseweb="tag"] * {{
        color: {NAVY} !important;
    }}
    /* Position the clear-all + chevron icons next to the Brand label */
    [data-testid="stSidebar"] .stMultiSelect {{
        position: relative;
    }}
    [data-testid="stSidebar"] [data-baseweb="select"] > div > div:last-child {{
        position: absolute !important;
        top: -30px !important;
        right: 0 !important;
        z-index: 10;
    }}
    [data-testid="stSidebar"] [data-baseweb="select"] svg,
    [data-testid="stSidebar"] [data-baseweb="select"] svg path {{
        fill: {WHITE} !important;
        color: {WHITE} !important;
    }}

    /* Tabs */
    .stTabs [data-baseweb="tab-list"] {{
        gap: 6px;
    }}
    .stTabs [data-baseweb="tab"] {{
        background: {WHITE};
        border-radius: 8px 8px 0 0;
        padding: 8px 24px;
        font-weight: 600;
        font-size: 0.85rem;
        letter-spacing: 0.1px;
        transition: all 0.2s ease;
    }}
    .stTabs [data-baseweb="tab"]:hover {{
        background: #e8ecf1;
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

    /* Streamlit metric labels - cleaner typography */
    [data-testid="stMetricLabel"] {{
        font-size: 0.75rem !important;
        font-weight: 600 !important;
        letter-spacing: 0.3px !important;
        text-transform: uppercase !important;
        color: #666 !important;
    }}
    [data-testid="stMetricValue"] {{
        font-weight: 800 !important;
        letter-spacing: -0.5px !important;
    }}

    /* Plotly chart titles */
    .js-plotly-plot .plotly .gtitle {{
        font-family: 'Inter', sans-serif !important;
    }}

    /* Dataframe text */
    .stDataFrame {{
        font-size: 0.82rem;
    }}

    /* Reduce top whitespace above header */
    .block-container {{
        padding-top: 1rem !important;
    }}

    /* Hide Streamlit branding */
    #MainMenu {{ visibility: hidden; }}
    footer {{ visibility: hidden; }}
    header[data-testid="stHeader"] {{
        background: transparent !important;
        height: 2rem !important;
        min-height: 2rem !important;
        padding: 0 !important;
    }}
    /* Style sidebar toggle as clean icon-only button */
    [data-testid="stSidebarCollapsedControl"],
    [data-testid="stSidebarCollapseButton"],
    [data-testid="collapsedControl"] {{
        display: none !important;
    }}

    /* Ensure Inter font applies broadly but not to icons */
    .stMarkdown, .stText, .stDataFrame, .stTable,
    [data-testid="stMetricLabel"], [data-testid="stMetricValue"],
    .stSelectbox, .stMultiSelect, .stTextInput,
    .stTabs [data-baseweb="tab"],
    .stCaption, p, span, div, td, th, label, input {{
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
    }}
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
            <div style="text-align: center; padding: 0 0 1rem 0; margin-top: -2.5rem;">
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

        # Data freshness indicator
        reviews_df_fresh = get_reviews_df()
        if not reviews_df_fresh.empty and "review_date" in reviews_df_fresh.columns:
            latest_review = reviews_df_fresh["review_date"].max()
            if pd.notna(latest_review):
                synced_str = latest_review.strftime("%b %d, %Y")
                st.markdown(f"""
                <div style="text-align: center; padding: 6px 0 2px 0; font-size: 0.72rem; opacity: 0.8; line-height: 1.4;">
                    <span style="display: inline-block; width: 6px; height: 6px; background: #4CAF50; border-radius: 50%; margin-right: 4px; vertical-align: middle;"></span>
                    Last synced: {synced_str}
                </div>
                """, unsafe_allow_html=True)

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
        <div style="text-align: center; font-size: 0.65rem; opacity: 0.4;">
            Powered by Avelle Solutions
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
# KPI Scorecard Component (Enhanced)
# =============================================================================

def _sparkline_chars(data_points):
    """Generate Unicode block-character sparkline (plain text, no HTML)."""
    if not data_points or len(data_points) < 2:
        return ""
    blocks = "\u2581\u2582\u2583\u2584\u2585\u2586\u2587\u2588"
    mn, mx = min(data_points), max(data_points)
    rng = mx - mn if mx != mn else 1
    chars = []
    for v in data_points:
        idx = int((v - mn) / rng * 7)
        chars.append(blocks[min(idx, 7)])
    return " ".join(chars)


def render_kpi_card(label, value, delta=None, delta_label="", prefix="", suffix="", sparkline=None, spark_color=None, compact=False):
    """Render an enhanced KPI card with glassmorphism, sparkline, and delta."""
    delta_html = ""
    if delta is not None and delta != 0:
        delta_class = "positive" if delta >= 0 else "negative"
        delta_icon = "↑" if delta >= 0 else "↓"
        delta_html = f'<div class="kpi-delta {delta_class}">{delta_icon} {abs(delta):.1f}{delta_label}</div>'

    spark_html = ""
    if sparkline and len(sparkline) >= 2:
        spark_text = _sparkline_chars(sparkline)
        spark_html = f'<div class="kpi-sparkline">{spark_text}</div>'

    card_class = "kpi-card compact" if compact else "kpi-card"
    st.markdown(f"""
    <div class="{card_class}">
        <div class="kpi-label">{label}</div>
        <div class="kpi-value">{prefix}{value}{suffix}</div>
        {delta_html}
        {spark_html}
    </div>
    """, unsafe_allow_html=True)


def _weekly_trend(df, weeks=4, metric="count"):
    """Compute weekly data points for sparkline and delta."""
    if df.empty or "review_date" not in df.columns:
        return [], None
    today = pd.Timestamp.now().normalize()
    points = []
    for w in range(weeks - 1, -1, -1):
        end = today - timedelta(weeks=w)
        start = end - timedelta(weeks=1)
        week_df = df[(df["review_date"] >= start) & (df["review_date"] < end)]
        if metric == "count":
            points.append(len(week_df))
        elif metric == "avg_rating":
            points.append(week_df["rating"].mean() if not week_df.empty else 0)
        elif metric == "five_star_pct":
            total = len(week_df)
            five = len(week_df[week_df["rating"] == 5]) if total > 0 else 0
            points.append((five / total * 100) if total > 0 else 0)
        elif metric == "response_pct":
            total = len(week_df)
            resp = len(week_df[week_df["owner_response"].notna() & (week_df["owner_response"] != "")]) if total > 0 else 0
            points.append((resp / total * 100) if total > 0 else 0)

    # Delta: this week vs last week
    delta = None
    if len(points) >= 2:
        if metric in ("response_pct", "five_star_pct"):
            # For metrics already in %, show percentage-point difference
            delta = points[-1] - points[-2]
        elif points[-2] != 0:
            delta = ((points[-1] - points[-2]) / abs(points[-2])) * 100
        elif points[-1] > 0:
            delta = 100.0
    return points, delta


# =============================================================================
# Page 1: Executive Overview
# =============================================================================

def page_overview(reviews_df, stores_df, selected_brands):
    """Render the Executive Overview page."""

    # Filter stores by selected brands
    filtered_stores = stores_df[stores_df["brand"].isin(selected_brands)] if selected_brands else stores_df

    # ── KPI Calculations ─────────────────────────────────────────────────
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

    # ── Weekly Trends (4-week sparklines) ─────────────────────────────────
    vol_trend, vol_delta = _weekly_trend(reviews_df, metric="count")
    rat_trend, rat_delta = _weekly_trend(reviews_df, metric="avg_rating")
    five_trend, five_delta = _weekly_trend(reviews_df, metric="five_star_pct")
    resp_trend, resp_delta = _weekly_trend(reviews_df, metric="response_pct")

    # ── Above 4.5 delta (per-store weekly review avg) ─────────────────────
    above_45_delta = None
    if not reviews_df.empty and "review_date" in reviews_df.columns and "place_id" in reviews_df.columns:
        today = pd.Timestamp.now().normalize()
        cur_start, cur_end = today - timedelta(weeks=1), today
        prv_start, prv_end = today - timedelta(weeks=2), today - timedelta(weeks=1)
        cur_wk = reviews_df[(reviews_df["review_date"] >= cur_start) & (reviews_df["review_date"] < cur_end)]
        prv_wk = reviews_df[(reviews_df["review_date"] >= prv_start) & (reviews_df["review_date"] < prv_end)]
        if not cur_wk.empty and not prv_wk.empty:
            cur_store_avg = cur_wk.groupby("place_id")["rating"].mean()
            prv_store_avg = prv_wk.groupby("place_id")["rating"].mean()
            cur_pct = (cur_store_avg >= 4.5).sum() / len(cur_store_avg) * 100
            prv_pct = (prv_store_avg >= 4.5).sum() / len(prv_store_avg) * 100
            above_45_delta = cur_pct - prv_pct  # percentage point change

    # ── KPI Row ──────────────────────────────────────────────────────────
    cols = st.columns(6)
    with cols[0]:
        render_kpi_card("Total Stores", total_stores)
    with cols[1]:
        render_kpi_card("Avg Rating", f"{avg_rating:.2f}", suffix=" ★",
                        delta=rat_delta, delta_label="% vs last wk")
    with cols[2]:
        render_kpi_card("Total Reviews", f"{total_reviews:,}",
                        delta=vol_delta, delta_label="% vs last wk")
    with cols[3]:
        render_kpi_card("5-Star Reviews", f"{five_star_pct:.0f}", suffix="%",
                        delta=five_delta, delta_label=" pts vs last wk")
    with cols[4]:
        render_kpi_card("Above 4.5 ★", f"{above_45_pct:.0f}", suffix="%",
                        delta=above_45_delta, delta_label=" pts vs last wk")
    with cols[5]:
        render_kpi_card("Response Rate", f"{response_pct:.0f}", suffix="%",
                        delta=resp_delta, delta_label=" pts vs last wk")

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
                    hovertemplate=f"{row['brand']}: {row['current_rating']:.2f} ★<extra></extra>",
                ))

            fig.update_layout(
                height=220,
                margin=dict(l=0, r=20, t=10, b=0),
                xaxis=dict(range=[0, 5], title="Average Rating"),
                yaxis=dict(title=""),
                plot_bgcolor="white",
                paper_bgcolor="white",
            )
            st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
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
            fig.update_traces(hovertemplate="%{x}<br>%{y} reviews<extra></extra>")
            fig.update_layout(
                height=220,
                margin=dict(l=0, r=20, t=10, b=0),
                xaxis_title="Month",
                yaxis_title="# Reviews",
                plot_bgcolor="white",
                paper_bgcolor="white",
                legend=dict(orientation="h", y=-0.3),
            )
            st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
        else:
            st.info("No review data available")

    # ── Week vs Month Performance ─────────────────────────────────────────
    st.markdown('<div class="section-header">📊 This Week vs This Month</div>', unsafe_allow_html=True)

    if not reviews_df.empty and "review_date" in reviews_df.columns:
        today = date.today()
        # Current week boundaries (Mon–Sun)
        week_start = today - timedelta(days=today.weekday())
        week_end = today

        # Current month boundaries
        month_start = today.replace(day=1)
        _, days_in_month = monthrange(today.year, today.month)

        # Days elapsed in month & weeks elapsed
        days_elapsed = (today - month_start).days + 1
        weeks_elapsed = max(1, days_elapsed / 7)
        day_of_week = today.weekday() + 1  # Mon=1, Sun=7

        # Filter data
        week_reviews = reviews_df[
            (reviews_df["review_date"].dt.date >= week_start) &
            (reviews_df["review_date"].dt.date <= week_end)
        ]
        month_reviews = reviews_df[
            (reviews_df["review_date"].dt.date >= month_start) &
            (reviews_df["review_date"].dt.date <= today)
        ]

        # Overall totals
        total_week = len(week_reviews)
        total_month = len(month_reviews)
        weekly_pace = total_month / weeks_elapsed if weeks_elapsed > 0 else 0
        projected_month = (total_month / days_elapsed) * days_in_month if days_elapsed > 0 else 0
        week_pct_of_month = (total_week / total_month * 100) if total_month > 0 else 0
        month_name = today.strftime("%B")

        # Avg rating this week vs month
        week_avg = week_reviews["rating"].mean() if not week_reviews.empty else 0
        month_avg = month_reviews["rating"].mean() if not month_reviews.empty else 0

        # CSS to force equal-height columns
        st.markdown("""
        <style>
        div[data-testid="stHorizontalBlock"]:has(> div[data-testid="stColumn"] .wm-card) {
            align-items: stretch !important;
        }
        div[data-testid="stHorizontalBlock"]:has(> div[data-testid="stColumn"] .wm-card) > div[data-testid="stColumn"] {
            display: flex !important;
            flex-direction: column !important;
        }
        div[data-testid="stHorizontalBlock"]:has(> div[data-testid="stColumn"] .wm-card) > div[data-testid="stColumn"] > div {
            flex: 1 !important;
        }
        </style>
        """, unsafe_allow_html=True)

        CARD_HEIGHT = 440
        wm_col1, wm_col2 = st.columns(2)

        with wm_col1:
            st.markdown('<div class="section-header">Reviews This Week vs Monthly Pace</div>', unsafe_allow_html=True)
            # ── Grouped bar: This Week vs Weekly Pace per Brand ──
            brands_in_data = sorted(set(
                list(week_reviews["brand"].unique()) +
                list(month_reviews["brand"].unique())
            ))

            chart_data = []
            for brand in brands_in_data:
                wk_count = len(week_reviews[week_reviews["brand"] == brand])
                mo_count = len(month_reviews[month_reviews["brand"] == brand])
                pace = mo_count / weeks_elapsed if weeks_elapsed > 0 else 0
                chart_data.append({
                    "Brand": brand,
                    "This Week": wk_count,
                    "Monthly Avg/Week": round(pace, 1),
                })

            if chart_data:
                fig_wm = go.Figure()

                # Monthly weekly pace bars (background)
                fig_wm.add_trace(go.Bar(
                    name="Month Avg/Week",
                    x=[d["Brand"] for d in chart_data],
                    y=[d["Monthly Avg/Week"] for d in chart_data],
                    marker_color=[BRAND_COLORS.get(d["Brand"], NAVY) for d in chart_data],
                    opacity=0.25,
                    text=[f'{d["Monthly Avg/Week"]:.0f}' for d in chart_data],
                    textposition="outside",
                    hovertemplate="%{x}<br>Month avg/week: %{y:.1f}<extra></extra>",
                ))

                # This week bars (foreground)
                fig_wm.add_trace(go.Bar(
                    name="This Week",
                    x=[d["Brand"] for d in chart_data],
                    y=[d["This Week"] for d in chart_data],
                    marker_color=[BRAND_COLORS.get(d["Brand"], NAVY) for d in chart_data],
                    opacity=0.85,
                    text=[str(d["This Week"]) for d in chart_data],
                    textposition="outside",
                    hovertemplate="%{x}<br>This week: %{y}<extra></extra>",
                ))

                fig_wm.update_layout(
                    height=CARD_HEIGHT,
                    margin=dict(l=0, r=10, t=10, b=0),
                    barmode="group",
                    plot_bgcolor="white",
                    paper_bgcolor="white",
                    xaxis_title="",
                    yaxis_title="# Reviews",
                    legend=dict(orientation="h", y=-0.12),
                    font=dict(size=11),
                )
                st.plotly_chart(fig_wm, use_container_width=True, config={'displayModeBar': False})

        with wm_col2:
            st.markdown('<div class="section-header">Week Contribution to Month</div>', unsafe_allow_html=True)
            # ── Creative donut visualization: Week as portion of Month ──
            is_above = total_week >= weekly_pace
            status_color = SUCCESS if is_above else ORANGE
            rest_of_month = total_month - total_week
            week_pct = (total_week / total_month * 100) if total_month > 0 else 0
            day_of_week = today.weekday() + 1  # Mon=1

            fig_donut = go.Figure()

            # Donut: This Week vs Rest of Month
            fig_donut.add_trace(go.Pie(
                values=[total_week, max(0, rest_of_month)],
                labels=["This Week", f"Rest of {month_name}"],
                hole=0.72,
                marker=dict(
                    colors=[status_color, "#E8EAF0"],
                    line=dict(color="white", width=3),
                ),
                textinfo="none",
                hovertemplate=(
                    "%{label}<br>"
                    "%{value} reviews (%{percent})<extra></extra>"
                ),
                direction="clockwise",
                sort=False,
                rotation=90,
                domain=dict(x=[0.15, 0.85], y=[0.22, 1.0]),
            ))

            # Center annotation — big week count + context
            fig_donut.add_annotation(
                text=(
                    f"<b style='font-size:42px;color:{NAVY}'>{total_week}</b><br>"
                    f"<span style='font-size:13px;color:#888'>of {total_month} in {month_name}</span><br>"
                    f"<span style='font-size:16px;color:{status_color};font-weight:700'>{week_pct:.1f}%</span>"
                ),
                x=0.5, y=0.65,
                showarrow=False,
                font=dict(size=14),
            )

            # Stats annotations at the bottom
            stats = [
                ("Week Avg", f"{week_avg:.1f} ★"),
                (f"{month_name} Total", f"{total_month}"),
                ("Projected", f"{projected_month:.0f}"),
            ]
            for i, (label, value) in enumerate(stats):
                x_pos = 0.17 + i * 0.33
                fig_donut.add_annotation(
                    text=f"<span style='font-size:10px;color:#888;text-transform:uppercase'>{label}</span><br><b style='font-size:16px;color:{NAVY}'>{value}</b>",
                    x=x_pos, y=0.08,
                    showarrow=False,
                    xanchor="center",
                )

            # Context line at very bottom
            fig_donut.add_annotation(
                text=f"<span style='font-size:10px;color:#aaa'>Week day {day_of_week}/7 · Month day {days_elapsed}/{days_in_month}</span>",
                x=0.5, y=-0.02,
                showarrow=False,
                xanchor="center",
            )

            fig_donut.update_layout(
                height=CARD_HEIGHT,
                margin=dict(l=10, r=10, t=15, b=15),
                paper_bgcolor="white",
                showlegend=False,
            )

            st.plotly_chart(fig_donut, use_container_width=True, config={'displayModeBar': False})
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

            total_for_pct = star_counts.values.sum()
            custom_text = []
            text_positions = []
            for v in star_counts.values:
                pct = v / total_for_pct * 100
                custom_text.append(f"{v:,} ({pct:.1f}%)")
                text_positions.append("inside" if pct >= 5 else "outside")

            fig = go.Figure(data=[go.Pie(
                labels=star_labels,
                values=star_counts.values,
                hole=0.5,
                marker_colors=colors[:len(star_counts)],
                text=custom_text,
                textinfo="label+text",
                textposition=text_positions,
                insidetextorientation="horizontal",
                textfont=dict(size=11, color="white"),
                outsidetextfont=dict(size=10, color="#333"),
                hoverinfo="label+value+percent",
                direction="clockwise",
                sort=False,
                rotation=180,
            )])
            fig.update_layout(
                height=280,
                margin=dict(l=40, r=40, t=30, b=30),
                paper_bgcolor="white",
                showlegend=False,
                annotations=[dict(
                    text=f"{total_reviews:,}<br>Total",
                    x=0.5, y=0.5,
                    font_size=16, font_color=NAVY,
                    showarrow=False
                )]
            )
            st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
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
                    hovertemplate=f"{row['brand']}: {row['rate']:.0f}% ({row['responded']}/{row['total']})<extra></extra>",
                ))

            fig.update_layout(
                height=280,
                margin=dict(l=0, r=20, t=10, b=0),
                xaxis=dict(range=[0, 100], title="Response Rate (%)"),
                yaxis=dict(title=""),
                plot_bgcolor="white",
                paper_bgcolor="white",
            )
            st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
        else:
            st.info("No review data available")


    # ── Review Velocity Trendline ─────────────────────────────────────────
    st.markdown('<div class="section-header">Review Velocity — Weekly Trend</div>', unsafe_allow_html=True)
    if not reviews_df.empty and "review_date" in reviews_df.columns:
        weekly = reviews_df.copy()
        weekly["week"] = weekly["review_date"].dt.to_period("W").apply(lambda p: p.start_time)
        weekly_counts = weekly.groupby("week").size().reset_index(name="reviews")
        weekly_counts = weekly_counts.sort_values("week")

        # 4-week moving average
        weekly_counts["ma_4wk"] = weekly_counts["reviews"].rolling(window=4, min_periods=1).mean()
        # Overall weekly average (goal line)
        overall_avg = weekly_counts["reviews"].mean()

        fig_vel = go.Figure()

        # Weekly bars
        fig_vel.add_trace(go.Bar(
            x=weekly_counts["week"],
            y=weekly_counts["reviews"],
            name="Weekly Reviews",
            marker_color=NAVY,
            opacity=0.5,
            hovertemplate="Week of %{x|%b %d}<br>%{y} reviews<extra></extra>",
        ))

        # 4-week moving average line
        fig_vel.add_trace(go.Scatter(
            x=weekly_counts["week"],
            y=weekly_counts["ma_4wk"],
            name="4-Week Avg",
            mode="lines+markers",
            line=dict(color=BRAND_COLORS.get("Inspired Cannabis", "#E8792B"), width=3),
            marker=dict(size=5),
            hovertemplate="4-wk avg: %{y:.1f}<extra></extra>",
        ))

        # Average goal line
        fig_vel.add_hline(
            y=overall_avg,
            line_dash="dash",
            line_color="#999",
            annotation_text=f"Avg: {overall_avg:.0f}/wk",
            annotation_position="top right",
            annotation_font_color="#999",
        )

        fig_vel.update_layout(
            height=300,
            margin=dict(l=0, r=20, t=10, b=0),
            xaxis_title="",
            yaxis_title="# Reviews",
            plot_bgcolor="white",
            paper_bgcolor="white",
            legend=dict(orientation="h", y=-0.15),
            barmode="overlay",
        )
        st.plotly_chart(fig_vel, use_container_width=True, config={'displayModeBar': False})
    else:
        st.info("No review data available for velocity chart")

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

        # CSV Export for performance table (hidden for now)
        # csv = perf_df.to_csv(index=False)
        # st.download_button(
        #     label="📥 Export Performance Data",
        #     data=csv,
        #     file_name=f"store_performance_{date.today().isoformat()}.csv",
        #     mime="text/csv",
        #     key="perf_export"
        # )
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
        render_kpi_card("Total Reviews", len(display_df), compact=True)
    with col2:
        avg = display_df["rating"].mean() if not display_df.empty else 0
        render_kpi_card("Avg Rating", f"{avg:.2f}", suffix=" ★", compact=True)
    with col3:
        responded = len(display_df[display_df["Status"] == "✅ Responded"])
        render_kpi_card("Responded", responded, compact=True)
    with col4:
        pending = len(display_df[display_df["Status"] == "⚠️ No Response"])
        render_kpi_card("Pending Response", pending, compact=True)

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

    # CSV Export (hidden for now)
    # csv = display_cols.to_csv(index=False)
    # st.download_button(
    #     label="📥 Export to CSV",
    #     data=csv,
    #     file_name=f"all_reviews_{date.today().isoformat()}.csv",
    #     mime="text/csv",
    # )


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
        render_kpi_card("Total Negative", all_neg_count, compact=True)
    with col2:
        responded = all_neg_count - unresponded_count
        rate = (responded / all_neg_count * 100) if all_neg_count > 0 else 0
        render_kpi_card("Response Rate", f"{rate:.0f}", suffix="%", compact=True)
    with col3:
        st.markdown(f"""
        <a href="#unresponded-reviews" style="text-decoration: none; color: inherit; display: block;">
            <div class="kpi-card compact" style="cursor: pointer; transition: transform 0.2s, box-shadow 0.2s;" 
                 onmouseover="this.style.transform='scale(1.03)'; this.style.boxShadow='0 4px 16px rgba(211,47,47,0.3)';"
                 onmouseout="this.style.transform='scale(1)'; this.style.boxShadow='';">
                <div class="kpi-label">⚠️ UNRESPONDED</div>
                <div class="kpi-value" style="color: #D32F2F; font-size: 1.3rem;">{unresponded_count}</div>
            </div>
        </a>
        """, unsafe_allow_html=True)

    st.markdown("")

    if working_df.empty:
        st.success("🎉 All negative reviews have been responded to!")
        return

    # ── Filters (apply to charts AND table) ──────────────────────────────
    working_df["_month"] = working_df["review_date"].dt.to_period("M").astype(str)
    f1, f2, f3, f4 = st.columns([1.2, 1.5, 1.5, 1])
    with f1:
        month_options = ["All Months"] + sorted(working_df["_month"].unique().tolist(), reverse=True)
        sel_month = st.selectbox("📅 Date", month_options, index=0, key="attn_date")
    with f2:
        brand_options = ["All Brands"] + sorted(working_df["brand"].unique().tolist())
        sel_brand = st.selectbox("🏷️ Brand", brand_options, index=0, key="attn_brand")
    with f3:
        if sel_brand != "All Brands":
            store_list = sorted(working_df[working_df["brand"] == sel_brand]["store_name"].unique().tolist())
        else:
            store_list = sorted(working_df["store_name"].unique().tolist())
        store_options = ["All Stores"] + store_list
        sel_store = st.selectbox("📍 Store", store_options, index=0, key="attn_store")
    with f4:
        rating_options = ["All Ratings", "⭐", "⭐⭐"]
        sel_rating = st.selectbox("⭐ Rating", rating_options, index=0, key="attn_rating")

    # Apply filters
    working_df["Rating"] = working_df["rating"].apply(lambda r: "⭐" * int(r) if pd.notna(r) else "")
    if sel_month != "All Months":
        working_df = working_df[working_df["_month"] == sel_month]
    if sel_brand != "All Brands":
        working_df = working_df[working_df["brand"] == sel_brand]
    if sel_store != "All Stores":
        working_df = working_df[working_df["store_name"] == sel_store]
    if sel_rating != "All Ratings":
        working_df = working_df[working_df["Rating"] == sel_rating]

    if working_df.empty:
        st.info("No reviews match the selected filters.")
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
    fig.update_traces(hovertemplate="%{y}: %{x} reviews<extra></extra>")
    fig.update_layout(
        height=250,
        margin=dict(l=0, r=20, t=10, b=0),
        xaxis_title="",
        yaxis_title="# Negative Reviews",
        plot_bgcolor="white",
        paper_bgcolor="white",
        showlegend=False,
    )
    st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})

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
        fig_loc.update_layout(barmode="stack")

        chart_height = max(300, len(loc_neg) * 28)
        fig_loc.update_layout(
            height=chart_height,
            margin=dict(l=0, r=20, t=10, b=0),
            xaxis=dict(title="# Negative Reviews"),
            yaxis=dict(title="", tickfont=dict(size=11)),
            plot_bgcolor="white",
            paper_bgcolor="white",
        )
        st.plotly_chart(fig_loc, use_container_width=True, config={'displayModeBar': False})

    # Table with scroll anchor
    st.markdown('<div id="unresponded-reviews" class="section-header">Review Details</div>', unsafe_allow_html=True)

    display_df = working_df.sort_values("review_date", ascending=False).copy()
    display_df["Status"] = display_df["owner_response"].apply(
        lambda r: "✅ Responded" if pd.notna(r) and str(r).strip() else "⚠️ No Response"
    )
    display_df["Date"] = display_df["review_date"].dt.strftime("%Y-%m-%d")

    display_cols = display_df[[
        "Date", "brand", "store_name", "Rating", "review_text", "Status", "owner_response"
    ]].rename(columns={
        "brand": "Brand",
        "store_name": "Store",
        "review_text": "Review Text",
        "owner_response": "Owner Response",
    })

    st.caption(f"Showing {len(display_cols)} reviews")

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

    # CSV Export (hidden for now)
    # csv = display_cols.to_csv(index=False)
    # st.download_button(
    #     label="📥 Export Needs Attention",
    #     data=csv,
    #     file_name=f"needs_attention_{date.today().isoformat()}.csv",
    #     mime="text/csv",
    #     key="needs_attention_export"
    # )


# =============================================================================
# Main App
# =============================================================================

def page_weekly_report(reviews_df, stores_df):
    """Render the Weekly Report page."""
    st.markdown('<div class="section-header">Weekly Report</div>', unsafe_allow_html=True)

    from src.reports import period_metrics, pct_above_threshold

    today = date.today()

    # ── Build ALL available weeks (Sun-Sat) from Jan 2025 to now ──
    d = date(2025, 1, 1)
    while d.weekday() != 6:        # align to Sunday
        d -= timedelta(days=1)
    all_available_weeks = []
    while d <= today:
        w_start = d
        w_end = d + timedelta(days=6)
        label = f"{w_start.strftime('%b %d')} – {w_end.strftime('%b %d, %Y')}"
        all_available_weeks.append((label, w_start, w_end))
        d += timedelta(days=7)
    all_available_weeks.reverse()  # newest first

    # Group weeks by year for filtering
    years_available = sorted(set(w[2].year for w in all_available_weeks), reverse=True)

    # ── Filter Row ──────────────────────────────────────────────────
    fc1, fc2, fc3, fc4, fc5 = st.columns([1, 2.5, 2, 2, 1])

    with fc1:
        selected_year_w = st.selectbox(
            "📆 Year",
            options=years_available,
            index=0,
            key="weekly_year_select"
        )

    with fc2:
        # Filter weeks to selected year
        year_weeks = [w for w in all_available_weeks if w[2].year == selected_year_w]
        year_week_labels = [w[0] for w in year_weeks]
        default_weeks = year_week_labels[:5]  # top 5 (newest)

        selected_week_labels = st.multiselect(
            "📅 Weeks",
            options=year_week_labels,
            default=default_weeks,
            key="weekly_week_select"
        )

    with fc3:
        all_brands = sorted(stores_df["brand"].unique().tolist())
        selected_brands_w = st.multiselect(
            "🏷️ Brand",
            options=all_brands,
            default=all_brands,
            key="weekly_brand_filter"
        )

    with fc4:
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

    with fc5:
        min_reviews = st.selectbox(
            "📝 Min Reviews",
            options=[0, 1, 2, 3, 5],
            index=0,
            format_func=lambda x: "All" if x == 0 else f"≥ {x} reviews",
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

            # MTD average reviews per week
            days_in_month = (range_end - mtd_start).days + 1
            weeks_elapsed = max(1, days_in_month / 7)
            mtd_weekly_avg = round(mtd["review_count"] / weeks_elapsed, 1) if mtd["review_count"] > 0 else 0.0

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
                "Wk vs MTD Δ": round(weekly["avg_rating"] - mtd["avg_rating"], 2) if weekly["review_count"] > 0 and mtd["review_count"] > 0 else 0.0,
                "MTD Avg/Wk": mtd_weekly_avg,
                "Wk vs MTD Reviews Δ": round(weekly["review_count"] - mtd_weekly_avg, 1) if weekly["review_count"] > 0 else 0.0,
            })

    if not all_weekly_data:
        st.info("No data for the selected filters")
        return

    weekly_df = pd.DataFrame(all_weekly_data)

    # Apply min reviews filter
    if min_reviews > 0:
        weekly_df = weekly_df[weekly_df["# Reviews"] >= min_reviews]

    # ── KPI Cards ──
    total_reviews = weekly_df["# Reviews"].sum()
    avg_rating = weekly_df.loc[weekly_df["# Reviews"] > 0, "Avg Rating"].mean()
    stores_shown = weekly_df[["Brand", "Store"]].drop_duplicates().shape[0]

    # % Above 4.5: based on per-store avg ratings within selected weeks (not static DB rating)
    stores_with_reviews = weekly_df[weekly_df["# Reviews"] > 0]
    if not stores_with_reviews.empty:
        store_avg = stores_with_reviews.groupby(["Brand", "Store"])["Avg Rating"].mean()
        above_count = (store_avg >= 4.5).sum()
        above_pct = round(above_count / len(store_avg) * 100, 1)
    else:
        above_pct = 0.0

    # ── Week-over-week deltas ──
    review_delta = None
    rating_delta = None
    above_pct_delta = None
    stores_delta = None
    unique_weeks = weekly_df["Week"].unique().tolist()
    if len(unique_weeks) >= 2:
        newest_wk = unique_weeks[0]
        prev_wk = unique_weeks[1]
        cur = weekly_df[weekly_df["Week"] == newest_wk]
        prv = weekly_df[weekly_df["Week"] == prev_wk]
        cur_reviews = cur["# Reviews"].sum()
        prv_reviews = prv["# Reviews"].sum()
        if prv_reviews > 0:
            review_delta = ((cur_reviews - prv_reviews) / prv_reviews) * 100
        cur_rating = cur.loc[cur["# Reviews"] > 0, "Avg Rating"].mean()
        prv_rating = prv.loc[prv["# Reviews"] > 0, "Avg Rating"].mean()
        if pd.notna(prv_rating) and prv_rating > 0:
            rating_delta = ((cur_rating - prv_rating) / prv_rating) * 100

        # % Above 4.5 delta: stores with weekly avg >= 4.5 (this wk vs last wk)
        cur_with_reviews = cur[cur["# Reviews"] > 0]
        prv_with_reviews = prv[prv["# Reviews"] > 0]
        cur_above = len(cur_with_reviews[cur_with_reviews["Avg Rating"] >= 4.5])
        prv_above = len(prv_with_reviews[prv_with_reviews["Avg Rating"] >= 4.5])
        cur_above_pct = (cur_above / len(cur_with_reviews) * 100) if len(cur_with_reviews) > 0 else 0
        prv_above_pct = (prv_above / len(prv_with_reviews) * 100) if len(prv_with_reviews) > 0 else 0
        above_pct_delta = cur_above_pct - prv_above_pct  # percentage point change

        # Stores delta: count of stores that received reviews (this wk vs last wk)
        cur_stores = len(cur[cur["# Reviews"] > 0])
        prv_stores = len(prv[prv["# Reviews"] > 0])
        if prv_stores > 0:
            stores_delta = ((cur_stores - prv_stores) / prv_stores) * 100

    kc1, kc2, kc3, kc4 = st.columns(4)
    with kc1:
        render_kpi_card("Total Reviews", int(total_reviews),
                        delta=review_delta, delta_label="% vs last wk")
    with kc2:
        render_kpi_card("Avg Rating", f"{avg_rating:.2f}" if pd.notna(avg_rating) else "—",
                        delta=rating_delta, delta_label="% vs last wk")
    with kc3:
        render_kpi_card("% Above 4.5", f"{above_pct}%",
                        delta=above_pct_delta, delta_label=" pts vs last wk")
    with kc4:
        render_kpi_card("Stores", stores_shown)

    st.markdown("")

    # ── Charts ──────────────────────────────────────────────────────
    chart_colors = {
        "primary": "#4FC3F7",
        "secondary": "#81C784",
        "negative": "#E57373",
        "accent": "#FFD54F",
        "text": DARK_TEXT,
        "bg": "white",
        "grid": "rgba(0,0,0,0.06)",
    }
    chart_layout_w = dict(
        paper_bgcolor="white",
        plot_bgcolor="white",
        font=dict(color=DARK_TEXT, family="Inter, sans-serif"),
        margin=dict(l=40, r=20, t=30, b=30),
        height=220,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    grid_c = "rgba(0,0,0,0.06)"

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
        st.plotly_chart(fig1, use_container_width=True, config={'displayModeBar': False})

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
        st.plotly_chart(fig2, use_container_width=True, config={'displayModeBar': False})

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
        st.plotly_chart(fig3, use_container_width=True, config={'displayModeBar': False})

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
        st.plotly_chart(fig4, use_container_width=True, config={'displayModeBar': False})

    # ── Results summary ──
    st.caption(f"Showing {len(weekly_df)} rows • {len(weeks)} weeks • {stores_shown} stores")

    # ── Data Table with brand row coloring ──
    col_config = {
        "Current Rate": st.column_config.NumberColumn(format="%.1f"),
        "Avg Rating": st.column_config.NumberColumn(format="%.2f"),
        "5★ %": st.column_config.NumberColumn(format="%.1f%%"),
        "1★ %": st.column_config.NumberColumn(format="%.1f%%"),
        "MTD Avg": st.column_config.NumberColumn(format="%.2f"),
        "Wk vs MTD Δ": st.column_config.NumberColumn(format="%.2f"),
        "MTD Avg/Wk": st.column_config.NumberColumn(format="%.1f"),
        "Wk vs MTD Reviews Δ": st.column_config.NumberColumn(format="%.1f"),
    }

    brand_row_colors = {
        "Inspired Cannabis":    "background-color: rgba(255, 228, 196, 0.35)",  # light peach
        "Imagine Cannabis":     "background-color: rgba(200, 255, 200, 0.35)",  # light green
        "Dutch Love":           "background-color: rgba(220, 200, 255, 0.35)",  # light purple
        "Cannabis Supply Co.":  "background-color: rgba(200, 230, 255, 0.35)",  # light blue
        "Muse Cannabis":        "background-color: rgba(255, 220, 240, 0.35)",  # light pink
    }

    def color_brand_rows(row):
        brand = row.get("Brand", "")
        style = brand_row_colors.get(brand, "")
        return [style] * len(row)

    # Round float columns for clean display
    float_cols = ["Avg Rating", "Current Rate", "MTD Avg", "Wk vs MTD Δ", "MTD Avg/Wk", "Wk vs MTD Reviews Δ"]
    for col in float_cols:
        if col in weekly_df.columns:
            weekly_df[col] = weekly_df[col].round(2)

    styled_weekly = weekly_df.style.apply(color_brand_rows, axis=1)

    st.dataframe(
        styled_weekly,
        use_container_width=True,
        hide_index=True,
        height=400,
        column_config=col_config,
    )

    # CSV Export (hidden for now)
    # csv = weekly_df.to_csv(index=False)
    # st.download_button(
    #     label="📥 Export Weekly Data",
    #     data=csv,
    #     file_name=f"weekly_report_{range_start}_{range_end}.csv",
    #     mime="text/csv",
    #     key="weekly_export"
    # )


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
    # % Above 4.5: based on monthly avg review ratings (not static DB rating)
    stores_with_monthly_reviews = monthly_df[monthly_df["# Reviews"] > 0]
    if not stores_with_monthly_reviews.empty:
        above_45 = (stores_with_monthly_reviews["Avg Rating"] >= 4.5).sum()
        above_45_pct = round(above_45 / len(stores_with_monthly_reviews) * 100, 1)
    else:
        above_45_pct = 0.0
    avg_mom = monthly_df["MOM Shift"].mean()

    month_label = f"{month_names[selected_month - 1]} {selected_year}"

    # ── Month-over-month deltas ──
    reviews_delta = None
    rating_delta = None
    above_pct_delta = None
    mom_shift_delta = None

    # Calculate prior month
    if selected_month == 1:
        prior_year_m, prior_month_m = selected_year - 1, 12
    else:
        prior_year_m, prior_month_m = selected_year, selected_month - 1

    prior_rows = []
    for _, store in filtered.iterrows():
        pid = store["place_id"]
        pmm = monthly_metrics(pid, prior_year_m, prior_month_m)
        pshift = mom_shift(pid, prior_year_m, prior_month_m)
        prior_rows.append({
            "Brand": store["brand"],
            "Store": store["store_name"],
            "# Reviews": pmm["review_count"],
            "Avg Rating": pmm["avg_rating"],
            "MOM Shift": pshift if pshift is not None else 0.0,
        })

    if prior_rows:
        prior_df = pd.DataFrame(prior_rows)
        prior_total = prior_df["# Reviews"].sum()
        prior_with_reviews = prior_df[prior_df["# Reviews"] > 0]
        prior_avg = prior_with_reviews["Avg Rating"].mean() if not prior_with_reviews.empty else None
        # Use period-specific avg rating (not static DB rating)
        if not prior_with_reviews.empty:
            prior_above_45 = (prior_with_reviews["Avg Rating"] >= 4.5).sum()
            prior_above_pct = round(prior_above_45 / len(prior_with_reviews) * 100, 1)
        else:
            prior_above_pct = 0.0
        prior_avg_mom = prior_df["MOM Shift"].mean()

        if prior_total > 0:
            reviews_delta = ((total_reviews - prior_total) / prior_total) * 100
        if pd.notna(prior_avg) and prior_avg > 0 and pd.notna(avg_rating):
            rating_delta = ((avg_rating - prior_avg) / prior_avg) * 100
        above_pct_delta = above_45_pct - prior_above_pct  # percentage point change
        if pd.notna(prior_avg_mom) and prior_avg_mom != 0:
            mom_shift_delta = avg_mom - prior_avg_mom

    kc1, kc2, kc3, kc4 = st.columns(4)
    with kc1:
        render_kpi_card(f"Reviews in {month_label}", int(total_reviews),
                        delta=reviews_delta, delta_label="% vs prior mo")
    with kc2:
        render_kpi_card("Avg Rating", f"{avg_rating:.2f}" if pd.notna(avg_rating) else "—",
                        delta=rating_delta, delta_label="% vs prior mo")
    with kc3:
        render_kpi_card("% Above 4.5", f"{above_45_pct}%",
                        delta=above_pct_delta, delta_label=" pts vs prior mo")
    with kc4:
        delta_class = "positive" if avg_mom >= 0 else "negative"
        render_kpi_card("Avg MOM Shift", f"{avg_mom:+.2f}",
                        delta=mom_shift_delta, delta_label=" vs prior mo")

    st.markdown("")

    # ── Charts (matching Weekly Report visual style) ────────────────
    chart_colors = {
        "primary": "#4FC3F7", "secondary": "#81C784", "negative": "#E57373",
        "accent": "#FFD54F", "text": DARK_TEXT, "bg": "white",
        "grid": "rgba(0,0,0,0.06)",
    }
    chart_layout_m = dict(
        paper_bgcolor="white", plot_bgcolor="white",
        font=dict(color=DARK_TEXT, family="Inter, sans-serif"),
        margin=dict(l=40, r=20, t=30, b=30), height=220,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    grid_cm = "rgba(0,0,0,0.06)"

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
        st.plotly_chart(fig1, use_container_width=True, config={'displayModeBar': False})

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
        st.plotly_chart(fig2, use_container_width=True, config={'displayModeBar': False})

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
        st.plotly_chart(fig3, use_container_width=True, config={'displayModeBar': False})

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
        st.plotly_chart(fig4, use_container_width=True, config={'displayModeBar': False})

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
        st.plotly_chart(fig5, use_container_width=True, config={'displayModeBar': False})

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

    # Brand-based row coloring (matching Executive Overview)
    brand_row_colors_m = {
        "Inspired Cannabis":    "background-color: rgba(255, 228, 196, 0.35)",
        "Imagine Cannabis":     "background-color: rgba(200, 255, 200, 0.35)",
        "Dutch Love":           "background-color: rgba(220, 200, 255, 0.35)",
        "Cannabis Supply Co.":  "background-color: rgba(200, 230, 255, 0.35)",
        "Muse Cannabis":        "background-color: rgba(255, 220, 240, 0.35)",
    }

    def color_brand_rows_m(row):
        brand = row.get("Brand", "")
        style = brand_row_colors_m.get(brand, "")
        return [style] * len(row)

    styled = monthly_df.style.map(style_mom, subset=["MOM Shift"]).apply(color_brand_rows_m, axis=1)

    st.dataframe(
        styled,
        use_container_width=True,
        hide_index=True,
        height=400,
        column_config=col_config,
    )

    # CSV Export (hidden for now)
    # csv = monthly_df.to_csv(index=False)
    # st.download_button(
    #     label="📥 Export Monthly Data",
    #     data=csv,
    #     file_name=f"monthly_report_{month_label.replace(' ', '_')}.csv",
    #     mime="text/csv",
    #     key="monthly_export"
    # )


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
    logo_html = f'<img src="data:image/png;base64,{logo_b64}" style="width: 40px; height: 40px; border-radius: 50%; object-fit: cover;" />' if logo_b64 else "🌿"
    st.markdown(f"""
    <div class="dashboard-header">
        <div>
            <h1>{logo_html} Reviews Dashboard</h1>
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
