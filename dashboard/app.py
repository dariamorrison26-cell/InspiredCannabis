"""
Cannabis Reviews Dashboard — Streamlit App
==========================================
Interactive dashboard for Inspired Cannabis & brands.
Reads from the same SQLite DB as the Google Sheets pipeline.
"""

import base64
import sys
from pathlib import Path

# Add project root to path so we can import src modules
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from datetime import date, timedelta
from calendar import monthrange

from src import database as db
from src.reports import (
    period_metrics,
    ytd_metrics,
    monthly_metrics,
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
        padding: 1.2rem 2rem;
        border-radius: 12px;
        margin-bottom: 1.5rem;
        display: flex;
        align-items: center;
        justify-content: space-between;
    }}
    .dashboard-header h1 {{
        margin: 0;
        font-size: 1.6rem;
        font-weight: 700;
        letter-spacing: 0.5px;
    }}
    .dashboard-header .subtitle {{
        font-size: 0.85rem;
        opacity: 0.8;
        margin-top: 4px;
    }}

    /* KPI Cards */
    .kpi-card {{
        background: {WHITE};
        border-radius: 12px;
        padding: 1.2rem 1rem;
        text-align: center;
        box-shadow: 0 2px 8px rgba(0,0,0,0.06);
        border-top: 4px solid {ORANGE};
        transition: transform 0.2s;
    }}
    .kpi-card:hover {{
        transform: translateY(-2px);
        box-shadow: 0 4px 16px rgba(0,0,0,0.1);
    }}
    .kpi-value {{
        font-size: 2rem;
        font-weight: 800;
        color: {NAVY};
        margin: 0.3rem 0;
        line-height: 1;
    }}
    .kpi-label {{
        font-size: 0.8rem;
        color: #666;
        text-transform: uppercase;
        letter-spacing: 1px;
        font-weight: 600;
    }}
    .kpi-delta {{
        font-size: 0.75rem;
        margin-top: 4px;
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

    # Header
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
                height=300,
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
                height=300,
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
                height=300,
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
                height=300,
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

            # Star distributions (2026 — percentages only)
            row_data["1★ %"] = y2026["one_star_pct"]
            row_data["5★ %"] = y2026["five_star_pct"]

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
            "2025 Summary":     (["2025 Avg", "2025 Total"], True),
            "2026 Summary":     (["2026 Avg", "2026 Total"], True),
            "2026 Monthly":     ([f"{mn} 2026" for mn in month_names_full], True),
            "Star Distribution":(["1★ %", "5★ %"], True),
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
            "2025 Avg": st.column_config.NumberColumn(format="%.1f"),
            "2025 Total": st.column_config.NumberColumn(format="%d"),
            "2026 Avg": st.column_config.NumberColumn(format="%.1f"),
            "2026 Total": st.column_config.NumberColumn(format="%d"),
            "1★ %": st.column_config.NumberColumn(format="%.1f%%"),
            "5★ %": st.column_config.NumberColumn(format="%.1f%%"),
        }

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
            height=600,
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
        height=600,
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

def main():
    # Sidebar filters
    selected_brands, date_range, min_rating, search_term = render_sidebar()

    # Load data
    reviews_df = get_reviews_df()
    stores_df = get_stores_df()

    # Apply filters to reviews
    filtered_reviews = apply_filters(reviews_df, selected_brands, date_range, min_rating, search_term)

    # Navigation tabs
    tab1, tab2, tab3 = st.tabs([
        "📊 Executive Overview",
        "📝 All Reviews",
        "🚨 Needs Attention"
    ])

    with tab1:
        page_overview(filtered_reviews, stores_df, selected_brands)

    with tab2:
        page_all_reviews(filtered_reviews)

    with tab3:
        page_needs_attention(reviews_df)


if __name__ == "__main__":
    main()
