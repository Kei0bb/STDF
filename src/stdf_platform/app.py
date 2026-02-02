"""Streamlit Web UI for STDF Data Platform."""

import streamlit as st
import pandas as pd
from pathlib import Path

# Add parent path for imports
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from stdf_platform.config import Config
from stdf_platform.database import Database


# Page configuration
st.set_page_config(
    page_title="STDF Data Platform",
    page_icon="📊",
    layout="wide",
)

# Title
st.title("📊 STDF Data Platform")

# Load config
@st.cache_resource
def get_database():
    config = Config.load()
    db = Database(config.storage)
    db.connect()
    return db


db = get_database()


# Get available products and test types
@st.cache_data(ttl=60)
def get_products():
    try:
        results = db.query("SELECT DISTINCT product FROM lots ORDER BY product")
        return [r["product"] for r in results if r["product"]]
    except Exception:
        return []


@st.cache_data(ttl=60)
def get_test_types():
    try:
        results = db.query("SELECT DISTINCT test_type FROM lots ORDER BY test_type")
        return [r["test_type"] for r in results if r["test_type"]]
    except Exception:
        return []


# Get available lots (with product/test_type filter)
@st.cache_data(ttl=60)
def get_lots(product_filter: tuple = (), test_type_filter: tuple = ()):
    try:
        conditions = []
        if product_filter:
            prod_list = ", ".join(f"'{p}'" for p in product_filter)
            conditions.append(f"product IN ({prod_list})")
        if test_type_filter:
            tt_list = ", ".join(f"'{t}'" for t in test_type_filter)
            conditions.append(f"test_type IN ({tt_list})")
        
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        results = db.query(f"SELECT DISTINCT lot_id FROM lots {where} ORDER BY lot_id")
        return [r["lot_id"] for r in results]
    except Exception:
        return []


products = get_products()
test_types = get_test_types()

if not products and not test_types:
    st.warning("No data found. Please ingest STDF files first.")
    st.code("stdf-platform ingest <file.stdf> --product ABC --test-type CP")
    st.stop()


# ============================================
# MAIN AREA - Selection
# ============================================
st.header("🔍 Data Selection")

# Row 0: Product and Test Type filter
col1, col2 = st.columns(2)

with col1:
    selected_products = st.multiselect(
        "**Product Filter**",
        options=products,
        default=[],
        help="Filter by product (leave empty for all)"
    )

with col2:
    selected_test_types = st.multiselect(
        "**Test Type Filter**",
        options=test_types,
        default=[],
        help="Filter by test type (CP/FT)"
    )

# Get lots based on filter
lots = get_lots(tuple(selected_products), tuple(selected_test_types))

if not lots:
    st.info("No lots found for the selected filters.")
    st.stop()

# Row 1: Lot selection
selected_lots = st.multiselect(
    "**Select Lots**",
    options=lots,
    default=lots[:1] if lots else [],
    help="Select one or more lots to analyze"
)

if not selected_lots:
    st.info("👆 Please select at least one lot to continue.")
    st.stop()


# Check if this is FT (no wafer_id) or CP (has wafer_id)
@st.cache_data(ttl=60)
def check_has_wafers(lot_ids: tuple) -> bool:
    """Check if selected lots have wafer data (CP) or not (FT)."""
    lot_list = ", ".join(f"'{lot}'" for lot in lot_ids)
    try:
        results = db.query(f"""
            SELECT COUNT(*) as cnt FROM wafers 
            WHERE lot_id IN ({lot_list}) AND wafer_id IS NOT NULL AND wafer_id != ''
        """)
        return results[0]["cnt"] > 0 if results else False
    except Exception:
        return False


# Get wafers for selected lots (only for CP)
@st.cache_data(ttl=60)
def get_wafers(lot_ids: tuple):
    lot_list = ", ".join(f"'{lot}'" for lot in lot_ids)
    try:
        results = db.query(f"""
            SELECT DISTINCT wafer_id FROM wafers 
            WHERE lot_id IN ({lot_list}) AND wafer_id IS NOT NULL AND wafer_id != ''
            ORDER BY wafer_id
        """)
        return [r["wafer_id"] for r in results if r["wafer_id"]]
    except Exception:
        return []


# Get tests for selected lots as DataFrame
@st.cache_data(ttl=60)
def get_tests_df(lot_ids: tuple):
    lot_list = ", ".join(f"'{lot}'" for lot in lot_ids)
    try:
        return db.query_df(f"""
            SELECT DISTINCT 
                test_num,
                test_name,
                units,
                lo_limit,
                hi_limit
            FROM tests 
            WHERE lot_id IN ({lot_list}) 
            ORDER BY test_num
        """)
    except Exception:
        return pd.DataFrame()


has_wafers = check_has_wafers(tuple(selected_lots))
tests_df = get_tests_df(tuple(selected_lots))

# Wafer selection (only if CP data exists)
if has_wafers:
    wafers = get_wafers(tuple(selected_lots))
    selected_wafers = st.multiselect(
        "**Select Wafers**",
        options=wafers,
        default=wafers,
        help="Select wafers to include"
    )
else:
    selected_wafers = None  # FT mode - no wafer selection needed
    st.info("ℹ️ FT data detected - wafer selection not applicable")


# ============================================
# Parameter Selection with AG Grid
# ============================================
st.divider()
st.subheader("🔬 Parameter Selection")

if tests_df.empty:
    st.warning("No test parameters found for selected lots.")
    st.stop()

# Import AG Grid
from st_aggrid import AgGrid, GridOptionsBuilder, DataReturnMode

# Search filter
search_term = st.text_input(
    "🔍 Search Parameters",
    placeholder="Type to filter by test name...",
    help="Enter partial text to filter parameters"
)

# Filter tests by search
if search_term:
    filtered_df = tests_df[
        tests_df["test_name"].str.contains(search_term, case=False, na=False) |
        tests_df["test_num"].astype(str).str.contains(search_term)
    ].copy()
else:
    filtered_df = tests_df.copy()

# Show statistics
col1, col2 = st.columns(2)
col1.metric("Total Parameters", len(tests_df))
col2.metric("Filtered", len(filtered_df))

st.caption("💡 **Shift+クリック** で範囲選択、**Ctrl+クリック** で個別追加選択")

# Configure AG Grid
gb = GridOptionsBuilder.from_dataframe(filtered_df)
gb.configure_selection(
    selection_mode="multiple",
    use_checkbox=True,
    header_checkbox=True,  # Header checkbox to select all
)
gb.configure_column("test_num", header_name="Test#", width=80)
gb.configure_column("test_name", header_name="Test Name", flex=2)
gb.configure_column("rec_type", header_name="Type", width=70)
gb.configure_column("units", header_name="Units", width=80)
gb.configure_column("lo_limit", header_name="Lo Limit", width=100, type=["numericColumn"])
gb.configure_column("hi_limit", header_name="Hi Limit", width=100, type=["numericColumn"])
gb.configure_grid_options(domLayout='normal')
grid_options = gb.build()

# Display AG Grid
grid_response = AgGrid(
    filtered_df,
    gridOptions=grid_options,
    height=400,
    data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
    fit_columns_on_grid_load=True,
    theme="streamlit",
    update_on=["selectionChanged"],  # Replaces deprecated GridUpdateMode
)

# Get selected rows
selected_rows = grid_response["selected_rows"]
if selected_rows is not None and len(selected_rows) > 0:
    if isinstance(selected_rows, pd.DataFrame):
        selected_test_nums = selected_rows["test_num"].tolist()
    else:
        selected_test_nums = [row["test_num"] for row in selected_rows]
else:
    selected_test_nums = []

if not selected_test_nums:
    st.warning("⚠️ Please select at least one parameter from the table above.")
    st.stop()

st.success(f"✅ {len(selected_test_nums)} parameters selected")


# ============================================
# Summary Section
# ============================================
st.divider()
st.header("📈 Lot Summary")

lot_list = ", ".join(f"'{lot}'" for lot in selected_lots)
summary_df = db.query_df(f"""
    SELECT 
        l.lot_id,
        l.product,
        l.test_type,
        l.part_type,
        l.job_name,
        COUNT(DISTINCT CASE WHEN w.wafer_id != '' THEN w.wafer_id END) as wafers,
        SUM(COALESCE(w.part_count, 0)) as total_parts,
        SUM(COALESCE(w.good_count, 0)) as good_parts,
        ROUND(100.0 * SUM(COALESCE(w.good_count, 0)) / NULLIF(SUM(COALESCE(w.part_count, 0)), 0), 2) as yield_pct
    FROM lots l
    LEFT JOIN wafers w ON l.lot_id = w.lot_id
    WHERE l.lot_id IN ({lot_list})
    GROUP BY l.lot_id, l.product, l.test_type, l.part_type, l.job_name
    ORDER BY l.product, l.test_type, l.lot_id
""")

st.dataframe(summary_df, width="stretch", hide_index=True)


# ============================================
# Build WHERE clause helper
# ============================================
def build_where_clause(lot_list: str, wafer_list: str | None, test_list: str) -> str:
    """Build WHERE clause for queries, handling FT (no wafer) and CP (with wafer)."""
    conditions = [f"tr.lot_id IN ({lot_list})", f"tr.test_num IN ({test_list})"]
    if wafer_list:
        conditions.append(f"tr.wafer_id IN ({wafer_list})")
    return " AND ".join(conditions)


# ============================================
# Preview Section (Optional - Expandable)
# ============================================
with st.expander("👁️ Data Preview (Optional)", expanded=False):
    if selected_test_nums:
        wafer_list = ", ".join(f"'{w}'" for w in selected_wafers) if selected_wafers else None
        test_list = ", ".join(str(t) for t in selected_test_nums[:10])  # Limit for preview
        where_clause = build_where_clause(lot_list, wafer_list, test_list)
        
        preview_df = db.query_df(f"""
            SELECT 
                tr.lot_id,
                tr.wafer_id,
                tr.part_id,
                p.x_coord,
                p.y_coord,
                p.hard_bin,
                p.soft_bin,
                t.test_name,
                tr.result,
                tr.passed
            FROM test_results tr
            JOIN parts p ON tr.part_id = p.part_id AND tr.lot_id = p.lot_id
            JOIN tests t ON tr.test_num = t.test_num AND tr.lot_id = t.lot_id
            WHERE {where_clause}
            ORDER BY tr.lot_id, tr.wafer_id, tr.part_id, t.test_num
            LIMIT 500
        """)
        
        st.write(f"Showing {len(preview_df):,} rows (max 500, first 10 params)")
        st.dataframe(preview_df, width="stretch", hide_index=True)
    else:
        st.info("Select parameters to preview data.")


# ============================================
# Export Section
# ============================================
st.divider()
st.header("⬇️ Export to CSV")

col1, col2, col3 = st.columns([2, 2, 1])

with col1:
    export_format = st.radio(
        "**Format**",
        ["Pivot (one row per part)", "Long (one row per test)"],
        horizontal=True,
        help="Pivot format is recommended for JMP correlation analysis"
    )

with col2:
    export_filename = st.text_input("**Filename**", value="stdf_export.csv")

with col3:
    st.write("")  # Spacer
    st.write("")
    generate_btn = st.button("🔄 Generate", type="primary", use_container_width=True)

if generate_btn:
    if selected_test_nums:
        wafer_list = ", ".join(f"'{w}'" for w in selected_wafers) if selected_wafers else None
        test_list = ", ".join(str(t) for t in selected_test_nums)
        where_clause = build_where_clause(lot_list, wafer_list, test_list)
        
        with st.spinner("Generating export..."):
            if "Pivot" in export_format:
                # Pivot format
                export_df = db.query_df(f"""
                    PIVOT (
                        SELECT 
                            tr.lot_id,
                            tr.wafer_id,
                            tr.part_id,
                            p.x_coord,
                            p.y_coord,
                            p.hard_bin,
                            p.soft_bin,
                            p.passed as part_passed,
                            t.test_name,
                            tr.result
                        FROM test_results tr
                        JOIN parts p ON tr.part_id = p.part_id AND tr.lot_id = p.lot_id
                        JOIN tests t ON tr.test_num = t.test_num AND tr.lot_id = t.lot_id
                        WHERE {where_clause}
                    )
                    ON test_name
                    USING first(result)
                    GROUP BY lot_id, wafer_id, part_id, x_coord, y_coord, hard_bin, soft_bin, part_passed
                    ORDER BY lot_id, wafer_id, part_id
                """)
            else:
                # Long format
                export_df = db.query_df(f"""
                    SELECT 
                        tr.lot_id,
                        tr.wafer_id,
                        tr.part_id,
                        p.x_coord,
                        p.y_coord,
                        p.hard_bin,
                        p.soft_bin,
                        t.test_num,
                        t.test_name,
                        tr.result,
                        tr.passed,
                        t.lo_limit,
                        t.hi_limit,
                        t.units
                    FROM test_results tr
                    JOIN parts p ON tr.part_id = p.part_id AND tr.lot_id = p.lot_id
                    JOIN tests t ON tr.test_num = t.test_num AND tr.lot_id = t.lot_id
                    WHERE {where_clause}
                    ORDER BY tr.lot_id, tr.wafer_id, tr.part_id, t.test_num
                """)
            
            if not export_df.empty:
                st.success(f"✅ Generated {len(export_df):,} rows")
                
                # Download button
                csv = export_df.to_csv(index=False)
                st.download_button(
                    label="⬇️ Download CSV",
                    data=csv,
                    file_name=export_filename,
                    mime="text/csv",
                    type="primary",
                )
                
                # Show first few rows
                with st.expander("Preview exported data", expanded=True):
                    st.dataframe(export_df.head(100), width="stretch", hide_index=True)
            else:
                st.warning("No data found for the selected filters.")
    else:
        st.warning("⚠️ Please select parameters first.")


# Footer
st.divider()
st.caption("STDF Data Platform v0.3.0 | Product → Lot → Wafer (CP only) → Parameters → Export")
