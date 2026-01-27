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


# Get wafers for selected lots
@st.cache_data(ttl=60)
def get_wafers(lot_ids: tuple):
    lot_list = ", ".join(f"'{lot}'" for lot in lot_ids)
    try:
        results = db.query(f"SELECT DISTINCT wafer_id FROM wafers WHERE lot_id IN ({lot_list}) ORDER BY wafer_id")
        return [r["wafer_id"] for r in results]
    except Exception:
        return []


# Get tests for selected lots
@st.cache_data(ttl=60)
def get_tests(lot_ids: tuple):
    lot_list = ", ".join(f"'{lot}'" for lot in lot_ids)
    try:
        results = db.query(f"""
            SELECT DISTINCT test_num, test_name 
            FROM tests 
            WHERE lot_id IN ({lot_list}) 
            ORDER BY test_num
        """)
        return {r["test_num"]: r["test_name"] or f"Test_{r['test_num']}" for r in results}
    except Exception:
        return {}


wafers = get_wafers(tuple(selected_lots))
tests = get_tests(tuple(selected_lots))

# Row 2: Wafer and Parameter selection (side by side)
col1, col2 = st.columns(2)

with col1:
    selected_wafers = st.multiselect(
        "**Select Wafers**",
        options=wafers,
        default=wafers,
        help="Select wafers to include"
    )

with col2:
    test_options = [f"{num}: {name}" for num, name in tests.items()]
    selected_tests = st.multiselect(
        "**Select Parameters**",
        options=test_options,
        default=test_options[:20] if len(test_options) > 20 else test_options,
        help="Select test parameters to include"
    )

# Extract test numbers from selection
selected_test_nums = [int(t.split(":")[0]) for t in selected_tests]


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
        COUNT(DISTINCT w.wafer_id) as wafers,
        SUM(w.part_count) as total_parts,
        SUM(w.good_count) as good_parts,
        ROUND(100.0 * SUM(w.good_count) / NULLIF(SUM(w.part_count), 0), 2) as yield_pct
    FROM lots l
    LEFT JOIN wafers w ON l.lot_id = w.lot_id
    WHERE l.lot_id IN ({lot_list})
    GROUP BY l.lot_id, l.product, l.test_type, l.part_type, l.job_name
    ORDER BY l.product, l.test_type, l.lot_id
""")

st.dataframe(summary_df, use_container_width=True, hide_index=True)


# ============================================
# Preview Section (Optional - Expandable)
# ============================================
with st.expander("👁️ Data Preview (Optional)", expanded=False):
    if selected_wafers and selected_test_nums:
        wafer_list = ", ".join(f"'{w}'" for w in selected_wafers)
        test_list = ", ".join(str(t) for t in selected_test_nums[:10])  # Limit for preview
        
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
            JOIN parts p ON tr.part_id = p.part_id
            JOIN tests t ON tr.test_num = t.test_num AND tr.lot_id = t.lot_id
            WHERE tr.lot_id IN ({lot_list})
              AND tr.wafer_id IN ({wafer_list})
              AND tr.test_num IN ({test_list})
            ORDER BY tr.lot_id, tr.wafer_id, tr.part_id, t.test_num
            LIMIT 500
        """)
        
        st.write(f"Showing {len(preview_df):,} rows (max 500, first 10 params)")
        st.dataframe(preview_df, use_container_width=True, hide_index=True)
    else:
        st.info("Select wafers and parameters to preview data.")


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
    if selected_wafers and selected_test_nums:
        wafer_list = ", ".join(f"'{w}'" for w in selected_wafers)
        test_list = ", ".join(str(t) for t in selected_test_nums)
        
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
                        JOIN parts p ON tr.part_id = p.part_id
                        JOIN tests t ON tr.test_num = t.test_num AND tr.lot_id = t.lot_id
                        WHERE tr.lot_id IN ({lot_list})
                          AND tr.wafer_id IN ({wafer_list})
                          AND tr.test_num IN ({test_list})
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
                    JOIN parts p ON tr.part_id = p.part_id
                    JOIN tests t ON tr.test_num = t.test_num AND tr.lot_id = t.lot_id
                    WHERE tr.lot_id IN ({lot_list})
                      AND tr.wafer_id IN ({wafer_list})
                      AND tr.test_num IN ({test_list})
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
                    st.dataframe(export_df.head(100), use_container_width=True, hide_index=True)
            else:
                st.warning("No data found for the selected filters.")
    else:
        st.warning("⚠️ Please select wafers and parameters first.")


# Footer
st.divider()
st.caption("STDF Data Platform v0.2.0 | Select Lots → Wafers → Parameters → Export")
