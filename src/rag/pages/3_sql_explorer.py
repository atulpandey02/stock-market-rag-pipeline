"""
Page 3: SQL Explorer
Run SQL queries directly against Snowflake
"""
import os
import warnings
import logging
from pathlib import Path

import streamlit as st
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv

logging.getLogger("snowflake").setLevel(logging.ERROR)
warnings.filterwarnings("ignore")

_this_dir = os.path.dirname(os.path.abspath(__file__))
_root_dir  = os.path.dirname(_this_dir)
for _p in [Path(_root_dir)/".env", Path(_root_dir).parent/".env",
           Path(_root_dir).parent.parent/".env"]:
    if _p.exists():
        load_dotenv(_p)
        break


def run_query(sql: str, database: str) -> pd.DataFrame:
    conn = snowflake.connector.connect(
        account   = os.getenv("SNOWFLAKE_ACCOUNT"),
        user      = os.getenv("SNOWFLAKE_USER"),
        password  = os.getenv("SNOWFLAKE_PASSWORD"),
        role      = os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database  = database,
        schema    = "PUBLIC",
    )
    df = pd.read_sql(sql, conn)
    conn.close()
    return df


# ── Preset queries ────────────────────────────────────────────────────────────
PRESETS = {
    "Top 5 by avg return": """SELECT symbol,
    ROUND(AVG(daily_return_pct), 4) AS avg_return,
    COUNT(*) AS days
FROM HISTORICAL_STOCK
GROUP BY symbol
ORDER BY avg_return DESC
LIMIT 5;""",

    "SMA golden cross": """WITH crossovers AS (
    SELECT symbol, date, sma_5, sma_20,
        LAG(sma_5)  OVER (PARTITION BY symbol ORDER BY date) AS prev_sma_5,
        LAG(sma_20) OVER (PARTITION BY symbol ORDER BY date) AS prev_sma_20
    FROM HISTORICAL_STOCK
    WHERE sma_5 IS NOT NULL AND sma_20 IS NOT NULL
)
SELECT symbol, date,
    ROUND(sma_5, 2) AS sma_5,
    ROUND(sma_20, 2) AS sma_20
FROM crossovers
WHERE prev_sma_5 < prev_sma_20 AND sma_5 > sma_20
ORDER BY date DESC LIMIT 10;""",

    "Buy/sell signals": """SELECT symbol, trade_date, close_price,
    daily_return_pct, sma_signal, overall_signal
FROM STOCK_PERFORMANCE
ORDER BY symbol;""",

    "Monthly volume": """SELECT symbol,
    DATE_TRUNC('month', date) AS month,
    SUM(volume) AS total_volume,
    ROUND(AVG(close_price), 2) AS avg_close
FROM HISTORICAL_STOCK
GROUP BY symbol, DATE_TRUNC('month', date)
ORDER BY symbol, month DESC
LIMIT 30;""",

    "Data quality check": """SELECT
    DATE_TRUNC('day', date) AS batch_date,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT symbol) AS symbols,
    SUM(CASE WHEN close_price < 0 THEN 1 ELSE 0 END) AS negative_prices,
    SUM(CASE WHEN high_price < low_price THEN 1 ELSE 0 END) AS high_lt_low
FROM HISTORICAL_STOCK
GROUP BY DATE_TRUNC('day', date)
ORDER BY batch_date DESC;""",

    "Realtime stream latest": """SELECT symbol, window_start,
    ROUND(ma_15m, 2) AS ma_15m,
    ROUND(ma_1h,  2) AS ma_1h,
    ROUND(volatility_15m, 4) AS vol_15m,
    volume_sum_1h
FROM REALTIME_STOCK
QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY window_start DESC) = 1
ORDER BY symbol;""",
}


# ── Header ────────────────────────────────────────────────────────────────────
st.markdown("""
    <h1 style='font-family:Space Mono,monospace;font-size:24px;font-weight:700;
               color:#7c3aed;margin:0;padding:20px 0 4px 0;'>
        SQL Explorer
    </h1>
    <p style='color:#64748b;font-size:13px;margin:0 0 20px 0;'>
        Query Snowflake directly · STOCKMARKETBATCH · STOCKMARKETSTREAM
    </p>
""", unsafe_allow_html=True)

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
    st.markdown("**Preset queries**")
    for name in PRESETS:
        if st.button(name, key=f"preset_{name}", use_container_width=True):
            st.session_state["sql_query"]  = PRESETS[name]
            st.session_state["sql_db"]     = "STOCKMARKETSTREAM" if "Realtime" in name else "STOCKMARKETBATCH"
            st.rerun()

    st.markdown("---")
    st.markdown("**Tables available**")
    st.markdown("""
        <div style='font-size:12px;color:#64748b;line-height:1.9;'>
            <b style='color:#94a3b8;'>STOCKMARKETBATCH</b><br>
            · HISTORICAL_STOCK<br>
            · STG_HISTORICAL_STOCK<br>
            · STOCK_DAILY_METRICS<br>
            · STOCK_PERFORMANCE<br><br>
            <b style='color:#94a3b8;'>STOCKMARKETSTREAM</b><br>
            · REALTIME_STOCK<br>
            · STG_REALTIME_STOCK<br>
            · STOCK_REALTIME_SUMMARY
        </div>
    """, unsafe_allow_html=True)


# ── Query Editor ──────────────────────────────────────────────────────────────
col1, col2 = st.columns([3, 1])
with col1:
    db = st.selectbox(
        "Database",
        ["STOCKMARKETBATCH", "STOCKMARKETSTREAM"],
        index=0 if st.session_state.get("sql_db","STOCKMARKETBATCH") == "STOCKMARKETBATCH" else 1,
        label_visibility="collapsed"
    )
with col2:
    run = st.button("Run query ▶", use_container_width=True, type="primary")

default_sql = st.session_state.get("sql_query", PRESETS["Top 5 by avg return"])
sql = st.text_area(
    "SQL Query",
    value     = default_sql,
    height    = 200,
    label_visibility = "collapsed",
    placeholder      = "SELECT * FROM HISTORICAL_STOCK LIMIT 10;"
)
st.session_state["sql_query"] = sql

# ── Run and Display ───────────────────────────────────────────────────────────
if run and sql.strip():
    with st.spinner("Running query against Snowflake..."):
        try:
            df = run_query(sql, db)
            st.markdown(f"""
                <div style='font-family:Space Mono,monospace;font-size:11px;
                            color:#64748b;letter-spacing:1px;margin:12px 0 8px 0;'>
                    {len(df):,} ROWS · {len(df.columns)} COLUMNS · {db}
                </div>
            """, unsafe_allow_html=True)
            st.dataframe(df, use_container_width=True)

            # Download button
            csv = df.to_csv(index=False)
            st.download_button(
                label    = "Download CSV",
                data     = csv,
                file_name= "query_result.csv",
                mime     = "text/csv",
            )
        except Exception as e:
            st.markdown(f"""
                <div style='background:#2d0a0a;border:1px solid #7f1d1d;
                            border-radius:10px;padding:16px;color:#fca5a5;
                            font-size:13px;'>
                    ⚠️ Query error: {str(e)}
                </div>
            """, unsafe_allow_html=True)

elif not run:
    st.markdown("""
        <div style='text-align:center;padding:40px 0;color:#334155;'>
            <div style='font-size:36px;margin-bottom:12px;'>🔍</div>
            <div style='font-size:14px;'>
                Select a preset from the sidebar or write your own SQL
            </div>
        </div>
    """, unsafe_allow_html=True)