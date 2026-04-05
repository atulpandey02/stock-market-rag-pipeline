"""
Page 3 — SQL Explorer
Uses cursor.execute() instead of pd.read_sql() — correct Snowflake pattern
"""
import os
import warnings
import logging
from pathlib import Path

import streamlit as st
import pandas as pd
from dotenv import load_dotenv, find_dotenv

logging.getLogger("snowflake").setLevel(logging.ERROR)
warnings.filterwarnings("ignore")

_this_dir    = os.path.dirname(os.path.abspath(__file__))
_root_dir    = os.path.dirname(_this_dir)
_dotenv_path = find_dotenv(usecwd=True, raise_error_if_not_found=False)
if _dotenv_path:
    load_dotenv(_dotenv_path, override=True)
else:
    for _p in [Path(_root_dir)/".env", Path(_root_dir).parent/".env",
               Path(_root_dir).parent.parent/".env"]:
        if _p.exists():
            load_dotenv(_p, override=True)
            break

try:
    import snowflake.connector
    SNOWFLAKE_OK = True
except ImportError:
    SNOWFLAKE_OK = False

SNOWFLAKE_ACCOUNT  = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER     = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")


def run_query(sql: str, database: str) -> pd.DataFrame:
    """
    ✅ Correct Snowflake pattern — cursor.execute() not pd.read_sql()
    """
    conn = snowflake.connector.connect(
        account   = SNOWFLAKE_ACCOUNT,
        user      = SNOWFLAKE_USER,
        password  = SNOWFLAKE_PASSWORD,
        role      = os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database  = database,
        schema    = "PUBLIC",
    )
    cur  = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    cols = [desc[0] for desc in cur.description]
    cur.close()
    conn.close()
    return pd.DataFrame(rows, columns=cols)


# ── Presets ───────────────────────────────────────────────────────────────────
PRESETS = {
    "Top 5 by avg return": {
        "db":  "STOCKMARKETBATCH",
        "sql": """SELECT SYMBOL,
    ROUND(AVG(DAILY_RETURN_PCT), 4) AS AVG_RETURN,
    COUNT(*) AS TRADING_DAYS
FROM HISTORICAL_STOCK
GROUP BY SYMBOL
ORDER BY AVG_RETURN DESC
LIMIT 5;"""
    },
    "Golden cross (SMA crossover)": {
        "db":  "STOCKMARKETBATCH",
        "sql": """WITH CROSSOVERS AS (
    SELECT SYMBOL, DATE, SMA_5, SMA_20,
        LAG(SMA_5)  OVER (PARTITION BY SYMBOL ORDER BY DATE) AS PREV_SMA5,
        LAG(SMA_20) OVER (PARTITION BY SYMBOL ORDER BY DATE) AS PREV_SMA20
    FROM HISTORICAL_STOCK
    WHERE SMA_5 IS NOT NULL AND SMA_20 IS NOT NULL
)
SELECT SYMBOL, DATE,
    ROUND(SMA_5, 2)  AS SMA_5,
    ROUND(SMA_20, 2) AS SMA_20
FROM CROSSOVERS
WHERE PREV_SMA5 < PREV_SMA20 AND SMA_5 > SMA_20
ORDER BY DATE DESC
LIMIT 10;"""
    },
    "Buy/sell signals (dbt)": {
        "db":  "STOCKMARKETBATCH",
        "sql": """SELECT SYMBOL, TRADE_DATE, CLOSE_PRICE,
    DAILY_RETURN_PCT, SMA_SIGNAL, OVERALL_SIGNAL
FROM STOCK_PERFORMANCE
ORDER BY SYMBOL;"""
    },
    "Monthly volume by symbol": {
        "db":  "STOCKMARKETBATCH",
        "sql": """SELECT SYMBOL,
    DATE_TRUNC('month', DATE) AS MONTH,
    SUM(VOLUME)               AS TOTAL_VOLUME,
    ROUND(AVG(CLOSE_PRICE),2) AS AVG_CLOSE
FROM HISTORICAL_STOCK
GROUP BY SYMBOL, DATE_TRUNC('month', DATE)
ORDER BY SYMBOL, MONTH DESC
LIMIT 30;"""
    },
    "Data quality check": {
        "db":  "STOCKMARKETBATCH",
        "sql": """SELECT
    DATE_TRUNC('day', DATE)  AS BATCH_DATE,
    COUNT(*)                 AS TOTAL_ROWS,
    COUNT(DISTINCT SYMBOL)   AS SYMBOLS,
    SUM(CASE WHEN CLOSE_PRICE < 0        THEN 1 ELSE 0 END) AS NEG_PRICES,
    SUM(CASE WHEN HIGH_PRICE < LOW_PRICE THEN 1 ELSE 0 END) AS HIGH_LT_LOW
FROM HISTORICAL_STOCK
GROUP BY DATE_TRUNC('day', DATE)
ORDER BY BATCH_DATE DESC;"""
    },
    "Realtime latest window": {
        "db":  "STOCKMARKETSTREAM",
        "sql": """SELECT SYMBOL, WINDOW_START,
    ROUND(MA_15M, 2)         AS MA_15M,
    ROUND(MA_1H,  2)         AS MA_1H,
    ROUND(VOLATILITY_15M, 4) AS VOL_15M,
    VOLUME_SUM_1H
FROM REALTIME_STOCK
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY SYMBOL ORDER BY WINDOW_START DESC
) = 1
ORDER BY SYMBOL;"""
    },
}

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### Preset queries")
    for name, preset in PRESETS.items():
        if st.button(name, key=f"preset_{name}", use_container_width=True):
            st.session_state["sql_query"] = preset["sql"]
            st.session_state["sql_db"]    = preset["db"]
            st.rerun()

    st.divider()
    st.markdown("### Tables")
    st.caption("**STOCKMARKETBATCH**")
    for t in ["HISTORICAL_STOCK","STG_HISTORICAL_STOCK",
              "STOCK_DAILY_METRICS","STOCK_PERFORMANCE"]:
        st.code(t, language=None)
    st.caption("**STOCKMARKETSTREAM**")
    for t in ["REALTIME_STOCK","STG_REALTIME_STOCK","STOCK_REALTIME_SUMMARY"]:
        st.code(t, language=None)

# ── Header ────────────────────────────────────────────────────────────────────
st.title("🔍 SQL Explorer")
st.caption("Query Snowflake directly · STOCKMARKETBATCH · STOCKMARKETSTREAM")
st.divider()

if not SNOWFLAKE_OK:
    st.error("Run: pip install snowflake-connector-python")
    st.stop()

# ── Editor ────────────────────────────────────────────────────────────────────
db_options  = ["STOCKMARKETBATCH","STOCKMARKETSTREAM"]
default_db  = st.session_state.get("sql_db","STOCKMARKETBATCH")
db = st.selectbox("Database", db_options,
                  index=db_options.index(default_db))

default_sql = st.session_state.get("sql_query",
              PRESETS["Top 5 by avg return"]["sql"])
sql = st.text_area("SQL query", value=default_sql, height=200,
                   placeholder="SELECT * FROM HISTORICAL_STOCK LIMIT 10;")
st.session_state["sql_query"] = sql

col_run, _ = st.columns([1, 4])
run = col_run.button("▶ Run query", type="primary", use_container_width=True)

# ── Results ───────────────────────────────────────────────────────────────────
if run and sql.strip():
    with st.spinner("Running against Snowflake..."):
        try:
            df = run_query(sql, db)
            st.success(f"{len(df):,} rows · {len(df.columns)} columns · {db}")
            st.dataframe(df, use_container_width=True, hide_index=True)
            st.download_button(
                label     = "Download as CSV",
                data      = df.to_csv(index=False),
                file_name = "query_result.csv",
                mime      = "text/csv",
            )
        except Exception as e:
            st.error(f"Query error: {str(e)}")
else:
    st.info("Select a preset from the sidebar or write your own SQL, then click Run query")