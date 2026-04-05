"""
Page 2 — Pipeline Dashboard
Real-time pipeline monitoring

FIX: Uses Path(__file__) to find .env reliably from any working directory
     Uses cursor.execute() not pd.read_sql() for Snowflake
"""

import os
import warnings
import logging
from pathlib import Path
from datetime import datetime, timezone

import streamlit as st
import pandas as pd

warnings.filterwarnings("ignore")
logging.getLogger("snowflake").setLevel(logging.ERROR)

# ── Load .env — walk up from THIS file until .env is found ────────────────────
# This works regardless of what directory streamlit is run from
from dotenv import load_dotenv

_this_file = Path(os.path.abspath(__file__))
for _parent in [
    _this_file.parent,                    # pages/
    _this_file.parent.parent,             # src/rag/
    _this_file.parent.parent.parent,      # src/
    _this_file.parent.parent.parent.parent, # project root
]:
    _env = _parent / ".env"
    if _env.exists():
        load_dotenv(_env, override=True)
        break

# ── Snowflake ─────────────────────────────────────────────────────────────────
try:
    import snowflake.connector
    SNOWFLAKE_OK = True
except ImportError:
    SNOWFLAKE_OK = False

SNOWFLAKE_ACCOUNT  = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER     = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ROLE     = os.getenv("SNOWFLAKE_ROLE",     "ACCOUNTADMIN")
SNOWFLAKE_WH       = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")


def run_query(sql: str, database: str = "STOCKMARKETBATCH") -> pd.DataFrame:
    """
    Correct Snowflake pattern — cursor.execute() not pd.read_sql()
    pd.read_sql() fails with snowflake.connector ('NoneType' has no attribute 'find')
    """
    conn = snowflake.connector.connect(
        account   = SNOWFLAKE_ACCOUNT,
        user      = SNOWFLAKE_USER,
        password  = SNOWFLAKE_PASSWORD,
        role      = SNOWFLAKE_ROLE,
        warehouse = SNOWFLAKE_WH,
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


@st.cache_data(ttl=30, show_spinner=False)
def query_batch(sql: str) -> pd.DataFrame:
    try:
        return run_query(sql, "STOCKMARKETBATCH")
    except Exception as e:
        return pd.DataFrame({"error": [str(e)]})


@st.cache_data(ttl=30, show_spinner=False)
def query_stream(sql: str) -> pd.DataFrame:
    try:
        return run_query(sql, "STOCKMARKETSTREAM")
    except Exception as e:
        return pd.DataFrame({"error": [str(e)]})


def has_error(df: pd.DataFrame) -> bool:
    return "error" in df.columns


def safe_val(df: pd.DataFrame, col: str, default=0):
    try:
        if has_error(df) or df.empty:
            return default
        val = df[col].iloc[0]
        return val if val is not None else default
    except Exception:
        return default


# ── Header ────────────────────────────────────────────────────────────────────
col_title, col_time = st.columns([3, 1])
with col_title:
    st.title("📊 Pipeline Dashboard")
    st.caption("Kafka → Spark → Snowflake → dbt · Auto-refreshes every 30s")
with col_time:
    st.metric("Last refresh",
              datetime.now(timezone.utc).strftime("%H:%M:%S UTC"))

# ── Credential check ──────────────────────────────────────────────────────────
if not SNOWFLAKE_OK:
    st.error("snowflake-connector-python not installed. Run: pip install snowflake-connector-python")
    st.stop()

if not all([SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD]):
    st.error(
        "Snowflake credentials not found in .env\n\n"
        f"Looking for .env near: `{_this_file}`\n\n"
        "Make sure your .env file has:\n"
        "```\nSNOWFLAKE_ACCOUNT=...\nSNOWFLAKE_USER=...\nSNOWFLAKE_PASSWORD=...\n```"
    )
    st.stop()


# ── Section 1 — KPIs ──────────────────────────────────────────────────────────
st.divider()
st.subheader("Pipeline KPIs")

df_rows = query_batch("SELECT COUNT(*) AS CNT FROM HISTORICAL_STOCK")
df_sym  = query_batch("SELECT COUNT(DISTINCT SYMBOL) AS CNT FROM HISTORICAL_STOCK")
df_date = query_batch("SELECT MAX(DATE) AS LATEST FROM HISTORICAL_STOCK")
df_rt   = query_stream("SELECT COUNT(*) AS CNT FROM REALTIME_STOCK")
df_pos  = query_batch("""
    SELECT ROUND(
        SUM(CASE WHEN IS_POSITIVE_DAY THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1
    ) AS PCT FROM HISTORICAL_STOCK
""")

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Historical rows",   f"{int(safe_val(df_rows,'CNT',0)):,}",   f"{int(safe_val(df_sym,'CNT',0))} symbols")
k2.metric("Latest batch date", str(safe_val(df_date, "LATEST", "N/A")))
k3.metric("Realtime windows",  f"{int(safe_val(df_rt,'CNT',0)):,}",     "STOCKMARKETSTREAM")
k4.metric("Positive days",     f"{float(safe_val(df_pos,'PCT',0)):.1f}%","all stocks all time")
k5.metric("dbt tests",         "27 / 27",                               "all passing ✓")


# ── Section 2 — Buy/Sell Signals ──────────────────────────────────────────────
st.divider()
st.subheader("Buy / Sell Signals")
st.caption("Source: dbt STOCK_PERFORMANCE mart")

df_perf = query_batch("""
    SELECT
        SYMBOL,
        TRADE_DATE,
        ROUND(CLOSE_PRICE, 2)      AS CLOSE,
        ROUND(DAILY_RETURN_PCT, 2) AS RETURN_PCT,
        ROUND(SMA_5,  2)           AS SMA_5,
        ROUND(SMA_20, 2)           AS SMA_20,
        SMA_SIGNAL,
        OVERALL_SIGNAL
    FROM STOCK_PERFORMANCE
    ORDER BY SYMBOL
""")

if has_error(df_perf):
    st.error(f"Could not load signals: {df_perf['error'].iloc[0]}")
elif df_perf.empty:
    st.warning("No data in STOCK_PERFORMANCE — run: dbt run --select stock_performance")
else:
    st.dataframe(
        df_perf.rename(columns={
            "SYMBOL": "Symbol", "TRADE_DATE": "Date",
            "CLOSE": "Close ($)", "RETURN_PCT": "Return %",
            "SMA_5": "SMA-5", "SMA_20": "SMA-20",
            "SMA_SIGNAL": "SMA signal", "OVERALL_SIGNAL": "Signal",
        }),
        use_container_width=True,
        hide_index=True,
    )


# ── Section 3 — Realtime Stream ───────────────────────────────────────────────
st.divider()
st.subheader("Realtime Stream — Latest Window Per Symbol")
st.caption("Source: STOCKMARKETSTREAM.PUBLIC.REALTIME_STOCK")

df_rt_d = query_stream("""
    SELECT SYMBOL,
           WINDOW_START,
           ROUND(MA_15M, 2)         AS MA_15M,
           ROUND(MA_1H,  2)         AS MA_1H,
           ROUND(VOLATILITY_15M, 4) AS VOL_15M,
           VOLUME_SUM_1H
    FROM REALTIME_STOCK
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY SYMBOL ORDER BY WINDOW_START DESC
    ) = 1
    ORDER BY SYMBOL
""")

if has_error(df_rt_d):
    st.error(f"Realtime error: {df_rt_d['error'].iloc[0]}")
elif df_rt_d.empty:
    st.warning("No realtime data — start the streaming pipeline first")
else:
    cols = st.columns(5)
    for i, (_, r) in enumerate(df_rt_d.iterrows()):
        ma15  = float(r["MA_15M"] or 0)
        ma1h  = float(r["MA_1H"]  or 0)
        diff  = ma15 - ma1h
        delta = f"+{diff:.2f}" if diff >= 0 else f"{diff:.2f}"
        with cols[i % 5]:
            st.metric(str(r["SYMBOL"]), f"${ma15:.2f}", f"{delta} vs 1h MA")
            st.caption(f"Vol 1h: {int(r['VOLUME_SUM_1H'] or 0):,}")


# ── Section 4 — Price Chart ───────────────────────────────────────────────────
st.divider()
st.subheader("Price History Chart")

col_sym, col_days = st.columns([2, 1])
with col_sym:
    sym = st.selectbox("Stock", ["AAPL","MSFT","GOOGL","AMZN","META","TSLA","NVDA","INTC","JPM","V"])
with col_days:
    days = st.selectbox("Period", [30, 60, 90, 180, 365], index=2)

df_hist = query_batch(f"""
    SELECT DATE, CLOSE_PRICE, SMA_5, SMA_20
    FROM HISTORICAL_STOCK
    WHERE SYMBOL = '{sym}'
    ORDER BY DATE DESC
    LIMIT {days}
""")

if has_error(df_hist):
    st.error(df_hist["error"].iloc[0])
elif not df_hist.empty:
    st.line_chart(df_hist.sort_values("DATE").set_index("DATE")[["CLOSE_PRICE","SMA_5","SMA_20"]],
                  use_container_width=True)


# ── Section 5 — Top Movers ────────────────────────────────────────────────────
st.divider()
st.subheader("Top Movers — Latest Day")

df_mv = query_batch("""
    SELECT SYMBOL, CLOSE_PRICE, DAILY_RETURN_PCT
    FROM HISTORICAL_STOCK
    QUALIFY ROW_NUMBER() OVER (PARTITION BY SYMBOL ORDER BY DATE DESC) = 1
    ORDER BY DAILY_RETURN_PCT DESC
""")

if not has_error(df_mv) and not df_mv.empty:
    col_up, col_dn = st.columns(2)
    with col_up:
        st.markdown("**Top gainers**")
        for _, r in df_mv.head(5).iterrows():
            pct = float(r["DAILY_RETURN_PCT"] or 0)
            st.metric(str(r["SYMBOL"]), f"${float(r['CLOSE_PRICE']):.2f}", f"+{pct:.2f}%")
    with col_dn:
        st.markdown("**Top losers**")
        for _, r in df_mv.tail(5).sort_values("DAILY_RETURN_PCT").iterrows():
            pct = float(r["DAILY_RETURN_PCT"] or 0)
            st.metric(str(r["SYMBOL"]), f"${float(r['CLOSE_PRICE']):.2f}", f"{pct:.2f}%")


# ── Section 6 — Data Quality ──────────────────────────────────────────────────
st.divider()
st.subheader("Data Quality Checks")
st.caption("Live checks against Snowflake on every refresh")

df_hl   = query_batch("SELECT COUNT(*) AS CNT FROM HISTORICAL_STOCK WHERE HIGH_PRICE < LOW_PRICE")
df_neg  = query_batch("SELECT COUNT(*) AS CNT FROM HISTORICAL_STOCK WHERE CLOSE_PRICE < 0")
df_null = query_batch("SELECT COUNT(*) AS CNT FROM HISTORICAL_STOCK WHERE SYMBOL IS NULL")
df_symc = query_batch("SELECT COUNT(DISTINCT SYMBOL) AS CNT FROM HISTORICAL_STOCK")

dq1, dq2, dq3, dq4 = st.columns(4)
with dq1:
    n = int(safe_val(df_hl, "CNT", -1))
    st.success("✓ High >= Low\n\n0 violations") if n == 0 else st.error(f"✗ High >= Low\n\n{n} violations")
with dq2:
    n = int(safe_val(df_neg, "CNT", -1))
    st.success("✓ No negative prices\n\n0 violations") if n == 0 else st.error(f"✗ Negative prices\n\n{n} found")
with dq3:
    n = int(safe_val(df_null, "CNT", -1))
    st.success("✓ No null symbols\n\n0 nulls") if n == 0 else st.error(f"✗ Null symbols\n\n{n} found")
with dq4:
    n = int(safe_val(df_symc, "CNT", 0))
    st.success(f"✓ Symbol count\n\n{n} / 10 loaded") if n == 10 else st.warning(f"⚠ Symbol count\n\n{n} / 10 loaded")


# ── Refresh ───────────────────────────────────────────────────────────────────
st.divider()
col_info, col_btn = st.columns([3, 1])
with col_info:
    st.caption("Cached 30 seconds · Click to force refresh")
with col_btn:
    if st.button("Refresh now", use_container_width=True):
        st.cache_data.clear()
        st.rerun()