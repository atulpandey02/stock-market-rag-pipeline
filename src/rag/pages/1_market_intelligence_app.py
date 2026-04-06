"""
Page 1 — Market Intelligence (RAG Chat)

What changed vs previous version:
  + get_stock_metrics_from_snowflake()  pulls latest dbt mart data
  + generate_with_groq()               now accepts pipeline_metrics arg
  + system prompt                      injects BOTH news + dbt signals
  + UI                                 shows pipeline metrics card above answer

Why this matters:
  RAG now consumes dbt STOCK_PERFORMANCE output directly.
  The answer is grounded in both quantitative pipeline signals
  (SMA crossovers, BUY/SELL, daily return) AND financial news.
  The two pipelines are genuinely connected — not just co-existing.
"""

import os
import sys
import warnings
import logging
import requests
import streamlit as st
from pathlib import Path
from dotenv import load_dotenv, find_dotenv

logging.getLogger("sentence_transformers").setLevel(logging.ERROR)
warnings.filterwarnings("ignore")

# ── Path setup ────────────────────────────────────────────────────────────────
_this_dir = os.path.dirname(os.path.abspath(__file__))
_root_dir  = os.path.dirname(_this_dir)
sys.path.insert(0, _root_dir)

# ── Load .env ─────────────────────────────────────────────────────────────────
_this_file = Path(os.path.abspath(__file__))
for _parent in [
    _this_file.parent,
    _this_file.parent.parent,
    _this_file.parent.parent.parent,
    _this_file.parent.parent.parent.parent,
]:
    _env = _parent / ".env"
    if _env.exists():
        load_dotenv(_env, override=True)
        break


# ── Helpers ───────────────────────────────────────────────────────────────────

def safe_str(val, default="") -> str:
    return str(val) if val is not None else default

def safe_float(val, default=0.0) -> float:
    try:
        return float(val) if val is not None else default
    except (TypeError, ValueError):
        return default

def safe_date(val) -> str:
    s = safe_str(val)
    return s[:10] if s else "N/A"


# ── NEW: Snowflake — Pull dbt STOCK_PERFORMANCE data ─────────────────────────

def get_stock_metrics_from_snowflake(symbol: str) -> dict:
    """
    Query the dbt STOCK_PERFORMANCE mart for a given stock.

    This is the key connection between the data engineering pipeline
    and the RAG intelligence layer. The STOCK_PERFORMANCE table only
    exists because the full pipeline ran:
      Finnhub → Kafka → MinIO → Spark → Snowflake → dbt run

    Returns a dict with all metrics, or empty dict on failure.
    """
    try:
        import snowflake.connector

        conn = snowflake.connector.connect(
            account   = os.getenv("SNOWFLAKE_ACCOUNT"),
            user      = os.getenv("SNOWFLAKE_USER"),
            password  = os.getenv("SNOWFLAKE_PASSWORD"),
            role      = os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
            warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
            database  = "STOCKMARKETBATCH",
            schema    = "PUBLIC",
        )
        cur = conn.cursor()
        cur.execute(f"""
            SELECT
                TRADE_DATE,
                ROUND(CLOSE_PRICE,      2) AS CLOSE,
                ROUND(SMA_5,            2) AS SMA_5,
                ROUND(SMA_20,           2) AS SMA_20,
                ROUND(DAILY_RETURN_PCT, 2) AS RETURN_PCT,
                OVERALL_SIGNAL,
                SMA_SIGNAL
            FROM STOCKMARKETBATCH.PUBLIC.STOCK_PERFORMANCE
            WHERE SYMBOL = '{symbol.upper()}'
            ORDER BY TRADE_DATE DESC
            LIMIT 1
        """)
        row  = cur.fetchone()
        cols = [desc[0] for desc in cur.description]
        cur.close()
        conn.close()

        if row:
            return dict(zip(cols, row))
        return {}

    except Exception as e:
        # Gracefully degrade — if Snowflake is unavailable RAG still works
        # just without the pipeline data context
        return {"error": str(e)}


def format_metrics_for_prompt(metrics: dict, symbol: str) -> str:
    """
    Format dbt metrics into a clean string for injection into Groq prompt.
    """
    if not metrics or "error" in metrics:
        return ""

    signal    = safe_str(metrics.get("OVERALL_SIGNAL"), "N/A")
    sma_sig   = safe_str(metrics.get("SMA_SIGNAL"),     "N/A")
    close     = safe_float(metrics.get("CLOSE"),        0.0)
    sma5      = safe_float(metrics.get("SMA_5"),        0.0)
    sma20     = safe_float(metrics.get("SMA_20"),       0.0)
    ret_pct   = safe_float(metrics.get("RETURN_PCT"),   0.0)
    date      = safe_str(metrics.get("TRADE_DATE"),     "N/A")

    return f"""
=== QUANTITATIVE PIPELINE DATA for {symbol.upper()} (as of {date}) ===
Close Price    : ${close}
SMA-5          : ${sma5}   (5-day moving average)
SMA-20         : ${sma20}  (20-day moving average)
Daily Return   : {ret_pct}%
SMA Signal     : {sma_sig}
Overall Signal : {signal}
Data source    : dbt STOCK_PERFORMANCE mart (Snowflake)
=== END PIPELINE DATA ===
"""


# ── Backend — Embedding ───────────────────────────────────────────────────────

@st.cache_resource(show_spinner="Loading embedding model...")
def load_embedding_model():
    from sentence_transformers import SentenceTransformer
    return SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")


# ── Backend — Pinecone Retrieval ──────────────────────────────────────────────

def retrieve_from_pinecone(question: str, symbol: str = None, top_k: int = 5):
    from pinecone import Pinecone

    model     = load_embedding_model()
    query_vec = model.encode([question])[0].tolist()

    pc    = Pinecone(api_key=os.getenv("PINECONE_API_KEY"))
    index = pc.Index(os.getenv("PINECONE_INDEX_NAME", "stock-market-rag"))

    params = {"vector": query_vec, "top_k": top_k, "include_metadata": True}
    if symbol and symbol != "All Stocks":
        params["filter"] = {"symbol": symbol.upper()}

    results = index.query(**params)

    seen, unique = set(), []
    for m in results.get("matches", []):
        meta = m.get("metadata") or {}
        url  = safe_str(meta.get("url"))
        if url and url not in seen:
            seen.add(url)
            unique.append({
                "title":        safe_str(meta.get("title"),        "N/A"),
                "summary":      safe_str(meta.get("summary"),      ""),
                "symbol":       safe_str(meta.get("symbol"),       ""),
                "source":       safe_str(meta.get("source"),       ""),
                "url":          url,
                "published_at": safe_str(meta.get("published_at"), ""),
                "sentiment":    safe_str(meta.get("sentiment"),    "Neutral"),
                "score":        safe_float(m.get("score"),         0.0),
            })
    return unique


# ── Backend — Groq LLM (UPDATED — now receives pipeline metrics) ──────────────

def generate_with_groq(
    question:         str,
    chunks:           list,
    pipeline_metrics: str = ""   # ← NEW: dbt STOCK_PERFORMANCE data
) -> str:
    """
    Calls Groq with BOTH quantitative pipeline data AND news chunks.

    The system prompt now has two information sources:
      1. pipeline_metrics  — from dbt STOCK_PERFORMANCE (Snowflake)
      2. news context      — from Pinecone semantic search

    This grounds the answer in your actual pipeline output, not just news.
    If pipeline metrics are unavailable (Snowflake down), falls back
    gracefully to news-only mode — same as before.
    """
    groq_key = os.getenv("GROQ_API_KEY")
    if not groq_key:
        return "⚠️ GROQ_API_KEY is missing — check your .env file."
    if not chunks and not pipeline_metrics:
        return "⚠️ No data available — check Pinecone index and Snowflake connection."

    # Build news context from Pinecone chunks
    news_context = "\n\n".join([
        f"[{c['symbol']}] {c['title']}\n"
        f"Published: {safe_date(c['published_at'])} | Source: {c['source']}\n"
        f"Sentiment: {c['sentiment']}\n"
        f"Summary: {c['summary']}"
        for c in chunks
    ]) if chunks else "No news articles retrieved."

    # ── System prompt — combines both data sources ─────────────────────────
    if pipeline_metrics:
        system_prompt = """You are a professional financial market analyst with access to \
two data sources:
1. Quantitative signals computed by a real-time data pipeline (Spark + dbt transformations)
2. Recent financial news articles retrieved from a vector database

Your job is to synthesise BOTH sources into a coherent analysis.
Rules:
- Always reference the pipeline signal (BULLISH/BEARISH/NEUTRAL) explicitly
- If the pipeline signal conflicts with news sentiment, flag this clearly
- If SMA-5 > SMA-20, mention this is a bullish crossover signal
- If SMA-5 < SMA-20, mention this is a bearish crossover signal
- Be concise — 4-6 sentences
- Do not use external knowledge beyond what is provided"""
    else:
        # Fallback — Snowflake unavailable, news-only mode
        system_prompt = """You are a professional financial market analyst.
Answer questions using ONLY the provided news articles.
Be concise (3-5 sentences). Cite sources where relevant.
Do not use external knowledge."""

    # ── User message — injects both data sources ───────────────────────────
    user_content = ""
    if pipeline_metrics:
        user_content += f"QUANTITATIVE PIPELINE DATA:\n{pipeline_metrics}\n\n"
    user_content += f"RECENT NEWS ARTICLES:\n{news_context}\n\n"
    user_content += f"Question: {question}\n\nAnalysis:"

    try:
        response = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {groq_key}",
                "Content-Type":  "application/json",
            },
            json={
                "model": "llama-3.3-70b-versatile",
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user",   "content": user_content},
                ],
                "temperature": 0.1,
                "max_tokens":  600,
            },
            timeout=30,
        )
        if response.status_code == 200:
            return response.json()["choices"][0]["message"]["content"].strip()
        return f"⚠️ Groq API error {response.status_code}: {response.text[:300]}"
    except requests.exceptions.Timeout:
        return "⚠️ Groq API timed out — try again."
    except Exception as e:
        return f"⚠️ Error: {str(e)}"


# ── Backend — Pinecone Stats ──────────────────────────────────────────────────

def get_index_stats():
    try:
        from pinecone import Pinecone
        pc    = Pinecone(api_key=os.getenv("PINECONE_API_KEY"))
        index = pc.Index(os.getenv("PINECONE_INDEX_NAME", "stock-market-rag"))
        stats = index.describe_index_stats()
        return {
            "total_vectors": stats.total_vector_count or 0,
            "dimension":     stats.dimension or 384,
            "status":        "Connected ✓"
        }
    except Exception as e:
        return {"total_vectors": 0, "dimension": 384, "status": f"Error: {e}"}


# ── NEW: Render pipeline metrics card ─────────────────────────────────────────

def render_pipeline_metrics(metrics: dict, symbol: str):
    """
    Show a compact metrics card pulled from the dbt mart.
    This makes the pipeline connection visible in the UI.
    """
    if not metrics or "error" in metrics:
        return

    signal  = safe_str(metrics.get("OVERALL_SIGNAL"), "N/A")
    close   = safe_float(metrics.get("CLOSE"),        0.0)
    sma5    = safe_float(metrics.get("SMA_5"),        0.0)
    sma20   = safe_float(metrics.get("SMA_20"),       0.0)
    ret_pct = safe_float(metrics.get("RETURN_PCT"),   0.0)
    date    = safe_str(metrics.get("TRADE_DATE"),     "")

    signal_color = (
    "🟢" if signal.upper() in ("BULLISH", "BUY")  else
    "🔴" if signal.upper() in ("BEARISH", "SELL") else
    "🟡"
    )

    with st.expander(
        f"📊 Pipeline Data — {symbol} · {signal_color} {signal} · from dbt STOCK_PERFORMANCE",
        expanded=True
    ):
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Close",     f"${close}")
        c2.metric("SMA-5",     f"${sma5}")
        c3.metric("SMA-20",    f"${sma20}")
        c4.metric("Return",    f"{ret_pct}%",
                  delta=f"{ret_pct}%",
                  delta_color="normal")
        st.caption(
            f"Source: Snowflake → dbt STOCK_PERFORMANCE mart · "
            f"As of: {str(date)[:10]}"
        )


# ── Helper — render sources ───────────────────────────────────────────────────

def render_sources(chunks: list):
    if not chunks:
        return
    with st.expander(f"📰 News Sources ({len(chunks)})"):
        for i, src in enumerate(chunks, 1):
            sent  = safe_str(src.get("sentiment"), "Neutral")
            score = safe_float(src.get("score"),   0.0)
            icon  = "📈" if sent == "Positive" else "📉" if sent == "Negative" else "➖"
            pub   = safe_date(src.get("published_at"))
            url   = safe_str(src.get("url"), "#")
            title = safe_str(src.get("title"), "N/A")

            col_a, col_b = st.columns([3, 1])
            with col_a:
                st.markdown(f"**[{i}] {title}**")
                st.caption(
                    f"{safe_str(src.get('symbol'))} · "
                    f"{safe_str(src.get('source'))} · "
                    f"{pub}"
                )
                if url and url != "#":
                    st.markdown(f"[View article →]({url})")
            with col_b:
                st.metric("Score", f"{score:.3f}")
                st.caption(f"{icon} {sent}")

            if i < len(chunks):
                st.divider()


# ── Sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("### Filter")
    stocks = ["All Stocks","AAPL","MSFT","GOOGL","AMZN",
              "META","TSLA","NVDA","INTC","JPM","V"]
    selected_stock = st.selectbox("Stock", stocks, label_visibility="collapsed")

    st.divider()
    st.markdown("### Quick questions")
    ticker = selected_stock if selected_stock != "All Stocks" else "AAPL"
    for q in [
        f"What is the outlook for {ticker}?",
        f"Should I be concerned about {ticker}?",
        "What is the current market sentiment?",
        "Any supply chain concerns for tech stocks?",
        "What are analysts saying about earnings?",
    ]:
        if st.button(q, key=f"qq_{q}", use_container_width=True):
            st.session_state["quick_q"] = q
            st.rerun()

    st.divider()
    st.markdown("### Data sources")
    st.caption("📊 Pipeline: Snowflake dbt mart")
    st.caption("📰 News: Finnhub API → Pinecone")
    st.caption("🔢 Embeddings: all-MiniLM-L6-v2")
    st.caption("🧠 LLM: Groq llama-3.3-70b-versatile")

    st.divider()
    if st.button("Check Pinecone status", use_container_width=True):
        with st.spinner("Checking..."):
            stats = get_index_stats()
        st.metric("Vectors stored", f"{stats['total_vectors']:,}")
        st.caption(f"Dim: {stats['dimension']} · {stats['status']}")

    st.divider()
    if st.button("Clear chat", use_container_width=True):
        st.session_state["messages"] = []
        st.rerun()


# ── Header ────────────────────────────────────────────────────────────────────

st.title("📈 Market Intelligence")
st.caption(
    "Grounded in pipeline data (dbt STOCK_PERFORMANCE) "
    "+ financial news (Pinecone) · LLM: Groq llama-3.3-70b-versatile"
)
st.write(" · ".join([f"`{t}`" for t in
    ["AAPL","MSFT","GOOGL","NVDA","TSLA","META","AMZN","JPM","INTC","V"]]))
st.divider()


# ── Session state ─────────────────────────────────────────────────────────────

if "messages" not in st.session_state:
    st.session_state["messages"] = []
if "quick_q" not in st.session_state:
    st.session_state["quick_q"] = None


# ── Chat history ──────────────────────────────────────────────────────────────

for msg in st.session_state["messages"]:
    with st.chat_message(msg["role"],
                         avatar="🧑" if msg["role"] == "user" else "🤖"):
        # Show pipeline metrics card if stored with message
        if msg["role"] == "assistant" and msg.get("metrics"):
            sym = msg.get("symbol", "")
            if sym and sym != "All Stocks":
                render_pipeline_metrics(msg["metrics"], sym)
        st.write(safe_str(msg.get("content"), ""))
        if msg["role"] == "assistant" and msg.get("sources"):
            render_sources(msg["sources"])


# ── Chat input ────────────────────────────────────────────────────────────────

user_input = st.chat_input("Ask anything about your tracked stocks...")

if st.session_state.get("quick_q") and not user_input:
    user_input = st.session_state["quick_q"]
    st.session_state["quick_q"] = None


# ── Process input ─────────────────────────────────────────────────────────────

if user_input and user_input.strip():
    question = user_input.strip()

    with st.chat_message("user", avatar="🧑"):
        st.write(question)

    st.session_state["messages"].append({
        "role":    "user",
        "content": question,
    })

    with st.chat_message("assistant", avatar="🤖"):
        with st.spinner("Fetching pipeline data · Searching Pinecone · Generating with Groq..."):
            try:
                sym = selected_stock if selected_stock != "All Stocks" else None

                # ── Step 1: Get dbt metrics from Snowflake ─────────────────
                metrics = {}
                if sym:
                    metrics = get_stock_metrics_from_snowflake(sym)

                # ── Step 2: Semantic search Pinecone ───────────────────────
                chunks = retrieve_from_pinecone(question, symbol=sym, top_k=5)

                # ── Step 3: Format metrics for prompt ──────────────────────
                metrics_str = format_metrics_for_prompt(metrics, sym) if sym else ""

                # ── Step 4: Generate answer with BOTH data sources ─────────
                answer = generate_with_groq(question, chunks, pipeline_metrics=metrics_str)

            except Exception as e:
                metrics = {}
                chunks  = []
                answer  = f"⚠️ Pipeline error: {str(e)}"

        # Show pipeline metrics card above the answer
        if metrics and sym and sym != "All Stocks":
            render_pipeline_metrics(metrics, sym)

        st.write(answer)
        render_sources(chunks)

    st.session_state["messages"].append({
        "role":    "assistant",
        "content": answer,
        "sources": chunks,
        "metrics": metrics,          # store so it rerenders in chat history
        "symbol":  selected_stock,
    })


# ── Empty state ───────────────────────────────────────────────────────────────

if not st.session_state["messages"]:
    st.info(
        "Ask anything about your 10 tracked stocks.\n\n"
        "Answers are grounded in **quantitative pipeline signals** "
        "(SMA crossovers, BUY/SELL signals from dbt) "
        "**and** real financial news (Pinecone semantic search)."
    )
    c1, c2, c3 = st.columns(3)
    if c1.button("AAPL outlook?", key="ex1"):
        st.session_state["quick_q"] = "What is the outlook for AAPL?"
        st.rerun()
    if c2.button("NVDA supply chain?", key="ex2"):
        st.session_state["quick_q"] = "Any supply chain risks for NVDA?"
        st.rerun()
    if c3.button("Market sentiment?", key="ex3"):
        st.session_state["quick_q"] = "What is the overall market sentiment?"
        st.rerun()