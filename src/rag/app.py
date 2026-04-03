"""
Stock Market Intelligence Platform
Multi-page Streamlit application

Run: streamlit run app.py
Pages:
  1. Market Intelligence  — RAG chat (Finnhub + Pinecone + Groq)
  2. Pipeline Dashboard   — Real-time pipeline monitoring
  3. SQL Explorer         — Query Snowflake directly
"""

import streamlit as st

st.set_page_config(
    page_title = "Stock Market Intelligence",
    page_icon  = "📈",
    layout     = "wide",
    initial_sidebar_state = "expanded"
)

# ── Global dark theme ─────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=DM+Sans:wght@300;400;500;600&display=swap');
:root {
    --bg:      #0a0e1a;
    --surface: #111827;
    --border:  #1e293b;
    --accent:  #00d4ff;
    --green:   #10b981;
    --red:     #ef4444;
    --yellow:  #f59e0b;
    --text:    #e2e8f0;
    --muted:   #64748b;
}
html, body, [class*="css"] {
    background-color: var(--bg) !important;
    color: var(--text) !important;
    font-family: 'DM Sans', sans-serif !important;
}
#MainMenu, footer { visibility: hidden; }
[data-testid="stSidebar"] {
    background: var(--surface) !important;
    border-right: 1px solid var(--border) !important;
}
[data-testid="stSidebarNav"] a {
    color: var(--muted) !important;
    font-size: 13px !important;
}
[data-testid="stSidebarNav"] a:hover,
[data-testid="stSidebarNav"] a[aria-selected="true"] {
    color: var(--accent) !important;
    background: var(--border) !important;
    border-radius: 8px !important;
}
</style>
""", unsafe_allow_html=True)

# ── Landing page ──────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
        <div style="font-family:'Space Mono',monospace;font-size:13px;
                    font-weight:700;color:#00d4ff;letter-spacing:2px;
                    padding:16px 0 8px 0;">
            ◈ STOCK MARKET<br>INTELLIGENCE
        </div>
        <div style="font-size:11px;color:#64748b;
                    padding-bottom:16px;border-bottom:1px solid #1e293b;">
            Kafka · Spark · Snowflake · dbt · RAG
        </div>
    """, unsafe_allow_html=True)

st.markdown("""
    <div style="text-align:center;padding:80px 20px 40px 20px;">
        <div style="font-family:'Space Mono',monospace;font-size:42px;
                    font-weight:700;color:#00d4ff;margin-bottom:16px;">
            ◈
        </div>
        <h1 style="font-family:'Space Mono',monospace;font-size:28px;
                   font-weight:700;color:#e2e8f0;margin-bottom:12px;">
            Stock Market Intelligence
        </h1>
        <p style="color:#64748b;font-size:16px;max-width:500px;
                  margin:0 auto 40px auto;line-height:1.7;">
            End-to-end data engineering pipeline with real-time streaming,
            batch processing, and AI-powered market intelligence.
        </p>
    </div>
""", unsafe_allow_html=True)

col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("""
        <div style="background:#111827;border:1px solid #1e293b;
                    border-top:3px solid #00d4ff;border-radius:12px;
                    padding:24px;text-align:center;">
            <div style="font-size:32px;margin-bottom:12px;">🤖</div>
            <div style="font-family:'Space Mono',monospace;font-size:14px;
                        font-weight:700;color:#00d4ff;margin-bottom:8px;">
                Market Intelligence
            </div>
            <div style="font-size:13px;color:#64748b;line-height:1.6;">
                Ask questions about stocks using
                RAG-powered analysis from real
                financial news via Groq Llama3
            </div>
        </div>
    """, unsafe_allow_html=True)

with col2:
    st.markdown("""
        <div style="background:#111827;border:1px solid #1e293b;
                    border-top:3px solid #10b981;border-radius:12px;
                    padding:24px;text-align:center;">
            <div style="font-size:32px;margin-bottom:12px;">📊</div>
            <div style="font-family:'Space Mono',monospace;font-size:14px;
                        font-weight:700;color:#10b981;margin-bottom:8px;">
                Pipeline Dashboard
            </div>
            <div style="font-size:13px;color:#64748b;line-height:1.6;">
                Real-time pipeline monitoring,
                buy/sell signals from dbt models,
                and live data quality checks
            </div>
        </div>
    """, unsafe_allow_html=True)

with col3:
    st.markdown("""
        <div style="background:#111827;border:1px solid #1e293b;
                    border-top:3px solid #7c3aed;border-radius:12px;
                    padding:24px;text-align:center;">
            <div style="font-size:32px;margin-bottom:12px;">🔍</div>
            <div style="font-family:'Space Mono',monospace;font-size:14px;
                        font-weight:700;color:#7c3aed;margin-bottom:8px;">
                SQL Explorer
            </div>
            <div style="font-size:13px;color:#64748b;line-height:1.6;">
                Run SQL queries directly against
                Snowflake and explore your
                stock market data interactively
            </div>
        </div>
    """, unsafe_allow_html=True)

st.markdown("""
    <div style="text-align:center;padding:32px 0 0 0;">
        <p style="color:#64748b;font-size:13px;">
            Use the sidebar to navigate between pages
        </p>
    </div>
""", unsafe_allow_html=True)

# ── Tech stack footer ─────────────────────────────────────────────────────────
st.markdown("<div style='height:40px'></div>", unsafe_allow_html=True)
st.markdown("""
    <div style="border-top:1px solid #1e293b;padding:20px 0;
                display:flex;justify-content:center;gap:24px;flex-wrap:wrap;">
""", unsafe_allow_html=True)

techs = ["Kafka","Spark 3.5","Airflow","MinIO","Snowflake","dbt","Pinecone","Groq Llama3","Finnhub"]
badges = "".join([
    f'<span style="background:#1e293b;color:#64748b;font-size:11px;'
    f'font-family:Space Mono,monospace;padding:4px 12px;border-radius:20px;">{t}</span>'
    for t in techs
])
st.markdown(f"""
    <div style="display:flex;justify-content:center;gap:8px;flex-wrap:wrap;padding:8px 0;">
        {badges}
    </div>
""", unsafe_allow_html=True)