"""
Stock Market Intelligence Platform
Multi-page Streamlit app — entry point

Run: streamlit run app.py
"""
import streamlit as st

st.set_page_config(
    page_title = "Stock Market Intelligence",
    page_icon  = "📈",
    layout     = "wide",
    initial_sidebar_state = "expanded"
)

st.title("📈 Stock Market Intelligence Platform")
st.caption("Kafka · Spark · Airflow · Snowflake · dbt · Pinecone · Groq Llama3.1")

st.divider()

col1, col2, col3 = st.columns(3)

with col1:
    st.info("**🤖 Market Intelligence**\n\nAsk questions about stocks using RAG-powered analysis from real financial news via Groq Llama3.1")

with col2:
    st.success("**📊 Pipeline Dashboard**\n\nReal-time pipeline monitoring, buy/sell signals from dbt models, and live data quality checks")

with col3:
    st.warning("**🔍 SQL Explorer**\n\nRun SQL queries directly against Snowflake and explore your stock market data interactively")

st.divider()
st.caption("Use the sidebar to navigate between pages")