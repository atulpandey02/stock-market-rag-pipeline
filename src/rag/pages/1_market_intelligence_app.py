"""
Page 1 — Market Intelligence (RAG Chat)

Key decisions kept from working version:
- st.chat_input()         → no button state reset issues
- llama-3.1-8b-instant    → latest fast free Groq model
- find_dotenv()           → reliable .env discovery
- @st.cache_resource      → embedding model loads once only
- retrieve_from_pinecone  → direct Pinecone SDK (no LangChain)
- generate_with_groq      → direct requests.post (no LangChain)
"""

import os
import sys
import warnings
import logging
import requests
import streamlit as st
from pathlib import Path
from dotenv import load_dotenv, find_dotenv

# ── Suppress noisy logs ───────────────────────────────────────────────────────
logging.getLogger("sentence_transformers").setLevel(logging.ERROR)
warnings.filterwarnings("ignore")

# ── Path setup (works inside pages/ subfolder) ────────────────────────────────
_this_dir = os.path.dirname(os.path.abspath(__file__))
_root_dir  = os.path.dirname(_this_dir)
sys.path.insert(0, _root_dir)

# ── Load .env (override=True beats stale shell exports) ──────────────────────
_dotenv_path = find_dotenv(usecwd=True, raise_error_if_not_found=False)
if _dotenv_path:
    load_dotenv(_dotenv_path, override=True)
else:
    for _p in [
        Path(_root_dir) / ".env",
        Path(_root_dir).parent / ".env",
        Path(_root_dir).parent.parent / ".env",
        Path(_root_dir).parent.parent.parent / ".env",
    ]:
        if _p.exists():
            load_dotenv(_p, override=True)
            break


# ── Backend — Embedding ───────────────────────────────────────────────────────

@st.cache_resource(show_spinner="Loading embedding model...")
def load_embedding_model():
    """
    Loads all-MiniLM-L6-v2 once and caches it.
    This is the EMBEDDING model (not LLM) — converts text to 384-dim vectors.
    Runs locally on CPU — completely free, no API key needed.
    """
    from sentence_transformers import SentenceTransformer
    return SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")


# ── Backend — Pinecone Retrieval ──────────────────────────────────────────────

def retrieve_from_pinecone(question: str, symbol: str = None, top_k: int = 5):
    """
    Semantic search over Pinecone vector index.
    1. Embeds the question locally
    2. Queries Pinecone for most similar chunks
    3. Returns deduplicated list of article metadata
    """
    from pinecone import Pinecone

    model     = load_embedding_model()
    query_vec = model.encode([question])[0].tolist()

    pc    = Pinecone(api_key=os.getenv("PINECONE_API_KEY"))
    index = pc.Index(os.getenv("PINECONE_INDEX_NAME", "stock-market-rag"))

    params = {"vector": query_vec, "top_k": top_k, "include_metadata": True}
    if symbol and symbol != "All Stocks":
        params["filter"] = {"symbol": symbol.upper()}

    results = index.query(**params)

    # Deduplicate by URL
    seen, unique = set(), []
    for m in results.get("matches", []):
        url = m["metadata"].get("url", "")
        if url not in seen:
            seen.add(url)
            unique.append({
                "title":        m["metadata"].get("title")        or "N/A",
                "summary":      m["metadata"].get("summary")       or "",
                "symbol":       m["metadata"].get("symbol")        or "",
                "source":       m["metadata"].get("source")        or "",
                "url":          url,
                "published_at": m["metadata"].get("published_at") or "",
                "sentiment":    m["metadata"].get("sentiment")     or "Neutral",
                "score":        round(m.get("score") or 0, 3),
            })
    return unique


# ── Backend — Groq LLM ────────────────────────────────────────────────────────

def generate_with_groq(question: str, chunks: list) -> str:
    """
    Calls Groq API directly with retrieved chunks as context.
    Model: llama-3.1-8b-instant (free, fast, reliable)
    Grounded: uses ONLY the provided news articles
    """
    groq_key = os.getenv("GROQ_API_KEY")
    if not groq_key:
        return "⚠️ GROQ_API_KEY is missing — check your .env file."
    if not chunks:
        return "⚠️ No relevant articles found in Pinecone for this query."

    context = "\n\n".join([
        f"[{c['symbol']}] {c['title']}\n"
        f"Published: {c['published_at'][:10]} | Source: {c['source']}\n"
        f"Sentiment: {c['sentiment']}\n"
        f"Summary: {c['summary']}"
        for c in chunks
    ])

    try:
        response = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {groq_key}",
                "Content-Type":  "application/json",
            },
            json={
                "model": "llama-3.1-8b-instant",
                "messages": [
                    {
                        "role": "system",
                        "content": (
                            "You are a professional financial market analyst. "
                            "Answer questions using ONLY the provided news articles. "
                            "Be concise (3-5 sentences). Cite sources where relevant. "
                            "Do not use external knowledge."
                        ),
                    },
                    {
                        "role": "user",
                        "content": (
                            f"News Articles:\n{context}\n\n"
                            f"Question: {question}\n\nAnalysis:"
                        ),
                    },
                ],
                "temperature": 0.1,
                "max_tokens":  500,
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
            "total_vectors": stats.total_vector_count,
            "dimension":     stats.dimension,
            "status":        "Connected ✓"
        }
    except Exception as e:
        return {"total_vectors": 0, "dimension": 384, "status": f"Error: {e}"}


# ── UI — Sidebar ──────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("### Filter")
    stocks = ["All Stocks","AAPL","MSFT","GOOGL","AMZN",
              "META","TSLA","NVDA","INTC","JPM","V"]
    selected_stock = st.selectbox("Stock", stocks, label_visibility="collapsed")

    st.divider()

    st.markdown("### Quick questions")
    ticker = selected_stock if selected_stock != "All Stocks" else "AAPL"
    for q in [
        "What is the current market sentiment?",
        f"What is the outlook for {ticker}?",
        "Any supply chain concerns for tech stocks?",
        "What are analysts saying about earnings?",
        "Any regulatory risks to watch?",
    ]:
        if st.button(q, key=f"qq_{q}", use_container_width=True):
            st.session_state["quick_q"] = q
            st.rerun()

    st.divider()

    st.markdown("### Pipeline info")
    st.caption("📰 News: Finnhub API")
    st.caption("🔢 Embeddings: all-MiniLM-L6-v2")
    st.caption("🗄️ Vector DB: Pinecone")
    st.caption("🧠 LLM: Groq Llama3.1-8b")

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


# ── UI — Header ───────────────────────────────────────────────────────────────

st.title("📈 Market Intelligence")
st.caption("RAG-powered financial analysis · Finnhub News · Pinecone · Groq Llama3.1")

# Ticker strip
tickers = ["AAPL","MSFT","GOOGL","NVDA","TSLA","META","AMZN","JPM"]
st.write(" · ".join([f"`{t}`" for t in tickers]))
st.divider()


# ── UI — Session state ────────────────────────────────────────────────────────

if "messages" not in st.session_state:
    st.session_state["messages"] = []
if "quick_q" not in st.session_state:
    st.session_state["quick_q"] = None


# ── UI — Chat history ─────────────────────────────────────────────────────────

for msg in st.session_state["messages"]:
    with st.chat_message(msg["role"], avatar="🧑" if msg["role"] == "user" else "🤖"):
        st.write(msg["content"])

        # Show sources for assistant messages
        if msg["role"] == "assistant" and msg.get("sources"):
            with st.expander(f"📰 Sources ({len(msg['sources'])})"):
                for i, src in enumerate(msg["sources"], 1):
                    sent  = src.get("sentiment", "Neutral")
                    score = src.get("score", 0)
                    icon  = "📈" if sent == "Positive" else "📉" if sent == "Negative" else "➖"

                    col_a, col_b = st.columns([3, 1])
                    with col_a:
                        st.markdown(f"**[{i}] {src.get('title','N/A')}**")
                        st.caption(
                            f"{src.get('symbol','')} · "
                            f"{src.get('source','')} · "
                            f"{src.get('published_at','')[:10]}"
                        )
                        st.markdown(f"[View article →]({src.get('url','#')})")
                    with col_b:
                        st.metric("Score", f"{score:.3f}")
                        st.caption(f"{icon} {sent}")

                    if i < len(msg["sources"]):
                        st.divider()


# ── UI — Chat input ───────────────────────────────────────────────────────────
# st.chat_input is the correct widget — no button state reset issues
# Returns the typed string directly without any form gymnastics

user_input = st.chat_input("Ask anything about your tracked stocks...")

# Handle quick question from sidebar
if st.session_state["quick_q"] and not user_input:
    user_input = st.session_state["quick_q"]
    st.session_state["quick_q"] = None


# ── UI — Process input ────────────────────────────────────────────────────────

if user_input and user_input.strip():
    question = user_input.strip()

    # Show user message immediately
    with st.chat_message("user", avatar="🧑"):
        st.write(question)

    # Save user message
    st.session_state["messages"].append({
        "role":    "user",
        "content": question,
    })

    # Generate response
    with st.chat_message("assistant", avatar="🤖"):
        with st.spinner("Searching Pinecone · Generating with Groq Llama3.1..."):
            try:
                sym    = selected_stock if selected_stock != "All Stocks" else None
                chunks = retrieve_from_pinecone(question, symbol=sym, top_k=5)
                answer = generate_with_groq(question, chunks)
            except Exception as e:
                chunks = []
                answer = f"⚠️ Pipeline error: {str(e)}"

        st.write(answer)

        if chunks:
            with st.expander(f"📰 Sources ({len(chunks)})"):
                for i, src in enumerate(chunks, 1):
                    sent  = src.get("sentiment", "Neutral")
                    score = src.get("score", 0)
                    icon  = "📈" if sent == "Positive" else "📉" if sent == "Negative" else "➖"

                    col_a, col_b = st.columns([3, 1])
                    with col_a:
                        st.markdown(f"**[{i}] {src.get('title','N/A')}**")
                        st.caption(
                            f"{src.get('symbol','')} · "
                            f"{src.get('source','')} · "
                            f"{src.get('published_at','')[:10]}"
                        )
                        st.markdown(f"[View article →]({src.get('url','#')})")
                    with col_b:
                        st.metric("Score", f"{score:.3f}")
                        st.caption(f"{icon} {sent}")

                    if i < len(chunks):
                        st.divider()

    # Save assistant message
    st.session_state["messages"].append({
        "role":    "assistant",
        "content": answer,
        "sources": chunks,
    })


# ── UI — Empty state ──────────────────────────────────────────────────────────

if not st.session_state["messages"]:
    st.info(
        "Ask anything about your 10 tracked stocks — "
        "AAPL, MSFT, GOOGL, AMZN, META, TSLA, NVDA, INTC, JPM, V\n\n"
        "AI-powered analysis grounded in real financial news from Finnhub."
    )
    col1, col2, col3 = st.columns(3)
    col1.button("What's the AAPL outlook?",       key="ex1",
                on_click=lambda: st.session_state.update({"quick_q": "What is the outlook for AAPL?"}))
    col2.button("Any NVDA supply chain risks?",   key="ex2",
                on_click=lambda: st.session_state.update({"quick_q": "Are there any supply chain risks for NVDA?"}))
    col3.button("Overall market sentiment?",      key="ex3",
                on_click=lambda: st.session_state.update({"quick_q": "What is the overall market sentiment?"}))