<div align="center">

# Stock Market Intelligence Pipeline

**A production-grade data engineering system that ingests, transforms, and analyses stock market data — then answers natural language questions about it using AI grounded in your own pipeline's output.**

[![CI](https://github.com/atulpandey02/stock-market-data-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/atulpandey02/stock-market-data-pipeline/actions)
[![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=flat-square&logo=python&logoColor=white)](https://python.org)
[![Kafka](https://img.shields.io/badge/Apache_Kafka-231F20?style=flat-square&logo=apache-kafka&logoColor=white)](https://kafka.apache.org)
[![Spark](https://img.shields.io/badge/Apache_Spark-E25A1C?style=flat-square&logo=apache-spark&logoColor=white)](https://spark.apache.org)
[![Airflow](https://img.shields.io/badge/Apache_Airflow-017CEE?style=flat-square&logo=apache-airflow&logoColor=white)](https://airflow.apache.org)
[![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=flat-square&logo=snowflake&logoColor=white)](https://snowflake.com)
[![dbt](https://img.shields.io/badge/dbt-FF694B?style=flat-square&logo=dbt&logoColor=white)](https://getdbt.com)
[![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat-square&logo=docker&logoColor=white)](https://docker.com)

<br/>

**9 Docker containers · 2 Airflow DAGs · 5 dbt models · 27 data quality tests · 300+ Pinecone chunks · 10 stocks tracked**

<br/>

> *Tracks 10 major equities — AAPL · MSFT · GOOGL · AMZN · META · TSLA · NVDA · INTC · JPM · V*

</div>

---

## What Makes This Different

Most RAG projects pull from a static document store. This one is different — **the AI answers are grounded in data your pipeline computed**.

When you ask *"Should I buy AAPL?"* the system:

1. Queries the `STOCK_PERFORMANCE` dbt mart for the latest SMA crossover signal
2. Retrieves the 5 most semantically relevant news chunks from Pinecone
3. Feeds both into Groq Llama3 — if the quantitative signal conflicts with news sentiment, the model flags it explicitly

```
📊 Pipeline Data — AAPL · 🟢 BULLISH · from dbt STOCK_PERFORMANCE
   Close: $255.92  |  SMA-5: $255.38  |  SMA-20: $251.74  |  Return: +0.68%

"The pipeline signal is BULLISH with SMA-5 above SMA-20 — a classic
bullish crossover. Recent news has mixed sentiment however, with analysts
at Evercore maintaining a $330 price target but broader market concerns
flagged by multiple sources. The conflicting signals warrant caution..."
```

---

## Architecture

<div align="center">
  <img src="docs/image/Architecture_diagram.png" width="85%" alt="Architecture Diagram"/>
</div>

<br/>

The system runs two parallel pipelines that converge at the intelligence layer:

- **Batch pipeline** — fetches a full year of OHLCV history daily via Finnhub, processes it through Spark, loads into Snowflake via MERGE, and runs dbt transformations to produce BUY/SELL signals
- **Streaming pipeline** — generates real-time price ticks every 30 seconds, computes 3-minute and 5-minute windowed aggregations via Spark Streaming, loads into a separate Snowflake database
- **Intelligence layer** — the RAG engine synthesises **both** quantitative pipeline signals from dbt **and** financial news from Pinecone to answer natural language questions — the answer is only as good as your pipeline

---

## Pipeline in Action

### Orchestration — Airflow DAGs

<div align="center">
  <table>
    <tr>
      <td align="center" width="50%">
        <img src="docs/image/Batch_aiflow_dag.png" width="100%" alt="Batch DAG"/>
        <br/><sub><b>Batch pipeline — all 6 tasks green</b></sub>
      </td>
      <td align="center" width="50%">
        <img src="docs/image/stream_airflow_dag.png" width="100%" alt="Stream DAG"/>
        <br/><sub><b>Streaming pipeline — all 8 tasks green</b></sub>
      </td>
    </tr>
  </table>
</div>

### Intelligence Layer — Streamlit Dashboard

<div align="center">
  <img src="docs/image/streamlit_chat.png" width="85%" alt="Streamlit Market Intelligence Chat"/>
  <br/><sub><b>Page 1 — RAG chat grounded in live dbt pipeline signals</b></sub>
</div>

<br/>

> The dashboard has three pages. **Page 1** is the RAG chat — it shows the quantitative BUY/SELL signal from the `STOCK_PERFORMANCE` dbt mart alongside retrieved Pinecone news chunks before generating a Groq Llama3 response, and explicitly flags when quantitative and sentiment signals conflict. **Page 2** queries Snowflake directly for live ingestion stats and pipeline health. **Page 3** is a SQL explorer for ad-hoc queries against both `STOCKMARKETBATCH` and `STOCKMARKETSTREAM` databases.

<div align="center">
  <img src="docs/image/streamlit_dashboard.png" width="85%" alt="Streamlit Pipeline Dashboard"/>
  <br/><sub><b>Page 2 — live pipeline monitor querying Snowflake directly</b></sub>
</div>

### Data Quality — dbt Lineage + Pinecone Index

<div align="center">
  <table>
    <tr>
      <td align="center" width="50%">
        <img src="docs/image/Lineage_Graph.png" width="100%" alt="dbt Lineage Graph"/>
        <br/><sub><b>dbt — model lineage and 27 passing tests</b></sub>
      </td>
      <td align="center" width="50%">
        <img src="docs/image/pinecone_index.png" width="100%" alt="Pinecone Vector Index"/>
        <br/><sub><b>Pinecone — 300+ embedded financial news chunks</b></sub>
      </td>
    </tr>
  </table>
</div>

---

## Tech Stack

| Layer | Technology |
|---|---|
| Message broker | Apache Kafka — dual topics (batch + realtime) |
| Data lake | MinIO — Hive-partitioned `year=/month=/day=/symbol=` |
| Processing | Apache Spark 3.5.1 — batch transforms + windowed aggregations |
| Warehouse | Snowflake — two databases, incremental MERGE loading |
| Transformation | dbt — 5 models, 27 automated quality tests |
| Orchestration | Apache Airflow 2.9.3 — custom MinIO sensors, retry logic |
| Vector DB | Pinecone — 300+ embedded financial news chunks |
| Embeddings | sentence-transformers/all-MiniLM-L6-v2 — 384-dim, free, local |
| LLM | Groq Llama3-8b — grounded financial Q&A |
| UI | Streamlit — 3-page dashboard (RAG chat, pipeline monitor, SQL explorer) |
| Infrastructure | Docker Compose — 9 containerised services |
| CI/CD | GitHub Actions — syntax checks, unit tests, dbt validation |

---

## Project Structure

```
stockmarketdatapipeline/
│
├── docker-compose.yaml
├── requirements.txt
├── .env.example                              ← copy to .env and fill credentials
├── .gitignore
│
├── docs/images/                              ← all screenshots for this README
│
├── src/
│   │
│   ├── kafka/
│   │   ├── producer/
│   │   │   ├── batch_data_producer.py        ← Finnhub OHLCV → Kafka
│   │   │   └── stream_data_producer.py       ← price ticks → Kafka
│   │   └── consumer/
│   │       ├── batch_data_consumer.py        ← Kafka → MinIO raw/historical/
│   │       └── realtime_data_consumer.py     ← Kafka → MinIO raw/realtime/
│   │
│   ├── spark/jobs/
│   │   ├── spark_batch_processor.py          ← SMA-5/20, returns, daily range
│   │   ├── spark_stream_processor.py         ← always-on structured streaming
│   │   └── spark_stream_batch_processor.py   ← 3-min/5-min windowed aggregations
│   │
│   ├── snowflake/
│   │   ├── load_to_snowflake.py              ← historical → Snowflake MERGE
│   │   └── load_stream_to_snowflake.py       ← realtime → Snowflake MERGE
│   │
│   ├── dbt/
│   │   ├── models/
│   │   │   ├── staging/                      ← stg_historical_stock, stg_realtime_stock
│   │   │   └── marts/                        ← stock_daily_metrics, stock_performance
│   │   └── tests/                            ← 4 custom SQL data quality tests
│   │
│   ├── rag/
│   │   ├── rag_pipeline.py                   ← Finnhub news → Pinecone ingestion
│   │   ├── app.py                            ← Streamlit entry point
│   │   └── pages/
│   │       ├── 1_Market_Intelligence.py      ← RAG chat + dbt metrics card
│   │       ├── 2_Pipeline_Dashboard.py       ← live Snowflake monitoring
│   │       └── 3_SQL_Explorer.py             ← ad-hoc SQL queries
│   │
│   └── airflow/dags/
│       ├── stock_market_batch_dag.py         ← 6-task batch orchestration
│       ├── stock_market_stream_dag.py        ← 8-task stream orchestration
│       └── scripts/                          ← script copies for Airflow workers
│
├── tests/
│   └── unit_tests.py
│
└── .github/workflows/
    └── ci.yml                                ← syntax checks, unit tests, dbt parse
```

---

## Data Flow

### Batch (runs daily via Airflow)
```
Finnhub API  →  Kafka  →  MinIO raw/historical/
             →  Spark (SMA-5, SMA-20, daily_return_pct, is_positive_day)
             →  MinIO processed/historical/
             →  Snowflake HISTORICAL_STOCK  (MERGE on symbol + date)
             →  dbt run  → stg_historical_stock
                         → stock_daily_metrics
                         → stock_performance  ← BUY/SELL signals
             →  dbt test → 27 checks pass or pipeline fails
```

### Streaming (continuous via Airflow)
```
Price generator  →  Kafka  →  MinIO raw/realtime/
                 →  Spark windowed aggregations (3-min, 5-min MA)
                 →  Snowflake REALTIME_STOCK  (MERGE on symbol + window_start)
```

### Intelligence (on demand via Streamlit)
```
User question
  →  Snowflake STOCK_PERFORMANCE  (dbt BUY/SELL signal, SMA crossover)
  →  Pinecone semantic search      (top 5 relevant news chunks)
  →  Groq Llama3                   (synthesises both, flags conflicts)
  →  Streamlit                     (answer + metrics card + sources)
```

---

## Snowflake Schema

**STOCKMARKETBATCH.PUBLIC.HISTORICAL_STOCK**

| Column | Type | Description |
|---|---|---|
| symbol | STRING | Stock ticker |
| date | DATE | Trading date |
| open_price / close_price | FLOAT | OHLC prices |
| daily_return_pct | FLOAT | `(close - open) / open × 100` |
| daily_range | FLOAT | `high - low` |
| is_positive_day | BOOLEAN | `close > open` |
| sma_5 / sma_20 | FLOAT | Moving averages computed by Spark |

**STOCKMARKETSTREAM.PUBLIC.REALTIME_STOCK**

| Column | Type | Description |
|---|---|---|
| symbol | STRING | Stock ticker |
| window_start | TIMESTAMP | Aggregation window start |
| ma_3m / ma_5m | FLOAT | Windowed moving averages |
| volatility_3m / volatility_5m | FLOAT | Price std deviation per window |
| volume_sum_3m / volume_sum_5m | BIGINT | Volume per window |

---

## dbt Quality Gates

27 tests run after every pipeline execution. Any failure stops the pipeline before bad data reaches consumers.

```sql
-- Built-in dbt tests
not_null           → symbol, date, close_price, volume
accepted_values    → symbol in 10 tracked stocks only

-- Custom SQL tests
assert_high_gte_low        → high_price >= low_price always
assert_price_not_negative  → close_price > 0 always
assert_no_future_dates     → date <= current_date always
assert_expected_symbols    → exactly 10 stocks, no drift
```

---

## Key Engineering Decisions

**Why MERGE instead of INSERT?**
Idempotency — if Airflow retries a failed task, MERGE updates existing rows instead of creating duplicates. Critical for production pipelines where task reruns are expected.

**Why custom MinIO sensor instead of time-based wait?**
A time-based sleep is fragile. The custom `MinIODataSensor` polls every 30 seconds and only unblocks Spark when files are confirmed present — preventing silent failures on empty partitions.

**Why `datetime.now(UTC)` in the sensor instead of `context['ds']`?**
Airflow's `ds` is the logical schedule date — when you manually trigger a DAG it lags behind the actual date. Files written by the consumer use real UTC time, so the sensor must match that.

**Why sentence-transformers instead of OpenAI embeddings?**
Free, runs locally, no API key, no rate limits. `all-MiniLM-L6-v2` at 384 dimensions is fast on CPU and sufficient for financial news retrieval.

**Why separate Snowflake databases for batch and stream?**
Different update patterns and SLAs. Batch loads once daily with full MERGE semantics. Stream loads every few minutes with window-based keys. Separating them prevents schema conflicts and makes access control simpler.

---

## Lessons Learned

> These are real bugs hit during development — each one taught something about production data engineering that no tutorial covers.

| Problem | Root Cause | Fix |
|---|---|---|
| Silent data loss in Snowflake | UTC/EST timezone mismatch — consumer wrote `day=29`, Spark read `day=28` | Standardised everything to explicit `datetime.now(timezone.utc)` |
| MinIO sensor checking wrong date | `context['ds']` returns logical schedule date, not trigger date | Changed sensor to use `datetime.now(UTC)` directly |
| Kafka consumer crashes on startup | `group.id=None` — env var missing from docker-compose | Added `KAFKA_GROUP_BATCH_ID` to `x-airflow-common` |
| Spark `PATH_NOT_FOUND` | `recursiveFileLookup` treated single CSV as directory | Switched to glob pattern |
| Streamlit credentials not found | `find_dotenv(usecwd=True)` starts from CWD not file location | Walk up from `Path(__file__)` instead |
| dbt alias conflict | `context['ds']` undefined in `run_spark_processing` | Restored the assignment and replaced `now` references with `ds` |

---

## Getting Started

### Prerequisites

All external services below have free tiers — no credit card required for Finnhub, Pinecone, or Groq.

| Requirement | Where to get it |
|---|---|
| Docker Desktop | [docker.com/products/docker-desktop](https://docker.com/products/docker-desktop) |
| Snowflake account | [snowflake.com](https://snowflake.com) — free 30-day trial |
| `FINNHUB_API_KEY` | [finnhub.io](https://finnhub.io) — free tier |
| `PINECONE_API_KEY` | [app.pinecone.io](https://app.pinecone.io) — free starter plan |
| `GROQ_API_KEY` | [console.groq.com](https://console.groq.com) — free tier |

### Environment Variables

Copy `.env.example` to `.env` and fill in the following:

| Variable | Description |
|---|---|
| `SNOWFLAKE_ACCOUNT` | Account identifier from Snowflake console — e.g. `abc12345.us-east-1` |
| `SNOWFLAKE_USER` | Your Snowflake username |
| `SNOWFLAKE_PASSWORD` | Your Snowflake password |
| `SNOWFLAKE_WAREHOUSE` | Warehouse name — default `COMPUTE_WH` |
| `FINNHUB_API_KEY` | Finnhub API key for OHLCV history and news |
| `PINECONE_API_KEY` | Pinecone API key for vector storage and search |
| `GROQ_API_KEY` | Groq API key for Llama3 inference |

### Run It

```bash
# 1. Clone
git clone https://github.com/atulpandey02/stock-market-data-pipeline.git
cd stock-market-data-pipeline

# 2. Configure
cp .env.example .env
# Fill in your credentials — see the table above

# 3. Start all 9 Docker services
docker-compose up -d

# 4. Trigger batch pipeline
# Open Airflow UI → http://localhost:8080  (admin / admin)
# Find stock_market_batch_pipeline → click ▶ Trigger

# 5. Run dbt transformations (run on your local terminal, not inside Docker)
cd src/dbt
source ../../dbt_venv/bin/activate
dbt run --profiles-dir . --project-dir .
dbt test --profiles-dir . --project-dir .

# 6. Ingest news and launch the dashboard
cd src/rag
python rag_pipeline.py     # fetches Finnhub news and loads into Pinecone (~2 min)
streamlit run app.py       # opens dashboard at http://localhost:8501
```

---

## Future Enhancements

- **Snowpipe auto-ingestion** — replace Airflow trigger with Snowpipe watching MinIO via webhook for true event-driven loading
- **Near-realtime dbt** — run dbt every 15 minutes on `REALTIME_STOCK` so RAG answers incorporate short-term momentum alongside daily trends
- **Unified RAG context** — inject both `STOCK_PERFORMANCE` (daily SMA) and `REALTIME_STOCK` (3-min MA) into the prompt with explicit timestamp labelling so the model can reason about momentum across timeframes
- **Anomaly detection** — flag unusual price movements via Z-score alerts through Airflow email notifications

---

<div align="center">

**Atul Kumar Pandey**

[GitHub](https://github.com/atulpandey02) · [LinkedIn](https://linkedin.com/in/atulkumarpandey)

</div>