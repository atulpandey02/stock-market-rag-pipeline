<div align="center">

# Stock Market Intelligence Pipeline

**A production-grade data engineering system that ingests, transforms, and analyses stock market data — then answers natural language questions about it using AI grounded in your own pipeline's output.**

[![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=flat-square&logo=python&logoColor=white)](https://python.org)
[![Kafka](https://img.shields.io/badge/Apache_Kafka-231F20?style=flat-square&logo=apache-kafka&logoColor=white)](https://kafka.apache.org)
[![Spark](https://img.shields.io/badge/Apache_Spark-E25A1C?style=flat-square&logo=apache-spark&logoColor=white)](https://spark.apache.org)
[![Airflow](https://img.shields.io/badge/Apache_Airflow-017CEE?style=flat-square&logo=apache-airflow&logoColor=white)](https://airflow.apache.org)
[![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=flat-square&logo=snowflake&logoColor=white)](https://snowflake.com)
[![dbt](https://img.shields.io/badge/dbt-FF694B?style=flat-square&logo=dbt&logoColor=white)](https://getdbt.com)
[![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat-square&logo=docker&logoColor=white)](https://docker.com)

<br/>

> *Tracks 10 major equities — AAPL · MSFT · GOOGL · AMZN · META · TSLA · NVDA · INTC · JPM · V*

</div>

---

## Architecture

![Architecture Diagram](docs/images/architecture.png)

The system runs two parallel pipelines that converge at the intelligence layer:

- **Batch pipeline** — fetches a full year of OHLCV history daily, processes it through Spark, loads into Snowflake, and runs dbt transformations to produce BUY/SELL signals
- **Streaming pipeline** — generates real-time price ticks every 30 seconds, computes 3-minute and 5-minute windowed aggregations, loads into a separate Snowflake database
- **Intelligence layer** — the RAG engine synthesises **both** quantitative pipeline signals from dbt **and** financial news from Pinecone to answer natural language questions — the answer is only as good as your pipeline

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
| UI | Streamlit — RAG chat, pipeline dashboard, SQL explorer |
| Infrastructure | Docker Compose — 9 containerised services |
| CI/CD | GitHub Actions — syntax checks, unit tests, dbt validation |

---

## Pipeline in Action

### Airflow — All Tasks Green
![Airflow DAG](docs/images/airflow_dag.png)

### Streamlit — Market Intelligence Chat
![Streamlit Chat](docs/images/streamlit_chat.png)

### dbt — Models and Test Results
![dbt Results](docs/images/dbt_results.png)

### Snowflake — Historical Stock Data
![Snowflake](docs/images/snowflake_data.png)

### Pinecone — Vector Index
![Pinecone](docs/images/pinecone_index.png)

---

## Data Flow

### Batch (runs daily at 9AM)
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

### Streaming (continuous)
```
Price generator  →  Kafka  →  MinIO raw/realtime/
                 →  Spark windowed aggregations (3-min, 5-min MA)
                 →  Snowflake REALTIME_STOCK  (MERGE on symbol + window_start)
```

### Intelligence (on demand)
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
| sma_5 / sma_20 | FLOAT | Moving averages |

**STOCKMARKETSTREAM.PUBLIC.REALTIME_STOCK**

| Column | Type | Description |
|---|---|---|
| symbol | STRING | Stock ticker |
| window_start | TIMESTAMP | Window start |
| ma_3m / ma_5m | FLOAT | Windowed moving averages |
| volatility_3m / volatility_5m | FLOAT | Price std deviation |
| volume_sum_3m / volume_sum_5m | BIGINT | Volume per window |

---

## dbt Quality Gates

27 tests run after every pipeline execution. Any failure stops the pipeline before bad data reaches consumers.

```sql
not_null           → symbol, date, close_price, volume
accepted_values    → symbol in 10 tracked stocks only
assert_high_gte_low        → high_price >= low_price always
assert_price_not_negative  → close_price > 0 always
assert_no_future_dates     → date <= current_date always
assert_expected_symbols    → exactly 10 stocks, no drift
```

---

## Key Engineering Decisions

**Why MERGE instead of INSERT?**
Idempotency — if Airflow retries a failed task, MERGE updates existing rows instead of creating duplicates. Critical for production pipelines.

**Why custom MinIO sensor instead of time-based wait?**
A time-based sleep is fragile. The custom sensor polls every 30 seconds and only unblocks Spark when ≥10 files are confirmed present — preventing silent failures on empty partitions.

**Why `datetime.now(UTC)` in the sensor instead of `context['ds']`?**
Airflow's `ds` is the logical schedule date — when you manually trigger a DAG it lags behind the actual date. Files written by the consumer use real UTC time, so the sensor must match that.

**Why sentence-transformers instead of OpenAI embeddings?**
Free, runs locally, no API key, no rate limits. all-MiniLM-L6-v2 at 384 dimensions is fast on CPU and sufficient for financial news retrieval.

---

## Lessons Learned

| Problem | Root Cause | Fix |
|---|---|---|
| Silent data loss in Snowflake | UTC/EST timezone mismatch — consumer wrote `day=29`, Spark read `day=28` | Standardised everything to explicit `datetime.now(timezone.utc)` |
| MinIO sensor checking wrong date | `context['ds']` returns logical schedule date, not trigger date | Changed sensor to use `datetime.now(UTC)` directly |
| Kafka consumer crashes on startup | `group.id=None` — env var missing from docker-compose | Added `KAFKA_GROUP_BATCH_ID` to `x-airflow-common` |
| Spark `PATH_NOT_FOUND` | `recursiveFileLookup` treated single CSV as directory | Switched to glob pattern |
| Streamlit credentials not found | `find_dotenv(usecwd=True)` starts from CWD not file location | Walk up from `Path(__file__)` instead |

---

## Getting Started

```bash
# 1. Clone
git clone https://github.com/atulpandey02/stock-market-data-pipeline.git
cd stock-market-data-pipeline

# 2. Configure
cp .env.example .env
# Edit .env with your Finnhub, Pinecone, Groq, Snowflake credentials

# 3. Start infrastructure
docker-compose up -d

# 4. Trigger batch pipeline
# Airflow UI → http://localhost:8080
# DAG: stock_market_batch_pipeline → ▶ Trigger

# 5. Run dbt (on Mac terminal)
cd src/dbt
dbt run --profiles-dir . --project-dir .
dbt test --profiles-dir . --project-dir .

# 6. Launch intelligence layer
cd src/rag
python rag_pipeline.py     # ingest news into Pinecone
streamlit run app.py       # → http://localhost:8501
```

---

## Future Enhancements

- **Snowpipe auto-ingestion** — replace Airflow trigger with Snowpipe watching MinIO via webhook for true event-driven loading
- **Near-realtime dbt** — run dbt every 15 minutes on `REALTIME_STOCK` so RAG answers incorporate short-term momentum alongside daily trends
- **Unified RAG context** — inject both `STOCK_PERFORMANCE` (daily SMA) and `REALTIME_STOCK` (3-min MA) into the prompt with explicit timestamp labelling
- **Anomaly detection** — flag unusual price movements via Z-score alerts through Airflow

---

<div align="center">

**Atul Kumar Pandey**

[GitHub](https://github.com/atulpandey02) · [LinkedIn](https://linkedin.com/in/atulkumarpandey)

</div>