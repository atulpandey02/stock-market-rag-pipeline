# Real-Time Stock Market Intelligence Platform

An end-to-end data engineering project combining real-time streaming, batch processing, AI-powered market intelligence, and an interactive multi-page dashboard.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                     BATCH PIPELINE  (Daily 9AM)                     │
│   Alpha Vantage → Kafka → MinIO → Spark → Snowflake → dbt → RAG    │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│                   STREAMING PIPELINE  (Continuous)                  │
│   Price Generator → Kafka → MinIO → Spark → Snowflake              │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│                       INTELLIGENCE LAYER                            │
│   Finnhub News → Pinecone → Groq Llama3.1 → Streamlit UI           │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Ingestion | Apache Kafka (Confluent), Alpha Vantage API, Finnhub API |
| Storage | MinIO (S3-compatible data lake) |
| Processing | Apache Spark 3.5.1 |
| Orchestration | Apache Airflow 2.9.3 |
| Warehouse | Snowflake (2 databases) |
| Transformation | dbt-snowflake |
| Vector DB | Pinecone |
| Embeddings | sentence-transformers/all-MiniLM-L6-v2 (local, free) |
| LLM | Groq Llama3.1-8b-instant (free tier) |
| UI | Streamlit (3-page app) |
| Infrastructure | Docker Compose |
| CI/CD | GitHub Actions |

---

## Project Structure

```
stockmarketdatapipeline/
│
├── docker-compose.yaml
├── requirements.txt
├── .env.example
│
├── src/
│   ├── kafka/
│   │   ├── producer/
│   │   │   ├── batch_data_producer.py      # Alpha Vantage → Kafka
│   │   │   └── stream_data_producer.py     # Fake prices → Kafka
│   │   └── consumer/
│   │       ├── batch_data_consumer.py      # Kafka → MinIO historical
│   │       └── realtime_data_consumer.py   # Kafka → MinIO realtime
│   │
│   ├── spark/
│   │   └── jobs/
│   │       ├── spark_batch_processor.py         # SMA, returns, volatility
│   │       ├── spark_stream_processor.py        # Always-on streaming
│   │       └── spark_stream_batch_processor.py  # Windowed analytics
│   │
│   ├── snowflake/
│   │   ├── load_to_snowflake.py            # Historical → Snowflake MERGE
│   │   └── load_stream_to_snowflake.py     # Realtime → Snowflake MERGE
│   │
│   ├── dbt/
│   │   ├── dbt_project.yml
│   │   ├── profiles.yml
│   │   ├── models/
│   │   │   ├── staging/
│   │   │   │   ├── sources.yml
│   │   │   │   ├── stg_historical_stock.sql
│   │   │   │   └── stg_realtime_stock.sql
│   │   │   └── marts/
│   │   │       ├── schema.yml
│   │   │       ├── stock_daily_metrics.sql
│   │   │       ├── stock_realtime_summary.sql
│   │   │       └── stock_performance.sql
│   │   └── tests/
│   │       ├── assert_price_not_negative.sql
│   │       ├── assert_no_future_dates.sql
│   │       ├── assert_high_gte_low.sql
│   │       └── assert_expected_symbols.sql
│   │
│   ├── rag/
│   │   ├── app.py                          # Streamlit entry point
│   │   ├── rag_pipeline.py                 # Finnhub → Pinecone → Groq
│   │   └── pages/
│   │       ├── 1_Market_Intelligence.py    # RAG chat interface
│   │       ├── 2_Pipeline_Dashboard.py     # Real-time monitoring
│   │       └── 3_SQL_Explorer.py           # Snowflake query tool
│   │
│   └── airflow/
│       └── dags/
│           ├── stock_market_batch_dag.py   # Daily batch orchestration
│           └── stock_market_stream_dag.py  # Streaming orchestration
│
└── .github/
    └── workflows/
        └── ci.yml
```

---

## Data Flow

### Batch Pipeline
```
Alpha Vantage API  →  10 stocks × 365 days OHLCV
        ↓
Kafka  (stock-market-batch topic)
        ↓
MinIO  raw/historical/year=/month=/day=/
        ↓  Spark computes:
           SMA-5, SMA-20, daily_return_pct,
           daily_range, is_positive_day
MinIO  processed/historical/ (parquet by symbol)
        ↓  Incremental MERGE on (symbol, date)
Snowflake  STOCKMARKETBATCH.PUBLIC.HISTORICAL_STOCK
        ↓
dbt staging views → mart tables
  stock_daily_metrics    (SMA-50, volatility, volume ratio)
  stock_realtime_summary (hourly aggregations, trend signal)
  stock_performance      (BUY / SELL / HOLD signals)
        ↓
Finnhub News → chunk → embed → Pinecone (315+ vectors)
```

### Streaming Pipeline
```
Price Generator  →  fake tick every 30 seconds
        ↓
Kafka  (stock-market-realtime topic)
        ↓
MinIO  raw/realtime/year=/month=/day=/
        ↓  Spark windowed analytics:
           3-min + 15-min moving averages
           volatility per window
Snowflake  STOCKMARKETSTREAM.PUBLIC.REALTIME_STOCK
```

### Intelligence Layer
```
User: "What is the outlook for Apple?"
        ↓
Embed question with all-MiniLM-L6-v2  (384-dim vector)
        ↓
Pinecone cosine similarity search  →  top 5 relevant chunks
        ↓
Groq Llama3.1-8b generates grounded answer
        ↓
Streamlit displays answer + source cards with scores
```

---

## Snowflake Schema

### STOCKMARKETBATCH.PUBLIC.HISTORICAL_STOCK
| Column | Type | Description |
|---|---|---|
| symbol | STRING | Stock ticker (AAPL, MSFT...) |
| date | DATE | Trading date |
| open_price | FLOAT | Opening price |
| high_price | FLOAT | Daily high |
| low_price | FLOAT | Daily low |
| close_price | FLOAT | Closing price |
| volume | BIGINT | Trading volume |
| daily_range | FLOAT | high - low (Spark) |
| daily_return_pct | FLOAT | (close-open)/open × 100 (Spark) |
| is_positive_day | BOOLEAN | close > open (Spark) |
| sma_5 | FLOAT | 5-day moving average (Spark) |
| sma_20 | FLOAT | 20-day moving average (Spark) |
| batch_date | DATE | Date this batch was processed |

### STOCKMARKETSTREAM.PUBLIC.REALTIME_STOCK
| Column | Type | Description |
|---|---|---|
| symbol | STRING | Stock ticker |
| window_start | TIMESTAMP | Window start time |
| window_15m_end | TIMESTAMP | 15-min window end |
| window_1h_end | TIMESTAMP | 1-hour window end |
| ma_15m | FLOAT | 15-min moving average |
| ma_1h | FLOAT | 1-hour moving average |
| volatility_15m | FLOAT | Price stddev 15 min |
| volatility_1h | FLOAT | Price stddev 1 hour |
| volume_sum_15m | BIGINT | Volume in 15-min window |
| volume_sum_1h | BIGINT | Volume in 1-hour window |

---

## dbt Models

```
sources
  batch.HISTORICAL_STOCK    → STOCKMARKETBATCH
  stream.REALTIME_STOCK     → STOCKMARKETSTREAM

staging (views — clean + type-cast only)
  stg_historical_stock
  stg_realtime_stock

marts (tables — analytical layer)
  stock_daily_metrics       SMA-50, volatility_20d, volume_ratio
  stock_realtime_summary    Hourly OHLC + BULLISH/BEARISH/NEUTRAL
  stock_performance         JOIN historical + realtime → BUY/SELL/HOLD
```

### dbt Test Results
```
dbt run  → PASS=5  WARN=0  ERROR=0  ✅
dbt test → PASS=27 WARN=0  ERROR=0  ✅
```

Custom SQL tests:
- `assert_price_not_negative` — no negative close prices
- `assert_no_future_dates` — no future trade dates
- `assert_high_gte_low` — high price always >= low price
- `assert_expected_symbols` — only 10 tracked tickers

---

## RAG Architecture

```
Why semantic search over keyword search:
  "AAPL surging" and "Apple stock rising" get similar
  384-dim vectors — keyword search (BM25) would miss this.

Ingestion (daily via Airflow):
  Finnhub /company-news → 150 articles × 10 stocks
  chunk_text() → 500-char overlapping chunks → 179 docs
  all-MiniLM-L6-v2 (local, free) → 315+ vectors in Pinecone

Query (per user question in Streamlit):
  question → embed → Pinecone cosine similarity → top 5 chunks
  Groq Llama3.1-8b-instant generates grounded answer
  Sources shown with similarity scores + sentiment badges
```

---

## Airflow DAGs

### stock_market_batch_pipeline (Mon–Fri 9AM UTC)
```
fetch_historical_data
  → consume_historical_data
  → wait_for_raw_data      (MinIO sensor, min 10 files)
  → process_data           (Spark, passes {{ ds }} date)
  → load_historical_to_snowflake
  → run_dbt_models
  → run_dbt_tests
  → run_rag_ingestion
  → pipeline_complete
```

### stock_streaming_pipeline (Daily)
```
cleanup_processes
  → collect_streaming_data  (5 min window)
  → wait_for_raw_data       (MinIO sensor)
  → spark_analytics_processing
  → validate_analytics_data (MinIO sensor)
  → load_to_snowflake
  → pipeline_summary
  → final_cleanup
```

---

## Streamlit App (3 pages)

```
streamlit run app.py

Page 1 — Market Intelligence
  RAG chat powered by Finnhub + Pinecone + Groq Llama3.1
  Stock filter, quick questions, source cards with scores

Page 2 — Pipeline Dashboard
  Live KPI cards (row counts, latest date, dbt test status)
  Buy/sell signals from dbt stock_performance mart
  Realtime stream cards (15-min MA, 1-hour MA, trend)
  Top gainers / top losers
  4 live data quality checks against Snowflake
  Auto-refresh every 30 seconds

Page 3 — SQL Explorer
  6 preset queries (golden cross, volatility rank, etc.)
  Write custom SQL against STOCKMARKETBATCH or STOCKMARKETSTREAM
  Download results as CSV
```

---

## Getting Started

### Prerequisites
- Docker Desktop
- Python 3.10+
- Free API keys: Alpha Vantage, Finnhub, Pinecone, Groq

### 1. Clone
```bash
git clone https://github.com/atulpandey02/realtime-stock-intelligence.git
cd realtime-stock-intelligence
```

### 2. Configure environment
```bash
cp .env.example .env
# Edit .env with your API keys
```

### 3. Start infrastructure
```bash
docker-compose up -d
# Starts: Kafka, Zookeeper, MinIO, Spark, Airflow
```

### 4. Run batch pipeline
```bash
# Via Airflow UI at http://localhost:8080
# Trigger: stock_market_batch_pipeline
```

### 5. Run streaming pipeline
```bash
# Terminal 1
python src/kafka/producer/stream_data_producer.py
# Terminal 2
python src/kafka/consumer/realtime_data_consumer.py
# Then trigger: stock_streaming_pipeline in Airflow
```

### 6. Run dbt
```bash
cd src/dbt
dbt run   # creates 5 models
dbt test  # runs 27 quality tests
```

### 7. Ingest news and launch UI
```bash
cd src/rag
python rag_pipeline.py          # ingest Finnhub news → Pinecone
streamlit run app.py            # http://localhost:8501
```

---

## Environment Variables

```bash
# Kafka
KAFKA_BOOTSTRAP_SERVER=localhost:29092
KAFKA_TOPIC_BATCH=stock-market-batch
KAFKA_TOPIC_REALTIME=stock-market-realtime
KAFKA_GROUP_BATCH_ID=batch-consumer-group
KAFKA_GROUP_REALTIME_ID=realtime-consumer-group

# MinIO
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_BUCKET=stock-market-data
MINIO_ENDPOINT=minio:9000

# Snowflake
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_DATABASE=STOCKMARKETBATCH
SNOWFLAKE_STREAM_DATABASE=STOCKMARKETSTREAM
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_ROLE=ACCOUNTADMIN

# APIs
ALPHA_VANTAGE_API_KEY=your_key   # alphavantage.co
FINNHUB_API_KEY=your_key         # finnhub.io
PINECONE_API_KEY=your_key        # app.pinecone.io
PINECONE_INDEX_NAME=stock-market-rag
GROQ_API_KEY=your_key            # console.groq.com
```

---

## Tracked Stocks

`AAPL` `MSFT` `GOOGL` `AMZN` `META` `TSLA` `NVDA` `INTC` `JPM` `V`

---

## Key Design Decisions

| Decision | Why |
|---|---|
| UTC everywhere | Airflow `{{ ds }}` uses UTC — mismatches caused silent data loss |
| MERGE not INSERT | Idempotent loads — failed DAGs can safely reprocess |
| Hive partitioning | Spark partition pruning — reads only the date needed |
| Groq over HuggingFace | Free tier, reliable inference, 500 tokens/sec |
| Semantic over keyword search | Finds "AAPL surging" when asked about "Apple rising" |
| dbt + Spark hybrid | Spark for scale/streaming, dbt for SQL models and governance |

---

## CI/CD

GitHub Actions on every push:
1. Lint (flake8, black, isort)
2. Unit tests (pytest with mocked infrastructure)
3. dbt compile validation
4. Docker build
5. Airflow DAG syntax check
6. Integration tests (main branch only)

---

## Author

**Atul Kumar Pandey**
GitHub: [@atulpandey02](https://github.com/atulpandey02)

---

## License

MIT License
