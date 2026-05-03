# AsiaStats — LoL Esports Analytics Pipeline

Real-time data engineering pipeline ingesting League of Legends match data 
from the Pandascore API, normalizing it into a structured schema, and serving 
live analytics on an interactive dashboard.

## Architecture

Pandascore API → pandas normalization → PostgreSQL (next) → Streamlit dashboard

## What it does

- Ingests live + historical LoL match data across multiple regions and tournament tiers
- Normalizes nested JSON into three clean tables: `match_facts`, `team_match_stats`, `game_facts`
- Handles CSV and live API modes with 5-minute auto-refresh
- Interactive filters: region, league, tier, minimum matches
- Head-to-head team comparison with radar chart
- Weighted performance scoring that accounts for tournament tier

## Tech stack

| Layer | Tool |
|---|---|
| Data source | Pandascore API |
| Ingestion & transform | Python, pandas |
| Dashboard | Streamlit, Plotly |
| Next layer | Apache Kafka, Spark Structured Streaming |
| Cloud target | GCP / AWS |

## Run locally

git clone https://github.com/YOUR_USERNAME/asiastats-lol-pipeline
cd asiastats-lol-pipeline
pip install -r requirements.txt
streamlit run dashboard.py

Add your Pandascore API key in the dashboard sidebar to switch to live mode.

## Roadmap

- [ ] Kafka producer streaming live match events
- [ ] Spark Structured Streaming for windowed aggregations
- [ ] dbt models on top of PostgreSQL warehouse
- [ ] Deploy on GCP (Pub/Sub + BigQuery + Cloud Run)
