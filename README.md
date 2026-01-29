# Smart Energy Consumption Analytics Platform

This project simulates an end-to-end data engineering pipeline for energy consumption analytics using streaming ingestion, batch processing, and interactive visualization.

## Architecture

Simulated IoT Sensors  
→ Kafka (Streaming Ingestion)  
→ Data Lake (Raw JSON)  
→ Apache Spark (Aggregation)  
→ Parquet (Processed Analytics Data)  
→ Streamlit Dashboard  

## Tech Stack
- Kafka (streaming ingestion)
- Apache Spark (batch processing)
- Parquet (analytics storage)
- Streamlit (dashboard)
- Pandas / Matplotlib (visualization)

## Live Dashboard
(Coming soon)

## How to Run Dashboard Locally
```bash
streamlit run dashboards/app.py
