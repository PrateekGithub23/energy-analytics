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

## Running it

Dashboard only (uses the processed Parquet already in `data-lake/processed/`):

```bash
pip install -r dashboards/requirements.txt
streamlit run dashboards/app.py
```

Full pipeline, from the repository root:

```bash
docker compose -f docker/docker-compose.kafka.yml up -d     # Zookeeper + Kafka on :9092
pip install -r requirements.txt
python -m ingestion.consumer          # terminal 1: writes readings to data-lake/raw/energy/date=*/data.json
python -m data_generator.generate_data   # terminal 2: simulated sensors publish to the energy_readings topic

docker compose -f docker/docker-compose.spark.yml up -d     # Spark master + worker, repo mounted at /workspace
docker exec spark-master /opt/spark/bin/spark-submit /workspace/spark-jobs/energy_aggregation.py
streamlit run dashboards/app.py
```

The Spark job writes daily totals and hourly usage (partitioned by date and hour) as Parquet.
`data-lake/raw_backup/` keeps a small sample of raw readings.
