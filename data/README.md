# Data Lake Zones

Populated at runtime — nothing here is committed except this contract.

| Zone | Producer | Contract |
|------|----------|----------|
| `raw/` | `data_generator/generate_data.py` (in production: device feeds) | Files exactly as they arrive: `sensor_readings_YYYYMMDD.csv`, `camera_events_YYYYMMDD.json`, `weather_observations_YYYYMMDD.json`, plus `_manifest.json` for reconciliation tests |
| `bronze/` | Spark job 01 | As-landed Parquet (zstd) + audit columns (`_ingest_ts`, `_source_file`, `_batch_id`); immutable, partitioned by `event_date` |
| `silver/` | Spark job 02 | Typed, validated, deduplicated; quarantined rows in `silver/_quarantine` with `reject_reason`; `quality_metrics` per rule/day |
| `gold/` | Spark jobs 03–04 | Business grain: `hourly_traffic`, `traffic_events` (enriched), `kpi_segment_daily`, `kpi_weather_penalty`, `kpi_rush_hours`, `quality_daily` |
