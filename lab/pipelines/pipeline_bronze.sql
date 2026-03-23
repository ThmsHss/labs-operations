-- Bronze: Ingest raw inventory events from Volume
CREATE OR REFRESH STREAMING TABLE bronze_inventory
COMMENT 'Raw inventory events ingested from JSON files'
AS
SELECT
    *,
    current_timestamp() AS _ingested_at,
    _metadata.file_path AS _source_file
FROM read_files(
    '/Volumes/puma_ops_lab/workshop/raw_data/inventory_raw/',
    format => 'json',
    schemaHints => 'event_id STRING, product_id STRING, warehouse STRING, event_type STRING, quantity_change INT, event_timestamp TIMESTAMP'
);
