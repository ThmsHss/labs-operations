-- Bronze: Ingest raw inventory events — BROKEN VERSION
-- BUG: The source path is wrong (typo: 'inventori_raw' instead of 'inventory_raw')
CREATE OR REFRESH STREAMING TABLE bronze_inventory_broken
COMMENT 'Raw inventory events — BROKEN: bad source path'
AS
SELECT
    *,
    current_timestamp() AS _ingested_at,
    _metadata.file_path AS _source_file
FROM read_files(
    '/Volumes/puma_ops_lab/workshop/raw_data/inventori_raw/',
    format => 'json',
    schemaHints => 'event_id STRING, product_id STRING, warehouse STRING, event_type STRING, quantity_change INT, event_timestamp TIMESTAMP'
);
