-- Silver: Clean and validate inventory events
CREATE OR REFRESH STREAMING TABLE silver_inventory(
    CONSTRAINT valid_event_id EXPECT (event_id IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT valid_product EXPECT (product_id IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT valid_warehouse EXPECT (warehouse IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT valid_timestamp EXPECT (event_timestamp IS NOT NULL AND event_timestamp <= current_timestamp()) ON VIOLATION DROP ROW
)
COMMENT 'Cleaned and validated inventory events'
AS
SELECT
    event_id,
    product_id,
    warehouse,
    event_type,
    quantity_change,
    event_timestamp,
    _ingested_at,
    _source_file
FROM STREAM(LIVE.bronze_inventory);
