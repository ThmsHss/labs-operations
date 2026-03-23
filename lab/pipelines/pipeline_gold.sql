-- Gold: Inventory summary by warehouse and product
CREATE OR REFRESH MATERIALIZED VIEW gold_inventory_summary
COMMENT 'Aggregated inventory levels by warehouse and product'
AS
SELECT
    warehouse,
    product_id,
    event_type,
    SUM(quantity_change) AS total_quantity_change,
    COUNT(*) AS event_count,
    MIN(event_timestamp) AS first_event,
    MAX(event_timestamp) AS last_event
FROM LIVE.silver_inventory
GROUP BY warehouse, product_id, event_type;
