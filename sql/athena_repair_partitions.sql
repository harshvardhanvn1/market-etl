-- Discover and register all partitions for the trades_binance table
-- Run this ONCE after the table is created or after new data is loaded
-- This scans S3 and adds partition metadata to the Glue Catalog

MSCK REPAIR TABLE market_data.trades_binance;

-- Verify partitions were discovered
SHOW PARTITIONS market_data.trades_binance;

-- Quick check: count partitions
SELECT COUNT(*) as total_partitions
FROM (SHOW PARTITIONS market_data.trades_binance);