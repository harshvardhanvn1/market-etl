-- Sample Analytical Queries for Binance Trades Table
-- Demonstrates partition pruning, aggregations, and common patterns

-- =============================================================================
-- QUERY 1: Basic Row Counts (Test Partition Pruning)
-- =============================================================================
-- This query shows how partition pruning works - Athena only scans relevant partitions

SELECT 
    year,
    month,
    symbol,
    COUNT(*) as trade_count,
    SUM(quote_qty) as total_volume_usd,
    MIN(price) as low_price,
    MAX(price) as high_price
FROM market_data.trades_binance
WHERE year = 2025 
  AND month = 7
GROUP BY year, month, symbol
ORDER BY symbol;

-- Expected: Fast query (only scans month=7 partitions)
-- Typical data scanned: 500-800 MB per symbol


-- =============================================================================
-- QUERY 2: Intraday VWAP (Volume-Weighted Average Price)
-- =============================================================================
-- Calculate minute-by-minute VWAP for BTCUSDT on a specific day

SELECT 
    date_trunc('minute', trade_time) as minute,
    COUNT(*) as num_trades,
    SUM(quantity) as total_volume,
    SUM(price * quantity) / NULLIF(SUM(quantity), 0) as vwap,
    MIN(price) as low,
    MAX(price) as high
FROM market_data.trades_binance
WHERE year = 2025 
  AND month = 7
  AND day = 15
  AND symbol = 'BTCUSDT'
GROUP BY date_trunc('minute', trade_time)
ORDER BY minute;

-- Expected: 1,440 rows (24 hours × 60 minutes)
-- Use case: Generate 1-minute candlestick data


-- =============================================================================
-- QUERY 3: Order Flow Analysis - Maker vs Taker
-- =============================================================================
-- Analyze buy/sell pressure by looking at maker/taker distribution

SELECT 
    symbol,
    CASE 
        WHEN is_buyer_maker THEN 'Sell (Market Sell)'
        ELSE 'Buy (Market Buy)'
    END as order_type,
    COUNT(*) as trade_count,
    SUM(quantity) as total_quantity,
    SUM(quote_qty) as total_value_usd,
    AVG(quantity) as avg_trade_size
FROM market_data.trades_binance
WHERE year = 2025 
  AND month = 7
GROUP BY symbol, is_buyer_maker
ORDER BY symbol, is_buyer_maker;

-- Use case: Understand market microstructure and order flow


-- =============================================================================
-- QUERY 4: Hourly Trading Volume Heatmap
-- =============================================================================
-- Find peak trading hours for each symbol

SELECT 
    symbol,
    EXTRACT(HOUR FROM trade_time) as hour_utc,
    COUNT(*) as trade_count,
    SUM(quote_qty) / 1e6 as volume_millions_usd,
    AVG(price) as avg_price
FROM market_data.trades_binance
WHERE year = 2025 
  AND month = 7
GROUP BY symbol, EXTRACT(HOUR FROM trade_time)
ORDER BY symbol, hour_utc;

-- Use case: Identify liquidity patterns and optimal trading windows


-- =============================================================================
-- QUERY 5: Large Trade Detection (Whale Watching)
-- =============================================================================
-- Find trades above 99th percentile by value

WITH percentiles AS (
    SELECT 
        symbol,
        APPROX_PERCENTILE(quote_qty, 0.99) as p99_trade_value
    FROM market_data.trades_binance
    WHERE year = 2025 AND month = 7
    GROUP BY symbol
)
SELECT 
    t.symbol,
    t.trade_time,
    t.price,
    t.quantity,
    t.quote_qty as trade_value_usd,
    t.is_buyer_maker
FROM market_data.trades_binance t
JOIN percentiles p ON t.symbol = p.symbol
WHERE t.year = 2025 
  AND t.month = 7
  AND t.quote_qty >= p.p99_trade_value
ORDER BY t.quote_qty DESC
LIMIT 100;

-- Use case: Detect institutional or "whale" activity


-- =============================================================================
-- QUERY 6: Multi-Symbol Price Correlation Setup
-- =============================================================================
-- Prepare minute-level data for correlation analysis

WITH minute_prices AS (
    SELECT 
        symbol,
        date_trunc('minute', trade_time) as minute,
        SUM(price * quantity) / NULLIF(SUM(quantity), 0) as vwap
    FROM market_data.trades_binance
    WHERE year = 2025 
      AND month = 7
      AND day = 15
    GROUP BY symbol, date_trunc('minute', trade_time)
)
SELECT 
    bt.minute,
    bt.vwap as btc_price,
    et.vwap as eth_price,
    bn.vwap as bnb_price
FROM (SELECT * FROM minute_prices WHERE symbol = 'BTCUSDT') bt
FULL OUTER JOIN (SELECT * FROM minute_prices WHERE symbol = 'ETHUSDT') et 
    ON bt.minute = et.minute
FULL OUTER JOIN (SELECT * FROM minute_prices WHERE symbol = 'BNBUSDT') bn 
    ON bt.minute = bn.minute
ORDER BY bt.minute;

-- Use case: Export to Python/R for correlation matrix, pairs trading


-- =============================================================================
-- QUERY 7: Data Quality Check
-- =============================================================================
-- Validate data integrity and spot anomalies

SELECT 
    year,
    month,
    day,
    symbol,
    COUNT(*) as row_count,
    COUNT(DISTINCT trade_id) as unique_trades,
    COUNT(*) - COUNT(DISTINCT trade_id) as duplicate_count,
    MIN(price) as min_price,
    MAX(price) as max_price,
    AVG(price) as avg_price,
    SUM(CASE WHEN price <= 0 THEN 1 ELSE 0 END) as invalid_price_count,
    SUM(CASE WHEN quantity <= 0 THEN 1 ELSE 0 END) as invalid_qty_count
FROM market_data.trades_binance
WHERE year = 2025 AND month = 7
GROUP BY year, month, day, symbol
ORDER BY year, month, day, symbol;

-- Use case: Monitor data quality, detect pipeline issues


-- =============================================================================
-- QUERY 8: Partition Efficiency Test
-- =============================================================================
-- Compare query performance with and without partition filters

-- GOOD: Uses partition pruning (fast)
SELECT COUNT(*) as trade_count
FROM market_data.trades_binance
WHERE year = 2025 
  AND month = 7
  AND symbol = 'BTCUSDT';

-- BAD: Full table scan (slow, expensive)
-- Uncomment to test, but be careful with large datasets!
-- SELECT COUNT(*) as trade_count
-- FROM market_data.trades_binance
-- WHERE symbol = 'BTCUSDT';

-- Expected: First query scans ~1-2 GB, second scans entire table (10+ GB)


-- =============================================================================
-- QUERY 9: Daily Summary Statistics
-- =============================================================================
-- Quick overview of trading activity by date

SELECT 
    year,
    month,
    day,
    symbol,
    COUNT(*) as trades,
    SUM(quote_qty) / 1e6 as volume_millions_usd,
    MIN(price) as low,
    MAX(price) as high,
    SUM(price * quantity) / NULLIF(SUM(quantity), 0) as vwap
FROM market_data.trades_binance
WHERE year = 2025 
  AND month = 7
GROUP BY year, month, day, symbol
ORDER BY year, month, day, symbol;

-- Use case: Daily trading summary, dashboard metrics


-- =============================================================================
-- PERFORMANCE TIPS
-- =============================================================================
-- 1. Always include year/month/day in WHERE clause (partition pruning)
-- 2. Use LIMIT for exploratory queries to reduce costs
-- 3. Use APPROX_PERCENTILE instead of PERCENTILE for large datasets
-- 4. Pre-aggregate to minute/hour level for faster correlation analysis
-- 5. Monitor "Data scanned" in Athena console - you pay per GB scannedt.is_buyer_maker
FROM market_data.trades_binance t
JOIN percentiles p ON t.symbol = p.symbol
WHERE t.year = 2025 
  AND t.month = 9
  AND t.quote_qty >= p.p99_trade_value
ORDER BY t.quote_qty DESC
LIMIT 100;

-- Use case: Detect institutional or "whale" activity


-- =============================================================================
-- QUERY 6: Multi-Symbol Price Correlation Setup
-- =============================================================================
-- Prepare minute-level data for correlation analysis

WITH minute_prices AS (
    SELECT 
        symbol,
        date_trunc('minute', trade_time) as minute,
        SUM(price * quantity) / NULLIF(SUM(quantity), 0) as vwap
    FROM market_data.trades_binance
    WHERE year = 2025 
      AND month = 9 
      AND day = 15
    GROUP BY symbol, date_trunc('minute', trade_time)
)
SELECT 
    bt.minute,
    bt.vwap as btc_price,
    et.vwap as eth_price,
    bn.vwap as bnb_price
FROM (SELECT * FROM minute_prices WHERE symbol = 'BTCUSDT') bt
FULL OUTER JOIN (SELECT * FROM minute_prices WHERE symbol = 'ETHUSDT') et 
    ON bt.minute = et.minute
FULL OUTER JOIN (SELECT * FROM minute_prices WHERE symbol = 'BNBUSDT') bn 
    ON bt.minute = bn.minute
ORDER BY bt.minute;

-- Use case: Export to Python/R for correlation matrix, pairs trading


-- =============================================================================
-- QUERY 7: Data Quality Check
-- =============================================================================
-- Validate data integrity and spot anomalies

SELECT 
    year,
    month,
    day,
    symbol,
    COUNT(*) as row_count,
    COUNT(DISTINCT trade_id) as unique_trades,
    COUNT(*) - COUNT(DISTINCT trade_id) as duplicate_count,
    MIN(price) as min_price,
    MAX(price) as max_price,
    AVG(price) as avg_price,
    SUM(CASE WHEN price <= 0 THEN 1 ELSE 0 END) as invalid_price_count,
    SUM(CASE WHEN quantity <= 0 THEN 1 ELSE 0 END) as invalid_qty_count
FROM market_data.trades_binance
WHERE year = 2025 AND month = 9
GROUP BY year, month, day, symbol
ORDER BY year, month, day, symbol;

-- Use case: Monitor data quality, detect pipeline issues


-- =============================================================================
-- QUERY 8: Partition Efficiency Test
-- =============================================================================
-- Compare query performance with and without partition filters

-- GOOD: Uses partition pruning (fast)
SELECT COUNT(*) as trade_count
FROM market_data.trades_binance
WHERE year = 2025 
  AND month = 9 
  AND symbol = 'BTCUSDT';

-- BAD: Full table scan (slow, expensive)
-- Uncomment to test, but be careful with large datasets!
-- SELECT COUNT(*) as trade_count
-- FROM market_data.trades_binance
-- WHERE symbol = 'BTCUSDT';

-- Expected: First query scans ~1-2 GB, second scans entire table (10+ GB)


-- =============================================================================
-- QUERY 9: Recent Activity Summary (Dashboard Query)
-- =============================================================================
-- Quick overview of latest trading activity

SELECT 
    symbol,
    COUNT(*) as trades_last_hour,
    SUM(quote_qty) as volume_usd,
    MIN(price) as low,
    MAX(price) as high,
    SUM(price * quantity) / NULLIF(SUM(quantity), 0) as vwap,
    SUM(CASE WHEN is_buyer_maker THEN quote_qty ELSE 0 END) / 
        NULLIF(SUM(quote_qty), 0) * 100 as pct_sell_pressure
FROM market_data.trades_binance
WHERE trade_time >= date_add('hour', -1, current_timestamp)
GROUP BY symbol
ORDER BY volume_usd DESC;

-- Use case: Real-time dashboard, monitoring


-- =============================================================================
-- PERFORMANCE TIPS
-- =============================================================================
-- 1. Always include year/month/day in WHERE clause (partition pruning)
-- 2. Use LIMIT for exploratory queries to reduce costs
-- 3. Use APPROX_PERCENTILE instead of PERCENTILE for large datasets
-- 4. Pre-aggregate to minute/hour level for faster correlation analysis
-- 5. Monitor "Data scanned" in Athena console - you pay per GB scanned