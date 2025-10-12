-- Create External Table for Binance Trades in Athena
-- Points to partitioned Parquet files in S3
-- Enables SQL analytics on 300M+ rows

CREATE EXTERNAL TABLE IF NOT EXISTS market_data.trades_binance (
    trade_id BIGINT COMMENT 'Unique trade identifier from Binance',
    trade_time TIMESTAMP COMMENT 'Trade execution timestamp (UTC)',
    price DOUBLE COMMENT 'Trade price in quote currency (USDT)',
    quantity DOUBLE COMMENT 'Trade quantity in base currency (BTC/ETH/BNB)',
    quote_qty DOUBLE COMMENT 'Total trade value (price × quantity)',
    is_buyer_maker BOOLEAN COMMENT 'True if buyer placed limit order (maker)',
    is_best_match BOOLEAN COMMENT 'True if trade matched best bid/ask',
    load_dt DATE COMMENT 'Date when data was loaded into the pipeline'
)
COMMENT 'Binance spot market trades - tick-level execution data'
PARTITIONED BY (
    year INT COMMENT 'Year of trade (for partition pruning)',
    month INT COMMENT 'Month of trade (1-12)',
    day INT COMMENT 'Day of trade (1-31)',
    symbol STRING COMMENT 'Trading pair symbol (BTCUSDT, ETHUSDT, BNBUSDT)'
)
STORED AS PARQUET
LOCATION 's3://market-data-etl-dev-785100679003/processed/binance/spot/trades/'
TBLPROPERTIES (
    'parquet.compression'='SNAPPY',
    'classification'='parquet'
);

-- Discover and register all partitions
-- This scans S3 and adds partition metadata to Glue Catalog
MSCK REPAIR TABLE market_data.trades_binance;

-- Verify partitions were loaded
SHOW PARTITIONS market_data.trades_binance;

-- Quick sanity check: count rows by symbol
SELECT 
    symbol,
    COUNT(*) as trade_count,
    MIN(trade_time) as first_trade,
    MAX(trade_time) as last_trade
FROM market_data.trades_binance
GROUP BY symbol
ORDER BY symbol;