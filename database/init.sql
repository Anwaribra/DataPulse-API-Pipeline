-- Create crypto_prices table if not exists
CREATE TABLE IF NOT EXISTS crypto_prices (
    id SERIAL PRIMARY KEY,
    coin VARCHAR(50) NOT NULL,
    price_usd NUMERIC NOT NULL,
    timestamp TIMESTAMP NOT NULL
);

-- Create crypto_prices_daily table if not exists
CREATE TABLE IF NOT EXISTS crypto_prices_daily (
    coin VARCHAR(50),
    date DATE,
    avg_price NUMERIC,
    min_price NUMERIC,
    max_price NUMERIC,
    price_std NUMERIC,
    PRIMARY KEY (coin, date)
);

-- Create index on timestamp for better query performance
CREATE INDEX IF NOT EXISTS idx_crypto_prices_timestamp ON crypto_prices(timestamp);

