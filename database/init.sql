CREATE TABLE crypto_prices (
    id SERIAL PRIMARY KEY,
    coin TEXT,
    price_usd NUMERIC,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

