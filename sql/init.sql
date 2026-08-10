CREATE TABLE IF NOT EXISTS orders (
    event_id UUID PRIMARY KEY,
    order_id UUID NOT NULL,
    customer_id UUID NOT NULL,
    product_id TEXT NOT NULL,
    category TEXT NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(12,2) NOT NULL CHECK (unit_price >= 0),
    total_amount NUMERIC(14,2) NOT NULL CHECK (total_amount >= 0),
    country CHAR(2) NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_orders_event_time ON orders (event_time DESC);
CREATE INDEX IF NOT EXISTS idx_orders_category_time ON orders (category, event_time DESC);

CREATE TABLE IF NOT EXISTS pipeline_metrics (
    id BIGSERIAL PRIMARY KEY,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    event_id UUID NOT NULL UNIQUE,
    processing_latency_ms INTEGER NOT NULL,
    consumer_partition INTEGER NOT NULL,
    consumer_offset BIGINT NOT NULL
);

CREATE OR REPLACE VIEW sales_by_minute AS
SELECT
    date_trunc('minute', event_time) AS minute,
    category,
    COUNT(*) AS orders,
    SUM(quantity) AS units,
    ROUND(SUM(total_amount), 2) AS revenue
FROM orders
GROUP BY 1, 2;

