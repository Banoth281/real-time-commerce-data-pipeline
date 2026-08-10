from contextlib import asynccontextmanager

from fastapi import FastAPI, Query
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from src.common.config import settings

pool: ConnectionPool | None = None


@asynccontextmanager
async def lifespan(_: FastAPI):
    global pool
    pool = ConnectionPool(settings.postgres_dsn, min_size=1, max_size=5, kwargs={"row_factory": dict_row})
    yield
    pool.close()


app = FastAPI(title="Real-Time Commerce Analytics API", version="1.0.0", lifespan=lifespan)


@app.get("/health")
def health() -> dict:
    assert pool
    with pool.connection() as conn:
        conn.execute("SELECT 1").fetchone()
    return {"status": "ok"}


@app.get("/metrics/summary")
def summary(minutes: int = Query(60, ge=1, le=1440)) -> dict:
    assert pool
    query = """
    SELECT COUNT(*) AS orders,
           COALESCE(ROUND(SUM(total_amount), 2), 0) AS revenue,
           COALESCE(ROUND(AVG(total_amount), 2), 0) AS average_order_value,
           COUNT(DISTINCT customer_id) AS customers
    FROM orders
    WHERE event_time >= NOW() - make_interval(mins => %s)
    """
    with pool.connection() as conn:
        return dict(conn.execute(query, (minutes,)).fetchone())


@app.get("/metrics/categories")
def categories(minutes: int = Query(60, ge=1, le=1440)) -> list[dict]:
    assert pool
    query = """
    SELECT category, COUNT(*) AS orders, SUM(quantity) AS units,
           ROUND(SUM(total_amount), 2) AS revenue
    FROM orders
    WHERE event_time >= NOW() - make_interval(mins => %s)
    GROUP BY category
    ORDER BY revenue DESC
    """
    with pool.connection() as conn:
        return [dict(row) for row in conn.execute(query, (minutes,)).fetchall()]


@app.get("/metrics/latency")
def latency() -> dict:
    assert pool
    query = """
    SELECT COUNT(*) AS processed_events,
           COALESCE(ROUND(AVG(processing_latency_ms), 1), 0) AS average_ms,
           COALESCE(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY processing_latency_ms), 0) AS p95_ms
    FROM pipeline_metrics
    WHERE recorded_at >= NOW() - INTERVAL '1 hour'
    """
    with pool.connection() as conn:
        return dict(conn.execute(query).fetchone())

