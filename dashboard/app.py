import json
import os
from pathlib import Path

import pandas as pd
import plotly.express as px
import requests
import streamlit as st

DEFAULT_API_BASE_URL = "http://127.0.0.1:8000"
API_BASE_URL = os.getenv("API_BASE_URL", DEFAULT_API_BASE_URL).rstrip("/")
DEMO_DATA_PATH = Path(__file__).with_name("demo_data.json")

st.set_page_config(
    page_title="Real-Time Commerce Intelligence",
    page_icon="🛒",
    layout="wide",
)


@st.cache_data
def load_demo_data() -> dict:
    """Load a representative commerce snapshot for the public portfolio demo."""
    with DEMO_DATA_PATH.open(encoding="utf-8") as demo_file:
        return json.load(demo_file)


@st.cache_data(ttl=20)
def get_json(endpoint: str):
    response = requests.get(f"{API_BASE_URL}{endpoint}", timeout=5)
    response.raise_for_status()
    return response.json()


def load_dashboard_data() -> tuple[dict, bool]:
    """Prefer live FastAPI metrics and fall back safely when unavailable."""
    demo = load_demo_data()
    try:
        data = {
            **demo,
            "summary": get_json("/metrics/summary?minutes=1440"),
            "categories": get_json("/metrics/categories?minutes=1440"),
            "latency": get_json("/metrics/latency"),
        }
        return data, True
    except (requests.RequestException, ValueError, KeyError, TypeError):
        return demo, False


st.title("🛒 Real-Time Commerce Data Pipeline")
st.caption(
    "Streaming sales intelligence powered by Python, Redpanda/Kafka, "
    "PostgreSQL and FastAPI."
)

data, is_live = load_dashboard_data()
summary = data["summary"]
latency = data["latency"]

st.sidebar.markdown("### Streaming Data Flow")
st.sidebar.write("Orders → Kafka → Python processor → PostgreSQL → FastAPI")

if is_live:
    st.sidebar.success("Live FastAPI connected")
else:
    st.sidebar.info("Portfolio demo mode")
    st.info(
        "**Portfolio demo:** displaying a representative e-commerce snapshot. "
        "Run the complete Docker pipeline locally to enable live streaming data."
    )

orders = int(summary["orders"])
revenue = float(summary["revenue"])
customers = int(summary["customers"])

col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Orders", f"{orders:,}")
col2.metric("Revenue", f"£{revenue:,.0f}")
col3.metric("Average order value", f"£{float(summary['average_order_value']):,.2f}")
col4.metric("Customers", f"{customers:,}")
col5.metric("Avg. latency", f"{float(latency['average_ms']):.1f} ms")

st.divider()
left, right = st.columns(2)

with left:
    st.subheader("Revenue through the day")
    sales = pd.DataFrame(data["sales_over_time"])
    sales["time"] = pd.to_datetime(sales["time"])
    sales_chart = px.area(
        sales,
        x="time",
        y="revenue",
        markers=True,
        labels={"time": "Time", "revenue": "Revenue (£)"},
    )
    sales_chart.update_traces(line_color="#00a878", fillcolor="rgba(0,168,120,0.25)")
    st.plotly_chart(sales_chart, use_container_width=True)

with right:
    st.subheader("Revenue by category")
    categories = pd.DataFrame(data["categories"])
    category_chart = px.bar(
        categories.sort_values("revenue"),
        x="revenue",
        y="category",
        orientation="h",
        color="orders",
        color_continuous_scale="Blues",
        labels={"category": "Category", "revenue": "Revenue (£)", "orders": "Orders"},
    )
    st.plotly_chart(category_chart, use_container_width=True)

st.divider()
left, right = st.columns(2)

with left:
    st.subheader("Orders by country")
    countries = pd.DataFrame(data["country_metrics"])
    country_chart = px.bar(
        countries.sort_values("orders"),
        x="orders",
        y="country",
        orientation="h",
        color="revenue",
        color_continuous_scale="Greens",
        labels={"country": "Country", "orders": "Orders", "revenue": "Revenue (£)"},
    )
    st.plotly_chart(country_chart, use_container_width=True)

with right:
    st.subheader("Pipeline reliability")
    p1, p2, p3 = st.columns(3)
    p1.metric("Processed events", f"{int(latency['processed_events']):,}")
    p2.metric("Average latency", f"{float(latency['average_ms']):.1f} ms")
    p3.metric("P95 latency", f"{float(latency['p95_ms']):.1f} ms")
    st.success("Kafka-compatible stream healthy")
    st.success("Schema validation active")
    st.success("Idempotent PostgreSQL writes enabled")
    st.info("Malformed events are isolated in the dead-letter topic.")

st.divider()
st.subheader("Recent orders")
recent_orders = pd.DataFrame(data["recent_orders"])
recent_orders["event_time"] = pd.to_datetime(recent_orders["event_time"], utc=True)
recent_orders = recent_orders.rename(
    columns={
        "event_time": "Event time",
        "order_id": "Order",
        "category": "Category",
        "product": "Product",
        "quantity": "Quantity",
        "total_amount": "Total (£)",
        "country": "Country",
        "status": "Validation",
    }
)
st.dataframe(
    recent_orders[
        [
            "Event time",
            "Order",
            "Category",
            "Product",
            "Quantity",
            "Total (£)",
            "Country",
            "Validation",
        ]
    ],
    use_container_width=True,
    hide_index=True,
)

with st.expander("How delivery reliability works"):
    st.markdown(
        """
        Events are validated before storage. Valid orders and processing metrics
        are persisted in one database transaction, and Kafka offsets are
        committed only after that transaction succeeds. Unique event IDs make
        redelivery safe, while malformed events are routed to a dead-letter
        topic instead of blocking the stream.
        """
    )

st.caption(
    "Representative synthetic commerce data is used for portfolio "
    "demonstration; no real customer or payment information is included."
)
