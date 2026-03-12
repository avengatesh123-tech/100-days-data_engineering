import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import glob
import os
import time

st.set_page_config(
    page_title="ETL Late Data Monitor",
    page_icon="🔥",
    layout="wide"
)

st.title("Real-Time ETL Pipeline - Late Data Monitor")
st.caption("Monitoring ingestion and windowed aggregates")

DELTA_RAW = "/app/delta_tables/raw_orders"
DELTA_AGG = "/app/delta_tables/agg_orders"

def load_delta_as_df(path):
    """Load Delta Lake parquet files into a DataFrame"""
    files = glob.glob(f"{path}/**/*.parquet", recursive=True)
    if not files:
        return pd.DataFrame()
    dfs = [pd.read_parquet(f) for f in files]
    return pd.concat(dfs, ignore_index=True)

refresh = st.sidebar.slider("Auto-refresh (seconds)", 5, 60, 15)
st.sidebar.info(f"Refreshing every {refresh}s")

placeholder = st.empty()

while True:
    with placeholder.container():

        raw_df = load_delta_as_df(DELTA_RAW)
        agg_df = load_delta_as_df(DELTA_AGG)

        if raw_df.empty:
            st.warning("⏳ Waiting for streaming data... Make sure Spark streaming is running!")
            st.info("""
            **Start the streaming job:**
            ```bash
            docker exec spark-master spark-submit \\
              --packages io.delta:delta-core_2.12:2.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \\
              /app/consumer/spark_streaming.py
            ```
            """)
        else:
            raw_df["event_ts"]  = pd.to_datetime(raw_df.get("event_ts",  raw_df.get("event_time")))
            raw_df["ingest_ts"] = pd.to_datetime(raw_df.get("ingest_ts", raw_df.get("ingest_time")))
            raw_df["latency_min"] = (raw_df["ingest_ts"] - raw_df["event_ts"]).dt.total_seconds() / 60

            total       = len(raw_df)
            late_count  = raw_df["is_late"].sum() if "is_late" in raw_df.columns else 0
            late_pct    = round(late_count / total * 100, 1) if total else 0
            avg_latency = round(raw_df["latency_min"].mean(), 1)

            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Total Orders", f"{total:,}")
            col2.metric("Late Orders", f"{int(late_count):,}", f"{late_pct}%")
            col3.metric("Avg Latency (min)", f"{avg_latency}")
            col4.metric("Total Revenue",
                        f"₹{raw_df['amount'].sum():,.0f}" if "amount" in raw_df.columns else "N/A")

            st.divider()
            c1, c2 = st.columns(2)
            with c1:
                st.subheader("Orders by City")
                if "city" in raw_df.columns:
                    city_df = raw_df.groupby("city").size().reset_index(name="count")
                    fig = px.bar(city_df, x="city", y="count", color="city",
                                 color_discrete_sequence=px.colors.qualitative.Set2)
                    st.plotly_chart(fig, use_container_width=True)

            with c2:
                st.subheader("Late vs Normal Orders")
                if "is_late" in raw_df.columns:
                    pie_df = raw_df["is_late"].value_counts().reset_index()
                    pie_df.columns = ["type", "count"]
                    pie_df["type"] = pie_df["type"].map({True: "Late", False: "Normal"})
                    fig = px.pie(pie_df, values="count", names="type",
                                 color_discrete_map={"Late": "#FF6B6B", "Normal": "#51CF66"})
                    st.plotly_chart(fig, use_container_width=True)

            st.subheader("Latency Distribution (minutes)")
            fig = px.histogram(raw_df, x="latency_min", nbins=30,
                               color_discrete_sequence=["#339AF0"])
            fig.add_vline(x=2, line_dash="dash", line_color="red",
                          annotation_text="2min watermark")
            st.plotly_chart(fig, use_container_width=True)

            st.subheader("Recent Orders")
            display_cols = [c for c in ["order_id","customer_id","product","amount",
                                         "city","status","event_ts","latency_min","is_late"]
                            if c in raw_df.columns]
            st.dataframe(
                raw_df[display_cols].sort_values("event_ts", ascending=False).head(20),
                use_container_width=True
            )

    time.sleep(refresh)
    placeholder.empty()
