import streamlit as st
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import requests

@st.cache_data
def get_data():
    try:
        response = requests.get("http://fastapi:8000/orders")
        data = response.json()

        if isinstance(data, dict) and "orders" in data:
            orders = data["orders"]
        elif isinstance(data, list):
            orders = data
        else:
            st.error("Invalid API response format")
            return pd.DataFrame()

        if not orders:
            return pd.DataFrame()

        if isinstance(orders[0], list):
            columns = ["order_id", "status", "category", "price", "city", "payment_method", "tax"]
            return pd.DataFrame(orders, columns=columns)
        else:
            return pd.DataFrame(orders)

    except Exception as e:
        st.error(f"Request failed: {e}")
        return pd.DataFrame()

# === Streamlit UI ===
st.set_page_config(page_title="Real-Time Orders Dashboard", layout="wide")
st.title(" Real-Time Orders Dashboard")

# Refresh button
if st.button("Refresh Data"):
    st.cache_data.clear()

df = get_data()

if df.empty:
    st.info("No records found in the database.")
else:
    st.dataframe(df, use_container_width=True)

    # Metrics
    st.markdown("### Summary")
    cols = st.columns(3)
    cols[0].metric("Total records", len(df["order_id"]))
    if "price" in df.columns:
        cols[1].metric("Avg price", f"{round(df['price'].mean(),2)} MAD")
    if "final_price" in df.columns:
        cols[2].metric("Avg final price", f"{round(df['final_price'].mean(),2)} MAD")

    # Charts
    st.markdown("### Charts")
    st.markdown("#### Category:")
    if "category" in df.columns:
        chart_data = df["category"].value_counts().reset_index()
        chart_data.columns = ["category","count"]
        st.bar_chart(chart_data.set_index("category"))
    st.markdown("#### Payment Method:")
    if "payment_method" in df.columns:
        st.bar_chart(df["payment_method"].value_counts())
