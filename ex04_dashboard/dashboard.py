import streamlit as st
import pandas as pd
import sqlalchemy as sa
import plotly.express as px

# -----------------------------
# CONFIG
# -----------------------------
st.set_page_config(page_title="Statistiques Globales", layout="wide")
st.title("Statistiques Globales")

# -----------------------------
# DB CONNECTION
# -----------------------------
engine = sa.create_engine(
    "postgresql://mon_user:mon_password@localhost:5432/ma_base"
)

# -----------------------------
# FILTERS
# -----------------------------
with engine.connect() as conn:
    boroughs = pd.read_sql(
        "SELECT DISTINCT borough_name FROM dim_zones ORDER BY borough_name", conn
    )
    vendors = pd.read_sql(
        "SELECT DISTINCT vendor_id FROM fact_trips ORDER BY vendor_id", conn
    )

col1, col2 = st.columns(2)

with col1:
    selected_borough = st.selectbox(
        "Arrondissement", ["Tous"] + boroughs["borough_name"].tolist()
    )

with col2:
    selected_vendor = st.selectbox(
        "Vendor", ["Tous"] + vendors["vendor_id"].astype(str).tolist()
    )

# -----------------------------
# WHERE CLAUSE
# -----------------------------
filters = []

if selected_borough != "Tous":
    filters.append(f"z.borough_name = '{selected_borough}'")

if selected_vendor != "Tous":
    filters.append(f"f.vendor_id = {selected_vendor}")

where_clause = ""
if filters:
    where_clause = "WHERE " + " AND ".join(filters)

# -----------------------------
# KPI QUERY
# -----------------------------
kpi_query = f"""
SELECT
    COUNT(*) AS total_trips,
    SUM(f.total_amount) AS total_revenue,
    COUNT(DISTINCT f.pickup_location_id) AS total_zones,
    SUM(f.trip_distance) AS total_distance
FROM fact_trips f
JOIN dim_zones z ON f.pickup_location_id = z.location_id
{where_clause}
"""

with engine.connect() as conn:
    kpi = pd.read_sql(kpi_query, conn).iloc[0]

# -----------------------------
# KPI DISPLAY
# -----------------------------
k1, k2, k3, k4 = st.columns(4)

k1.metric("Courses totales", f"{int(kpi.total_trips):,}")
k2.metric("Dollars gagnés", f"${kpi.total_revenue:,.2f}")
k3.metric("Zones couvertes", int(kpi.total_zones))
k4.metric("Distance totale (miles)", f"{kpi.total_distance:,.2f}")

st.divider()

# -----------------------------
# HOURLY PROFIT (PLOTLY)
# -----------------------------
hourly_query = f"""
SELECT
    t.hour,
    SUM(f.total_amount) AS profit
FROM fact_trips f
JOIN dim_time t ON f.pickup_time_key = t.time_key
JOIN dim_zones z ON f.pickup_location_id = z.location_id
{where_clause}
GROUP BY t.hour
ORDER BY t.hour
"""

with engine.connect() as conn:
    hourly = pd.read_sql(hourly_query, conn)

# -----------------------------
# HISTOGRAM
# -----------------------------
fig = px.bar(
    hourly,
    x="hour",
    y="profit",
    labels={
        "hour": "Heure de la journée",
        "profit": "Profit ($)"
    },
    title="Profit par heure"
)

fig.update_layout(
    xaxis=dict(dtick=1),
    bargap=0.1
)

st.plotly_chart(fig, use_container_width=True)
