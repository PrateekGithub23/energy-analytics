import streamlit as st
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

# dashboard
st.set_page_config(
    page_title="Energy Analytics Dashboard",
    layout="wide"
)

# title and description
st.title("⚡ Energy Consumption Analytics")
st.markdown(
    "Analytics dashboard built on top of a Spark-powered data lake pipeline."
)

# load data from data lake
def load_data():
    daily = pd.read_parquet(BASE_DIR / "data-lake/processed/energy/daily_usage")
    hourly = pd.read_parquet(BASE_DIR / "data-lake/processed/energy/hourly_usage")

    raw_files = list((BASE_DIR / "data-lake/raw/energy").glob("date=*/data.json"))
    dfs = []
    for f in raw_files:
        df = pd.read_json(f, lines=True)
        dfs.append(df)

    if dfs:
        raw = pd.concat(dfs, ignore_index=True)
    else:
        raw = pd.DataFrame()


    daily["event_date"] = pd.to_datetime(daily["event_date"])
    hourly["event_date"] = pd.to_datetime(hourly["event_date"])

    return daily, hourly, raw

daily_df, hourly_df, raw_df = load_data()

# check for empty dataframes
raw_available = (
    raw_df is not None
    and not raw_df.empty
    and "sensor_id" in raw_df.columns
)



# sidebar
st.sidebar.subheader("Sensor Filter")
if raw_available:
    sensor_options = ["All Sensors"] + sorted(raw_df["sensor_id"].unique().tolist())
else:
    sensor_options = ["All Sensors"]

selected_sensor = st.sidebar.selectbox(
    "Select Sensor",
    sensor_options
)



st.sidebar.header("Filters")

# Below is the date filtering logic
min_date = daily_df['event_date'].min()
max_date = daily_df['event_date'].max()

date_range = st.sidebar.date_input(
    "Select Date Range",
    value = (min_date, max_date),
    min_value = min_date,
    max_value = max_date
)

start_date = pd.to_datetime(date_range[0])
end_date = pd.to_datetime(date_range[1])

filtered_daily = daily_df[
    (daily_df["event_date"] >= start_date) &
    (daily_df["event_date"] <= end_date)
]

filtered_hourly = hourly_df[
    (hourly_df["event_date"] >= start_date) &
    (hourly_df["event_date"] <= end_date)
]


# Raw sensor data filtering based on selected sensor
if raw_available:
    # convert timestamp
    raw_df["timestamp"] = pd.to_datetime(raw_df["timestamp"])

    raw_filtered = raw_df[
        (raw_df["timestamp"] >= start_date) &
        (raw_df["timestamp"] <= end_date)
    ]

    if selected_sensor != "All Sensors":
        raw_filtered = raw_filtered[raw_filtered["sensor_id"] == selected_sensor]

    raw_filtered["event_data"] = raw_filtered["timestamp"].dt.date
    raw_filtered["event_hour"] = raw_filtered["timestamp"].dt.hour

    daily_sensor = (
        raw_filtered
        .groupby("event_data")["energy_kwh"]
        .sum()
        .reset_index()
        .rename(columns={"energy_kwh": "total_energy_kwh"})
    )

    hourly_sensor = (
        raw_filtered
        .groupby(["event_data", "event_hour"])["energy_kwh"]
        .sum()
        .reset_index()
        .rename(columns={"energy_kwh": "total_energy_kwh"})
    )

else:
    st.info("Raw sensor-level data is not available in the deployed environment.")
    daily_sensor = pd.DataFrame(columns=["event_data", "total_energy_kwh"])
    hourly_sensor = pd.DataFrame(columns=["event_data", "event_hour", "total_energy_kwh"])



# KPI Metrics
st.subheader("📊 Key Metrics")

col1, col2, col3 = st.columns(3)

total_energy = daily_sensor["total_energy_kwh"].sum()
avg_daily = daily_sensor["total_energy_kwh"].mean() if not daily_sensor.empty else 0


# this calculates the peak usage hour, a single value representing the hour of the day (0-23) with highest average energy consumption across the selected date range
peak_hour = (
    hourly_sensor
    .groupby("event_hour")["total_energy_kwh"]
    .mean()
    .idxmax()
    if not hourly_sensor.empty else 0
)


col1.metric("Total Energy (kWh)", f"{total_energy:.2f}")
col2.metric("Avg Daily Energy (kWh)", f"{avg_daily:.2f}")
col3.metric("Peak Usage Hour", f"{peak_hour}:00")


# Daily Energy Consumption Trend
st.subheader("📈 Daily Energy Consumption Trend")

fig1, ax1 = plt.subplots(figsize=(10, 4))
ax1.plot(
    daily_sensor["event_data"],
    daily_sensor["total_energy_kwh"],
    marker="o"
)
ax1.set_xlabel("Date")
ax1.set_ylabel("Energy (kWh)")
ax1.set_title("Daily Energy Usage")
plt.xticks(rotation=45)
st.pyplot(fig1)


# Hourly Average Consumption
st.subheader("📈 Average Hourly Energy Consumption Trend")

hourly_avg = (
    hourly_sensor
    .groupby("event_hour")["total_energy_kwh"]
    .mean()
    .reset_index()
)

fig2, ax2 = plt.subplots(figsize=(10, 4))
sns.barplot(
    data=hourly_avg,
    x="event_hour",
    y="total_energy_kwh",
    ax=ax2
)
ax2.set_xlabel("Hour of Day")
ax2.set_ylabel("Avg Energy (kWh)")
ax2.set_title("Average Hourly Energy Consumption")
st.pyplot(fig2)


# ---------------------------
with st.expander("🔍 View Raw Aggregated Data"):
    st.dataframe(filtered_daily)