"""
Translink Transit Dashboard

Run with:
    streamlit run translink/dashboard.py
"""

from datetime import date, timedelta

import pandas as pd
import streamlit as st

import warehouse

st.set_page_config(
    page_title="Translink Transit Dashboard",
    page_icon="🚌",
    layout="wide",
)

st.title("Translink Transit Dashboard")
st.caption("Real-time and historical performance data for Metro Vancouver transit.")


@st.cache_resource
def get_connection():
    return warehouse.connect()


con = get_connection()

realtime_tab, historical_tab = st.tabs(["Real-time", "Historical"])

# ---------------------------------------------------------------------------
# Real-time tab
# ---------------------------------------------------------------------------
with realtime_tab:
    st.subheader("Current Delays by Route")

    today = date.today().strftime("%Y%m%d")

    col1, col2 = st.columns([2, 1])
    with col1:
        mode_filter = st.multiselect(
            "Transit mode",
            options=["Bus", "SkyTrain", "SeaBus", "West Coast Express"],
            default=["Bus", "SkyTrain", "SeaBus", "West Coast Express"],
            key="rt_mode",
        )
    with col2:
        if st.button("Refresh", key="rt_refresh"):
            st.cache_resource.clear()
            st.rerun()

    try:
        rt_df = warehouse.query_realtime_delays(con, today).df()

        if mode_filter:
            rt_df = rt_df[rt_df["transit_mode"].isin(mode_filter)]

        if rt_df.empty:
            st.info("No real-time data available yet for today. Check back after the pipeline has run.")
        else:
            rt_df["avg_delay_min"] = (rt_df["avg_delay_seconds"] / 60).round(1)

            col_a, col_b, col_c = st.columns(3)
            col_a.metric("Routes reporting", len(rt_df))
            col_b.metric(
                "Avg delay across all routes",
                f"{rt_df['avg_delay_min'].mean():.1f} min",
            )
            col_c.metric(
                "Most delayed route",
                rt_df.iloc[0]["route_name"] if not rt_df.empty else "—",
                f"{rt_df.iloc[0]['avg_delay_min']:.1f} min" if not rt_df.empty else "",
            )

            st.bar_chart(
                rt_df.set_index("route_name")[["avg_delay_min"]].head(20),
                use_container_width=True,
            )

            st.dataframe(
                rt_df[["route_id", "route_name", "transit_mode", "avg_delay_min", "snapshot_count"]].rename(
                    columns={
                        "route_id": "Route ID",
                        "route_name": "Route",
                        "transit_mode": "Mode",
                        "avg_delay_min": "Avg Delay (min)",
                        "snapshot_count": "Observations",
                    }
                ),
                use_container_width=True,
                hide_index=True,
            )
    except Exception as e:
        st.error(f"Could not load real-time data: {e}")

# ---------------------------------------------------------------------------
# Historical tab
# ---------------------------------------------------------------------------
with historical_tab:
    st.subheader("Historical Delay Trends")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        start_date = st.date_input("Start date", value=date.today() - timedelta(days=7), key="hist_start")
    with col2:
        end_date = st.date_input("End date", value=date.today(), key="hist_end")
    with col3:
        hist_mode = st.selectbox(
            "Transit mode",
            options=["All", "Bus", "SkyTrain", "SeaBus", "West Coast Express"],
            key="hist_mode",
        )
    with col4:
        try:
            routes_df = warehouse.query_routes(con).df()
            route_options = ["All"] + routes_df["route_name"].tolist()
        except Exception:
            route_options = ["All"]
        hist_route_name = st.selectbox("Route", options=route_options, key="hist_route")

    # Resolve route_name → route_id for the warehouse query, this was made by Claude Code
    hist_route_id = None
    if hist_route_name != "All" and not routes_df.empty:
        match = routes_df[routes_df["route_name"] == hist_route_name]
        if not match.empty:
            hist_route_id = match.iloc[0]["route_id"]

    stop_options = ["All"]
    if hist_route_id:
        try:
            stops_df = warehouse.query_stops(con, hist_route_id).df()
            stop_options = ["All"] + stops_df["stop_name"].tolist()
        except Exception:
            pass

    hist_stop_name = st.selectbox("Stop", options=stop_options, key="hist_stop")
    hist_stop_id = None
    if hist_stop_name != "All" and hist_route_id:
        match = stops_df[stops_df["stop_name"] == hist_stop_name]
        if not match.empty:
            hist_stop_id = match.iloc[0]["stop_id"]

    try:
        hist_df = warehouse.query_historical_delays(
            con,
            start_date=start_date.strftime("%Y%m%d"),
            end_date=end_date.strftime("%Y%m%d"),
            transit_mode=None if hist_mode == "All" else hist_mode,
            route_id=hist_route_id,
            stop_id=hist_stop_id,
        ).df()

        if hist_df.empty:
            st.info("No historical data found for the selected filters.")
        else:
            hist_df["avg_delay_min"] = (hist_df["avg_delay_seconds"] / 60).round(1)

            # Trend line: avg delay per day
            daily = (
                hist_df.groupby("date")["avg_delay_min"]
                .mean()
                .reset_index()
                .rename(columns={"avg_delay_min": "Avg Delay (min)", "date": "Date"})
                .set_index("Date")
            )

            st.line_chart(daily, use_container_width=True)

            st.dataframe(
                hist_df[["date", "route_name", "transit_mode", "stop_name", "avg_delay_min", "observation_count"]].rename(
                    columns={
                        "date": "Date",
                        "route_name": "Route",
                        "transit_mode": "Mode",
                        "stop_name": "Stop",
                        "avg_delay_min": "Avg Delay (min)",
                        "observation_count": "Observations",
                    }
                ),
                use_container_width=True,
                hide_index=True,
            )
    except Exception as e:
        st.error(f"Could not load historical data: {e}")
