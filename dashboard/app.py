"""Week 11: Cascade Mobility dashboard.

    uv run streamlit run dashboard/app.py          # or: uv run python -m cascade.demo

Reads ONLY the exported gold Parquet files in <data_root>/gold/ (default data/).
It never opens warehouse.duckdb, so it can't collide with a pipeline run.

What's here is a starting point: it reads one gold file and shows one number.
TODO(week11): build the dashboard. Delete this TODO line when it is done. The
Week 11 tests then check that it:
  * renders from data/gold alone, with no errors,
  * shows a metric labeled "Trips" with the number of valid trips (97,626 on the sample),
  * has at least three charts,
  * still shows the data credits at the bottom (required by both licenses),
  * shows an error message, not a crash, when there is no gold data yet.
Ideas: trips per day by rider type, top stations, the weather effect, and the
Week 8 station availability (gold/fct_station_availability.parquet) if you have it.
Write down what each metric means.
"""

import os
from pathlib import Path

import duckdb
import streamlit as st

GOLD = Path(os.environ.get("CASCADE_DATA_ROOT", "data")) / "gold"

st.set_page_config(page_title="Cascade Mobility", page_icon="🚲", layout="wide")
st.title("Cascade Mobility: bike share dashboard")

if not (GOLD / "fct_trips.parquet").exists():
    st.error(f"No gold data in `{GOLD}`. Build it first: `uv run python -m cascade.demo`")
    st.stop()


@st.cache_data
def query(sql: str):
    """Run SQL on the gold Parquet files and return a table (cached between reruns).

    Every file in data/gold becomes a view named after it, e.g. fct_trips, dim_station.
    """
    con = duckdb.connect()
    for table in GOLD.glob("*.parquet"):
        con.execute(f"CREATE VIEW {table.stem} AS SELECT * FROM read_parquet('{table}')")
    return con.sql(sql).df()


# ----- WORKED EXAMPLE: one number from one gold file ------------------------------------

trips = query("SELECT count(*) AS trips FROM fct_trips").iloc[0]["trips"]
st.metric("Trips", f"{int(trips):,}")
st.caption("Trips: valid trips in fct_trips (one row per trip).")

st.subheader("A first look at fct_trips")
st.dataframe(query("SELECT * FROM fct_trips LIMIT 20"))

# ----- Credits (keep these) -----------------------------------------------------------------

st.divider()
st.caption("Trip data: Divvy / Lyft Bikes and Scooters, LLC, under the Divvy Data License Agreement. "
           "[Weather data by Open-Meteo.com](https://open-meteo.com/) (CC BY 4.0).")
