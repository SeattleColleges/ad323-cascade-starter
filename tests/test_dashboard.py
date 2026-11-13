"""Week 11 test: the dashboard renders from gold Parquet alone, with the required credit."""

import pytest
from streamlit.testing.v1 import AppTest

from cascade import todo
from conftest import REPO_ROOT, SAMPLE_DIR, land_gold

pytestmark = pytest.mark.week11

APP = REPO_ROOT / "dashboard" / "app.py"


@pytest.fixture(scope="module")
def gold_only_root(tmp_path_factory):
    """Build gold, then delete everything except data/gold to prove the app needs nothing else."""
    import shutil

    todo.raise_if_unfinished([APP])            # still the starter app -> skip
    data_root = tmp_path_factory.mktemp("dash") / "data"
    land_gold(data_root, before_transform=write_stream_windows)
    for item in data_root.iterdir():
        if item.name != "gold":
            shutil.rmtree(item) if item.is_dir() else item.unlink()
    return data_root


def write_stream_windows(data_root):
    """Two 5-minute windows for three real GBFS stations, in the Week 8 consumer's file format.

    Written directly (not by your consumer), so Week 11 never depends on Week 8.
    If your Week 8 dbt models are done, they turn these into fct_station_availability.
    """
    import gzip
    import json
    from datetime import datetime, timedelta

    import duckdb

    with gzip.open(SAMPLE_DIR / "gbfs" / "station_information.json.gz", "rt") as f:
        stations = [s["station_id"] for s in json.load(f)["stations"] if s.get("short_name")][:3]
    out_dir = data_root / "stream" / "station_availability"
    out_dir.mkdir(parents=True, exist_ok=True)
    for i, start in enumerate([datetime(2026, 11, 12, 16, 30), datetime(2026, 11, 12, 16, 35)]):
        rows = [(start, start + timedelta(minutes=5), sid, 5, 2.0 * n, 1.0, 10.0, 0 if n == 0 else 1,
                 5 if n == 0 else 0, n == 0) for n, sid in enumerate(stations)]
        con = duckdb.connect()
        con.execute("""CREATE TABLE w (window_start TIMESTAMP, window_end TIMESTAMP, station_id VARCHAR,
            readings INTEGER, avg_bikes_available DOUBLE, avg_ebikes_available DOUBLE,
            avg_docks_available DOUBLE, min_bikes_available INTEGER, empty_readings INTEGER, is_empty BOOLEAN)""")
        con.executemany("INSERT INTO w VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
        con.execute(f"COPY w TO '{out_dir / f'{start:%Y-%m-%dT%H-%M}.parquet'}' (FORMAT parquet)")
        con.close()


def run_app(data_root, monkeypatch):
    monkeypatch.setenv("CASCADE_DATA_ROOT", str(data_root))
    app = AppTest.from_file(str(APP), default_timeout=60)
    app.run()
    return app


CHART_TYPES = {"vega_lite_chart", "arrow_vega_lite_chart", "plotly_chart", "pyplot", "graphviz_chart",
               "deck_gl_json_chart", "map"}


def charts(app):
    """Every chart element on the page (st.line_chart, st.bar_chart, st.altair_chart, ...)."""
    return [e for chart_type in CHART_TYPES for e in app.get(chart_type)]


def test_dashboard_renders_from_gold_only(gold_only_root, monkeypatch):
    app = run_app(gold_only_root, monkeypatch)
    assert not app.exception
    trips = [m for m in app.metric if m.label == "Trips"]
    assert trips and trips[0].value == "97,626"                  # valid trips, both sample months


def test_dashboard_has_at_least_three_charts(gold_only_root, monkeypatch):
    app = run_app(gold_only_root, monkeypatch)
    assert len(charts(app)) >= 3


def test_availability_data_does_not_break_the_app(gold_only_root, monkeypatch):
    if not (gold_only_root / "gold" / "fct_station_availability.parquet").exists():
        pytest.skip("no fct_station_availability in gold (Week 8 dbt models not done)")
    app = run_app(gold_only_root, monkeypatch)
    assert not app.exception


def test_data_credits_are_shown(gold_only_root, monkeypatch):
    app = run_app(gold_only_root, monkeypatch)
    text = " ".join(c.value for c in app.caption) + " ".join(m.value for m in app.markdown)
    assert "Weather data by Open-Meteo.com" in text
    assert "Divvy" in text


def test_missing_gold_gives_a_helpful_message(tmp_path, monkeypatch):
    todo.raise_if_unfinished([APP])
    app = run_app(tmp_path / "empty", monkeypatch)
    assert app.error
