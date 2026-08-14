"""Tier 1 — presentation-layer aggregation. Pure pandas: no Spark, no SQL Server.

Guards a defect that produced a chart which was not merely ugly but WRONG.

mart.vTrafficByWeather is at condition x precipitation band x severity grain, so
"Rain" is three rows. The dashboard plotted ConditionName on the x axis without
collapsing them first, and Plotly's default barmode ('relative') STACKS bars
that share an x position. The Rain bar therefore rendered at

    36.7 + 41.2 + 33.0 = 110.9 km/h

- a sum of averages, physically impossible, and taller than the Dry bar at 45.0,
so the chart said rain makes traffic faster. The volume-weighted truth is
37.2 km/h: rain slows traffic by about 17%.

`weighted_by` is the fix, and it is the presentation-side twin of the rule the
mart views already apply in SQL: a measure that is ALREADY an average must be
rolled up as SUM(measure * weight) / SUM(weight), never with a plain mean and
certainly never summed.
"""
import sys
from pathlib import Path

import pytest

# Host tier: the dashboard runs on the host, not in the Spark container, which
# ships a bare Python 3.8 without pandas. importorskip keeps the container run
# green by SKIPPING with a reason rather than failing collection - the same
# "never silently pass, never falsely fail" rule the rest of the suite follows.
pd = pytest.importorskip("pandas", reason="dashboard tests are host-tier (need pandas)")

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dashboard"))

# imported from data_access, NOT app: importing app would execute the whole
# Streamlit script and open a database connection as a side effect
from data_access import weighted_by  # noqa: E402


@pytest.fixture
def weather():
    """The exact shape mart.vTrafficByWeather returns for a 7-day load."""
    return pd.DataFrame({
        "ConditionName": ["Dry", "Rain", "Rain", "Rain"],
        "PrecipBand":    ["None", "Moderate", "Light", "Heavy"],
        "TotalVehicles": [194122, 70164, 17125, 7436],
        "AvgSpeedKmh":   [45.0, 36.7, 41.2, 33.0],
    })


def _speed(df, condition):
    return float(df.loc[df.ConditionName == condition, "AvgSpeedKmh"].iloc[0])


def test_collapses_the_three_rain_bands_into_one_row(weather):
    """Three Rain rows at one x position are what made Plotly stack them."""
    out = weighted_by(weather, "ConditionName", "AvgSpeedKmh", "TotalVehicles")
    assert len(out) == 2
    assert set(out.ConditionName) == {"Dry", "Rain"}


def test_rain_speed_is_volume_weighted_not_summed(weather):
    """The regression guard: 37.2, never 110.9."""
    out = weighted_by(weather, "ConditionName", "AvgSpeedKmh", "TotalVehicles")
    rain = weather[weather.ConditionName == "Rain"]
    expected = (rain.AvgSpeedKmh * rain.TotalVehicles).sum() / rain.TotalVehicles.sum()

    assert _speed(out, "Rain") == pytest.approx(expected, abs=0.05)
    assert _speed(out, "Rain") == pytest.approx(37.2, abs=0.05)
    assert _speed(out, "Rain") != pytest.approx(110.9, abs=0.5), "averages were summed"


def test_weighting_is_not_a_plain_mean():
    """Weighted and plain-mean differ by the weight distribution, not by luck.

    On the real weather data the two land within 0.3 km/h of each other, so that
    data cannot demonstrate the property. Skew the weights hard instead: one
    band carrying essentially all the traffic must dominate the result, while a
    plain mean would let a single-vehicle band drag it 22 km/h off.
    """
    df = pd.DataFrame({
        "ConditionName": ["Rain", "Rain"],
        "TotalVehicles": [1_000_000, 1],
        "AvgSpeedKmh":   [50.0, 5.0],
    })
    out = weighted_by(df, "ConditionName", "AvgSpeedKmh", "TotalVehicles")

    assert _speed(out, "Rain") == pytest.approx(50.0, abs=0.1)
    assert df.AvgSpeedKmh.mean() == pytest.approx(27.5, abs=0.1)  # what a plain mean gives


def test_single_row_group_is_left_untouched(weather):
    """Dry has one band, so weighting must be a no-op."""
    out = weighted_by(weather, "ConditionName", "AvgSpeedKmh", "TotalVehicles")
    assert _speed(out, "Dry") == 45.0


def test_vehicle_totals_are_preserved(weather):
    """Collapsing changes the grain, never the underlying volume."""
    out = weighted_by(weather, "ConditionName", "AvgSpeedKmh", "TotalVehicles")
    assert int(out.TotalVehicles.sum()) == int(weather.TotalVehicles.sum())
    assert int(out.loc[out.ConditionName == "Rain", "TotalVehicles"].iloc[0]) == 94725


def test_rain_is_slower_than_dry(weather):
    """The business fact the chart exists to convey - it read the other way."""
    out = weighted_by(weather, "ConditionName", "AvgSpeedKmh", "TotalVehicles")
    assert _speed(out, "Rain") < _speed(out, "Dry")


def test_zero_weight_group_does_not_divide_by_zero(weather):
    """A condition with no traffic must yield NaN, not an exception.

    Zero-traffic hours are filtered out of the view, but the helper is generic
    and a caller could pass a group whose weights sum to zero.
    """
    df = pd.concat([weather, pd.DataFrame({
        "ConditionName": ["Fog"], "PrecipBand": ["None"],
        "TotalVehicles": [0], "AvgSpeedKmh": [0.0]})], ignore_index=True)

    out = weighted_by(df, "ConditionName", "AvgSpeedKmh", "TotalVehicles")
    fog = out.loc[out.ConditionName == "Fog", "AvgSpeedKmh"].iloc[0]
    assert pd.isna(fog) or fog == 0
    assert _speed(out, "Rain") == pytest.approx(37.2, abs=0.05), "other groups unaffected"
