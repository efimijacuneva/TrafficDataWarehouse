"""Synthetic source-data generator for the Smart City Traffic platform.

Produces the three raw feeds the Spark pipeline ingests, with realistic
behaviour AND deliberate data-quality problems (to exercise the silver gate):

  data/raw/sensor_readings_YYYYMMDD.csv   — detector telemetry
  data/raw/camera_events_YYYYMMDD.json    — nested ANPR camera events
  data/raw/weather_observations_YYYYMMDD.json

Realism:
  * diurnal volume curve with AM/PM rush peaks, weekend damping
  * speed inversely correlated with volume (congestion), weather penalty
  * speed generated RELATIVE TO EACH SEGMENT's posted limit and road category,
    so the congestion index ranks local streets above arterials above highway
  * heavy-vehicle share higher at night
Injected defects (~1.5% of rows): missing keys, speed=999, occupancy>100,
duplicated event_ids, whitespace/sentinel strings.

Usage:
  python data_generator/generate_data.py --days 7 --sensors 200 --start 2026-06-01 --seed 42
"""
from __future__ import annotations  # keeps list[dict] importable on Python 3.8
# ^ the generator itself targets 3.10+, but the Spark container ships 3.8 and the
#   test suite imports this module there. Deferring annotation evaluation costs
#   nothing and makes the module import-safe on both.

import argparse
import csv
import json
import random
from datetime import date, datetime, timedelta
from pathlib import Path

RAW_DIR = Path(__file__).resolve().parents[1] / "data" / "raw"

VEHICLE_TYPES = ["CAR"] * 70 + ["VAN"] * 8 + ["TRK"] * 6 + ["ART"] * 2 + \
                ["BUS"] * 6 + ["MBS"] * 2 + ["MCY"] * 5 + ["BIC"] * 1
DIRECTIONS = ["NB", "SB", "EB", "WB"]
CONDITIONS = ["DRY"] * 70 + ["RAIN"] * 18 + ["FOG"] * 6 + ["SNOW"] * 4 + ["STORM"] * 2
STATIONS = ["WS-01", "WS-02", "WS-03"]

# hourly demand profile (weekday): index = hour, value = relative intensity
WEEKDAY_PROFILE = [0.06, 0.04, 0.03, 0.03, 0.05, 0.15, 0.45, 0.90, 1.00, 0.70,
                   0.55, 0.55, 0.60, 0.60, 0.60, 0.70, 0.90, 1.00, 0.85, 0.60,
                   0.45, 0.30, 0.20, 0.10]
WEEKEND_DAMPING = 0.55

CONDITION_SPEED_PENALTY = {"DRY": 0.0, "RAIN": 0.12, "FOG": 0.20, "SNOW": 0.30, "STORM": 0.25}


def seg_code(i: int) -> str:
    return f"SEG-{i:03d}"


def sensor_serial(i: int) -> str:
    return f"SNS-{i:04d}"


# --------------------------------------------------------------- road model --
# MUST mirror oltp.Road / oltp.RoadSegment in sql/oltp/03_sample_data.sql:
# 120 segments, 12 per road, roads 3 and 10 highway, road 7 a local street.
# Generating speed WITHOUT this mapping is what made the congestion index rank
# road categories backwards - free flow was assigned by segment index, so the
# 30 km/h local street was handed a 55 km/h free flow (traffic "never"
# congested, and habitually 135% of the posted limit) while the 100 km/h
# highway also got 55 (permanently "congested"). Speed must be generated
# relative to the road it happens on.
ROAD_CATEGORY = {1: "Arterial", 2: "Arterial", 3: "Highway",  4: "Arterial",
                 5: "Collector", 6: "Arterial", 7: "Local",   8: "Collector",
                 9: "Arterial", 10: "Highway"}

# free_flow_ratio: empty-road speed as a fraction of the posted limit - drivers
#   habitually exceed it slightly, and the margin is widest where enforcement is
#   loosest and the road is straightest.
# sensitivity:     how much speed is lost at peak demand. Grade-separated
#   highway holds its speed until capacity; a local street loses most of it to
#   signals, parked cars and pedestrians. This ordering is what makes local
#   streets the most congested and the highway the least - as in a real city.
ROAD_BEHAVIOUR = {
    "Highway":   {"free_flow_ratio": 1.08, "sensitivity": 0.30},
    "Arterial":  {"free_flow_ratio": 1.06, "sensitivity": 0.45},
    "Collector": {"free_flow_ratio": 1.04, "sensitivity": 0.52},
    "Local":     {"free_flow_ratio": 1.02, "sensitivity": 0.62},
}


def segment_profile(segment_i: int) -> tuple[str, int, float, float]:
    """(category, posted limit, free-flow speed, congestion sensitivity)."""
    road = 1 + (segment_i - 1) // 12                     # road 1..10
    category = ROAD_CATEGORY[road]
    if category == "Highway":
        limit = 100
    elif road == 7:
        limit = 30
    else:
        limit = 50 + 10 * (segment_i % 2)
    behaviour = ROAD_BEHAVIOUR[category]
    return (category, limit,
            limit * behaviour["free_flow_ratio"], behaviour["sensitivity"])


def daily_weather(rng: random.Random, day: date) -> list[dict]:
    """Hourly observations per station; one condition regime per day."""
    condition = rng.choice(CONDITIONS)
    base_temp = rng.uniform(8, 24) - (6 if condition in ("SNOW", "STORM") else 0)
    rows = []
    for hour in range(24):
        temp = round(base_temp + 6 * (1 - abs(hour - 14) / 14) + rng.uniform(-1, 1), 1)
        precip = round(rng.uniform(0.5, 8.0), 1) if condition in ("RAIN", "SNOW", "STORM") else 0.0
        vis = rng.randint(200, 800) if condition == "FOG" else rng.randint(4000, 10000)
        for station in STATIONS:
            rows.append({
                "station_code": station,
                "observed_at": f"{day}T{hour:02d}:00:00",
                "temperature_c": temp + rng.uniform(-0.5, 0.5),
                "precipitation_mm": precip,
                "wind_speed_kmh": round(rng.uniform(2, 35), 1),
                "visibility_m": vis,
                "condition_code": condition,
            })
    return rows


def generate_day(rng: random.Random, day: date, n_sensors: int,
                 events_scale: int, defect_rate: float) -> tuple[int, int]:
    stamp = day.strftime("%Y%m%d")
    is_weekend = day.weekday() >= 5
    weather = daily_weather(rng, day)
    day_condition = weather[0]["condition_code"]
    speed_penalty = CONDITION_SPEED_PENALTY.get(day_condition, 0.1)

    csv_path = RAW_DIR / f"sensor_readings_{stamp}.csv"
    cam_path = RAW_DIR / f"camera_events_{stamp}.json"
    met_path = RAW_DIR / f"weather_observations_{stamp}.json"

    n_csv = 0
    camera_events = []

    with open(csv_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["event_id", "event_ts", "sensor_serial", "segment_code",
                         "direction", "vehicle_type_code", "speed_kmh",
                         "headway_seconds", "occupancy_pct"])
        for hour in range(24):
            intensity = WEEKDAY_PROFILE[hour] * (WEEKEND_DAMPING if is_weekend else 1.0)
            n_events = int(events_scale * intensity)
            for _ in range(n_events):
                sensor_i = rng.randint(1, n_sensors)
                segment_i = 1 + (sensor_i - 1) % 120
                ts = datetime(day.year, day.month, day.day, hour,
                              rng.randint(0, 59), rng.randint(0, 59),
                              rng.randint(0, 999) * 1000)
                event_id = f"EVT-{stamp}-{n_csv + 1:07d}"
                vt = rng.choice(VEHICLE_TYPES)
                # congestion: high intensity → low speed; weather penalty on top.
                # Free flow and sensitivity both come from the SEGMENT's own
                # road, so speed is always meaningful against its posted limit.
                _, _, free_flow, sensitivity = segment_profile(segment_i)
                # spread scales with speed: a 100 km/h road varies far more in
                # absolute terms than a 30 km/h street, and a fixed sigma would
                # push a congested local street into the 5 km/h floor.
                speed = max(5.0, rng.gauss(
                    free_flow * (1 - sensitivity * intensity) * (1 - speed_penalty),
                    0.12 * free_flow))
                occupancy = min(98.0, max(1.0, 90 * intensity + rng.uniform(-10, 10)))
                headway = max(0.4, rng.expovariate(intensity * 0.9 + 0.1))

                row = [event_id, ts.isoformat(timespec="milliseconds"),
                       sensor_serial(sensor_i), seg_code(segment_i),
                       rng.choice(DIRECTIONS), vt, round(speed, 1),
                       round(headway, 2), round(occupancy, 2)]

                # ---- defect injection --------------------------------------
                roll = rng.random()
                if roll < defect_rate:
                    defect = rng.choice(["null_sensor", "bad_speed", "bad_occ",
                                         "dup", "sentinel", "future_ts"])
                    if defect == "null_sensor":
                        row[2] = ""
                    elif defect == "bad_speed":
                        row[6] = 999.0
                    elif defect == "bad_occ":
                        row[8] = 150.0
                    elif defect == "sentinel":
                        row[3] = "N/A"
                    elif defect == "future_ts":
                        row[1] = (ts + timedelta(days=365)).isoformat(timespec="milliseconds")
                    elif defect == "dup":
                        writer.writerow(row)  # duplicate event_id, same payload
                        n_csv += 1
                writer.writerow(row)
                n_csv += 1

    # camera events ≈ 5% extra detections, nested JSON shape
    n_cam = max(50, n_csv // 20)
    for i in range(n_cam):
        hour = rng.choices(range(24), weights=WEEKDAY_PROFILE)[0]
        segment_i = rng.randint(1, 120)
        ts = datetime(day.year, day.month, day.day, hour, rng.randint(0, 59), rng.randint(0, 59))
        # ANPR speed follows the same road model as the sensors - a camera on
        # the airport corridor must not report the same 48 km/h as one on a
        # 30 km/h side street, or SpeedOverLimitKmh is meaningless for cameras.
        cam_intensity = WEEKDAY_PROFILE[hour] * (WEEKEND_DAMPING if is_weekend else 1.0)
        _, _, cam_free_flow, cam_sensitivity = segment_profile(segment_i)
        cam_speed = max(5.0, rng.gauss(
            cam_free_flow * (1 - cam_sensitivity * cam_intensity) * (1 - speed_penalty),
            0.12 * cam_free_flow))
        camera_events.append({
            "event_id": f"CAM-{stamp}-{i + 1:06d}",
            "camera_code": f"CAM-{rng.randint(1, 50):03d}",
            "detection": {
                "timestamp": ts.isoformat(),
                "segment_code": seg_code(segment_i),
                "direction": rng.choice(DIRECTIONS),
                "vehicle_class": rng.choice(VEHICLE_TYPES),
                "speed_kmh": round(cam_speed, 1),
                "plate_hash": f"{rng.getrandbits(128):032x}",
            },
        })

    with open(cam_path, "w", encoding="utf-8") as fh:
        json.dump(camera_events, fh, indent=1)
    with open(met_path, "w", encoding="utf-8") as fh:
        json.dump(weather, fh, indent=1)

    return n_csv, n_cam


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--sensors", type=int, default=200)
    parser.add_argument("--start", default="2026-06-01")
    parser.add_argument("--events-per-hour", type=int, default=4000,
                        help="events at peak intensity 1.0 (scale factor)")
    parser.add_argument("--defect-rate", type=float, default=0.015)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    rng = random.Random(args.seed)
    start = date.fromisoformat(args.start)

    manifest = []
    for offset in range(args.days):
        day = start + timedelta(days=offset)
        n_csv, n_cam = generate_day(rng, day, args.sensors,
                                    args.events_per_hour, args.defect_rate)
        manifest.append({"date": str(day), "sensor_csv_rows": n_csv, "camera_events": n_cam})
        print(f"{day}: {n_csv:,} sensor rows, {n_cam:,} camera events")

    with open(RAW_DIR / "_manifest.json", "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2)
    print(f"\nDone -> {RAW_DIR}  (manifest written for reconciliation tests)")


if __name__ == "__main__":
    main()
