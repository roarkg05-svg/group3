"""Deterministic raw data generator for NYC Citi Bike trips, GBFS stations, and NOAA weather."""

import csv
import json
import math
import os
import random
from datetime import datetime, timedelta
from typing import Any, Dict, List, Tuple


NYC_STREETS_AND_AVENUES: List[Tuple[str, str, float, float]] = [
    ("W 21 St & 6 Ave", "MANHATTAN_MIDTOWN", 40.7417, -73.9942),
    ("Broadway & E 14 St", "MANHATTAN_DOWNTOWN", 40.7345, -73.9907),
    ("Central Park S & 6 Ave", "MANHATTAN_MIDTOWN", 40.7659, -73.9763),
    ("Lafayette St & Jersey St", "MANHATTAN_DOWNTOWN", 40.7243, -73.9960),
    ("E 47 St & Park Ave", "MANHATTAN_MIDTOWN", 40.7551, -73.9749),
    ("8 Ave & W 31 St", "MANHATTAN_MIDTOWN", 40.7506, -73.9947),
    ("Vesey Pl & River Terrace", "MANHATTAN_DOWNTOWN", 40.7153, -74.0166),
    ("1 Ave & E 68 St", "MANHATTAN_UPTOWN", 40.7650, -73.9582),
    ("Amsterdam Ave & W 79 St", "MANHATTAN_UPTOWN", 40.7829, -73.9786),
    ("Columbus Ave & W 95 St", "MANHATTAN_UPTOWN", 40.7919, -73.9681),
    ("Bedford Ave & N 7 St", "BROOKLYN_NORTH", 40.7173, -73.9568),
    ("Wythe Ave & Metropolitan Ave", "BROOKLYN_NORTH", 40.7169, -73.9632),
    ("Atlantic Ave & Furman St", "BROOKLYN_DOWNTOWN", 40.6917, -73.9999),
    ("Jay St & Tech Pl", "BROOKLYN_DOWNTOWN", 40.6951, -73.9865),
    ("Flatbush Ave & Hanson Pl", "BROOKLYN_DOWNTOWN", 40.6841, -73.9766),
    ("Vernon Blvd & 50 Ave", "QUEENS_WEST", 40.7423, -73.9541),
    ("Queens Plaza North & Crescent St", "QUEENS_WEST", 40.7512, -73.9403),
    ("44 Dr & Jackson Ave", "QUEENS_WEST", 40.7470, -73.9433),
    ("Hudson St & Reade St", "MANHATTAN_DOWNTOWN", 40.7163, -74.0091),
    ("Lexington Ave & E 29 St", "MANHATTAN_MIDTOWN", 40.7431, -73.9821),
]


def _haversine_km(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Calculate great-circle distance in kilometers between two coordinate pairs."""
    radius = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlng = math.radians(lng2 - lng1)
    a = (
        math.sin(dlat / 2.0) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(dlng / 2.0) ** 2
    )
    a = min(1.0, max(0.0, a))
    return 2.0 * radius * math.asin(math.sqrt(a))


def _generate_stations(rng: random.Random, num_stations: int = 120) -> List[Dict[str, Any]]:
    """Deterministically generate 120 realistic NYC GBFS stations."""
    stations: List[Dict[str, Any]] = []
    for idx in range(1, num_stations + 1):
        base_name, region_id, base_lat, base_lng = NYC_STREETS_AND_AVENUES[
            (idx - 1) % len(NYC_STREETS_AND_AVENUES)
        ]
        sector_num = (idx - 1) // len(NYC_STREETS_AND_AVENUES) + 1
        station_name = base_name if sector_num == 1 else f"{base_name} #{sector_num}"

        lat_jitter = rng.uniform(-0.012, 0.012)
        lng_jitter = rng.uniform(-0.012, 0.012)
        station_lat = round(min(40.86, max(40.66, base_lat + lat_jitter)), 6)
        station_lng = round(min(-73.91, max(-74.02, base_lng + lng_jitter)), 6)

        stations.append(
            {
                "station_id": f"ST_{idx:04d}",
                "station_name": station_name,
                "capacity": rng.randint(15, 60),
                "region_id": region_id,
                "station_lat": station_lat,
                "station_lng": station_lng,
                "is_active": True if idx <= 116 else rng.choice([True, False]),
            }
        )
    return stations


def _generate_weather(
    rng: random.Random, start_dt: datetime, total_hours: int = 240
) -> List[Dict[str, Any]]:
    """Deterministically generate hourly NOAA weather records covering the full trip window."""
    weather_rows: List[Dict[str, Any]] = []
    conditions_cycle = [
        ("Clear", 0.0, 0.0, 14.0, 21.0, 6.0, 16.0),
        ("Partly Cloudy", 0.0, 0.0, 12.0, 19.0, 8.0, 20.0),
        ("Cloudy", 0.0, 0.1, 10.0, 16.0, 10.0, 24.0),
        ("Light Rain", 0.6, 4.2, 9.0, 14.0, 15.0, 30.0),
        ("Heavy Rain", 5.5, 18.0, 7.5, 12.0, 22.0, 42.0),
    ]
    weights = [0.42, 0.26, 0.16, 0.11, 0.05]

    for hour_offset in range(total_hours):
        current_dt = start_dt + timedelta(hours=hour_offset)
        cond_info = rng.choices(conditions_cycle, weights=weights, k=1)[0]
        cond_name, p_min, p_max, t_min, t_max, w_min, w_max = cond_info

        # Diurnal temperature modulation
        diurnal = math.sin((current_dt.hour - 6) * math.pi / 12.0) * 2.5
        temp_c = round(rng.uniform(t_min, t_max) + diurnal, 1)
        precip_mm = 0.0 if p_max == 0.0 else round(rng.uniform(p_min, p_max), 2)
        wind_kmh = round(rng.uniform(w_min, w_max), 1)

        weather_rows.append(
            {
                "weather_timestamp": current_dt.strftime("%Y-%m-%d %H:%M:%S"),
                "temp_c": temp_c,
                "precipitation_mm": precip_mm,
                "wind_kmh": wind_kmh,
                "condition": cond_name,
            }
        )
    return weather_rows


def generate_raw_batches(
    raw_dir: str, num_trips: int = 35000, seed: int = 42
) -> Dict[str, Any]:
    """Deterministically generate raw stations JSON, NOAA hourly weather CSV, and 2 trip CSV batches.

    Injects controlled proportions (~9% total) of all 7 data quality anomalies, duplicate ride_ids,
    missing end coordinates (0.0, 0.0) for coordinate imputation, and orphan start_station_ids.
    """
    rng = random.Random(seed)

    stations_dir = os.path.join(raw_dir, "stations")
    weather_dir = os.path.join(raw_dir, "weather")
    trips_dir = os.path.join(raw_dir, "trips")
    os.makedirs(stations_dir, exist_ok=True)
    os.makedirs(weather_dir, exist_ok=True)
    os.makedirs(trips_dir, exist_ok=True)

    # 1. Generate 120 GBFS stations
    stations = _generate_stations(rng, num_stations=120)
    stations_path = os.path.join(stations_dir, "gbfs_stations.json")
    with open(stations_path, "w", encoding="utf-8") as f_st:
        json.dump(stations, f_st, indent=2)

    # 2. Generate NOAA hourly weather covering 2026-10-01 through 2026-10-10 (240 hours)
    base_start_dt = datetime(2026, 10, 1, 0, 0, 0)
    weather_rows = _generate_weather(rng, start_dt=base_start_dt, total_hours=240)
    weather_path = os.path.join(weather_dir, "noaa_hourly_weather.csv")
    weather_fields = [
        "weather_timestamp",
        "temp_c",
        "precipitation_mm",
        "wind_kmh",
        "condition",
    ]
    with open(weather_path, "w", newline="", encoding="utf-8") as f_wx:
        writer = csv.DictWriter(f_wx, fieldnames=weather_fields)
        writer.writeheader()
        writer.writerows(weather_rows)

    # 3. Determine controlled anomaly & edge-case counts
    n_Duplicates = max(4, int(num_trips * 0.015))
    n_unique_rows = num_trips - n_Duplicates

    n_rule1_missing_key = max(3, int(num_trips * 0.012))
    n_rule2_neg_zero_dur = max(3, int(num_trips * 0.012))
    n_rule3_false_unlock = max(3, int(num_trips * 0.014))
    n_rule4_over_24h = max(3, int(num_trips * 0.011))
    n_rule5_circular = max(3, int(num_trips * 0.013))
    n_rule6_speed = max(3, int(num_trips * 0.014))
    n_rule7_bounds = max(3, int(num_trips * 0.014))

    total_anomalies = (
        n_rule1_missing_key
        + n_rule2_neg_zero_dur
        + n_rule3_false_unlock
        + n_rule4_over_24h
        + n_rule5_circular
        + n_rule6_speed
        + n_rule7_bounds
    )

    n_imputed_coords = max(5, int(num_trips * 0.030))
    n_orphan_stations = max(4, int(num_trips * 0.015))
    n_standard_valid = max(
        10, n_unique_rows - total_anomalies - n_imputed_coords - n_orphan_stations
    )

    orphan_station_ids = ["ST_9901", "ST_9902", "ST_9903", "ST_9904", "ST_9905"]
    residential_stations = [s for s in stations if "UPTOWN" in s["region_id"] or "BROOKLYN" in s["region_id"] or "QUEENS" in s["region_id"]]
    commercial_stations = [s for s in stations if "MIDTOWN" in s["region_id"] or "DOWNTOWN" in s["region_id"]]

    def _random_trip_start(day_min: int = 0, day_max: int = 6) -> datetime:
        day_offset = rng.randint(day_min, day_max)
        # Realistic commute peak weighting
        hour_choices = list(range(24))
        hour_weights = [
            1, 1, 1, 1, 1, 2, 5, 12, 16, 10, 6, 6,
            7, 7, 7, 9, 14, 18, 15, 10, 6, 4, 3, 2,
        ]
        hour = rng.choices(hour_choices, weights=hour_weights, k=1)[0]
        minute = rng.randint(0, 59)
        second = rng.randint(0, 59)
        return base_start_dt + timedelta(
            days=day_offset, hours=hour, minutes=minute, seconds=second
        )

    def _pick_distinct_stations(hour: int) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        if 7 <= hour <= 9 and rng.random() < 0.70:
            s_start = rng.choice(residential_stations)
            s_end = rng.choice(commercial_stations)
        elif 16 <= hour <= 19 and rng.random() < 0.70:
            s_start = rng.choice(commercial_stations)
            s_end = rng.choice(residential_stations)
        else:
            s_start = rng.choice(stations)
            s_end = rng.choice(stations)

        while s_end["station_id"] == s_start["station_id"]:
            s_end = rng.choice(stations)
        return s_start, s_end

    def _build_valid_trip(ride_idx: int, impute_end: bool = False, orphan_start: bool = False) -> Dict[str, Any]:
        started_dt = _random_trip_start()
        s_start, s_end = _pick_distinct_stations(started_dt.hour)

        start_lat = s_start["station_lat"]
        start_lng = s_start["station_lng"]
        true_end_lat = s_end["station_lat"]
        true_end_lng = s_end["station_lng"]

        dist_km = _haversine_km(start_lat, start_lng, true_end_lat, true_end_lng)
        # Ensure speed is strictly realistic (8 to 20 km/h) and duration >= 4.5 min
        target_speed_kmh = rng.uniform(9.0, 19.5)
        min_required_min = max(4.5, (dist_km / target_speed_kmh) * 60.0)
        duration_sec = int(round(min(180.0, min_required_min + rng.uniform(1.0, 15.0)) * 60))
        ended_dt = started_dt + timedelta(seconds=duration_sec)

        rideable_type = rng.choices(["classic_bike", "electric_bike"], weights=[0.58, 0.42], k=1)[0]
        member_casual = rng.choices(["member", "casual"], weights=[0.74, 0.26], k=1)[0]

        return {
            "ride_id": f"RIDE_{ride_idx:07d}",
            "bike_id": f"BIKE_{rng.randint(1000, 9999)}",
            "rideable_type": rideable_type,
            "started_at": started_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "ended_at": ended_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "start_station_id": rng.choice(orphan_station_ids) if orphan_start else s_start["station_id"],
            "end_station_id": s_end["station_id"],
            "start_lat": f"{start_lat:.6f}",
            "start_lng": f"{start_lng:.6f}",
            "end_lat": "0.0" if impute_end else f"{true_end_lat:.6f}",
            "end_lng": "0.0" if impute_end else f"{true_end_lng:.6f}",
            "member_casual": member_casual,
        }

    all_unique_trips: List[Dict[str, Any]] = []
    ride_counter = 1

    # A. Standard valid trips
    for _ in range(n_standard_valid):
        all_unique_trips.append(_build_valid_trip(ride_counter, impute_end=False, orphan_start=False))
        ride_counter += 1

    # B. Valid trips with missing end coordinates (0.0, 0.0) for Silver coordinate imputation
    for _ in range(n_imputed_coords):
        all_unique_trips.append(_build_valid_trip(ride_counter, impute_end=True, orphan_start=False))
        ride_counter += 1

    # C. Valid trips with orphan start_station_id for Silver enrichment conformance testing
    for _ in range(n_orphan_stations):
        all_unique_trips.append(_build_valid_trip(ride_counter, impute_end=False, orphan_start=True))
        ride_counter += 1

    # D. Rule 1: MISSING_CRITICAL_KEY (null/empty start_station_id, ended_at, started_at, or ride_id)
    for i in range(n_rule1_missing_key):
        row = _build_valid_trip(ride_counter)
        ride_counter += 1
        key_choice = i % 4
        if key_choice == 0:
            row["start_station_id"] = ""
        elif key_choice == 1:
            row["ended_at"] = ""
        elif key_choice == 2:
            row["started_at"] = ""
        else:
            row["ride_id"] = ""
        all_unique_trips.append(row)

    # E. Rule 2: NEGATIVE_OR_ZERO_DURATION (trip_duration_min <= 0)
    for i in range(n_rule2_neg_zero_dur):
        row = _build_valid_trip(ride_counter)
        ride_counter += 1
        started_dt = datetime.strptime(row["started_at"], "%Y-%m-%d %H:%M:%S")
        if i % 3 == 0:
            ended_dt = started_dt  # Zero duration
        else:
            ended_dt = started_dt - timedelta(minutes=rng.randint(2, 45))  # Negative duration
        row["ended_at"] = ended_dt.strftime("%Y-%m-%d %H:%M:%S")
        all_unique_trips.append(row)

    # F. Rule 3: FALSE_UNLOCK_UNDER_60S (0 < trip_duration_min < 1.0)
    for _ in range(n_rule3_false_unlock):
        row = _build_valid_trip(ride_counter)
        ride_counter += 1
        started_dt = datetime.strptime(row["started_at"], "%Y-%m-%d %H:%M:%S")
        ended_dt = started_dt + timedelta(seconds=rng.randint(10, 50))
        # Set end coords equal to start coords so speed is 0 km/h (only violates Rule 3)
        row["ended_at"] = ended_dt.strftime("%Y-%m-%d %H:%M:%S")
        row["end_lat"] = row["start_lat"]
        row["end_lng"] = row["start_lng"]
        all_unique_trips.append(row)

    # G. Rule 4: LOST_OR_STOLEN_OVER_24H (trip_duration_min > 1440.0)
    for _ in range(n_rule4_over_24h):
        row = _build_valid_trip(ride_counter)
        ride_counter += 1
        started_dt = datetime.strptime(row["started_at"], "%Y-%m-%d %H:%M:%S")
        ended_dt = started_dt + timedelta(hours=rng.randint(26, 72), minutes=rng.randint(5, 55))
        row["ended_at"] = ended_dt.strftime("%Y-%m-%d %H:%M:%S")
        all_unique_trips.append(row)

    # H. Rule 5: CIRCULAR_ABANDONED_TRIP (same station, 1.0 <= duration < 3.0 min, distance < 0.05 km)
    for _ in range(n_rule5_circular):
        row = _build_valid_trip(ride_counter)
        ride_counter += 1
        started_dt = datetime.strptime(row["started_at"], "%Y-%m-%d %H:%M:%S")
        ended_dt = started_dt + timedelta(seconds=rng.randint(70, 165))
        row["ended_at"] = ended_dt.strftime("%Y-%m-%d %H:%M:%S")
        row["end_station_id"] = row["start_station_id"]
        row["end_lat"] = row["start_lat"]
        row["end_lng"] = row["start_lng"]
        all_unique_trips.append(row)

    # I. Rule 6: IMPOSSIBLE_PHYSICAL_SPEED (>45 km/h classic or >55 km/h electric)
    for i in range(n_rule6_speed):
        row = _build_valid_trip(ride_counter)
        ride_counter += 1
        started_dt = datetime.strptime(row["started_at"], "%Y-%m-%d %H:%M:%S")
        # 4 minutes duration over ~9 km distance -> ~135 km/h
        ended_dt = started_dt + timedelta(minutes=4)
        row["ended_at"] = ended_dt.strftime("%Y-%m-%d %H:%M:%S")
        row["rideable_type"] = "classic_bike" if i % 2 == 0 else "electric_bike"
        row["start_lat"] = "40.712800"
        row["start_lng"] = "-74.006000"
        row["end_lat"] = "40.795000"
        row["end_lng"] = "-73.950000"
        # Inject multi-rule violation on a subset (both speed and out-of-bounds)
        if i % 5 == 0:
            row["end_lat"] = "41.150000"
        all_unique_trips.append(row)

    # J. Rule 7: OUT_OF_NYC_BOUNDS (lat outside [40.45, 40.95] or lng outside [-74.30, -73.65])
    for i in range(n_rule7_bounds):
        row = _build_valid_trip(ride_counter)
        ride_counter += 1
        started_dt = datetime.strptime(row["started_at"], "%Y-%m-%d %H:%M:%S")
        # Give long enough duration (e.g. 240 min) so speed stays < 25 km/h and Rule 7 is primary
        ended_dt = started_dt + timedelta(minutes=240)
        row["ended_at"] = ended_dt.strftime("%Y-%m-%d %H:%M:%S")
        if i % 2 == 0:
            row["start_lat"] = "41.020000"
            row["start_lng"] = "-73.980000"
            row["end_lat"] = "40.990000"
            row["end_lng"] = "-73.980000"
        else:
            row["start_lat"] = "40.750000"
            row["start_lng"] = "-74.380000"
            row["end_lat"] = "40.750000"
            row["end_lng"] = "-74.350000"
        all_unique_trips.append(row)

    # Create duplicate rows from standard valid trips (first n_Duplicates trips)
    duplicate_rows = [dict(all_unique_trips[i]) for i in range(n_Duplicates)]

    rng.shuffle(all_unique_trips)
    mid_idx = len(all_unique_trips) // 2
    batch1_rows = all_unique_trips[:mid_idx]
    batch2_rows = all_unique_trips[mid_idx:] + duplicate_rows
    rng.shuffle(batch2_rows)

    trip_fields = [
        "ride_id",
        "bike_id",
        "rideable_type",
        "started_at",
        "ended_at",
        "start_station_id",
        "end_station_id",
        "start_lat",
        "start_lng",
        "end_lat",
        "end_lng",
        "member_casual",
    ]

    batch1_path = os.path.join(trips_dir, "citibike_2026_10_batch1.csv")
    batch2_path = os.path.join(trips_dir, "citibike_2026_10_batch2.csv")

    with open(batch1_path, "w", newline="", encoding="utf-8") as f_b1:
        writer = csv.DictWriter(f_b1, fieldnames=trip_fields)
        writer.writeheader()
        writer.writerows(batch1_rows)

    with open(batch2_path, "w", newline="", encoding="utf-8") as f_b2:
        writer = csv.DictWriter(f_b2, fieldnames=trip_fields)
        writer.writeheader()
        writer.writerows(batch2_rows)

    total_written = len(batch1_rows) + len(batch2_rows)
    return {
        "raw_dir": raw_dir,
        "stations_file": stations_path,
        "weather_file": weather_path,
        "trip_files": [batch1_path, batch2_path],
        "stations_count": len(stations),
        "weather_rows": len(weather_rows),
        "total_trip_rows": total_written,
        "unique_trip_rows": len(all_unique_trips),
        "duplicate_trip_rows": len(duplicate_rows),
        "imputed_coord_rows": n_imputed_coords,
        "orphan_station_rows": n_orphan_stations,
        "injected_anomaly_rows": total_anomalies,
        "batch1_rows": len(batch1_rows),
        "batch2_rows": len(batch2_rows),
        "seed": seed,
    }


if __name__ == "__main__":
    from config.settings import get_env_paths

    env_name = os.getenv("DAT535_ENV", "dev")
    paths = get_env_paths(env_name)
    summary = generate_raw_batches(paths["raw_dir"])
    print(json.dumps(summary, indent=2))
