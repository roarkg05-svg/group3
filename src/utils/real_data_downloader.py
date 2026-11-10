"""Download and prepare real NYC / Jersey City Citi Bike trips, GBFS stations, and NOAA/Open-Meteo weather."""

import argparse
import csv
import io
import json
import logging
import os
import sys
import urllib.request
import zipfile
from typing import Any, Dict, List

logger = logging.getLogger("RealDataDownloader")

WMO_WEATHER_MAP = {
    0: "Clear",
    1: "Partly Cloudy",
    2: "Partly Cloudy",
    3: "Cloudy",
    45: "Cloudy",
    48: "Cloudy",
    51: "Light Rain",
    53: "Light Rain",
    55: "Light Rain",
    61: "Light Rain",
    63: "Heavy Rain",
    65: "Heavy Rain",
    80: "Light Rain",
    81: "Heavy Rain",
    82: "Heavy Rain",
    95: "Heavy Rain",
}


def download_real_trips(raw_trips_dir: str, scope: str = "jc", month: str = "202409") -> int:
    """Download real Citi Bike trip CSV zip from Amazon S3 and split into raw landing batches."""
    os.makedirs(raw_trips_dir, exist_ok=True)
    prefix = "JC-" if scope.lower() == "jc" else ""
    filename = f"{prefix}{month}-citibike-tripdata.csv.zip"
    url = f"https://s3.amazonaws.com/tripdata/{filename}"

    logger.info(f"Downloading real Citi Bike trips from: {url} ...")
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req) as resp:
        zip_bytes = resp.read()

    logger.info(f"Downloaded {len(zip_bytes) / (1024*1024):.2f} MB. Extracting CSV...")
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        csv_candidates = [n for n in z.namelist() if n.endswith(".csv") and not n.startswith("__MACOSX")]
        if not csv_candidates:
            raise FileNotFoundError("No CSV file found in downloaded zip.")
        csv_filename = csv_candidates[0]
        with z.open(csv_filename) as f_csv:
            all_lines = f_csv.read().decode("utf-8").splitlines()

    header = all_lines[0]
    data_lines = all_lines[1:]
    total_trips = len(data_lines)
    logger.info(f"Read {total_trips} real trip records from source.")

    # Split into two batches to conform to Bronze expectation of multi-file multi-batch raw ingest
    midpoint = total_trips // 2
    batch1_lines = data_lines[:midpoint]
    batch2_lines = data_lines[midpoint:]

    out_file1 = os.path.join(raw_trips_dir, "real_trips_batch_01.csv")
    out_file2 = os.path.join(raw_trips_dir, "real_trips_batch_02.csv")

    with open(out_file1, "w", encoding="utf-8") as f1:
        f1.write(header + "\n")
        f1.write("\n".join(batch1_lines) + "\n")

    with open(out_file2, "w", encoding="utf-8") as f2:
        f2.write(header + "\n")
        f2.write("\n".join(batch2_lines) + "\n")

    logger.info(
        f"Saved real trips to {out_file1} ({len(batch1_lines)} rows) and {out_file2} ({len(batch2_lines)} rows)."
    )
    return total_trips


def download_real_stations(raw_stations_dir: str) -> int:
    """Download live GBFS station catalog from Citi Bike and conform schema."""
    os.makedirs(raw_stations_dir, exist_ok=True)
    url = "https://gbfs.citibikenyc.com/gbfs/en/station_information.json"

    logger.info(f"Downloading live GBFS stations from: {url} ...")
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req) as resp:
        gbfs_payload = json.load(resp)

    raw_stations = gbfs_payload.get("data", {}).get("stations", [])
    conformed_stations: List[Dict[str, Any]] = []
    seen_ids = set()

    for st in raw_stations:
        name = st.get("name", "Unknown Station")
        lat = st.get("lat", 0.0)
        lon = st.get("lon", 0.0)
        cap = st.get("capacity", 30)
        reg = st.get("region_id", "NYC")

        # Map short_name (e.g., 'JC022', '6560.01') which Citi Bike trips use as start_station_id
        short_name = st.get("short_name")
        if short_name and short_name not in seen_ids:
            seen_ids.add(short_name)
            conformed_stations.append({
                "station_id": str(short_name),
                "station_name": name,
                "capacity": int(cap) if cap else 30,
                "region_id": str(reg) if reg else "NYC",
                "station_lat": float(lat),
                "station_lng": float(lon),
                "is_active": True,
            })

        # Also map standard UUID station_id
        uuid_id = st.get("station_id")
        if uuid_id and uuid_id not in seen_ids:
            seen_ids.add(uuid_id)
            conformed_stations.append({
                "station_id": str(uuid_id),
                "station_name": name,
                "capacity": int(cap) if cap else 30,
                "region_id": str(reg) if reg else "NYC",
                "station_lat": float(lat),
                "station_lng": float(lon),
                "is_active": True,
            })

    out_path = os.path.join(raw_stations_dir, "real_stations.json")
    with open(out_path, "w", encoding="utf-8") as f_out:
        json.dump(conformed_stations, f_out, indent=2)

    logger.info(f"Saved {len(conformed_stations)} conformed stations to {out_path}.")
    return len(conformed_stations)


def download_real_weather(raw_weather_dir: str, year_month: str = "202409") -> int:
    """Download historical hourly weather for Central Park NYC from Open-Meteo Archive."""
    os.makedirs(raw_weather_dir, exist_ok=True)
    year = year_month[:4]
    month = year_month[4:6]
    start_date = f"{year}-{month}-01"
    end_day = "30" if month in ("04", "06", "09", "11") else ("28" if month == "02" else "31")
    end_date = f"{year}-{month}-{end_day}"

    url = (
        f"https://archive-api.open-meteo.com/v1/archive?"
        f"latitude=40.7128&longitude=-74.0060&start_date={start_date}&end_date={end_date}&"
        f"hourly=temperature_2m,precipitation,wind_speed_10m,weather_code"
    )

    logger.info(f"Downloading real hourly weather from: {url} ...")
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req) as resp:
        weather_payload = json.load(resp)

    hourly = weather_payload.get("hourly", {})
    times = hourly.get("time", [])
    temps = hourly.get("temperature_2m", [])
    precips = hourly.get("precipitation", [])
    winds = hourly.get("wind_speed_10m", [])
    codes = hourly.get("weather_code", [])

    rows: List[Dict[str, Any]] = []
    for i, t_str in enumerate(times):
        formatted_ts = t_str.replace("T", " ") + ":00"
        code = codes[i] if i < len(codes) else 0
        cond = WMO_WEATHER_MAP.get(code, "Partly Cloudy")
        rows.append({
            "weather_timestamp": formatted_ts,
            "temp_c": temps[i] if i < len(temps) else 20.0,
            "precipitation_mm": precips[i] if i < len(precips) else 0.0,
            "wind_kmh": winds[i] if i < len(winds) else 10.0,
            "condition": cond,
        })

    out_path = os.path.join(raw_weather_dir, f"real_weather_{year_month}.csv")
    with open(out_path, "w", newline="", encoding="utf-8") as f_out:
        writer = csv.DictWriter(
            f_out,
            fieldnames=["weather_timestamp", "temp_c", "precipitation_mm", "wind_kmh", "condition"]
        )
        writer.writeheader()
        writer.writerows(rows)

    logger.info(f"Saved {len(rows)} hourly weather records to {out_path}.")
    return len(rows)


def download_all_real_data(raw_dir: str, scope: str = "jc", month: str = "202409") -> Dict[str, Any]:
    """Download and set up all real data in target raw_dir."""
    trips_dir = os.path.join(raw_dir, "trips")
    stations_dir = os.path.join(raw_dir, "stations")
    weather_dir = os.path.join(raw_dir, "weather")

    for d in (trips_dir, stations_dir, weather_dir):
        os.makedirs(d, exist_ok=True)
        for f in os.listdir(d):
            os.remove(os.path.join(d, f))

    n_trips = download_real_trips(trips_dir, scope=scope, month=month)
    n_stations = download_real_stations(stations_dir)
    n_weather = download_real_weather(weather_dir, year_month=month)

    summary = {
        "status": "success",
        "scope": scope,
        "month": month,
        "raw_dir": raw_dir,
        "trips_count": n_trips,
        "stations_count": n_stations,
        "weather_count": n_weather,
    }
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Download real NYC / JC Citi Bike and NOAA weather data")
    parser.add_argument("--env", default="dev", choices=["dev", "test", "prod"])
    parser.add_argument("--scope", default="jc", choices=["jc", "nyc"], help="jc (~100k trips) or nyc (~3.5M trips)")
    parser.add_argument("--month", default="202409", help="YYYYMM (default 202409)")
    args = parser.parse_args()

    from config.settings import get_env_paths
    paths = get_env_paths(args.env)
    raw_dir = paths["raw_dir"]

    print("=" * 70)
    print(f"DOWNLOADING REAL DATA FOR ENV: {args.env.upper()} (SCOPE: {args.scope.upper()}, MONTH: {args.month})")
    print(f"Target raw directory: {raw_dir}")
    print("=" * 70)

    summary = download_all_real_data(raw_dir=raw_dir, scope=args.scope, month=args.month)
    print("\nSummary:")
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    sys.exit(main())
