"""
openaq_loader.py — OpenAQ v3 data loader for Rome (or any coordinates)

Usage:
-------
from openaq_loader import load_and_aggregate_from_openaq, fetch_openaq_hours_rome

aq_daily = load_and_aggregate_from_openaq(
    city="Rome",
    parameters=("pm25","no2"),
    start_date="2024-01-01",
    end_date="2024-12-31",
    daily=True,
    api_key="<<YOUR_OPENAQ_API_KEY>>"  # or set env var OPENAQ_API_KEY
)
"""

from typing import Iterable, Tuple, Optional, Dict, Any
import os
import time
import pandas as pd

def _safe_requests():
    import importlib
    return importlib.import_module("requests")

def _headers(api_key: Optional[str]) -> Dict[str, str]:
    key = api_key or os.getenv("OPENAQ_API_KEY", "")
    return {"X-API-Key": key} if key else {}

def fetch_openaq_hours_rome(
    parameters: Iterable[str] = ("pm25", "no2"),
    start_date: str = "2024-01-01",
    end_date: str = "2024-12-31",
    daily: bool = False,
    api_key: Optional[str] = None,
    center: Tuple[float, float] = (41.9028, 12.4964),  # Rome center
    radius_m: int = 5000,
    page_limit: int = 100,
    sensor_limit: int = 1000,
    verbose: bool = False
) -> pd.DataFrame:
    """
    OpenAQ v3 pipeline:
      1) /v3/locations?coordinates=...&radius=...
      2) /v3/locations/{id}/sensors
      3) /v3/sensors/{id}/hours?date_from=...&date_to=...
      4) median across sensors per hour
      5) optional daily resample
    Returns:
      - hourly: ['datetime_utc', <params...>]
      - daily : ['date', <params...>]
    """
    requests = _safe_requests()

    def _get(url: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        r = requests.get(url, params=params, headers=_headers(api_key), timeout=60)
        r.raise_for_status()
        return r.json()

    # 1) Find locations near center
    lat, lon = center
    loc_url = "https://api.openaq.org/v3/locations"
    locations = []
    page = 1
    while True:
        params = {
            "coordinates": f"{lat},{lon}",
            "radius": radius_m,
            "limit": page_limit,
            "page": page,
            "order_by": "id",
            "sort_order": "asc"
        }
        payload = _get(loc_url, params)
        results = payload.get("results", [])
        if not results:
            break
        locations.extend(results)
        meta = payload.get("meta", {}) or {}
        found = meta.get("found")
        if not isinstance(found, int):
            found = len(locations)
        if page * page_limit >= found:
            break
        page += 1

    if verbose:
        print(f"Found {len(locations)} locations (radius {radius_m} m).")

    # 2) Collect sensors by parameter
    sensors_url_tpl = "https://api.openaq.org/v3/locations/{loc_id}/sensors"
    sensor_ids_by_param: Dict[str, set] = {p: set() for p in parameters}

    for loc in locations:
        loc_id = loc.get("id")
        if loc_id is None:
            continue
        page = 1
        while True:
            payload = _get(sensors_url_tpl.format(loc_id=loc_id), params={"limit": page_limit, "page": page})
            sens = payload.get("results", [])
            if not sens:
                break
            for s in sens:
                par = (s.get("parameter") or {}).get("name")
                sid = s.get("id")
                if par in parameters and sid is not None:
                    sensor_ids_by_param[par].add(sid)
            meta = payload.get("meta", {}) or {}
            found = meta.get("found")
            if not isinstance(found, int):
                found = len(sens)
            if page * page_limit >= found:
                break
            page += 1

    if verbose:
        for p, ids in sensor_ids_by_param.items():
            print(f"Parameter '{p}': {len(ids)} sensors")

    # 3) Fetch hours for each sensor, median per hour across sensors
    def _fetch_hours(sensor_id: int) -> pd.DataFrame:
        url = f"https://api.openaq.org/v3/sensors/{sensor_id}/hours"
        all_rows = []
        page = 1
        while True:
            params = {
                "date_from": f"{start_date}T00:00:00Z",
                "date_to": f"{end_date}T23:59:59Z",
                "limit": sensor_limit,
                "page": page
            }
            payload = _get(url, params)
            results = payload.get("results", [])
            if not results:
                break
            all_rows.extend(results)
            meta = payload.get("meta", {}) or {}
            found = meta.get("found")
            if not isinstance(found, int):
                found = len(all_rows)
            if page * sensor_limit >= found:
                break
            page += 1
            time.sleep(0.1)
        if not all_rows:
            return pd.DataFrame(columns=["datetime_utc", "value"])
        df = pd.json_normalize(all_rows)
        dt = pd.to_datetime(df.get("period.datetimeFrom.utc"), errors="coerce", utc=True)
        out = pd.DataFrame({"datetime_utc": dt, "value": pd.to_numeric(df.get("value"), errors="coerce")})
        out = out.dropna(subset=["datetime_utc"])
        out = out.groupby("datetime_utc", as_index=False)["value"].median()
        return out

    frames = []
    for p in parameters:
        rows = []
        for sid in sensor_ids_by_param.get(p, []):
            df = _fetch_hours(sid)
            if not df.empty:
                rows.append(df.set_index("datetime_utc"))
        if not rows:
            if verbose: print(f"No hourly data for '{p}'.")
            continue
        stacked = pd.concat(rows, axis=1)
        series = stacked.median(axis=1).rename(p)
        frames.append(series.to_frame())

    if not frames:
        return pd.DataFrame(columns=(["date"] if daily else ["datetime_utc"]))

    hourly = pd.concat(frames, axis=1).sort_index()

    if daily:
        daily_df = hourly.resample("D").mean().reset_index()
        daily_df["date"] = pd.to_datetime(daily_df["datetime_utc"], utc=True).dt.normalize()
        daily_df = daily_df.drop(columns=["datetime_utc"])
        return daily_df

    return hourly.reset_index().rename(columns={"index": "datetime_utc"})

def load_and_aggregate_from_openaq(
    city: str = "Rome",
    parameters: Iterable[str] = ("pm25", "no2"),
    start_date: str = "2024-01-01",
    end_date: str = "2024-12-31",
    daily: bool = True,
    api_key: Optional[str] = None,
    **kwargs
) -> pd.DataFrame:
    center = kwargs.get("center", (41.9028, 12.4964))
    radius_m = kwargs.get("radius_m", 5000)
    verbose = kwargs.get("verbose", False)

    df = fetch_openaq_hours_rome(
        parameters=parameters,
        start_date=start_date,
        end_date=end_date,
        daily=daily,
        api_key=api_key,
        center=center,
        radius_m=radius_m,
        verbose=verbose
    )
    if not df.empty:
        if daily and "date" in df.columns:
            df.insert(1, "city", city)
        elif not daily and "datetime_utc" in df.columns:
            df.insert(1, "city", city)
    return df

__all__ = ["fetch_openaq_hours_rome", "load_and_aggregate_from_openaq"]
