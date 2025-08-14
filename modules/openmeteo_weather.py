# modules/openmeteo_weather.py

from typing import Dict, Any, Optional
import time
import pandas as pd
import requests



def _get_with_retry(
    url: str,
    params: Dict[str, Any],
    headers: Optional[Dict[str, str]] = None,
    timeout: int = 60,
    max_retries: int = 5
) -> Dict[str, Any]:
    """
    GET with simple retry/backoff for transient errors (HTTP 429/5xx).
    Honors 'Retry-After' when present.
    """
    backoff = 1.0
    last_err: Optional[Exception] = None
    for i in range(max_retries):
        resp = requests.get(url, params=params, headers=headers, timeout=timeout)
        if resp.status_code in (429, 500, 502, 503, 504):
            ra = resp.headers.get("Retry-After")
            wait = float(ra) if ra and str(ra).replace(".", "", 1).isdigit() else backoff
            time.sleep(wait)
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=timeout)
            if resp.status_code in (429, 500, 502, 503, 504):
                ra = resp.headers.get("Retry-After")
                wait = float(ra) if ra and str(ra).replace(".", "", 1).isdigit() else backoff
                time.sleep(wait)
                backoff = min(backoff * 2, 30.0)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.RequestException as exc:  # includes ChunkedEncodingError
            last_err = exc
            time.sleep(backoff)
            backoff = min(backoff * 2, 30.0)
            continue
        resp.raise_for_status()
        return resp.json()
    raise RuntimeError(f"Exceeded retries for {url}")
    raise RuntimeError(f"Exceeded retries for {url}") from last_err

def build_weather_daily(lat: float,lon: float,start: str,end: str, api_key: Optional[str] = None
) -> pd.DataFrame:
    """
    Build a DAILY weather DataFrame (UTC) for the given coordinates and date range
    using the Open-Meteo Archive API (ERA5) aggregated from hourly data.

    Returns a DataFrame with columns:
      ['date','temp_c','rhum_pct','wind_speed_ms','precip_mm']
    where:
      - date: python date (no time)
      - temp_c, rhum_pct, wind_speed_ms: daily mean
      - precip_mm: daily sum
    """
    url = "https://archive-api.open-meteo.com/v1/era5"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start,
        "end_date": end,
        "hourly": ",".join([
            "temperature_2m",
            "relative_humidity_2m",
            "wind_speed_10m",
            "precipitation"
        ]),
        "timezone": "UTC"
    }
    data = _get_with_retry(url, params=params, headers=None).get("hourly", {})
    if not data or "time" not in data:
        raise RuntimeError("Open-Meteo returned no hourly data for the requested period.")

    # Orario → tipizzazione
    df = pd.DataFrame(data).rename(columns={
        "time": "datetime_utc",
        "temperature_2m": "temp_c",
        "relative_humidity_2m": "rhum_pct",
        "wind_speed_10m": "wind_speed_ms",
        "precipitation": "precip_mm"
    })
    df["datetime_utc"] = pd.to_datetime(df["datetime_utc"], utc=True)
    for c in ["temp_c", "rhum_pct", "wind_speed_ms", "precip_mm"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # Aggregazione giornaliera → 'date' come data pura
    df["date"] = df["datetime_utc"].dt.normalize().dt.date
    daily = df.groupby("date", as_index=False).agg({
        "temp_c": "mean",
        "rhum_pct": "mean",
        "wind_speed_ms": "mean",
        "precip_mm": "sum"
    })
    return daily
    