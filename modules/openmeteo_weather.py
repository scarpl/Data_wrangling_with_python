from typing import Dict, Any, Optional
import time
import pandas as pd

def _safe_requests():
    import importlib
    return importlib.import_module("requests")

def _get_with_retry(
    url: str,
    params: Dict[str, Any],
    headers: Optional[Dict[str, str]] = None,
    timeout: int = 60,
    max_retries: int = 5
) -> Dict[str, Any]:
    """GET con retry/backoff per errori transitori."""
    requests = _safe_requests()
    backoff = 1.0
    last_err = None
    for _ in range(max_retries):
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
        except Exception as e:
            last_err = e
            time.sleep(backoff)
            backoff = min(backoff * 2, 30.0)
    raise RuntimeError(f"Exceeded retries for {url}") from last_err


def build_weather_daily(
    lat: float,
    lon: float,
    start: str,
    end: str,
    api_key: Optional[str] = None,
    timezone: str = "Europe/Rome"
) -> pd.DataFrame:
    """
    Build DAILY weather DataFrame (local timezone) using Open-Meteo Archive API (ERA5).
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
        "timezone": timezone
    }
    headers = {"X-API-Key": api_key} if api_key else None
    data = _get_with_retry(url, params=params, headers=headers).get("hourly", {})
    if not data or "time" not in data:
        raise RuntimeError("No hourly data returned for the requested period.")

    # DataFrame e rinomina colonne
    df = pd.DataFrame(data).rename(columns={
        "time": "datetime_local",
        "temperature_2m": "temp_c",
        "relative_humidity_2m": "rhum_pct",
        "wind_speed_10m": "wind_speed_ms",
        "precipitation": "precip_mm"
    })

    df["datetime_local"] = pd.to_datetime(df["datetime_local"], errors="coerce")
    for c in ["temp_c", "rhum_pct", "wind_speed_ms", "precip_mm"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # Aggregazione giornaliera
    df["date"] = df["datetime_local"].dt.date
    daily = df.groupby("date", as_index=False).agg({
        "temp_c": "mean",
        "rhum_pct": "mean",
        "wind_speed_ms": "mean",
        "precip_mm": "sum"
    })
    return daily