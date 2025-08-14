from __future__ import annotations
from dataclasses import dataclass, field
from typing import Iterable, Optional, Dict, Any, List, Tuple
import os, time
import pandas as pd

def _safe_requests():
    import importlib
    return importlib.import_module("requests")

@dataclass
class OpenAQClient:
    api_key: Optional[str] = None
    center: Tuple[float, float] = (41.9028, 12.4964)  # Roma
    radius_m: int = 25_000
    page_limit: int = 100
    sensor_limit: int = 500
    sensors_per_param: int = 3
    verbose: bool = False
    use_session: bool = True

    _base: str = field(default="https://api.openaq.org/v3", init=False)
    _session: Any = field(default=None, init=False, repr=False)

    # --------- infra ---------
    def _headers(self) -> Dict[str, str]:
        key = self.api_key or os.getenv("OPENAQ_API_KEY", "")
        return {"X-API-Key": key} if key else {}

    def _http(self):
        if not self.use_session:
            return _safe_requests()
        if self._session is None:
            requests = _safe_requests()
            self._session = requests.Session()
        return self._session

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None, max_retries: int = 6) -> Dict[str, Any]:
        """
        GET request with retry/backoff when receiving an HTTP 429 (Too Many Requests).
        Respects the 'Retry-After' header value if provided by the server.
        """
        requests = _safe_requests()
        url = f"{self._base}{path}"
        backoff = 1.0
        for attempt in range(max_retries):
            if self.use_session and self._session is not None:
                resp = self._session.get(url, params=params, headers=self._headers(), timeout=60)
            else:
                resp = requests.get(url, params=params, headers=self._headers(), timeout=60)

            if resp.status_code == 429:
                ra = resp.headers.get("Retry-After")
                wait = float(ra) if ra and str(ra).replace(".","",1).isdigit() else backoff
                if self.verbose:
                    print(f"[429] rate limited {url} — waiting {wait:.1f}s (attempt {attempt+1}/{max_retries})")
                time.sleep(wait)
                backoff = min(backoff * 2, 60.0)
                continue

            resp.raise_for_status()
            return resp.json()
        raise RuntimeError(f"Exceeded retries (429) for {url}")

    # --------- discovery ---------
    def find_locations(self) -> List[Dict[str, Any]]:
        """Trova locations vicino a center/radius."""
        results, page = [], 1
        while True:
            payload = self._get(
                "/locations",
                params={
                    "coordinates": f"{self.center[0]},{self.center[1]}",
                    "radius": self.radius_m,
                    "limit": self.page_limit,
                    "page": page,
                    "order_by": "id",
                    "sort_order": "asc",
                },
            )
            batch = payload.get("results", [])
            if not batch:
                break
            results.extend(batch)
            meta = payload.get("meta", {}) or {}
            found = meta.get("found", len(results))
            if page * self.page_limit >= (found if isinstance(found, int) else len(results)):
                break
            page += 1
        if self.verbose:
            print(f"Locations found: {len(results)}")
        return results

    def list_sensors(self, parameters: Iterable[str]) -> Dict[str, List[int]]:
        """
        Restituisce {parametro: [sensor_id,...]} limitando a sensors_per_param.
        """
        wanted = set(parameters)
        bag: Dict[str, List[int]] = {p: [] for p in wanted}
        for loc in self.find_locations():
            loc_id = loc.get("id")
            if loc_id is None:
                continue
            page = 1
            while True:
                payload = self._get(f"/locations/{loc_id}/sensors", params={"limit": self.page_limit, "page": page})
                sens = payload.get("results", [])
                if not sens:
                    break
                for s in sens:
                    p = (s.get("parameter") or {}).get("name")
                    sid = s.get("id")
                    if p in wanted and sid is not None and len(bag[p]) < self.sensors_per_param:
                        bag[p].append(sid)
                meta = payload.get("meta", {}) or {}
                found = meta.get("found", len(sens))
                if page * self.page_limit >= (found if isinstance(found, int) else len(sens)):
                    break
                page += 1
            # stop early if we have enough sensors for all wanted parameters
            if all(len(bag[p]) >= self.sensors_per_param for p in wanted):
                break
        if self.verbose:
            for p, ids in bag.items():
                print(f"Parameter '{p}': {len(ids)} sensors -> {ids}")
        return bag

    # --------- sensor_parsing ---------
    def fetch_days_for_sensor(self, sensor_id: int, start_date: str, end_date: str) -> pd.DataFrame:
        payload = self._get(
            f"/sensors/{sensor_id}/days",
            params={
                "date_from": f"{start_date}T00:00:00Z",
                "date_to": f"{end_date}T23:59:59Z",
                "limit": 1000,
                "page": 1,
            },
        )
        rows = payload.get("results", [])
        if not rows:
            return pd.DataFrame(columns=["date", "value"])
        df = pd.json_normalize(rows)
        # transform date columns to UTC and normalize
        if "date.utc" in df.columns:
            dt = pd.to_datetime(df["date.utc"], utc=True).dt.normalize()
        elif "date.local" in df.columns:
            dt = pd.to_datetime(df["date.local"]).dt.tz_localize("UTC", nonexistent="NaT", ambiguous="NaT").dt.normalize()
        elif "date" in df.columns:
            dt = pd.to_datetime(df["date"], utc=True).dt.normalize()
        else:
            dt = pd.to_datetime(df.get("period.datetimeFrom.utc"), errors="coerce", utc=True).dt.normalize()
        # dopo (forza 'date' a oggetto date)
        out = pd.DataFrame({
            "date": dt,
            "value": pd.to_numeric(df.get("value"), errors="coerce")
        })
        out = out.dropna(subset=["date"]).groupby("date", as_index=False)["value"].median()
        out["date"] = pd.to_datetime(out["date"], utc=True, errors="coerce").dt.date
        return out
    

    # --------- API LEVEL 1---------
    def get_air_quality(
        self,
        parameters: Iterable[str],
        start_date: str,
        end_date: str,
        daily: bool = True,
    ) -> pd.DataFrame:
        """
        Returns a DataFrame with air quality data for the specified parameters and date range.:
         - daily=True  -> ['date', params...]
         - daily=False -> ['datetime_utc', params...]
        """
        sensors = self.list_sensors(parameters)
        frames = []
        for p in parameters:
            rows = []
            for sid in sensors.get(p, []):
                if self.verbose: print(f"Fetching {'days' if daily else 'hours'} for sensor {sid} ({p})")
                df = self.fetch_days_for_sensor(sid, start_date, end_date) if daily else self.fetch_hours_for_sensor(sid, start_date, end_date)
                if not df.empty:
                    rows.append(df.set_index("date" if daily else "datetime_utc"))
            if not rows:
                continue
            stacked = pd.concat(rows, axis=1)
            series = stacked.median(axis=1).rename(p)
            frames.append(series.to_frame())
        if not frames:
            return pd.DataFrame(columns=(["date"] if daily else ["datetime_utc"]) + list(parameters))
        out = pd.concat(frames, axis=1).sort_index()
        out = out.reset_index().rename(columns={"index": "date" if daily else "datetime_utc"})
        if daily and "date" in out.columns:
            out["date"] = pd.to_datetime(out["date"], utc=True, errors="coerce").dt.date

        return out

# --------- function to aggregate ---------
def load_and_aggregate_from_openaq(
    city: str = "Rome",
    parameters: Iterable[str] = ("pm25", "no2"),
    start_date: str = "2024-01-01",
    end_date: str = "2024-12-31",
    daily: bool = True,
    api_key: Optional[str] = None,
    **kwargs,
) -> pd.DataFrame:
    client = OpenAQClient(
        api_key=api_key,
        center=kwargs.get("center", (41.9028, 12.4964)),
        radius_m=kwargs.get("radius_m", 25_000),
        page_limit=kwargs.get("page_limit", 100),
        sensor_limit=kwargs.get("sensor_limit", 500),
        sensors_per_param=kwargs.get("sensors_per_param", 3),
        verbose=kwargs.get("verbose", False),
        use_session=kwargs.get("use_session", True),
    )
    df = client.get_air_quality(parameters=parameters, start_date=start_date, end_date=end_date, daily=daily)
    if daily and not df.empty and "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], utc=True, errors="coerce").dt.date

    if not df.empty:
        if daily and "date" in df.columns:
            df.insert(1, "city", city)
        elif not daily and ("datetime_utc" in df.columns):
            df.insert(1, "city", city)
    return df
