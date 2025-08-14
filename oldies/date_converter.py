import pandas as pd


def DateConverter(df, col="date", mode="date"):
    """
    Normalize a date/time key to a common type for safe merges.

    mode:
      - "date"           -> python datetime.date (dtype=object, no timezone)
      - "datetime_naive" -> datetime64[ns] at midnight, tz-naive
      - "datetime_utc"   -> datetime64[ns, UTC] at midnight
    """
    s = pd.to_datetime(df[col], errors="coerce", utc=True)

    if mode == "date":
        df[col] = s.dt.date
    elif mode == "datetime_naive":
        df[col] = s.dt.tz_localize(None).normalize()
    elif mode == "datetime_utc":
        df[col] = s.dt.normalize()
    else:
        raise ValueError("mode must be one of: 'date', 'datetime_naive', 'datetime_utc'")
    return df