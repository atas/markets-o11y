import pandas as pd


def _scalar(val):
    """Unwrap pandas Series/array to a plain Python scalar."""
    if isinstance(val, pd.Series):
        val = val.iloc[0] if len(val) == 1 else None
    return val


def safe_float(val) -> float | None:
    """Convert to float, returning None for NaN/None/NaT."""
    val = _scalar(val)
    if val is None or pd.isna(val):
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def safe_int(val) -> int | None:
    """Convert to int, returning None for NaN/None/NaT."""
    val = _scalar(val)
    if val is None or pd.isna(val):
        return None
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return None
