def parse_interval(value: str | int) -> int:
    """Parse interval string like '15m', '1h', '30s' to seconds."""
    if isinstance(value, int):
        return value
    value = str(value).strip().lower()
    if value.endswith("h"):
        return int(value[:-1]) * 3600
    if value.endswith("m"):
        return int(value[:-1]) * 60
    if value.endswith("s"):
        return int(value[:-1])
    return int(value)
