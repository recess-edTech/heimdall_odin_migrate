



def must_exist(d: dict, key: str) -> str:
    if key not in d or d[key] is None:
        raise ValueError(f"Missing required key: {key}")
    return d[key]