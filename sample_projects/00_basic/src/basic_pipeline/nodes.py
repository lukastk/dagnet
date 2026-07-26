"""Plain Python. No dagster import, no dagnet import — that is the whole point.

Each function takes `ctx` first and one parameter per declared input, and returns
a dict with one entry per declared output. The dict-shaped return annotation is
optional documentation; `dagnet check` validates it against `pipeline.toml`.
"""


def extract(ctx) -> {"readings": list[float]}:
    readings = [1.5, 2.0, -3.0, 4.25, 0.0, -0.5, 7.75]
    print(f"extracted {len(readings)} readings")
    return {"readings": readings}


def transform(ctx, readings: list[float]) -> {"clean": list[float], "rejected": list[float]}:
    clean = [r for r in readings if r > 0]
    rejected = [r for r in readings if r <= 0]
    print(f"kept {len(clean)}, rejected {len(rejected)}")
    return {"clean": clean, "rejected": rejected}


def summarise(ctx, values: list[float]) -> {"summary": dict}:
    summary = {
        "count": len(values),
        "total": round(sum(values), 4),
        "mean": round(sum(values) / len(values), 4) if values else None,
    }
    print(f"summary: {summary}")
    return {"summary": summary}
