"""`asset = false` changes nothing about how a node is written.

These four functions are indistinguishable from each other; whether a node is a
durable asset or transient plumbing is a decision made in `pipeline.toml`, not
in the code.
"""


def load_readings(ctx) -> {"readings": list[float | None]}:
    readings = [1.0, None, 3.0, None, 5.0, 7.0]
    print(f"loaded {len(readings)} raw readings")
    return {"readings": readings}


def drop_nulls(ctx, readings: list[float | None]) -> {"kept": list[float]}:
    kept = [r for r in readings if r is not None and r > ctx.vars["threshold"]]
    print(f"kept {len(kept)} of {len(readings)}")
    return {"kept": kept}


def normalise(ctx, kept: list[float]) -> {"scaled": list[float]}:
    largest = max(kept) if kept else 1.0
    return {"scaled": [round(r / largest, 4) for r in kept]}


def report(ctx, scaled: list[float]) -> {"summary": dict}:
    summary = {"n": len(scaled), "max": max(scaled, default=None), "run": ctx.run_name}
    print(f"summary: {summary}")
    return {"summary": summary}
