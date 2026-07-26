"""A diamond: raw -> clean -> {metrics, sessions} -> bundle."""

EVENTS = [
    {"user": "a", "day": "2026-07-24", "kind": "view", "ms": 900},
    {"user": "a", "day": "2026-07-24", "kind": "click", "ms": 120},
    {"user": "b", "day": "2026-07-25", "kind": "view", "ms": 0},
    {"user": "b", "day": "2026-07-25", "kind": "view", "ms": 4300},
    {"user": "c", "day": "2026-07-25", "kind": "click", "ms": 75},
]


def raw(ctx) -> {"events": list[dict]}:
    print(f"raw: {len(EVENTS)} events")
    return {"events": list(EVENTS)}


def clean(ctx, events: list[dict]) -> {"events": list[dict]}:
    kept = [e for e in events if e["ms"] > 0]
    print(f"clean: kept {len(kept)} of {len(events)}")
    return {"events": kept}


def metrics(ctx, events: list[dict]) -> {"daily": dict}:
    daily: dict[str, int] = {}
    for event in events:
        daily[event["day"]] = daily.get(event["day"], 0) + 1
    print(f"metrics: {daily}")
    return {"daily": daily}


def sessions(ctx, events: list[dict]) -> {"table": list[dict]}:
    users = sorted({e["user"] for e in events})
    table = [{"user": u, "ms": sum(e["ms"] for e in events if e["user"] == u)} for u in users]
    print(f"sessions: {len(table)} users")
    return {"table": table}


def bundle(ctx, daily: dict, sessions: list[dict]) -> {"release": dict}:
    release = {"daily": daily, "sessions": sessions}
    print(f"bundle: {release}")
    return {"release": release}
