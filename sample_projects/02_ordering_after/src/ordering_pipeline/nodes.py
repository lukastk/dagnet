"""Nodes that depend on each other's *effects*, not on each other's values.

Each fetcher writes a file into a shared scratch directory. They must run after
`reset_workspace` has emptied it — an ordering constraint with nothing to pass.
"""

import shutil
from pathlib import Path

SCRATCH = Path(__file__).parent / "_scratch"


def reset_workspace(ctx) -> {"ready": str}:
    shutil.rmtree(SCRATCH, ignore_errors=True)
    SCRATCH.mkdir(parents=True)
    print(f"cleared {SCRATCH}")
    return {"ready": str(SCRATCH)}


def _fetch(name: str, rows: int) -> int:
    (SCRATCH / f"{name}.txt").write_text("\n".join(str(i) for i in range(rows)))
    print(f"fetched {rows} {name}")
    return rows


def fetch_users(ctx) -> {"count": int}:
    return {"count": _fetch("users", 12)}


def fetch_orders(ctx) -> {"count": int}:
    return {"count": _fetch("orders", 40)}


def fetch_events(ctx) -> {"count": int}:
    return {"count": _fetch("events", 173)}


def publish(ctx, users: int) -> {"manifest": dict}:
    files = sorted(p.name for p in SCRATCH.iterdir())
    # All three fetchers have finished, though only one value was passed in.
    assert files == ["events.txt", "orders.txt", "users.txt"], files
    print(f"publishing {files} (users={users})")
    return {"manifest": {"files": files, "users": users}}
