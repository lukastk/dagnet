"""Nodes that report when they ran, so the pool limit is observable.

Each `heavy` node returns the wall-clock window it occupied. `collect` asserts
those windows do not overlap — which is only true if `heavy = 1` was enforced.
"""

import os
import time
from pathlib import Path

ATTEMPTS = Path(__file__).parent / "_attempts.txt"


def prepare(ctx) -> {"batches": list[int]}:
    return {"batches": list(range(8))}


def embed(ctx, batches: list[int]) -> {"window": tuple}:
    start = time.time()
    time.sleep(0.6)
    print(f"embedded {len(batches)} batches in pid {os.getpid()}")
    return {"window": (start, time.time())}


def call_api(ctx, batches: list[int]) -> {"payload": dict}:
    """Fails until it has been tried `flaky_attempts_needed` times."""
    attempts = int(ATTEMPTS.read_text()) if ATTEMPTS.exists() else 0
    ATTEMPTS.write_text(str(attempts + 1))
    needed = ctx.vars["flaky_attempts_needed"]
    if attempts + 1 < needed:
        raise RuntimeError(f"flaky API failed on attempt {attempts + 1} of {needed}")
    print(f"API succeeded on attempt {attempts + 1}")
    ATTEMPTS.unlink()
    return {"payload": {"attempts": attempts + 1, "n": len(batches)}}


def collect(ctx, a: tuple, b: tuple, c: tuple, payload: dict) -> {"report": dict}:
    windows = sorted([a, b, c])
    overlapping = [
        (earlier, later) for earlier, later in zip(windows, windows[1:]) if later[0] < earlier[1]
    ]
    assert not overlapping, f"pool heavy=1 was not enforced: {overlapping}"
    report = {"heavy_ran_serially": True, "api": payload}
    print(f"report: {report}")
    return {"report": report}
