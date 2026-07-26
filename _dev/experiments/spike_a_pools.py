"""Spike (a): are concurrency-pool limits enforced under library-mode `dagnet run`?

Three nodes all tagged pool "heavy" (limit 1) plus three tagged "main" (limit 4),
run with the multiprocess executor (max_concurrent high enough that only the pool
limit can serialize them). Each node records (start, end) wall times; overlap in
the heavy trio ⇒ the limit was NOT enforced.

Tested in three instance modes:
  1. persistent DAGSTER_HOME instance with pool limits configured   (`dagnet run`)
  2. ephemeral instance                                             (`--ephemeral`)

Run: uv run python _dev/experiments/spike_a_pools.py
"""

import inspect
import json
import os
import sys
import tempfile
import time
from pathlib import Path

import dagster as dg
from dagster._core.definitions.reconstruct import build_reconstructable_job

TIMES_DIR = os.environ.get("SPIKE_TIMES_DIR", "/tmp/spike_a_times")


def _timed(name):
    def fn(ctx):
        t0 = time.time()
        time.sleep(0.6)
        t1 = time.time()
        Path(TIMES_DIR).mkdir(parents=True, exist_ok=True)
        Path(TIMES_DIR, f"{name}.json").write_text(json.dumps([t0, t1]))
        return {"out": name}

    return fn


def make_node(name, fn, pool):
    def body():
        return fn(None)["out"]

    body.__name__ = name
    body.__signature__ = inspect.Signature([])
    return dg.multi_asset(
        name=name, outs={"out": dg.AssetOut(key=[name, "out"])}, pool=pool, can_subset=False
    )(body)


def build_job():
    assets = [make_node(f"heavy_{i}", _timed(f"heavy_{i}"), "heavy") for i in range(3)]
    assets += [make_node(f"main_{i}", _timed(f"main_{i}"), "main") for i in range(3)]
    return dg.Definitions(
        assets=assets,
        jobs=[
            dg.define_asset_job(
                "dagnet_job",
                selection="*",
                executor_def=dg.multiprocess_executor.configured({"max_concurrent": 6}),
            )
        ],
    ).resolve_job_def("dagnet_job")


def max_overlap(prefix):
    """Max number of <prefix>* steps that were running simultaneously."""
    spans = []
    for p in Path(TIMES_DIR).glob(f"{prefix}*.json"):
        spans.append(json.loads(p.read_text()))
    events = sorted([(s, +1) for s, _ in spans] + [(e, -1) for _, e in spans])
    cur = best = 0
    for _, d in events:
        cur += d
        best = max(best, cur)
    return best, len(spans)


def run(mode):
    for p in Path(TIMES_DIR).glob("*.json"):
        p.unlink()
    recon = build_reconstructable_job(__name__, "build_job", (), {})

    with tempfile.TemporaryDirectory() as home:
        if mode == "persistent":
            Path(home, "dagster.yaml").write_text(
                "concurrency:\n  pools:\n    granularity: op\n    default_limit: 4\n"
            )
            instance_cm = dg.DagsterInstance.from_config(home)
        else:
            instance_cm = dg.DagsterInstance.ephemeral()

        with instance_cm as instance:
            els = instance.event_log_storage
            supports = els.supports_global_concurrency_limits
            print(f"[{mode}] supports_global_concurrency_limits = {supports}")
            if supports:
                els.set_concurrency_slots("heavy", 1)
                els.set_concurrency_slots("main", 4)
                print(f"[{mode}] pool limits now = {els.get_pool_limits()}")
            with dg.execute_job(recon, instance=instance, raise_on_error=False) as result:
                print(f"[{mode}] success = {result.success}")

    h_overlap, h_n = max_overlap("heavy_")
    m_overlap, m_n = max_overlap("main_")
    print(f"[{mode}] heavy (limit 1): {h_n} steps, max simultaneous = {h_overlap}")
    print(f"[{mode}] main  (limit 4): {m_n} steps, max simultaneous = {m_overlap}")
    print(f"[{mode}] VERDICT: heavy limit {'ENFORCED' if h_overlap <= 1 else 'NOT enforced'}")
    print()


if __name__ == "__main__":
    for mode in sys.argv[1:] or ["persistent", "ephemeral"]:
        run(mode)
