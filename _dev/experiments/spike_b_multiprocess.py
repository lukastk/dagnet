"""Spike (b): async node functions under the REAL multiprocess executor.

Also verifies the mechanism `dagnet run` will need: `execute_in_process` /
`materialize` always run in-process, so genuine multiprocess execution requires
`dagster.execute_job(ReconstructableJob, ...)`. Since our job is built at runtime
from a manifest path, `build_reconstructable_job(module, fn_name, json_args)` is
the reconstructor — this spike proves that round-trip works.

Run: uv run --with numpy --with pandas python _dev/experiments/spike_b_multiprocess.py
"""

import asyncio
import inspect
import os
import tempfile

import dagster as dg
import numpy as np
from dagster._core.definitions.reconstruct import build_reconstructable_job


# --- "node functions": one sync, two async, zero framework imports ---------
def produce(ctx):
    return {"ad_ids": np.arange(1000, dtype=np.int64)}


async def slow_a(ctx, ad_ids):
    await asyncio.sleep(0.5)
    return {"out": (os.getpid(), int(ad_ids.sum()))}


async def slow_b(ctx, ad_ids):
    await asyncio.sleep(0.5)
    return {"out": (os.getpid(), len(ad_ids))}


def collect(ctx, a, b):
    return {"summary": {"pids": [a[0], b[0], os.getpid()], "vals": [a[1], b[1]]}}


def make_node(name, fn, outputs, ins):
    is_async = inspect.iscoroutinefunction(fn)

    if is_async:

        def body(**kwargs):
            return _shape(asyncio.run(fn(None, **kwargs)), outputs)
    else:

        def body(**kwargs):
            return _shape(fn(None, **kwargs), outputs)

    body.__name__ = name
    body.__signature__ = inspect.Signature(
        [inspect.Parameter(p, inspect.Parameter.POSITIONAL_OR_KEYWORD) for p in ins]
    )
    return dg.multi_asset(
        name=name,
        outs={o: dg.AssetOut(key=[name, o]) for o in outputs},
        ins={p: dg.AssetIn(key=dg.AssetKey(k.split("/"))) for p, k in ins.items()},
        can_subset=False,
    )(body)


def _shape(result, outputs):
    if len(outputs) == 1:
        return result[outputs[0]]
    return tuple(result[o] for o in outputs)


def build_job():
    """Module-level reconstructor — this is what build_reconstructable_job calls."""
    assets = [
        make_node("produce", produce, ["ad_ids"], {}),
        make_node("slow_a", slow_a, ["out"], {"ad_ids": "produce/ad_ids"}),
        make_node("slow_b", slow_b, ["out"], {"ad_ids": "produce/ad_ids"}),
        make_node("collect", collect, ["summary"], {"a": "slow_a/out", "b": "slow_b/out"}),
    ]
    return dg.Definitions(
        assets=assets,
        jobs=[dg.define_asset_job("dagnet_job", selection="*", executor_def=dg.multiprocess_executor)],
    ).resolve_job_def("dagnet_job")


if __name__ == "__main__":
    recon = build_reconstructable_job(__name__, "build_job", (), {})
    with tempfile.TemporaryDirectory() as home:
        with dg.instance_for_test(temp_dir=home) as instance:
            with dg.execute_job(recon, instance=instance) as result:
                print("SUCCESS:", result.success)
                summary = result.output_for_node("collect", "summary")
                print("pids seen:", summary["pids"], "-> distinct:", len(set(summary["pids"])))
                print("vals:", summary["vals"])
