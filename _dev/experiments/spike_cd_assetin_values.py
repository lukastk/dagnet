"""Spikes (c) + (d): programmatic multi_asset, AssetIn renaming, numpy/DataFrame transport.

(d) Can we build a multi_asset WITHOUT decorator syntax, name its outputs
    `node/output`, and rename an upstream asset key onto a differently-named
    function parameter (netrun's `successful_ad_ids` -> param `ad_ids`)?
(c) Do numpy arrays / pandas DataFrames survive the default (pickle) IO manager
    across a multiprocess run?

Run: uv run --with numpy --with pandas python _dev/experiments/spike_cd_assetin_values.py
"""

import tempfile
from pathlib import Path

import dagster as dg
import numpy as np
import pandas as pd


# --- "node functions": plain python, zero framework imports ----------------
def produce(ctx):
    return {"successful_ad_ids": np.arange(10_000, dtype=np.int64)}


def consume(ctx, ad_ids):  # NOTE: param name differs from upstream output name
    assert isinstance(ad_ids, np.ndarray), type(ad_ids)
    df = pd.DataFrame({"ad_id": ad_ids, "score": ad_ids * 0.5})
    return {"scores": df}


def sink(ctx, scores):
    assert isinstance(scores, pd.DataFrame), type(scores)
    return {"n": len(scores)}


# --- compiler-ish: build multi_assets programmatically ---------------------
def make_node(name, fn, outputs, ins):
    """ins: param_name -> upstream AssetKey string."""

    def body(**kwargs):
        result = fn(None, **kwargs)
        if len(outputs) == 1:
            return result[outputs[0]]
        return tuple(result[o] for o in outputs)

    body.__name__ = name
    # Give the wrapper an explicit signature so dagster can introspect params.
    import inspect

    body.__signature__ = inspect.Signature(
        [inspect.Parameter(p, inspect.Parameter.POSITIONAL_OR_KEYWORD) for p in ins]
    )

    return dg.multi_asset(
        name=name,
        outs={o: dg.AssetOut(key=[name, o]) for o in outputs},
        ins={p: dg.AssetIn(key=dg.AssetKey(k.split("/"))) for p, k in ins.items()},
        can_subset=False,
    )(body)


a = make_node("produce", produce, ["successful_ad_ids"], {})
b = make_node("consume", consume, ["scores"], {"ad_ids": "produce/successful_ad_ids"})
c = make_node("sink", sink, ["n"], {"scores": "consume/scores"})


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as home:
        with dg.instance_for_test(temp_dir=home) as instance:
            defs = dg.Definitions(assets=[a, b, c])
            job = dg.define_asset_job("all", selection="*", executor_def=dg.multiprocess_executor)
            resolved = dg.Definitions(
                assets=[a, b, c], jobs=[job]
            ).resolve_job_def("all")
            result = resolved.execute_in_process(instance=instance)
            print("SUCCESS:", result.success)
            for ev in result.get_asset_materialization_events():
                print("  materialized:", ev.asset_key.to_user_string())
            print("output value of sink:", result.output_for_node("sink"))
        print("storage dir contents:")
        for p in sorted(Path(home).rglob("*")):
            if p.is_file():
                print("   ", p.relative_to(home), p.stat().st_size)
