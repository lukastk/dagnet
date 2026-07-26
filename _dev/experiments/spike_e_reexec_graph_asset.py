"""Spike (e): re-execution-from-failure granularity for ops inside graph-backed assets.

Shape: asset `head/out` (plain) -> graph-backed asset `tail/out` whose graph is
op1 -> op2 -> op3.  op3 fails on the first attempt (a marker file flips it).
Re-execute from failure and see WHICH steps run again:
  - if only `tail.op3` re-runs  -> op-level granularity inside the graph
  - if tail.op1/op2 re-run too  -> asset-level granularity (whole graph replays)
  - if `head` re-runs           -> no resume at all

Run: uv run python _dev/experiments/spike_e_reexec_graph_asset.py
"""

import os
import tempfile
from pathlib import Path

import dagster as dg
from dagster._core.definitions.reconstruct import build_reconstructable_job

MARKER = os.environ.get("SPIKE_E_MARKER", "/tmp/spike_e_marker")


@dg.op
def head_op():
    return 1


@dg.op
def op1(x):
    return x + 1


@dg.op
def op2(x):
    return x + 1


@dg.op
def op3(x):
    if not Path(MARKER).exists():
        Path(MARKER).write_text("failed once")
        raise RuntimeError("boom (first attempt only)")
    return x + 1


@dg.graph
def tail_graph(x):
    return op3(op2(op1(x)))


def build_job():
    head = dg.AssetsDefinition.from_op(head_op, keys_by_output_name={"result": dg.AssetKey(["head", "out"])})
    tail = dg.AssetsDefinition.from_graph(
        tail_graph,
        keys_by_input_name={"x": dg.AssetKey(["head", "out"])},
        keys_by_output_name={"result": dg.AssetKey(["tail", "out"])},
    )
    return dg.Definitions(
        assets=[head, tail],
        jobs=[dg.define_asset_job("dagnet_job", selection="*", executor_def=dg.multiprocess_executor)],
    ).resolve_job_def("dagnet_job")


def steps_run(result):
    return sorted(
        {
            ev.step_key
            for ev in result.all_events
            if ev.event_type_value == "STEP_START" and ev.step_key
        }
    )


if __name__ == "__main__":
    Path(MARKER).unlink(missing_ok=True)
    recon = build_reconstructable_job(__name__, "build_job", (), {})
    with tempfile.TemporaryDirectory() as home:
        Path(home, "dagster.yaml").write_text("{}\n")
        with dg.DagsterInstance.from_config(home) as instance:
            with dg.execute_job(recon, instance=instance, raise_on_error=False) as r1:
                print("run 1 success:", r1.success)
                print("run 1 steps  :", steps_run(r1))
                run_id = r1.run_id
            with dg.execute_job(
                recon,
                instance=instance,
                reexecution_options=dg.ReexecutionOptions.from_failure(run_id, instance),
                raise_on_error=False,
            ) as r2:
                print("run 2 success:", r2.success)
                print("run 2 steps  :", steps_run(r2))
