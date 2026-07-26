"""Spike (f): the two compiler mechanics DESIGN §8 names but doesn't pin down.

1. An output bound to an artifact is written by the node, not returned (§7 rule 2).
   Does `AssetOut(dagster_type=Nothing)` + `yield Output(None, name)` still record
   a materialization, and does a downstream node get a proper ordering dep?
2. Variables compile to run config (§8). Does a per-node `config_schema` +
   a `ConfigurableResource` carrying `run_name` reach the node body, and does
   a missing required variable fail loudly at launch?

Run: uv run python _dev/experiments/spike_f_artifacts_and_config.py
"""

import inspect
import tempfile
from pathlib import Path

import dagster as dg


class DagnetRun(dg.ConfigurableResource):
    run_name: str = ""


def make_node(name, outs, ins=(), deps=(), config=None, body=None):
    def compute(context, **kwargs):
        return body(context, **kwargs)

    compute.__name__ = name
    compute.__signature__ = inspect.Signature(
        [inspect.Parameter("context", inspect.Parameter.POSITIONAL_OR_KEYWORD)]
        + [inspect.Parameter(p, inspect.Parameter.POSITIONAL_OR_KEYWORD) for p in ins]
    )
    return dg.multi_asset(
        name=name,
        outs=outs,
        ins={p: dg.AssetIn(key=dg.AssetKey(k.split("/"))) for p, k in ins.items()} if ins else None,
        deps=[dg.AssetKey(d.split("/")) for d in deps] or None,
        config_schema=config,
        required_resource_keys={"dagnet"},
        can_subset=False,
    )(compute)


def build(store_root):
    def write_file(context):
        # An artifact output: the node writes it, returns nothing.
        path = Path(store_root, "extracted.json")
        path.write_text('{"rows": 3}')
        yield dg.Output(None, "drug_ndc")

    def load(context, drug_ndc_path):
        assert isinstance(drug_ndc_path, Path), type(drug_ndc_path)
        assert drug_ndc_path.exists(), drug_ndc_path
        sample_n = context.op_config["sample_n"]
        run_name = context.resources.dagnet.run_name
        yield dg.Output({"n": sample_n, "run": run_name}, "summary")

    extract = make_node(
        "extract",
        outs={"drug_ndc": dg.AssetOut(key=dg.AssetKey(["openfda", "drug_ndc"]), dagster_type=dg.Nothing)},
        body=write_file,
    )
    # The consumer takes a *dep* on the artifact asset (ordering only); the
    # resolved location is injected by us, not by the IO manager.
    loader = make_node(
        "load",
        outs={"summary": dg.AssetOut(key=dg.AssetKey(["load", "summary"]))},
        deps=["openfda/drug_ndc"],
        config={"sample_n": dg.Field(int), "llm_model": dg.Field(str, default_value="qwen")},
        body=lambda context: load(context, Path(store_root, "extracted.json")),
    )
    return [extract, loader]


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as store_root, tempfile.TemporaryDirectory() as home:
        Path(home, "dagster.yaml").write_text("{}\n")
        assets = build(store_root)
        defs = dg.Definitions(assets=assets, resources={"dagnet": DagnetRun(run_name="test_api")})
        job = defs.resolve_job_def("__ASSET_JOB") if False else None

        with dg.DagsterInstance.from_config(home) as instance:
            result = dg.materialize(
                assets,
                instance=instance,
                resources={"dagnet": DagnetRun(run_name="test_api")},
                run_config={"ops": {"load": {"config": {"sample_n": 42}}}},
                raise_on_error=False,
            )
            print("== success:", result.success)
            print(
                "== materialized:",
                sorted(e.asset_key.to_user_string() for e in result.get_asset_materialization_events()),
            )
            print("== load output:", result.output_for_node("load", "summary"))

            # a required variable left unset must fail loudly, not default to None
            bad = dg.materialize(
                assets,
                instance=instance,
                resources={"dagnet": DagnetRun(run_name="test_api")},
                raise_on_error=False,
            )
            print("== missing required var -> success:", bad.success)
