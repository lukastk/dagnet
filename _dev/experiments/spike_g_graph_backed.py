"""Spike (g): building a graph-backed asset programmatically, for `asset = false`.

DESIGN §5.5 folds an op-node into the graph backing the nearest downstream asset.
This checks the three mechanics that needs:

1. ops built by calling `dg.op(...)` as a function, wired inside a `dg.graph(...)`
   body built as a closure over a topological order;
2. `AssetsDefinition.from_graph` has no `deps=` parameter, so ordering-only
   dependencies (`after`, artifact inputs) must arrive as `Nothing`-typed graph
   inputs mapped to upstream asset keys;
3. a multi-output op invoked inside a graph body — what does calling it return?

Run: uv run python _dev/experiments/spike_g_graph_backed.py
"""

import inspect
import os
import tempfile
from pathlib import Path

import dagster as dg


def make_op(name, outputs, value_ins, nothing_ins, body):
    def compute(context, **kwargs):
        result = body(context, **{k: v for k, v in kwargs.items() if k in value_ins})
        if len(outputs) == 1:
            return result[outputs[0]]
        return tuple(result[o] for o in outputs)

    compute.__name__ = name
    # A `Nothing` input carries no value, so Dagster forbids it as a parameter —
    # it is declared in `ins` and passed only when *invoking* the op in a graph.
    compute.__signature__ = inspect.Signature(
        [inspect.Parameter("context", inspect.Parameter.POSITIONAL_OR_KEYWORD)]
        + [inspect.Parameter(p, inspect.Parameter.POSITIONAL_OR_KEYWORD) for p in value_ins]
    )
    ins = {p: dg.In() for p in value_ins}
    ins.update({p: dg.In(dg.Nothing) for p in nothing_ins})
    return dg.op(
        name=name,
        ins=ins,
        out={o: dg.Out() for o in outputs},
    )(compute)


# head: a normal asset producing a value the graph consumes
head = dg.multi_asset(
    name="head", outs={"rows": dg.AssetOut(key=dg.AssetKey(["head", "rows"]))}
)(lambda: [1, 2, 3])

gate = dg.multi_asset(
    name="gate", outs={"done": dg.AssetOut(key=dg.AssetKey(["gate", "done"]))}
)(lambda: True)

# The cluster: two op-nodes feeding one asset node.
op_double = make_op("op_double", ["doubled"], ["rows"], [], lambda ctx, rows: {"doubled": [r * 2 for r in rows]})
op_split = make_op(
    "op_split",
    ["evens", "odds"],
    ["doubled"],
    [],
    lambda ctx, doubled: {"evens": [d for d in doubled if d % 2 == 0], "odds": [d for d in doubled if d % 2]},
)
asset_op = make_op(
    "sink",
    ["summary"],
    ["evens", "odds"],
    ["ordering"],
    lambda ctx, evens, odds: {"summary": {"evens": evens, "odds": odds, "pid": os.getpid()}},
)


def graph_body(rows, ordering):
    doubled = op_double(rows)
    evens, odds = op_split(doubled)  # (3) does a 2-output op unpack like this?
    return asset_op(evens=evens, odds=odds, ordering=ordering)


graph_body.__signature__ = inspect.Signature(
    [
        inspect.Parameter("rows", inspect.Parameter.POSITIONAL_OR_KEYWORD),
        inspect.Parameter("ordering", inspect.Parameter.POSITIONAL_OR_KEYWORD),
    ]
)
graph_def = dg.graph(name="sink_graph", out={"summary": dg.GraphOut()})(graph_body)

tail = dg.AssetsDefinition.from_graph(
    graph_def,
    keys_by_input_name={
        "rows": dg.AssetKey(["head", "rows"]),
        "ordering": dg.AssetKey(["gate", "done"]),  # (2) Nothing-typed ordering dep
    },
    keys_by_output_name={"summary": dg.AssetKey(["sink", "summary"])},
)


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as home:
        Path(home, "dagster.yaml").write_text("{}\n")
        with dg.DagsterInstance.from_config(home) as instance:
            result = dg.materialize([head, gate, tail], instance=instance, raise_on_error=False)
            print("== success:", result.success)
            print(
                "== materialized:",
                sorted(
                    e.asset_key.to_user_string()
                    for e in result.get_asset_materialization_events()
                ),
            )
            steps = sorted(
                {e.step_key for e in result.all_events if e.event_type_value == "STEP_START"}
            )
            print("== steps:", steps)
