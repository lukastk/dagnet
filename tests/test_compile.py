"""Compiling to Dagster `Definitions`, and running what comes out (DESIGN §8)."""

from __future__ import annotations


import dagster as dg
import pytest

from dagnet.compile import CompileError, build, build_job
from dagnet.diagnostics import CheckFailed


def materialize(project, run_name=None, select=None):
    """Build the same job `dagnet run` builds and execute it in-process."""
    job = build_job(
        str(project.manifest_path),
        [str(p) for p in project.runs_paths],
        run_name=run_name,
        select=select,
        executor="in_process",
    )
    home = project.root / ".dagster"
    home.mkdir(exist_ok=True)
    (home / "dagster.yaml").write_text("{}\n")
    with dg.DagsterInstance.from_config(str(home)) as instance:
        return job.execute_in_process(instance=instance, raise_on_error=False)


def keys(result):
    return sorted(e.asset_key.to_user_string() for e in result.get_asset_materialization_events())


# --- the all-assets path ---------------------------------------------------


def test_a_linear_pipeline_materializes_one_asset_per_output(project):
    written = project(
        """
        [nodes.extract]
        fn = "MOD.extract"
        outputs = ["rows"]

        [nodes.transform]
        fn = "MOD.transform"
        inputs = { rows = "extract.rows" }
        outputs = ["clean", "rejects"]
        """,
        module="""
        def extract(ctx):
            return {"rows": [1, 2, 3, -1]}

        def transform(ctx, rows):
            return {"clean": [r for r in rows if r > 0], "rejects": [r for r in rows if r <= 0]}
        """,
    )
    result = materialize(written)
    assert result.success, result
    assert keys(result) == ["extract/rows", "transform/clean", "transform/rejects"]


def test_a_renamed_input_arrives_under_the_parameter_name(project):
    """DESIGN §8: `inputs` value ref -> `AssetIn` with a key mapping."""
    written = project(
        """
        [nodes.llm_filter]
        fn = "MOD.llm_filter"
        outputs = ["successful_ad_ids"]

        [nodes.rerank]
        fn = "MOD.rerank"
        inputs = { ad_ids = "llm_filter.successful_ad_ids" }
        outputs = ["ad_ids"]
        """,
        module="""
        def llm_filter(ctx) -> {'successful_ad_ids': list}:
            return {"successful_ad_ids": [7, 8, 9]}

        def rerank(ctx, ad_ids: list) -> {'ad_ids': list}:
            assert ad_ids == [7, 8, 9], ad_ids
            return {"ad_ids": sorted(ad_ids, reverse=True)}
        """,
    )
    result = materialize(written)
    assert result.success, result
    assert result.output_for_node("rerank", "ad_ids") == [9, 8, 7]


def test_async_node_functions_are_awaited(project):
    written = project(
        """
        [nodes.a]
        fn = "MOD.a"
        outputs = ["out"]
        """,
        module="""
        import asyncio

        async def a(ctx):
            await asyncio.sleep(0)
            return {"out": "awaited"}
        """,
    )
    result = materialize(written)
    assert result.success, result
    assert result.output_for_node("a", "out") == "awaited"


def test_after_creates_ordering_without_passing_data(project):
    written = project(
        """
        [nodes.first]
        fn = "MOD.first"
        outputs = ["out"]

        [nodes.second]
        fn = "MOD.second"
        outputs = ["out"]
        after = ["first"]
        """,
        module="""
        ORDER = []

        def first(ctx):
            ORDER.append("first")
            return {"out": 1}

        def second(ctx):
            # No parameter for `first` — `after` is ordering only.
            return {"out": 2}
        """,
    )
    result = materialize(written)
    assert result.success, result
    steps = [e.step_key for e in result.all_events if e.event_type_value == "STEP_START"]
    assert steps.index("first") < steps.index("second")


def test_fan_out_is_native_so_one_output_can_feed_several_nodes(project):
    written = project(
        """
        [nodes.src]
        fn = "MOD.src"
        outputs = ["out"]

        [nodes.left]
        fn = "MOD.side"
        inputs = { x = "src.out" }
        outputs = ["out"]

        [nodes.right]
        fn = "MOD.side"
        inputs = { x = "src.out" }
        outputs = ["out"]
        """,
        module="""
        def src(ctx):
            return {"out": 5}

        def side(ctx, x):
            return {"out": x * 2}
        """,
    )
    result = materialize(written)
    assert result.success, result
    assert keys(result) == ["left/out", "right/out", "src/out"]


# --- artifacts -------------------------------------------------------------


def test_an_artifact_output_is_written_by_the_node_and_still_materializes(project):
    written = project(
        """
        [artifacts."extracted/rows"]
        kind = "file"
        path = "extracted/rows.txt"

        [nodes.extract]
        fn = "MOD.extract"
        outputs = ["rows"]
        artifacts = { rows = "extracted/rows" }

        [nodes.load]
        fn = "MOD.load"
        inputs = { rows_path = "extracted/rows" }
        outputs = ["n"]
        """,
        module="""
        def extract(ctx) -> None:
            ctx.artifact("extracted/rows").write_text("a\\nb\\nc\\n")

        def load(ctx, rows_path):
            return {"n": len(rows_path.read_text().splitlines())}
        """,
    )
    result = materialize(written)
    assert result.success, result
    assert keys(result) == ["extracted/rows", "load/n"]
    assert result.output_for_node("load", "n") == 3
    assert (written.root / "extracted" / "rows.txt").exists()


def test_a_node_that_does_not_write_its_declared_artifact_fails_loudly(project):
    written = project(
        """
        [artifacts."out/file"]
        kind = "file"
        path = "out/file.txt"

        [nodes.liar]
        fn = "MOD.liar"
        outputs = ["f"]
        artifacts = { f = "out/file" }
        """,
        module="def liar(ctx) -> None:\n    pass\n",
    )
    result = materialize(written)
    assert not result.success
    assert "ArtifactNotWritten" in str(result.all_events)


def test_ctx_artifact_rejects_an_undeclared_key(project):
    written = project(
        """
        [artifacts."a/b"]
        kind = "file"
        path = "a/b.txt"

        [nodes.n]
        fn = "MOD.n"
        outputs = ["f"]
        artifacts = { f = "a/b" }
        """,
        module="""
        def n(ctx) -> None:
            ctx.artifact("a/b").write_text("x")
            ctx.artifact("a/typo")
        """,
    )
    result = materialize(written)
    assert not result.success
    assert "UnknownArtifact" in str(result.all_events)


def test_a_duckdb_artifact_resolves_to_its_table_name(project):
    written = project(
        """
        [artifacts."db/drugs"]
        kind = "duckdb_table"
        table = "drugs"

        [nodes.load]
        fn = "MOD.load"
        outputs = ["t"]
        artifacts = { t = "db/drugs" }
        """,
        module="""
        def load(ctx) -> None:
            assert ctx.artifact("db/drugs") == "drugs"
        """,
    )
    assert materialize(written).success


# --- variables and run presets ---------------------------------------------


def test_run_preset_values_reach_ctx_vars(project):
    written = project(
        """
        [vars]
        sample_n = { type = "int", default = 1000 }
        llm_model = { type = "str", default = "qwen" }

        [nodes.a]
        fn = "MOD.a"
        outputs = ["seen"]

        [nodes.a.vars]
        chunk_size = { type = "int", default = 512 }
        """,
        module="""
        def a(ctx):
            return {"seen": dict(ctx.vars) | {"run": ctx.run_name}}
        """,
        runs="""
        [defaults]
        sample_n = 100
        [runs.test_api]
        sample_n = 10
        llm_model = "gpt-5.2"
        [runs.test_api.a]
        chunk_size = 8
        """,
    )
    result = materialize(written, run_name="test_api")
    assert result.success, result
    assert result.output_for_node("a", "seen") == {
        "sample_n": 10,
        "llm_model": "gpt-5.2",
        "chunk_size": 8,
        "run": "test_api",
    }


def test_declared_defaults_apply_when_no_run_sets_them(project):
    written = project(
        """
        [vars]
        sample_n = { type = "int", default = 1000 }

        [nodes.a]
        fn = "MOD.a"
        outputs = ["seen"]
        """,
        module="def a(ctx):\n    return {'seen': ctx.vars['sample_n']}\n",
    )
    result = materialize(written)
    assert result.success, result
    assert result.output_for_node("a", "seen") == 1000


def test_each_run_preset_becomes_a_job_so_the_launchpad_shows_it(project):
    written = project(
        """
        [vars]
        n = { type = "int", default = 1 }

        [nodes.a]
        fn = "MOD.a"
        outputs = ["out"]
        """,
        module="def a(ctx):\n    return {'out': ctx.vars['n']}\n",
        runs="[runs.small]\nn = 1\n\n[runs.big]\nn = 1000\n",
    )
    defs = build(written.manifest_path, written.runs_paths)
    assert sorted(j.name for j in defs.jobs) == ["big", "small"]


def test_ctx_vars_is_read_only(project):
    written = project(
        """
        [vars]
        n = { type = "int", default = 1 }

        [nodes.a]
        fn = "MOD.a"
        outputs = ["out"]
        """,
        module="""
        def a(ctx):
            try:
                ctx.vars["n"] = 2
            except TypeError:
                return {"out": "read-only"}
            return {"out": "mutable"}
        """,
    )
    result = materialize(written)
    assert result.output_for_node("a", "out") == "read-only"


# --- the node return contract ----------------------------------------------


@pytest.mark.parametrize(
    "body,fragment",
    [
        ("return {'wrong': 1}", "missing ['out']"),
        ("return {'out': 1, 'extra': 2}", "unexpected ['extra']"),
        ("return 5", "must return a dict"),
    ],
)
def test_a_return_that_does_not_match_the_declared_outputs_is_loud(project, body, fragment):
    written = project(
        """
        [nodes.a]
        fn = "MOD.a"
        outputs = ["out"]
        """,
        module=f"def a(ctx):\n    {body}\n",
    )
    result = materialize(written)
    assert not result.success
    assert "NodeReturnError" in str(result.all_events)


def test_a_node_with_only_artifact_outputs_must_return_nothing(project):
    written = project(
        """
        [artifacts."a/b"]
        kind = "file"
        path = "a/b.txt"

        [nodes.n]
        fn = "MOD.n"
        outputs = ["f"]
        artifacts = { f = "a/b" }
        """,
        module="""
        def n(ctx):
            ctx.artifact("a/b").write_text("x")
            return {"f": "should not be returned"}
        """,
    )
    result = materialize(written)
    assert not result.success
    assert "NodeReturnError" in str(result.all_events)


# --- retries, pools, grouping ----------------------------------------------


def test_retries_pool_group_and_description_land_on_the_definition(project):
    written = project(
        """
        [pools]
        heavy = 1

        [nodes.a]
        fn = "MOD.a"
        description = "does a thing"
        outputs = ["out"]
        pool = "heavy"
        group = "extract"
        retries = { max = 3, wait_s = 10 }
        """,
        module="def a(ctx):\n    return {'out': 1}\n",
    )
    defs = build(written.manifest_path)
    asset = next(iter(defs.assets))
    assert asset.op.retry_policy == dg.RetryPolicy(max_retries=3, delay=10.0)
    assert asset.op.pool == "heavy"
    assert asset.op.description == "does a thing"
    assert set(asset.group_names_by_key.values()) == {"extract"}


def test_retries_actually_retry(project):
    written = project(
        """
        [nodes.flaky]
        fn = "MOD.flaky"
        outputs = ["out"]
        retries = { max = 2 }
        """,
        module="""
        from pathlib import Path

        MARKER = Path(__file__).with_suffix(".attempts")

        def flaky(ctx):
            attempts = int(MARKER.read_text()) if MARKER.exists() else 0
            MARKER.write_text(str(attempts + 1))
            if attempts < 2:
                raise RuntimeError("flaky")
            return {"out": attempts}
        """,
    )
    result = materialize(written)
    assert result.success, result
    assert result.output_for_node("flaky", "out") == 2


# --- asset checks ----------------------------------------------------------


def test_a_passing_and_a_failing_check_are_both_reported(project):
    written = project(
        """
        [nodes.a]
        fn = "MOD.a"
        outputs = ["rows"]
        checks = { rows = ["MOD.not_empty", "MOD.all_positive"] }
        """,
        module="""
        def a(ctx):
            return {"rows": [1, -2, 3]}

        def not_empty(ctx, rows):
            return len(rows) > 0

        def all_positive(ctx, rows):
            return {"passed": all(r > 0 for r in rows), "metadata": {"n": len(rows)}}
        """,
    )
    result = materialize(written)
    evaluations = {e.check_name: e.passed for e in result.get_asset_check_evaluations()}
    assert evaluations == {"not_empty": True, "all_positive": False}


def test_a_check_on_an_artifact_output_receives_its_location(project):
    written = project(
        """
        [artifacts."out/rows"]
        kind = "file"
        path = "out/rows.txt"

        [nodes.a]
        fn = "MOD.a"
        outputs = ["rows"]
        artifacts = { rows = "out/rows" }
        checks = { rows = ["MOD.file_has_lines"] }
        """,
        module="""
        def a(ctx) -> None:
            ctx.artifact("out/rows").write_text("x\\ny\\n")

        def file_has_lines(ctx, location):
            return {"passed": len(location.read_text().splitlines()) == 2}
        """,
    )
    result = materialize(written)
    assert result.success, result
    assert [e.passed for e in result.get_asset_check_evaluations()] == [True]


def test_a_check_returning_the_wrong_shape_is_loud(project):
    written = project(
        """
        [nodes.a]
        fn = "MOD.a"
        outputs = ["rows"]
        checks = { rows = ["MOD.bad_check"] }
        """,
        module="""
        def a(ctx):
            return {"rows": [1]}

        def bad_check(ctx, rows):
            return "probably fine"
        """,
    )
    result = materialize(written)
    assert not result.success
    assert "CheckReturnError" in str(result.all_events)


# --- build-time failures ---------------------------------------------------


def test_build_refuses_a_manifest_that_does_not_check(project):
    written = project("""
        [nodes.a]
        fn = "m.a"
        inputs = { x = "ghost.out" }
        outputs = ["out"]
    """)
    with pytest.raises(CheckFailed):
        build(written.manifest_path)


def test_build_job_rejects_an_unknown_run_name(project):
    written = project(
        """
        [nodes.a]
        fn = "MOD.a"
        outputs = ["out"]
        """,
        module="def a(ctx):\n    return {'out': 1}\n",
        runs="[runs.small]\n",
    )
    with pytest.raises(CompileError, match="no run named 'nope'"):
        build_job(
            str(written.manifest_path),
            [str(p) for p in written.runs_paths],
            run_name="nope",
        )


# --- selection -------------------------------------------------------------


def test_select_pulls_a_key_and_everything_upstream_of_it(project):
    """DESIGN §8: `--select "+key"` is netrun's `run_to_targets`, natively."""
    written = project(
        """
        [nodes.extract]
        fn = "MOD.extract"
        outputs = ["rows"]

        [nodes.transform]
        fn = "MOD.transform"
        inputs = { rows = "extract.rows" }
        outputs = ["clean", "rejected"]

        [nodes.summarise]
        fn = "MOD.summarise"
        inputs = { values = "transform.clean" }
        outputs = ["summary"]
        """,
        module="""
        def extract(ctx):
            return {"rows": [1, -2, 3]}

        def transform(ctx, rows):
            return {"clean": [r for r in rows if r > 0], "rejected": [r for r in rows if r <= 0]}

        def summarise(ctx, values):
            return {"summary": sum(values)}
        """,
    )
    result = materialize(written, select="+transform/clean")
    assert result.success, result
    # `summarise` is downstream, so it does not run; `transform/rejected` is a
    # sibling output of a node that is atomic, so the node runs but only the
    # selected output is recorded.
    assert keys(result) == ["extract/rows", "transform/clean"]


def test_selecting_a_single_leaf_runs_only_its_chain(project):
    written = project(
        """
        [nodes.a]
        fn = "MOD.a"
        outputs = ["out"]

        [nodes.b]
        fn = "MOD.a"
        outputs = ["out"]
        """,
        module="def a(ctx):\n    return {'out': 1}\n",
    )
    result = materialize(written, select="a/out")
    assert result.success, result
    assert keys(result) == ["a/out"]


# --- asset = false: op-nodes folded into graph-backed assets ---------------


def test_an_op_node_becomes_a_step_inside_the_downstream_asset(project):
    written = project(
        """
        [nodes.src]
        fn = "MOD.src"
        outputs = ["rows"]

        [nodes.doubled]
        fn = "MOD.double"
        inputs = { rows = "src.rows" }
        outputs = ["out"]
        asset = false

        [nodes.sink]
        fn = "MOD.sink"
        inputs = { values = "doubled.out" }
        outputs = ["total"]
        """,
        module="""
        def src(ctx):
            return {"rows": [1, 2, 3]}

        def double(ctx, rows):
            return {"out": [r * 2 for r in rows]}

        def sink(ctx, values):
            return {"total": sum(values)}
        """,
    )
    result = materialize(written)
    assert result.success, result
    # The op-node has no asset identity, and its step is nested in the graph.
    assert keys(result) == ["sink/total", "src/rows"]
    steps = sorted({e.step_key for e in result.all_events if e.event_type_value == "STEP_START"})
    assert steps == ["sink_graph.doubled", "sink_graph.sink", "src"]
    assert result.output_for_node("sink_graph.sink", "total") == 12


def test_a_chain_of_op_nodes_all_runs_inside_one_graph(project):
    written = project(
        """
        [nodes.src]
        fn = "MOD.src"
        outputs = ["rows"]

        [nodes.a]
        fn = "MOD.step"
        inputs = { rows = "src.rows" }
        outputs = ["out"]
        asset = false

        [nodes.b]
        fn = "MOD.step"
        inputs = { rows = "a.out" }
        outputs = ["out"]
        asset = false

        [nodes.sink]
        fn = "MOD.sink"
        inputs = { rows = "b.out" }
        outputs = ["total"]
        """,
        module="""
        def src(ctx):
            return {"rows": [1, 2, 3]}

        def step(ctx, rows):
            return {"out": [r + 1 for r in rows]}

        def sink(ctx, rows):
            return {"total": sum(rows)}
        """,
    )
    result = materialize(written)
    assert result.success, result
    assert keys(result) == ["sink/total", "src/rows"]
    assert result.output_for_node("sink_graph.sink", "total") == 12


def test_an_op_node_feeding_two_assets_merges_them_into_one_multi_asset(project):
    written = project(
        """
        [nodes.src]
        fn = "MOD.src"
        outputs = ["rows"]

        [nodes.shared]
        fn = "MOD.shared"
        inputs = { rows = "src.rows" }
        outputs = ["out"]
        asset = false

        [nodes.left]
        fn = "MOD.total"
        inputs = { rows = "shared.out" }
        outputs = ["value"]

        [nodes.right]
        fn = "MOD.count"
        inputs = { rows = "shared.out" }
        outputs = ["value"]
        """,
        module="""
        def src(ctx):
            return {"rows": [1, 2, 3]}

        def shared(ctx, rows):
            return {"out": [r * 10 for r in rows]}

        def total(ctx, rows):
            return {"value": sum(rows)}

        def count(ctx, rows):
            return {"value": len(rows)}
        """,
    )
    result = materialize(written)
    assert result.success, result
    assert keys(result) == ["left/value", "right/value", "src/rows"]
    assert result.output_for_node("left_right_graph.left", "value") == 60
    assert result.output_for_node("left_right_graph.right", "value") == 3


def test_an_op_node_can_carry_after_and_variables(project):
    written = project(
        """
        [vars]
        factor = { type = "int", default = 2 }

        [nodes.gate]
        fn = "MOD.gate"
        outputs = ["ok"]

        [nodes.src]
        fn = "MOD.src"
        outputs = ["rows"]

        [nodes.scaled]
        fn = "MOD.scale"
        inputs = { rows = "src.rows" }
        outputs = ["out"]
        after = ["gate"]
        asset = false

        [nodes.sink]
        fn = "MOD.sink"
        inputs = { rows = "scaled.out" }
        outputs = ["total"]
        """,
        module="""
        def gate(ctx):
            return {"ok": True}

        def src(ctx):
            return {"rows": [1, 2, 3]}

        def scale(ctx, rows):
            return {"out": [r * ctx.vars["factor"] for r in rows]}

        def sink(ctx, rows):
            return {"total": sum(rows)}
        """,
        runs="[runs.big]\nfactor = 10\n",
    )
    result = materialize(written, run_name="big")
    assert result.success, result
    # The run preset reached a node nested one level down in the config tree.
    assert result.output_for_node("sink_graph.sink", "total") == 60
    steps = {e.step_key for e in result.all_events if e.event_type_value == "STEP_START"}
    assert "gate" in steps and "sink_graph.scaled" in steps


def test_an_op_node_may_consume_an_artifact(project):
    written = project(
        """
        [artifacts."raw/rows"]
        kind = "file"
        path = "raw/rows.txt"

        [nodes.extract]
        fn = "MOD.extract"
        outputs = ["rows"]
        artifacts = { rows = "raw/rows" }

        [nodes.parsed]
        fn = "MOD.parse"
        inputs = { path = "raw/rows" }
        outputs = ["out"]
        asset = false

        [nodes.sink]
        fn = "MOD.sink"
        inputs = { rows = "parsed.out" }
        outputs = ["total"]
        """,
        module="""
        def extract(ctx) -> None:
            ctx.artifact("raw/rows").write_text("1\\n2\\n3\\n")

        def parse(ctx, path):
            return {"out": [int(line) for line in path.read_text().split()]}

        def sink(ctx, rows):
            return {"total": sum(rows)}
        """,
    )
    result = materialize(written)
    assert result.success, result
    assert keys(result) == ["raw/rows", "sink/total"]
    assert result.output_for_node("sink_graph.sink", "total") == 6
