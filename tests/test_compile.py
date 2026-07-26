"""Compiling to Dagster `Definitions`, and running what comes out (DESIGN §8)."""

from __future__ import annotations

from pathlib import Path

import dagster as dg
import pytest

from dagnet.compile import CompileError, build, build_job
from dagnet.diagnostics import CheckFailed
from dagnet.runs import BadEnvironmentValue, UnresolvedVariable


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


def test_a_duckdb_artifact_resolves_to_a_table_and_its_database(project):
    """DESIGN §5.4: the database's location is in the map, not in a constant."""
    written = project(
        """
        [artifacts."db/warehouse"]
        kind = "file"
        path = "build/w.duckdb"

        [artifacts."db/drugs"]
        kind = "duckdb_table"
        table = "drugs"
        database = "db/warehouse"

        [nodes.load]
        fn = "MOD.load"
        outputs = ["t"]
        artifacts = { t = "db/drugs" }
        """,
        module="""
        def load(ctx) -> None:
            location = ctx.artifact("db/drugs")
            assert location.table == "drugs", location
            assert location.database.name == "w.duckdb", location
            assert location.database.is_absolute(), location
            # The handle is frozen: resolving a location must not move it.
            try:
                location.table = "elsewhere"
            except Exception as exc:
                assert type(exc).__name__ == "FrozenInstanceError", exc
            else:
                raise AssertionError("TableLocation should be frozen")
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


# --- blocking vs advisory checks (DESIGN §5.5) -----------------------------


CHECKED_MODULE = """
    def a(ctx):
        return {"rows": [1, -2, 3]}

    def all_positive(ctx, rows):
        return {"passed": all(r > 0 for r in rows), "metadata": {"n": len(rows)}}
"""


def test_a_failing_check_blocks_by_default_and_fails_the_run(project):
    """Exit 0 on a violated schema contract is the silent failure we forbid."""
    written = project(
        """
        [nodes.a]
        fn = "MOD.a"
        outputs = ["rows"]
        checks = { rows = ["MOD.all_positive"] }
        """,
        module=CHECKED_MODULE,
    )
    result = materialize(written)
    assert not result.success
    assert [e.passed for e in result.get_asset_check_evaluations()] == [False]


def test_an_advisory_check_records_the_failure_and_lets_the_run_finish(project):
    written = project(
        """
        [nodes.a]
        fn = "MOD.a"
        outputs = ["rows"]
        checks = { rows = [{ fn = "MOD.all_positive", blocking = false }] }
        """,
        module=CHECKED_MODULE,
    )
    result = materialize(written)
    assert result.success
    evaluation = result.get_asset_check_evaluations()[0]
    assert evaluation.passed is False
    assert evaluation.severity is dg.AssetCheckSeverity.WARN


def test_a_blocking_check_stops_the_assets_downstream_of_it(project):
    written = project(
        """
        [nodes.a]
        fn = "MOD.a"
        outputs = ["rows"]
        checks = { rows = ["MOD.all_positive"] }

        [nodes.b]
        fn = "MOD.b"
        inputs = { rows = "a.rows" }
        outputs = ["total"]
        """,
        module=CHECKED_MODULE
        + """
    def b(ctx, rows):
        return {"total": sum(rows)}
""",
    )
    result = materialize(written)
    assert not result.success
    assert keys(result) == ["a/rows"]


def test_a_passing_blocking_check_does_not_disturb_anything(project):
    written = project(
        """
        [nodes.a]
        fn = "MOD.a"
        outputs = ["rows"]
        checks = { rows = ["MOD.all_positive"] }
        """,
        module="""
        def a(ctx):
            return {"rows": [1, 2, 3]}

        def all_positive(ctx, rows):
            return all(r > 0 for r in rows)
        """,
    )
    result = materialize(written)
    assert result.success
    assert [e.passed for e in result.get_asset_check_evaluations()] == [True]


# --- store_root (DESIGN §5.1) ----------------------------------------------


def test_file_artifacts_resolve_under_the_declared_store_root(project, tmp_path):
    written = project(
        pipeline='store_root = "build/data"',
        manifest="""
        [artifacts."raw/rows"]
        kind = "file"
        path = "rows.txt"

        [nodes.write]
        fn = "MOD.write"
        outputs = ["rows"]
        artifacts = { rows = "raw/rows" }
        """,
        module="""
        def write(ctx) -> None:
            ctx.artifact("raw/rows").write_text("ok")
        """,
    )
    assert materialize(written).success
    assert (written.root / "build" / "data" / "rows.txt").read_text() == "ok"


def test_the_store_root_override_beats_the_manifest_field(project, tmp_path):
    written = project(
        pipeline='store_root = "build/data"',
        manifest="""
        [artifacts."raw/rows"]
        kind = "file"
        path = "rows.txt"

        [nodes.write]
        fn = "MOD.write"
        outputs = ["rows"]
        artifacts = { rows = "raw/rows" }
        """,
        module="""
        def write(ctx) -> None:
            ctx.artifact("raw/rows").write_text("ok")
        """,
    )
    elsewhere = tmp_path / "elsewhere"
    job = build_job(
        str(written.manifest_path), [], store_root=str(elsewhere), executor="in_process"
    )
    home = written.root / ".dagster"
    home.mkdir(exist_ok=True)
    (home / "dagster.yaml").write_text("{}\n")
    with dg.DagsterInstance.from_config(str(home)) as instance:
        assert job.execute_in_process(instance=instance, raise_on_error=False).success
    assert (elsewhere / "rows.txt").read_text() == "ok"
    assert not (written.root / "build").exists()


# --- global retries default (DESIGN §5.1) ----------------------------------


RETRY_MANIFEST = """
    [nodes.inherits]
    fn = "MOD.a"
    outputs = ["out"]

    [nodes.overrides]
    fn = "MOD.a"
    outputs = ["out"]
    retries = { max = 1 }

    [nodes.opts_out]
    fn = "MOD.a"
    outputs = ["out"]
    retries = { max = 0 }
"""


def policies(project_files):
    defs = build(project_files.manifest_path)
    return {a.op.name: a.op.retry_policy for a in defs.assets}


def test_nodes_inherit_the_pipeline_retry_policy(project):
    written = project(
        pipeline="retries = { max = 3, wait_s = 10 }",
        manifest=RETRY_MANIFEST,
        module="def a(ctx):\n    return {'out': 1}\n",
    )
    found = policies(written)
    assert found["inherits"] == dg.RetryPolicy(max_retries=3, delay=10.0)


def test_a_node_override_replaces_the_whole_policy_rather_than_merging(project):
    """`max = 1` alone means wait_s goes back to 0, not 10."""
    written = project(
        pipeline="retries = { max = 3, wait_s = 10 }",
        manifest=RETRY_MANIFEST,
        module="def a(ctx):\n    return {'out': 1}\n",
    )
    found = policies(written)
    assert found["overrides"] == dg.RetryPolicy(max_retries=1, delay=0.0)
    assert found["opts_out"] == dg.RetryPolicy(max_retries=0, delay=0.0)


def test_no_retries_anywhere_means_no_retry_policy(project):
    written = project(
        RETRY_MANIFEST.replace("    retries = { max = 1 }\n", "").replace(
            "    retries = { max = 0 }\n", ""
        ),
        module="def a(ctx):\n    return {'out': 1}\n",
    )
    assert set(policies(written).values()) == {None}


def test_the_inherited_policy_actually_retries(project):
    written = project(
        pipeline="retries = { max = 2 }",
        manifest="""
        [nodes.flaky]
        fn = "MOD.flaky"
        outputs = ["out"]
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


# --- env-sourced variables (DESIGN §5.3) -----------------------------------


ENV_MANIFEST = """
    [vars]
    token = { type = "str", env = "DAGNET_TEST_TOKEN" }
    workers = { type = "int", env = "DAGNET_TEST_WORKERS", default = 1 }

    [nodes.a]
    fn = "MOD.a"
    outputs = ["seen"]
"""

ENV_MODULE = "def a(ctx):\n    return {'seen': dict(ctx.vars)}\n"


def test_a_variable_takes_its_value_from_the_named_environment_variable(project, monkeypatch):
    monkeypatch.setenv("DAGNET_TEST_TOKEN", "from-the-environment")
    monkeypatch.setenv("DAGNET_TEST_WORKERS", "8")
    written = project(ENV_MANIFEST, module=ENV_MODULE)
    result = materialize(written)
    assert result.success, result
    assert result.output_for_node("a", "seen") == {
        "token": "from-the-environment",
        "workers": 8,
    }


def test_a_run_value_beats_the_environment(project, monkeypatch):
    monkeypatch.setenv("DAGNET_TEST_TOKEN", "from-the-environment")
    written = project(
        ENV_MANIFEST, module=ENV_MODULE, runs='[runs.explicit]\ntoken = "from-the-run"\n'
    )
    result = materialize(written, run_name="explicit")
    assert result.output_for_node("a", "seen")["token"] == "from-the-run"


def test_the_environment_beats_a_declared_default(project, monkeypatch):
    monkeypatch.setenv("DAGNET_TEST_TOKEN", "t")
    monkeypatch.setenv("DAGNET_TEST_WORKERS", "16")
    written = project(ENV_MANIFEST, module=ENV_MODULE)
    assert materialize(written).output_for_node("a", "seen")["workers"] == 16


def test_a_declared_default_applies_when_the_environment_is_silent(project, monkeypatch):
    monkeypatch.setenv("DAGNET_TEST_TOKEN", "t")
    monkeypatch.delenv("DAGNET_TEST_WORKERS", raising=False)
    written = project(ENV_MANIFEST, module=ENV_MODULE)
    assert materialize(written).output_for_node("a", "seen")["workers"] == 1


def test_an_unset_variable_names_both_itself_and_the_env_var(project, monkeypatch):
    monkeypatch.delenv("DAGNET_TEST_TOKEN", raising=False)
    written = project(ENV_MANIFEST, module=ENV_MODULE)
    with pytest.raises(UnresolvedVariable) as excinfo:
        build_job(str(written.manifest_path), [], executor="in_process")
    message = str(excinfo.value)
    assert "'token'" in message and "DAGNET_TEST_TOKEN" in message


def test_an_env_value_of_the_wrong_type_is_a_loud_error(project, monkeypatch):
    monkeypatch.setenv("DAGNET_TEST_TOKEN", "t")
    monkeypatch.setenv("DAGNET_TEST_WORKERS", "loads")
    written = project(ENV_MANIFEST, module=ENV_MODULE)
    with pytest.raises(BadEnvironmentValue, match="DAGNET_TEST_WORKERS"):
        build_job(str(written.manifest_path), [], executor="in_process")


@pytest.mark.parametrize(
    "raw,expected", [("true", True), ("FALSE", False), ("1", True), ("off", False)]
)
def test_booleans_from_the_environment(project, monkeypatch, raw, expected):
    monkeypatch.setenv("DAGNET_TEST_FLAG", raw)
    written = project(
        """
        [vars]
        flag = { type = "bool", env = "DAGNET_TEST_FLAG" }

        [nodes.a]
        fn = "MOD.a"
        outputs = ["seen"]
        """,
        module="def a(ctx):\n    return {'seen': ctx.vars['flag']}\n",
    )
    assert materialize(written).output_for_node("a", "seen") is expected


def test_a_node_local_env_declaration_shadows_the_global_one(project, monkeypatch):
    monkeypatch.setenv("DAGNET_TEST_GLOBAL", "global-value")
    monkeypatch.setenv("DAGNET_TEST_NODE", "node-value")
    written = project(
        """
        [vars]
        endpoint = { type = "str", env = "DAGNET_TEST_GLOBAL" }

        [nodes.plain]
        fn = "MOD.show"
        outputs = ["seen"]

        [nodes.special]
        fn = "MOD.show"
        outputs = ["seen"]

        [nodes.special.vars]
        endpoint = { type = "str", env = "DAGNET_TEST_NODE" }
        """,
        module="def show(ctx):\n    return {'seen': ctx.vars['endpoint']}\n",
    )
    result = materialize(written)
    assert result.output_for_node("plain", "seen") == "global-value"
    assert result.output_for_node("special", "seen") == "node-value"


def test_defaults_apply_to_a_run_with_no_preset(project):
    """`[defaults]` is the base for every run, including one that names no preset."""
    written = project(
        """
        [vars]
        n = { type = "int", default = 1 }

        [nodes.a]
        fn = "MOD.a"
        outputs = ["seen"]
        """,
        module="def a(ctx):\n    return {'seen': ctx.vars['n']}\n",
        runs="[defaults]\nn = 99\n\n[runs.other]\nn = 5\n",
    )
    assert materialize(written).output_for_node("a", "seen") == 99


# --- ctx.node_name / ctx.manifest_path -------------------------------------


IDENTITY_MODULE = """
    def show(ctx):
        return {"seen": {"node": ctx.node_name, "manifest": str(ctx.manifest_path)}}
"""


def test_each_node_sees_its_own_name_and_the_manifest_path(project):
    written = project(
        """
        [nodes.first]
        fn = "MOD.show"
        outputs = ["seen"]

        [nodes.second]
        fn = "MOD.show"
        outputs = ["seen"]
        """,
        module=IDENTITY_MODULE,
    )
    result = materialize(written)
    assert result.success, result
    assert result.output_for_node("first", "seen")["node"] == "first"
    assert result.output_for_node("second", "seen")["node"] == "second"
    for node in ("first", "second"):
        seen = Path(result.output_for_node(node, "seen")["manifest"])
        assert seen == written.manifest_path.resolve()
        assert seen.is_absolute()


def test_the_manifest_path_is_absolute_even_when_the_caller_passed_a_relative_one(
    project, monkeypatch
):
    written = project(
        """
        [nodes.only]
        fn = "MOD.show"
        outputs = ["seen"]
        """,
        module=IDENTITY_MODULE,
    )
    monkeypatch.chdir(written.root)
    job = build_job(written.manifest_path.name, [], executor="in_process")
    home = written.root / ".dagster"
    home.mkdir(exist_ok=True)
    (home / "dagster.yaml").write_text("{}\n")
    with dg.DagsterInstance.from_config(str(home)) as instance:
        result = job.execute_in_process(instance=instance, raise_on_error=False)
    assert result.success, result
    assert (
        Path(result.output_for_node("only", "seen")["manifest"]) == written.manifest_path.resolve()
    )


def test_a_node_folded_into_a_graph_backed_asset_still_knows_its_own_name(project):
    """The op path builds its own ctx, so it needs its own coverage."""
    written = project(
        """
        [nodes.src]
        fn = "MOD.src"
        outputs = ["out"]

        [nodes.transient]
        fn = "MOD.record"
        inputs = { x = "src.out" }
        outputs = ["out"]
        asset = false

        [nodes.sink]
        fn = "MOD.record"
        inputs = { x = "transient.out" }
        outputs = ["out"]
        """,
        module="""
        def src(ctx):
            return {"out": []}

        def record(ctx, x):
            return {"out": x + [(ctx.node_name, ctx.manifest_path.name)]}
        """,
    )
    result = materialize(written)
    assert result.success, result
    assert result.output_for_node("sink_graph.sink", "out") == [
        ("transient", written.manifest_path.name),
        ("sink", written.manifest_path.name),
    ]


def test_a_check_sees_the_node_that_produced_what_it_is_checking(project):
    written = project(
        """
        [nodes.producer]
        fn = "MOD.producer"
        outputs = ["rows"]
        checks = { rows = ["MOD.records_identity"] }
        """,
        module="""
        from pathlib import Path

        SEEN = Path(__file__).with_suffix(".seen")

        def producer(ctx):
            return {"rows": [1, 2]}

        def records_identity(ctx, rows):
            SEEN.write_text(f"{ctx.node_name}|{ctx.manifest_path}")
            return True
        """,
    )
    result = materialize(written)
    assert result.success, result
    node, manifest = (written.root / f"{written.module}.seen").read_text().split("|")
    assert node == "producer"
    assert Path(manifest) == written.manifest_path.resolve()


def test_both_are_read_only(project):
    written = project(
        """
        [nodes.only]
        fn = "MOD.only"
        outputs = ["refused"]
        """,
        module="""
        def only(ctx):
            refused = []
            for attribute, value in (("node_name", "elsewhere"), ("manifest_path", "/tmp/x")):
                try:
                    setattr(ctx, attribute, value)
                except AttributeError:
                    refused.append(attribute)
            return {"refused": refused}
        """,
    )
    result = materialize(written)
    assert result.output_for_node("only", "refused") == ["node_name", "manifest_path"]
