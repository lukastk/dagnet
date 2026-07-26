"""`dagnet check`: every problem reported at once, each with a location."""

from __future__ import annotations

import pytest

from dagnet.check import check


@pytest.fixture(name="project")
def checked_project(project):
    """Write a throwaway project and return its `check` result."""

    def _check(manifest: str, module: str | None = None, runs: str | None = None, **kwargs):
        written = project(manifest, module, runs)
        kwargs.setdefault("import_functions", module is not None)
        return check(written.manifest_path, written.runs_paths, **kwargs)

    return _check


# --- the headline behaviour ------------------------------------------------


def test_reports_every_problem_at_once_rather_than_the_first(project):
    result = project("""
        [pools]
        heavy = 1

        [nodes.a]
        fn = "m.a"
        outputs = ["out"]
        pool = "nosuch"

        [nodes.b]
        fn = "m.b"
        inputs = { x = "a.missing" }
        outputs = ["out"]
        after = ["ghost"]

        [nodes.c]
        fn = "m.c"
        outputs = []
    """)
    assert set(result.diagnostics.codes()) >= {
        "unknown-pool",
        "unresolved-input",
        "unknown-after",
        "no-outputs",
    }


def test_a_clean_manifest_has_no_diagnostics(project):
    result = project(
        """
        [nodes.extract]
        fn = "MOD.extract"
        outputs = ["rows"]

        [nodes.load]
        fn = "MOD.load"
        inputs = { rows = "extract.rows" }
        outputs = ["done"]
        """,
        module="""
        def extract(ctx):
            return {"rows": []}

        def load(ctx, rows):
            return {"done": True}
        """,
    )
    assert result.ok, result.diagnostics.render()
    assert result.diagnostics.items == []


def test_diagnostics_point_at_the_exact_manifest_key(project):
    result = project("""
        [nodes.a]
        fn = "m.a"
        outputs = ["out"]

        [nodes.b]
        fn = "m.b"
        inputs = { rows = "a.nope" }
        outputs = ["out"]
    """)
    assert result.diagnostics.errors[0].location.path == "nodes.b.inputs.rows"


# --- wiring ----------------------------------------------------------------


@pytest.mark.parametrize(
    "ref,fragment",
    [
        ("ghost.out", "references node 'ghost', which does not exist"),
        ("a.nope", "references output 'nope' of node 'a'"),
        ("bare_name", "neither <node>.<output> nor a declared artifact key"),
    ],
)
def test_unresolvable_input_says_which_half_is_wrong(project, ref, fragment):
    result = project(f"""
        [nodes.a]
        fn = "m.a"
        outputs = ["out"]

        [nodes.b]
        fn = "m.b"
        inputs = {{ x = "{ref}" }}
        outputs = ["out"]
    """)
    assert result.diagnostics.codes() == ["unresolved-input"]
    assert fragment in result.diagnostics.errors[0].message


def test_a_near_miss_gets_a_did_you_mean_hint(project):
    result = project("""
        [nodes.extract]
        fn = "m.a"
        outputs = ["rows"]

        [nodes.b]
        fn = "m.b"
        inputs = { x = "extract.row" }
        outputs = ["out"]
    """)
    assert "did you mean 'rows'?" in result.diagnostics.errors[0].hint


def test_a_node_cannot_consume_its_own_output(project):
    result = project("""
        [nodes.a]
        fn = "m.a"
        inputs = { x = "a.out" }
        outputs = ["out"]
    """)
    assert "self-input" in result.diagnostics.codes()


def test_a_cycle_is_reported_once_with_its_path(project):
    result = project("""
        [nodes.a]
        fn = "m.a"
        inputs = { x = "b.out" }
        outputs = ["out"]

        [nodes.b]
        fn = "m.b"
        inputs = { x = "a.out" }
        outputs = ["out"]
    """)
    assert result.diagnostics.codes() == ["cycle"]
    assert "a -> b -> a" in result.diagnostics.errors[0].message


# --- artifacts -------------------------------------------------------------


def test_artifact_input_creates_a_dependency_on_its_producer(project):
    result = project(
        """
        [artifacts."db/file"]
        kind = "file"
        path = "w.duckdb"

        [artifacts."db/drugs"]
        kind = "duckdb_table"
        table = "drugs"
        database = "db/file"

        [nodes.load]
        fn = "MOD.load"
        outputs = ["drugs"]
        artifacts = { drugs = "db/drugs" }

        [nodes.report]
        fn = "MOD.report"
        inputs = { drugs = "db/drugs" }
        outputs = ["done"]
        """,
        module="""
        def load(ctx) -> None:
            pass

        def report(ctx, drugs):
            return {"done": True}
        """,
    )
    assert result.ok, result.diagnostics.render()
    assert result.graph.data_deps["report"] == {"load"}


def test_consuming_an_artifact_nobody_produces_is_an_error(project):
    result = project("""
        [artifacts."raw/file"]
        kind = "file"
        path = "raw.json"

        [nodes.read]
        fn = "m.read"
        inputs = { f = "raw/file" }
        outputs = ["out"]
    """)
    assert result.diagnostics.codes() == ["unproduced-input"]


def test_two_nodes_producing_one_artifact_is_an_error(project):
    result = project("""
        [artifacts."db/file"]
        kind = "file"
        path = "w.duckdb"

        [artifacts."db/t"]
        kind = "duckdb_table"
        table = "t"
        database = "db/file"

        [nodes.a]
        fn = "m.a"
        outputs = ["t"]
        artifacts = { t = "db/t" }

        [nodes.b]
        fn = "m.b"
        outputs = ["t"]
        artifacts = { t = "db/t" }
    """)
    assert "duplicate-artifact-producer" in result.diagnostics.codes()


def test_binding_an_undeclared_artifact_or_output_is_an_error(project):
    result = project("""
        [nodes.a]
        fn = "m.a"
        outputs = ["out"]
        artifacts = { nope = "db/missing" }
    """)
    assert result.diagnostics.codes() == ["unknown-output", "unknown-artifact"]


def test_two_outputs_compiling_to_one_asset_key_is_an_error(project):
    result = project("""
        [artifacts."b/out"]
        kind = "file"
        path = "x.json"

        [nodes.a]
        fn = "m.a"
        outputs = ["out"]
        artifacts = { out = "b/out" }

        [nodes.b]
        fn = "m.b"
        outputs = ["out"]
    """)
    assert "asset-key-collision" in result.diagnostics.codes()


# --- pools -----------------------------------------------------------------


def test_unknown_pool_is_an_error_and_an_unused_pool_is_a_warning(project):
    result = project("""
        [pools]
        heavy = 1

        [nodes.a]
        fn = "m.a"
        outputs = ["out"]
        pool = "hevy"
    """)
    assert result.diagnostics.codes() == ["unknown-pool", "unused-pool"]
    assert "did you mean 'heavy'?" in result.diagnostics.errors[0].hint


def test_a_zero_pool_limit_is_an_error(project):
    result = project("""
        [pools]
        heavy = 0

        [nodes.a]
        fn = "m.a"
        outputs = ["out"]
        pool = "heavy"
    """)
    assert "invalid-pool-limit" in result.diagnostics.codes()


# --- asset = false ---------------------------------------------------------


def test_an_op_node_no_asset_consumes_is_dead_code(project):
    result = project("""
        [nodes.a]
        fn = "m.a"
        outputs = ["out"]

        [nodes.transient]
        fn = "m.t"
        inputs = { x = "a.out" }
        outputs = ["out"]
        asset = false
    """)
    assert result.diagnostics.codes() == ["orphan-op-node"]


def test_an_op_node_feeding_two_assets_warns_about_the_merge(project):
    result = project("""
        [nodes.src]
        fn = "m.s"
        outputs = ["out"]

        [nodes.transient]
        fn = "m.t"
        inputs = { x = "src.out" }
        outputs = ["out"]
        asset = false

        [nodes.left]
        fn = "m.l"
        inputs = { x = "transient.out" }
        outputs = ["out"]

        [nodes.right]
        fn = "m.r"
        inputs = { x = "transient.out" }
        outputs = ["out"]
    """)
    assert result.diagnostics.codes() == ["op-node-merges-assets"]
    assert result.ok


def test_an_op_node_cannot_have_checks_or_artifacts(project):
    result = project("""
        [artifacts."db/file"]
        kind = "file"
        path = "w.duckdb"

        [artifacts."db/t"]
        kind = "duckdb_table"
        table = "t"
        database = "db/file"

        [nodes.src]
        fn = "m.s"
        outputs = ["out"]

        [nodes.transient]
        fn = "m.t"
        inputs = { x = "src.out" }
        outputs = ["out"]
        artifacts = { out = "db/t" }
        checks = { out = ["m.check"] }
        asset = false

        [nodes.sink]
        fn = "m.k"
        inputs = { x = "transient.out" }
        outputs = ["out"]
    """)
    assert set(result.diagnostics.codes()) >= {"op-node-checks", "op-node-artifacts"}


# --- function signatures ---------------------------------------------------


def test_missing_and_extra_parameters_are_reported_separately(project):
    result = project(
        """
        [nodes.a]
        fn = "MOD.a"
        outputs = ["out"]

        [nodes.b]
        fn = "MOD.b"
        inputs = { rows = "a.out" }
        outputs = ["out"]
        """,
        module="""
        def a(ctx):
            return {"out": 1}

        def b(ctx, wrong_name):
            return {"out": 1}
        """,
    )
    assert set(result.diagnostics.codes()) == {
        "signature-missing-parameter",
        "signature-extra-parameter",
    }


def test_a_renamed_input_is_correct_when_the_parameter_matches(project):
    """netrun's rename case: output `successful_ad_ids` -> parameter `ad_ids`."""
    result = project(
        """
        [nodes.filter]
        fn = "MOD.filter_"
        outputs = ["successful_ad_ids"]

        [nodes.rerank]
        fn = "MOD.rerank"
        inputs = { ad_ids = "filter.successful_ad_ids" }
        outputs = ["ad_ids"]
        """,
        module="""
        async def filter_(ctx) -> {'successful_ad_ids': list[int]}:
            return {"successful_ad_ids": []}

        async def rerank(ctx, ad_ids: list[int]) -> {'ad_ids': list[int]}:
            return {"ad_ids": ad_ids}
        """,
    )
    assert result.ok, result.diagnostics.render()


def test_a_missing_ctx_parameter_suppresses_the_offset_parameter_errors(project):
    result = project(
        """
        [nodes.a]
        fn = "MOD.a"
        inputs = {}
        outputs = ["out"]
        """,
        module="def a(rows):\n    return {'out': 1}\n",
    )
    assert result.diagnostics.codes() == ["missing-ctx-parameter"]


def test_variadic_parameters_are_rejected(project):
    result = project(
        """
        [nodes.a]
        fn = "MOD.a"
        outputs = ["out"]
        """,
        module="def a(ctx, **kwargs):\n    return {'out': 1}\n",
    )
    assert "signature-var-params" in result.diagnostics.codes()


@pytest.mark.parametrize(
    "path,code",
    [
        ("no_such_module.main", "fn-not-importable"),
        ("MOD.missing", "fn-missing-attribute"),
        ("MOD.not_a_function", "fn-not-callable"),
    ],
)
def test_import_failures_have_distinct_codes(project, path, code):
    result = project(
        f"""
        [nodes.a]
        fn = "{path}"
        outputs = ["out"]
        """,
        module="not_a_function = 3\n",
    )
    assert result.diagnostics.codes() == [code]


def test_a_check_function_that_does_not_import_is_an_error(project):
    result = project(
        """
        [nodes.a]
        fn = "MOD.a"
        outputs = ["out"]
        checks = { out = ["MOD.no_such_check"] }
        """,
        module="def a(ctx):\n    return {'out': 1}\n",
    )
    assert result.diagnostics.codes() == ["check-missing-attribute"]


# --- return annotations ----------------------------------------------------


def test_return_annotation_must_name_the_declared_outputs(project):
    result = project(
        """
        [nodes.a]
        fn = "MOD.a"
        outputs = ["left", "right"]
        """,
        module="def a(ctx) -> {'left': int}:\n    return {'left': 1}\n",
    )
    assert result.diagnostics.codes() == ["return-annotation-mismatch"]
    assert "[left, right]" in result.diagnostics.errors[0].message


def test_artifact_bound_outputs_are_excluded_from_the_return_annotation(project):
    """DESIGN §7 rule 2: artifact outputs are written, not returned."""
    result = project(
        """
        [artifacts."db/file"]
        kind = "file"
        path = "w.duckdb"

        [artifacts."db/t"]
        kind = "duckdb_table"
        table = "t"
        database = "db/file"

        [nodes.a]
        fn = "MOD.a"
        outputs = ["rows", "table"]
        artifacts = { table = "db/t" }
        """,
        module="def a(ctx) -> {'rows': list}:\n    return {'rows': []}\n",
    )
    assert result.ok, result.diagnostics.render()


def test_a_none_annotation_is_only_valid_with_no_value_outputs(project):
    result = project(
        """
        [nodes.a]
        fn = "MOD.a"
        outputs = ["rows"]
        """,
        module="def a(ctx) -> None:\n    return None\n",
    )
    assert result.diagnostics.codes() == ["return-annotation-mismatch"]


def test_an_uncomparable_return_annotation_only_warns(project):
    result = project(
        """
        [nodes.a]
        fn = "MOD.a"
        outputs = ["rows"]
        """,
        module="def a(ctx) -> list[int]:\n    return []\n",
    )
    assert result.diagnostics.codes() == ["return-annotation-unrecognised"]
    assert result.ok


def test_disagreeing_annotations_across_an_edge_only_warn(project):
    result = project(
        """
        [nodes.a]
        fn = "MOD.a"
        outputs = ["out"]

        [nodes.b]
        fn = "MOD.b"
        inputs = { x = "a.out" }
        outputs = ["out"]
        """,
        module="""
        def a(ctx) -> {'out': list[int]}:
            return {"out": []}

        def b(ctx, x: dict[str, int]) -> {'out': int}:
            return {"out": 1}
        """,
    )
    assert result.diagnostics.codes() == ["type-mismatch"]
    assert result.ok


# --- run presets -----------------------------------------------------------


def test_run_keys_must_name_declared_variables(project):
    result = project(
        """
        [vars]
        sample_n = { type = "int", default = 10 }

        [nodes.a]
        fn = "m.a"
        outputs = ["out"]
        """,
        runs="[runs.test]\nsample_nn = 5\n",
    )
    assert result.diagnostics.codes() == ["unknown-var"]
    assert "did you mean 'sample_n'?" in result.diagnostics.errors[0].hint


def test_run_values_must_match_declared_types(project):
    result = project(
        """
        [vars]
        sample_n = { type = "int", default = 10 }

        [nodes.a]
        fn = "m.a"
        outputs = ["out"]
        """,
        runs='[runs.test]\nsample_n = "lots"\n',
    )
    assert result.diagnostics.codes() == ["var-type-mismatch"]


def test_a_bool_is_not_an_int(project):
    result = project(
        """
        [vars]
        sample_n = { type = "int", default = 10 }

        [nodes.a]
        fn = "m.a"
        outputs = ["out"]
        """,
        runs="[runs.test]\nsample_n = true\n",
    )
    assert result.diagnostics.codes() == ["var-type-mismatch"]


def test_a_per_node_subtable_must_name_a_node(project):
    result = project(
        """
        [nodes.a]
        fn = "m.a"
        outputs = ["out"]

        [nodes.a.vars]
        chunk = { type = "int", default = 1 }
        """,
        runs="[runs.test.aa]\nchunk = 2\n",
    )
    assert result.diagnostics.codes() == ["unknown-run-node"]


def test_a_run_that_leaves_a_required_variable_unset_is_an_error(project):
    result = project(
        """
        [vars]
        llm_model = { type = "str" }

        [nodes.a]
        fn = "m.a"
        outputs = ["out"]
        """,
        runs="[runs.test]\n",
    )
    assert result.diagnostics.codes() == ["unfilled-var"]


def test_a_node_local_variable_may_be_overridden_per_node(project):
    result = project(
        """
        [vars]
        sample_n = { type = "int", default = 10 }

        [nodes.a]
        fn = "m.a"
        outputs = ["out"]

        [nodes.a.vars]
        chunk = { type = "int", default = 512 }
        """,
        runs="[defaults]\nsample_n = 100\n[defaults.a]\nchunk = 8\n[runs.test]\nsample_n = 1\n",
    )
    assert result.ok, result.diagnostics.render()


# --- artifact databases (DESIGN §5.4) --------------------------------------


def test_a_table_artifact_must_name_a_declared_database(project):
    result = project("""
        [artifacts."db/t"]
        kind = "duckdb_table"
        table = "t"
        database = "db/nope"

        [nodes.a]
        fn = "m.a"
        outputs = ["t"]
        artifacts = { t = "db/t" }
    """)
    assert "unknown-artifact" in result.diagnostics.codes()
    assert result.diagnostics.errors[0].location.path == 'artifacts."db/t".database'


def test_a_table_artifacts_database_must_be_a_file(project):
    result = project("""
        [artifacts."db/one"]
        kind = "duckdb_table"
        table = "one"
        database = "db/two"

        [artifacts."db/two"]
        kind = "duckdb_table"
        table = "two"
        database = "db/one"

        [nodes.a]
        fn = "m.a"
        outputs = ["x", "y"]
        artifacts = { x = "db/one", y = "db/two" }
    """)
    assert result.diagnostics.codes().count("database-not-a-file") == 2


def test_a_table_artifact_without_a_database_is_a_structural_error(project):
    result = project("""
        [artifacts."db/t"]
        kind = "duckdb_table"
        table = "t"
    """)
    assert result.diagnostics.codes() == ["invalid-value"]
    assert "database" in result.diagnostics.errors[0].message


# --- check declarations, long form (DESIGN §5.5) ---------------------------


def test_both_check_forms_are_accepted_and_validated(project):
    result = project(
        """
        [nodes.a]
        fn = "MOD.a"
        outputs = ["rows"]
        checks = { rows = [
            "MOD.short_form",
            { fn = "MOD.long_form", blocking = false },
            { fn = "MOD.missing" },
        ] }
        """,
        module="""
        def a(ctx):
            return {"rows": [1]}

        def short_form(ctx, rows):
            return True

        def long_form(ctx, rows):
            return True
        """,
    )
    assert result.diagnostics.codes() == ["check-missing-attribute"]
    assert result.diagnostics.errors[0].location.path == "nodes.a.checks.rows[2]"
