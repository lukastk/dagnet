"""`[pipeline] pre_run` hooks: refusing a launch before anything materializes.

Hooks live in the generated node module, so each manifest names them
`<module>:<hook>` exactly as a real project would. The module records both what
the hooks were told and which nodes actually ran — the second is what makes
"aborts before any materialization" a real assertion rather than a hopeful one.
"""

from __future__ import annotations

import json

import pytest

from dagnet.check import check
from dagnet.cli import EXIT_FAILED, EXIT_OK, EXIT_USAGE, main

PIPELINE = """
    [nodes.raw]
    fn = "MOD.raw"
    outputs = ["rows"]

    [nodes.clean]
    fn = "MOD.clean"
    inputs = { rows = "raw.rows" }
    outputs = ["rows"]

    [nodes.report]
    fn = "MOD.report"
    inputs = { rows = "clean.rows" }
    outputs = ["summary"]
"""

MODULE = '''
    import json
    from pathlib import Path

    from dagnet.diagnostics import Diagnostics

    SAW = Path(__file__).with_suffix(".saw")
    RAN = Path(__file__).with_suffix(".ran")

    def _note(name):
        with RAN.open("a") as handle:
            handle.write(name + "\\n")

    def raw(ctx):
        _note("raw")
        return {"rows": [1, 2, 3]}

    def clean(ctx, rows):
        _note("clean")
        return {"rows": rows}

    def report(ctx, rows):
        _note("report")
        return {"summary": len(rows)}

    FLAKY = Path(__file__).with_suffix(".flaky")

    def flaky(ctx, rows):
        _note("flaky")
        if not FLAKY.exists():
            FLAKY.write_text("failed once")
            raise RuntimeError("first attempt fails")
        return {"summary": len(rows)}

    def record(context):
        """A hook that only observes."""
        SAW.write_text(json.dumps({
            "manifest": str(context.manifest_path),
            "run_name": context.run_name,
            "selection": context.selection,
            "is_everything": context.is_everything,
            "node_names": list(context.node_names),
            "asset_keys": list(context.asset_keys),
            "run_config_nodes": sorted(context.run_config.get("ops", {})),
            "is_resume": context.is_resume,
            "parent_run_id": context.parent_run_id,
        }))
        return Diagnostics()

    def refuse(context):
        diagnostics = Diagnostics()
        diagnostics.error(
            "dangerous-selection", "this selection would clobber history",
            context.location("pipeline.pre_run"),
            hint="narrow it with --select",
        )
        return diagnostics

    def grumble(context):
        diagnostics = Diagnostics()
        diagnostics.warning(
            "wide-selection", "this run touches everything",
            context.location("pipeline.pre_run"),
        )
        return diagnostics

    def refuse_on_resume(context):
        """The shape a real guard takes: resume is not the same as a fresh run."""
        diagnostics = Diagnostics()
        if context.is_resume:
            diagnostics.error(
                "no-resume", f"refusing to resume {context.parent_run_id}",
                context.location("pipeline.pre_run"),
            )
        return diagnostics

    def explode(context):
        raise RuntimeError("guard says no")

    def say_nothing(context):
        return None

    def return_junk(context):
        return "probably fine"

    SETUP = Path(__file__).with_suffix(".setup")

    def _setup(name):
        with SETUP.open("a") as handle:
            handle.write(name + "\\n")

    def clear_tables(context):
        """The shape dagnet-db's table-clearer takes."""
        _setup(f"clear:{context.is_resume}")
        return Diagnostics()

    def second_setup(context):
        _setup("second")
        return None

    def failing_setup(context):
        _setup("failing")
        raise RuntimeError("could not reach the warehouse")

    def refusing_setup(context):
        _setup("refusing")
        diagnostics = Diagnostics()
        diagnostics.error(
            "setup-failed", "the warehouse is read-only",
            context.location("pipeline.pre_execute"),
        )
        return diagnostics
'''

RUNS = """
    [runs.small]
    sample_n = 2
"""


@pytest.fixture
def hooked(project):
    """A three-node chain whose `[pipeline] pre_run` names the given hooks."""

    def _hooked(*hooks: str, runs: str | None = None):
        listed = ", ".join(f'"MOD:{hook}"' for hook in hooks)
        manifest = PIPELINE
        if runs is not None:
            manifest = '[vars]\nsample_n = { type = "int", default = 1 }\n' + PIPELINE
        return project(
            manifest,
            module=MODULE,
            runs=runs,
            pipeline=f"pre_run = [{listed}]" if hooks else "",
        )

    return _hooked


@pytest.fixture
def flaky_pipeline(project):
    """Like `hooked`, but the last node fails on its first attempt."""

    def _flaky(*hooks: str):
        listed = ", ".join(f'"MOD:{hook}"' for hook in hooks)
        return project(
            PIPELINE.replace('fn = "MOD.report"', 'fn = "MOD.flaky"'),
            module=MODULE,
            pipeline=f"pre_run = [{listed}]",
        )

    return _flaky


def launch(written, *argv, ephemeral: bool = True):
    argv = list(argv) + ["--manifest", str(written.manifest_path)]
    for path in written.runs_paths:
        argv += ["--runs", str(path)]
    if ephemeral:
        argv.insert(0, "--ephemeral")
    return main(["run", *argv])


def saw(written) -> dict:
    return json.loads((written.root / f"{written.module}.saw").read_text())


def steps_that_ran(written) -> list[str]:
    path = written.root / f"{written.module}.ran"
    return path.read_text().split() if path.exists() else []


# --- what the hook is told -------------------------------------------------


def test_a_plain_run_reports_everything(hooked):
    written = hooked("record")
    assert launch(written) == EXIT_OK
    seen = saw(written)
    assert seen["selection"] is None and seen["is_everything"] is True
    assert seen["node_names"] == ["clean", "raw", "report"]
    assert seen["asset_keys"] == ["clean/rows", "raw/rows", "report/summary"]
    assert seen["manifest"] == str(written.manifest_path.resolve())


def test_a_select_run_reports_the_resolved_selection_not_just_the_expression(hooked):
    written = hooked("record")
    assert launch(written, "--select", "+clean/rows") == EXIT_OK
    seen = saw(written)
    assert seen["selection"] == "+clean/rows"
    assert seen["is_everything"] is False
    # Resolved against the graph: `report` is downstream and out of scope.
    assert seen["node_names"] == ["clean", "raw"]
    assert seen["asset_keys"] == ["clean/rows", "raw/rows"]


def test_a_single_asset_selection_narrows_to_one_node(hooked):
    """The hook sees the one node, whatever the run then does.

    This launch goes on to fail, because selecting `clean/rows` alone asks
    Dagster to load `raw/rows` from a previous materialization and an ephemeral
    instance has none. That is expected, and beside the point: the hook is
    consulted before any of it.
    """
    written = hooked("record")
    launch(written, "--select", "clean/rows")
    assert saw(written)["node_names"] == ["clean"]
    assert saw(written)["asset_keys"] == ["clean/rows"]


def test_the_from_failure_path_is_gated_and_can_refuse_only_resumes(flaky_pipeline):
    """A hook can allow the first run and refuse the resume — the whole point of
    `is_resume`. The refused resume must not re-run a single step."""
    written = flaky_pipeline("refuse_on_resume")
    assert launch(written, ephemeral=False) == EXIT_FAILED  # fails at `flaky`
    after_first = steps_that_ran(written)
    assert after_first.count("flaky") == 1

    assert launch(written, "--from-failure", "last", ephemeral=False) == EXIT_FAILED
    assert steps_that_ran(written) == after_first, "a refused resume must re-run nothing"


def test_resuming_with_nothing_to_resume_is_a_usage_error(hooked):
    """The resume target is resolved before the hooks, so this is caught first —
    'no previous run' is a more useful message than anything a hook could say."""
    written = hooked("record")
    assert launch(written, "--from-failure", "last") == EXIT_USAGE


def test_a_plain_run_is_not_a_resume(hooked):
    written = hooked("record")
    assert launch(written) == EXIT_OK
    assert saw(written)["is_resume"] is False
    assert saw(written)["parent_run_id"] is None


def test_a_select_run_is_not_a_resume(hooked):
    written = hooked("record")
    assert launch(written, "--select", "+clean/rows") == EXIT_OK
    assert saw(written)["is_resume"] is False
    assert saw(written)["parent_run_id"] is None


def test_a_resume_says_so_and_names_the_run_it_resumes(flaky_pipeline):
    """A resume re-executes a subset of the selection, so a guard must be able
    to tell it apart from a deliberately narrow fresh run."""
    import dagster as dg

    written = flaky_pipeline("record")
    # First launch fails at `flaky`, leaving a failed run to resume from.
    assert launch(written, ephemeral=False) == EXIT_FAILED
    first = saw(written)
    assert first["is_resume"] is False

    home = written.root / ".dagster"
    with dg.DagsterInstance.from_config(str(home)) as instance:
        failed_run_id = instance.get_runs(limit=1)[0].run_id

    assert launch(written, "--from-failure", "last", ephemeral=False) == EXIT_OK
    resumed = saw(written)
    assert resumed["is_resume"] is True
    assert resumed["parent_run_id"] == failed_run_id


def test_the_run_preset_name_and_resolved_config_reach_the_hook(hooked):
    written = hooked("record", runs=RUNS)
    assert launch(written, "small") == EXIT_OK
    seen = saw(written)
    assert seen["run_name"] == "small"
    assert seen["run_config_nodes"] == ["clean", "raw", "report"]


# --- refusal ---------------------------------------------------------------


def test_an_error_aborts_before_anything_materializes(hooked):
    written = hooked("refuse")
    assert launch(written) == EXIT_FAILED
    assert steps_that_ran(written) == [], "a refused launch must not run a single node"


def test_the_refusal_prints_the_usual_aggregated_output(hooked, capsys):
    written = hooked("refuse")
    launch(written)
    err = capsys.readouterr().err
    assert "error[dangerous-selection]" in err
    assert "1 error(s), 0 warning(s)" in err
    assert "narrow it with --select" in err


def test_a_warning_prints_and_the_run_proceeds(hooked, capsys):
    written = hooked("grumble")
    assert launch(written) == EXIT_OK
    assert "warning[wide-selection]" in capsys.readouterr().err
    assert steps_that_ran(written) == ["raw", "clean", "report"]


def test_a_hook_that_raises_refuses_the_launch(hooked, capsys):
    written = hooked("explode")
    assert launch(written) == EXIT_FAILED
    assert steps_that_ran(written) == []
    err = capsys.readouterr().err
    assert "guard says no" in err and "RuntimeError" in err


def test_a_hook_may_return_nothing(hooked):
    written = hooked("say_nothing")
    assert launch(written) == EXIT_OK
    assert steps_that_ran(written) == ["raw", "clean", "report"]


def test_a_hook_returning_the_wrong_shape_is_loud(hooked, capsys):
    written = hooked("return_junk")
    assert launch(written) == EXIT_FAILED
    assert steps_that_ran(written) == []
    assert "must return a Diagnostics" in capsys.readouterr().err


def test_every_hook_runs_even_after_one_objects(hooked, capsys):
    written = hooked("refuse", "grumble", "record")
    assert launch(written) == EXIT_FAILED
    err = capsys.readouterr().err
    assert "dangerous-selection" in err and "wide-selection" in err
    # The third hook ran too, despite the first refusing.
    assert saw(written)["is_everything"] is True


def test_declaring_no_hooks_changes_nothing(hooked):
    written = hooked()
    assert launch(written) == EXIT_OK
    assert steps_that_ran(written) == ["raw", "clean", "report"]


# --- the multiprocess launch path ------------------------------------------


def test_the_default_multiprocess_path_is_gated_before_any_subprocess_starts(
    hooked, importable_in_subprocesses
):
    written = hooked("refuse")
    assert launch(written, ephemeral=False) == EXIT_FAILED
    assert steps_that_ran(written) == []


def test_the_multiprocess_path_proceeds_when_the_hook_is_satisfied(
    hooked, importable_in_subprocesses
):
    written = hooked("record")
    assert launch(written, ephemeral=False) == EXIT_OK
    assert sorted(steps_that_ran(written)) == ["clean", "raw", "report"]
    assert saw(written)["is_everything"] is True


# --- check-time validation -------------------------------------------------


def test_a_hook_that_does_not_import_is_a_check_error(project):
    written = project(PIPELINE, module=MODULE, pipeline='pre_run = ["nosuch.module:guard"]')
    result = check(written.manifest_path)
    assert result.diagnostics.codes() == ["pre-run-not-importable"]
    assert result.diagnostics.errors[0].location.path == "pipeline.pre_run[0]"


def test_a_dotted_hook_path_is_rejected_with_the_colon_form_suggested(project):
    written = project(PIPELINE, module=MODULE, pipeline='pre_run = ["pkg.module.guard"]')
    result = check(written.manifest_path)
    assert result.diagnostics.codes() == ["pre-run-malformed-path"]
    assert "did you mean 'pkg.module:guard'?" in result.diagnostics.errors[0].message


def test_a_hook_naming_a_missing_attribute_is_a_check_error(project):
    written = project(PIPELINE, module=MODULE, pipeline='pre_run = ["MOD:no_such_hook"]')
    result = check(written.manifest_path)
    assert result.diagnostics.codes() == ["pre-run-missing-attribute"]


# --- pre_execute: the side-effecting slot ----------------------------------


@pytest.fixture
def staged(project):
    """A chain with `pre_run` and/or `pre_execute` hooks declared."""

    def _staged(*, gates: tuple[str, ...] = (), setups: tuple[str, ...] = ()):
        lines = []
        if gates:
            lines.append("pre_run = [" + ", ".join(f'"MOD:{h}"' for h in gates) + "]")
        if setups:
            lines.append("pre_execute = [" + ", ".join(f'"MOD:{h}"' for h in setups) + "]")
        return project(PIPELINE, module=MODULE, pipeline="\n".join(lines))

    return _staged


def setups_that_ran(written) -> list[str]:
    path = written.root / f"{written.module}.setup"
    return path.read_text().split() if path.exists() else []


def test_pre_execute_runs_before_any_step(staged):
    written = staged(setups=("clear_tables",))
    assert launch(written) == EXIT_OK
    assert setups_that_ran(written) == ["clear:False"]
    assert steps_that_ran(written) == ["raw", "clean", "report"]


def test_pre_execute_runs_only_after_the_gate_fully_passes(staged):
    """A refused launch must never reach the side-effecting slot."""
    written = staged(gates=("refuse",), setups=("clear_tables",))
    assert launch(written) == EXIT_FAILED
    assert setups_that_ran(written) == [], "a refused launch must not clear anything"
    assert steps_that_ran(written) == []


def test_a_warning_from_the_gate_still_lets_setup_run(staged):
    written = staged(gates=("grumble",), setups=("clear_tables",))
    assert launch(written) == EXIT_OK
    assert setups_that_ran(written) == ["clear:False"]


def test_a_failing_setup_hook_aborts_before_any_step(staged):
    written = staged(setups=("failing_setup",))
    assert launch(written) == EXIT_FAILED
    assert steps_that_ran(written) == [], "nothing may execute after setup fails"


def test_a_setup_hook_returning_errors_aborts_before_any_step(staged, capsys):
    written = staged(setups=("refusing_setup",))
    assert launch(written) == EXIT_FAILED
    assert steps_that_ran(written) == []
    assert "error[setup-failed]" in capsys.readouterr().err


def test_setup_hooks_run_in_declared_order_and_stop_at_the_first_failure(staged):
    """Unlike `pre_run`, these change things: no running the next one on top of
    a half-applied one."""
    written = staged(setups=("clear_tables", "failing_setup", "second_setup"))
    assert launch(written) == EXIT_FAILED
    assert setups_that_ran(written) == ["clear:False", "failing"]
    assert "second" not in setups_that_ran(written)


def test_setup_hooks_all_run_when_none_fail(staged):
    written = staged(setups=("clear_tables", "second_setup"))
    assert launch(written) == EXIT_OK
    assert setups_that_ran(written) == ["clear:False", "second"]


def test_pre_execute_is_told_whether_this_is_a_resume(flaky_pipeline, project):
    """Acting differently on a resume is the hook's decision, so it must be told."""
    written = project(
        PIPELINE.replace('fn = "MOD.report"', 'fn = "MOD.flaky"'),
        module=MODULE,
        pipeline='pre_execute = ["MOD:clear_tables"]',
    )
    assert launch(written, ephemeral=False) == EXIT_FAILED
    assert setups_that_ran(written) == ["clear:False"]

    assert launch(written, "--from-failure", "last", ephemeral=False) == EXIT_OK
    assert setups_that_ran(written) == ["clear:False", "clear:True"]


def test_a_setup_hook_that_does_not_import_is_a_check_error(project):
    written = project(PIPELINE, module=MODULE, pipeline='pre_execute = ["nope.mod:setup"]')
    result = check(written.manifest_path)
    assert result.diagnostics.codes() == ["pre-execute-not-importable"]
    assert result.diagnostics.errors[0].location.path == "pipeline.pre_execute[0]"


def test_declaring_only_setup_hooks_needs_no_gate(staged):
    written = staged(setups=("clear_tables",))
    assert launch(written, ephemeral=False) == EXIT_OK
    assert setups_that_ran(written) == ["clear:False"]
