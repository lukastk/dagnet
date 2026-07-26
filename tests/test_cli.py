"""The `dagnet` command line."""

from __future__ import annotations

import pytest

from dagnet.cli import EXIT_FAILED, EXIT_OK, EXIT_USAGE, main

GOOD_MANIFEST = """
    [nodes.extract]
    fn = "MOD.extract"
    outputs = ["rows"]

    [nodes.load]
    fn = "MOD.load"
    inputs = { rows = "extract.rows" }
    outputs = ["n"]
"""

GOOD_MODULE = """
    def extract(ctx):
        return {"rows": [1, 2, 3]}

    def load(ctx, rows):
        return {"n": len(rows)}
"""


def run(argv, project=None):
    if project is not None:
        argv = argv + ["--manifest", str(project.manifest_path)]
        for path in project.runs_paths:
            argv += ["--runs", str(path)]
    return main(argv)


# --- check -----------------------------------------------------------------


def test_check_is_quiet_and_zero_on_a_clean_project(project, capsys):
    written = project(GOOD_MANIFEST, module=GOOD_MODULE)
    assert run(["check"], written) == EXIT_OK
    assert capsys.readouterr().out.strip() == "no problems found"


def test_check_prints_every_diagnostic_and_exits_one(project, capsys):
    written = project("""
        [nodes.a]
        fn = "m.a"
        inputs = { x = "ghost.out" }
        outputs = ["out"]
        pool = "nosuch"
    """)
    assert run(["check"], written) == EXIT_FAILED
    out = capsys.readouterr().out
    assert "unknown-pool" in out and "unresolved-input" in out
    # the un-importable `fn` is the third — one pass never hides another
    assert "fn-not-importable" in out
    assert "3 error(s)" in out


def test_a_missing_manifest_is_a_usage_error(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    assert main(["check"]) == EXIT_USAGE
    assert "no manifest found" in capsys.readouterr().err


def test_the_manifest_and_runs_file_are_found_by_convention(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    monkeypatch.syspath_prepend(str(tmp_path))
    (tmp_path / "conv_nodes.py").write_text("def a(ctx):\n    return {'out': 1}\n")
    (tmp_path / "pipeline.toml").write_text(
        '[pipeline]\nname = "p"\n\n[vars]\nn = { type = "int", default = 1 }\n'
        '\n[nodes.a]\nfn = "conv_nodes.a"\noutputs = ["out"]\n'
    )
    (tmp_path / "runs.toml").write_text("[runs.small]\nn = 2\n")
    assert main(["check"]) == EXIT_OK
    assert capsys.readouterr().out.strip() == "no problems found"


# --- run -------------------------------------------------------------------


def test_run_materializes_and_exits_zero(project):
    written = project(GOOD_MANIFEST, module=GOOD_MODULE)
    assert run(["run", "--ephemeral"], written) == EXIT_OK


def test_run_refuses_to_launch_a_manifest_that_does_not_check(project, capsys):
    written = project("""
        [nodes.a]
        fn = "m.a"
        inputs = { x = "ghost.out" }
        outputs = ["out"]
    """)
    assert run(["run", "--ephemeral"], written) == EXIT_FAILED
    assert "unresolved-input" in capsys.readouterr().err


def test_run_exits_one_when_a_node_raises(project):
    written = project(
        """
        [nodes.a]
        fn = "MOD.a"
        outputs = ["out"]
        """,
        module="def a(ctx):\n    raise RuntimeError('boom')\n",
    )
    assert run(["run", "--ephemeral"], written) == EXIT_FAILED


def test_an_unknown_run_name_is_a_usage_error(project, capsys):
    written = project(GOOD_MANIFEST, module=GOOD_MODULE, runs="[runs.small]\n")
    assert run(["run", "nope", "--ephemeral"], written) == EXIT_USAGE
    err = capsys.readouterr().err
    assert "no run named 'nope'" in err and "known runs: small" in err


def test_ephemeral_warns_that_pool_limits_are_not_enforced(project, capsys):
    """FINDINGS.md spike (a): an ephemeral instance cannot enforce pools at all."""
    written = project(
        """
        [pools]
        heavy = 1

        [nodes.a]
        fn = "MOD.a"
        outputs = ["out"]
        pool = "heavy"
        """,
        module="def a(ctx):\n    return {'out': 1}\n",
    )
    assert run(["run", "--ephemeral"], written) == EXIT_OK
    assert "are NOT enforced" in capsys.readouterr().err


def test_a_persistent_run_writes_a_dagster_home_and_syncs_pool_limits(project):
    import dagster as dg

    written = project(
        """
        [pools]
        heavy = 2

        [nodes.a]
        fn = "MOD.a"
        outputs = ["out"]
        pool = "heavy"
        """,
        module="def a(ctx):\n    return {'out': 1}\n",
    )
    assert run(["run"], written) == EXIT_OK
    home = written.root / ".dagster"
    assert (home / "dagster.yaml").read_text().count("granularity: op") == 1
    with dg.DagsterInstance.from_config(str(home)) as instance:
        limits = {p.name: p.limit for p in instance.event_log_storage.get_pool_limits()}
    assert limits["heavy"] == 2


def test_from_failure_resumes_and_skips_the_steps_that_already_passed(project):
    written = project(
        """
        [nodes.good]
        fn = "MOD.good"
        outputs = ["out"]

        [nodes.flaky]
        fn = "MOD.flaky"
        inputs = { x = "good.out" }
        outputs = ["out"]
        """,
        module="""
        from pathlib import Path

        MARKER = Path(__file__).with_suffix(".marker")
        LOG = Path(__file__).with_suffix(".log")

        def good(ctx):
            LOG.write_text(LOG.read_text() + "good\\n" if LOG.exists() else "good\\n")
            return {"out": 1}

        def flaky(ctx, x):
            if not MARKER.exists():
                MARKER.write_text("failed once")
                raise RuntimeError("boom")
            return {"out": x + 1}
        """,
    )
    assert run(["run"], written) == EXIT_FAILED
    assert run(["run", "--from-failure", "last"], written) == EXIT_OK
    # `good` succeeded the first time, so the resumed run must not have re-run it.
    log = (written.root / f"{written.module}.log").read_text()
    assert log.count("good") == 1, log


# --- graph -----------------------------------------------------------------


def test_graph_prints_mermaid_without_importing_node_functions(project, capsys):
    written = project(GOOD_MANIFEST)  # note: no module written, so `fn` won't import
    assert run(["graph"], written) == EXIT_OK
    out = capsys.readouterr().out
    assert out.startswith("flowchart LR")
    assert "n_extract" in out and "n_load" in out


def test_graph_can_write_to_a_file(project, tmp_path):
    written = project(GOOD_MANIFEST)
    target = tmp_path / "graph.mmd"
    assert run(["graph", "-o", str(target), "--direction", "TB"], written) == EXIT_OK
    assert target.read_text().startswith("flowchart TB")


def test_graph_still_reports_a_broken_manifest(project, capsys):
    written = project("""
        [nodes.a]
        fn = "m.a"
        inputs = { x = "ghost.out" }
        outputs = ["out"]
    """)
    assert run(["graph"], written) == EXIT_FAILED


# --- dev -------------------------------------------------------------------


def test_dev_generates_the_three_line_defs_module(project, monkeypatch):
    """DESIGN §4: the only Dagster-touching file in a consumer repo, generated."""
    calls = {}

    def fake_call(command, env=None):
        calls["command"] = command
        calls["env"] = env
        return 0

    monkeypatch.setattr("dagnet.cli.subprocess.call", fake_call)
    written = project(GOOD_MANIFEST, module=GOOD_MODULE)
    assert run(["dev"], written) == EXIT_OK

    defs_path = written.root / ".dagster" / "defs.py"
    assert calls["command"] == ["dagster", "dev", "-f", str(defs_path)]
    assert calls["env"]["DAGSTER_HOME"] == str(written.root / ".dagster")
    assert "dagnet.build(" in defs_path.read_text()


def test_dev_refuses_to_serve_a_manifest_that_does_not_check(project, monkeypatch):
    monkeypatch.setattr(
        "dagnet.cli.subprocess.call", lambda *a, **k: pytest.fail("should not launch")
    )
    written = project('[nodes.a]\nfn = "m.a"\ninputs = { x = "ghost.out" }\noutputs = ["out"]\n')
    assert run(["dev"], written) == EXIT_FAILED
