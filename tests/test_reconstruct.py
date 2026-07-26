"""End-to-end multiprocess execution — the path `dagnet run` actually takes.

`execute_in_process` ignores a job's executor, so everything else in the test
suite runs in-process. This module is the one place that proves the real thing:
steps in separate subprocesses, rebuilt from the manifest through
`dagnet._reconstruct` (see `_dev/experiments/FINDINGS.md`, spike (b)).

Assertions live *inside* the node functions and evidence is written to files,
because reading a value back out of a finished multiprocess run goes through the
IO manager under the asset key, not the op output name.
"""

from __future__ import annotations

import os

import dagster as dg
import pytest

from dagnet._reconstruct import reconstructable_job


@pytest.fixture
def importable_in_subprocesses(tmp_path, monkeypatch):
    """Step subprocesses don't inherit `sys.path`, only the environment.

    Real projects don't need this: their package is installed into the venv the
    subprocess starts from.
    """
    existing = os.environ.get("PYTHONPATH")
    monkeypatch.setenv(
        "PYTHONPATH", f"{tmp_path}{os.pathsep}{existing}" if existing else str(tmp_path)
    )


def run_multiprocess(project, run_name=None) -> bool:
    home = project.root / ".dagster"
    home.mkdir(exist_ok=True)
    (home / "dagster.yaml").write_text("{}\n")
    job = reconstructable_job(
        manifest=str(project.manifest_path),
        runs=[str(p) for p in project.runs_paths],
        run_name=run_name,
        select=None,
        store_root=None,
        executor="multiprocess",
    )
    with dg.DagsterInstance.from_config(str(home)) as instance:
        with dg.execute_job(job, instance=instance, raise_on_error=False) as result:
            return result.success


def test_steps_run_in_separate_processes_and_values_survive_the_boundary(
    project, importable_in_subprocesses
):
    written = project(
        """
        [nodes.src]
        fn = "MOD.src"
        outputs = ["rows"]

        [nodes.left]
        fn = "MOD.branch"
        inputs = { rows = "src.rows" }
        outputs = ["out"]

        [nodes.right]
        fn = "MOD.branch"
        inputs = { rows = "src.rows" }
        outputs = ["out"]

        [nodes.collect]
        fn = "MOD.collect"
        inputs = { a = "left.out", b = "right.out" }
        outputs = ["done"]
        """,
        module="""
        import os
        from pathlib import Path

        PIDS = Path(__file__).parent / "pids.txt"

        def src(ctx):
            return {"rows": list(range(1000))}

        def branch(ctx, rows):
            # The list crossed a process boundary through the IO manager.
            assert rows == list(range(1000))
            return {"out": (os.getpid(), sum(rows))}

        def collect(ctx, a, b):
            assert a[1] == b[1] == sum(range(1000)), (a, b)
            PIDS.write_text(repr([a[0], b[0], os.getpid()]))
            return {"done": True}
        """,
    )
    assert run_multiprocess(written)
    pids = eval((written.root / "pids.txt").read_text())
    assert len(set(pids)) == 3, pids


def test_run_preset_variables_survive_the_process_boundary(project, importable_in_subprocesses):
    written = project(
        """
        [vars]
        sample_n = { type = "int", default = 1 }

        [nodes.only]
        fn = "MOD.only"
        outputs = ["done"]
        """,
        module="""
        def only(ctx):
            assert ctx.vars["sample_n"] == 42, dict(ctx.vars)
            assert ctx.run_name == "big", ctx.run_name
            return {"done": True}
        """,
        runs="[runs.big]\nsample_n = 42\n",
    )
    assert run_multiprocess(written, run_name="big")


def test_node_name_and_manifest_path_survive_the_process_boundary(
    project, importable_in_subprocesses
):
    """`ctx` is rebuilt in each step subprocess, so both must be right there too.

    The subprocess has no reason to share the parent's working directory, which
    is exactly why `ctx.manifest_path` is absolute.
    """
    written = project(
        """
        [nodes.alpha]
        fn = "MOD.alpha"
        outputs = ["out"]

        [nodes.beta]
        fn = "MOD.beta"
        inputs = { upstream = "alpha.out" }
        outputs = ["out"]
        """,
        module="""
        import os
        from pathlib import Path

        SEEN = Path(__file__).with_suffix(".seen")

        def _record(ctx):
            assert ctx.manifest_path.is_absolute(), ctx.manifest_path
            assert ctx.manifest_path.exists(), ctx.manifest_path
            with SEEN.open("a") as handle:
                handle.write(f"{ctx.node_name} {ctx.manifest_path} {os.getpid()}\\n")
            return {"out": True}

        def alpha(ctx):
            return _record(ctx)

        def beta(ctx, upstream):
            return _record(ctx)
        """,
    )
    assert run_multiprocess(written)

    lines = [
        line.split()
        for line in (written.root / f"{written.module}.seen").read_text().split("\n")
        if line
    ]
    assert [line[0] for line in lines] == ["alpha", "beta"]
    assert {line[1] for line in lines} == {str(written.manifest_path.resolve())}
    # Two distinct subprocesses, neither of which is this one.
    pids = {line[2] for line in lines}
    assert len(pids) == 2 and str(os.getpid()) not in pids
