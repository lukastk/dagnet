"""Every sample project checks clean and runs — the samples are the spec.

These run in the repo's own venv rather than each sample's, so the sample's
`src/` goes on `sys.path` and the job runs in-process. That covers correctness;
the things in-process execution can't show — real subprocess isolation, pool
enforcement — are covered by `test_reconstruct.py` and the spikes.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import dagster as dg
import pytest

from dagnet.check import check
from dagnet.compile import build_job

SAMPLES = Path(__file__).parent.parent / "sample_projects"

#: (sample directory, run preset to execute) — None means "no preset".
CASES = [
    ("00_basic", None),
    ("01_fanout_join", None),
    ("02_ordering_after", None),
    ("03_artifacts", None),
    ("04_pools_retries", None),
    ("05_run_presets", "smoke"),
    ("05_run_presets", "test_api"),
    ("06_transient_ops", "strict"),
    ("07_checks", "good"),
    ("07_checks", "noisy"),
    ("08_pull_select", None),
]

#: Runs that are *supposed* to fail — a blocking check catching a real violation.
FAILING_CASES = [("07_checks", "bad")]

IDS = [f"{name}:{run or 'default'}" for name, run in CASES]


def sample_paths(name: str) -> tuple[Path, list[Path]]:
    root = SAMPLES / name
    manifest = root / "pipeline.toml"
    runs = [p for p in (root / "runs.toml", root / "runs") if p.exists()]
    return manifest, runs


@pytest.fixture
def on_path(monkeypatch):
    def _add(name: str) -> None:
        monkeypatch.syspath_prepend(str(SAMPLES / name / "src"))

    return _add


@pytest.mark.parametrize("name", sorted({name for name, _ in CASES}))
def test_every_sample_checks_clean(name, on_path):
    on_path(name)
    manifest, runs = sample_paths(name)
    result = check(manifest, runs)
    assert result.diagnostics.items == [], result.diagnostics.render()


@pytest.mark.parametrize("name,run_name", CASES, ids=IDS)
def test_every_sample_runs(name, run_name, on_path, tmp_path):
    on_path(name)
    manifest, runs = sample_paths(name)
    job = build_job(
        str(manifest),
        [str(p) for p in runs],
        run_name=run_name,
        store_root=str(tmp_path / "store"),
        executor="in_process",
    )
    home = tmp_path / "dagster"
    home.mkdir()
    (home / "dagster.yaml").write_text("{}\n")
    with dg.DagsterInstance.from_config(str(home)) as instance:
        result = job.execute_in_process(instance=instance, raise_on_error=False)
    assert result.success, [e.message for e in result.all_events if e.is_failure]


def test_the_checks_sample_actually_fails_its_contract_check(on_path, tmp_path):
    """07's whole point: the `bad` run violates the contract and the check says so."""
    on_path("07_checks")
    manifest, runs = sample_paths("07_checks")
    outcomes, successes = {}, {}
    for run_name in ("good", "bad"):
        job = build_job(
            str(manifest),
            [str(p) for p in runs],
            run_name=run_name,
            store_root=str(tmp_path / run_name),
            executor="in_process",
        )
        home = tmp_path / f"dagster_{run_name}"
        home.mkdir()
        (home / "dagster.yaml").write_text("{}\n")
        with dg.DagsterInstance.from_config(str(home)) as instance:
            result = job.execute_in_process(instance=instance, raise_on_error=False)
        outcomes[run_name] = {e.check_name: e.passed for e in result.get_asset_check_evaluations()}
        successes[run_name] = result.success

    assert all(outcomes["good"].values()), outcomes["good"]
    assert outcomes["bad"]["units_are_canonical"] is False
    assert outcomes["bad"]["no_missing_values"] is True
    # Blocking by default: the violation fails the run and stops what follows.
    assert successes == {"good": True, "bad": False}


@pytest.fixture(autouse=True)
def _clean_sample_side_effects():
    """Samples 02 and 04 write beside their own source; don't leave that behind."""
    yield
    for path in SAMPLES.glob("*/src/*/_scratch"):
        shutil.rmtree(path, ignore_errors=True)
    for path in SAMPLES.glob("*/src/*/_attempts.txt"):
        path.unlink(missing_ok=True)


def test_every_sample_directory_is_covered_by_a_case():
    """A new sample must be added to CASES, not silently untested."""
    on_disk = {p.name for p in SAMPLES.iterdir() if p.is_dir() and (p / "pipeline.toml").exists()}
    assert on_disk == {name for name, _ in CASES} | {name for name, _ in FAILING_CASES}


def test_every_sample_has_a_readme_and_a_self_contained_project():
    for path in SAMPLES.iterdir():
        if not path.is_dir():
            continue
        assert (path / "README.md").exists(), path
        assert (path / "pyproject.toml").exists(), path
        assert (path / "pipeline.toml").exists(), path
