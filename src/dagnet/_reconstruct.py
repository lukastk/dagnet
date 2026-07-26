"""The entry point Dagster's multiprocess executor rebuilds the job through.

`materialize()` and `execute_in_process()` always run in-process whatever executor
the job names; genuine multiprocess execution needs `execute_job` with a
*reconstructable* job, because each step subprocess rebuilds the definitions from
scratch (see `_dev/experiments/FINDINGS.md`, spike (b)).

`build_reconstructable_job` can only carry JSON-serializable arguments across that
boundary, which is why this takes a manifest path and plain strings rather than
the already-built `Definitions`.
"""

from __future__ import annotations

import dagster as dg
from dagster._core.definitions.reconstruct import build_reconstructable_job

from dagnet.compile import build_job


def job_from_manifest(**kwargs: object) -> dg.JobDefinition:
    """Module-level reconstructor. Do not rename: it is referenced by name."""
    return build_job(**kwargs)


def reconstructable_job(
    manifest: str,
    runs: list[str],
    run_name: str | None,
    select: str | None,
    store_root: str | None,
    executor: str,
):
    """A `ReconstructableJob` for the given manifest and run."""
    return build_reconstructable_job(
        __name__,
        job_from_manifest.__name__,
        reconstructable_kwargs={
            "manifest": manifest,
            "runs": runs,
            "run_name": run_name,
            "select": select,
            "store_root": store_root,
            "executor": executor,
        },
    )
