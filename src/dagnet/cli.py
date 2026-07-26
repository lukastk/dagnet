"""`dagnet check` / `run` / `dev` / `graph` (DESIGN §8).

Every command starts by validating: nothing is ever built or launched from a
manifest that hasn't passed `check`, and the diagnostics are printed in full
rather than one at a time.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

from dagnet import __version__
from dagnet.check import CheckResult, check
from dagnet.diagnostics import DagnetError
from dagnet.graph import PipelineGraph, asset_key
from dagnet.instance import open_instance, pool_granularity_is_op, sync_pools
from dagnet.locations import dagster_home
from dagnet.mermaid import to_mermaid

#: Where a project's map file is looked for when `--manifest` isn't given.
MANIFEST_NAMES = ("pipeline.toml", "pipeline.json")
#: Where run presets are looked for when `--runs` isn't given. A folder counts.
RUNS_NAMES = ("runs.toml", "runs.json", "runs")

EXIT_OK = 0
EXIT_FAILED = 1
EXIT_USAGE = 2


class UsageError(DagnetError):
    """The command line itself was wrong — a missing manifest, an unknown run."""


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        return args.handler(args)
    except UsageError as exc:
        print(f"dagnet: {exc}", file=sys.stderr)
        return EXIT_USAGE
    except DagnetError as exc:
        print(str(exc), file=sys.stderr)
        return EXIT_FAILED


# --- commands --------------------------------------------------------------


def cmd_check(args: argparse.Namespace) -> int:
    result = _check(args)
    print(result.diagnostics.render())
    return EXIT_OK if result.ok else EXIT_FAILED


def cmd_run(args: argparse.Namespace) -> int:
    import dagster as dg

    from dagnet._reconstruct import reconstructable_job

    manifest_path, runs_paths = _locate(args)
    result = _check(args)
    if not result.ok:
        print(result.diagnostics.render(), file=sys.stderr)
        return EXIT_FAILED
    _report_warnings(result)

    if args.run_name is not None and args.run_name not in result.runs.runs:
        known = ", ".join(sorted(result.runs.runs)) or "none"
        raise UsageError(f"no run named '{args.run_name}' (known runs: {known})")

    home = (
        None if args.ephemeral else dagster_home(result.manifest, manifest_path, args.dagster_home)
    )
    executor = "in_process" if args.ephemeral else "multiprocess"
    if args.ephemeral and result.manifest.pools:
        print(
            "dagnet: warning: --ephemeral runs in-process against a stateless instance, "
            "so the limits in [pools] are NOT enforced",
            file=sys.stderr,
        )

    job = reconstructable_job(
        manifest=str(manifest_path),
        runs=[str(p) for p in runs_paths],
        run_name=args.run_name,
        select=args.select,
        store_root=args.store_root,
        executor=executor,
    )

    with open_instance(home) as instance:
        # Resolve the resume target first: a `pre_run` hook is told whether this
        # is a resume and of which run, and neither is knowable without the
        # instance. Opening it and reading run history materializes nothing.
        options = _reexecution_options(args, instance)
        if result.manifest.pipeline.pre_run and _pre_run_refuses(
            args, result, manifest_path, runs_paths, options
        ):
            return EXIT_FAILED

        if result.manifest.pools and not args.ephemeral:
            if not sync_pools(instance, result.manifest.pools):
                print(
                    "dagnet: warning: this instance does not support concurrency pools; "
                    "[pools] limits are not enforced",
                    file=sys.stderr,
                )
            elif not pool_granularity_is_op(home):
                print(
                    f"dagnet: warning: {home}/dagster.yaml does not set "
                    f"concurrency.pools.granularity to 'op', so [pools] limits apply "
                    f"per run rather than per step",
                    file=sys.stderr,
                )
        with dg.execute_job(
            job, instance=instance, reexecution_options=options, raise_on_error=False
        ) as outcome:
            return EXIT_OK if outcome.success else EXIT_FAILED


def cmd_dev(args: argparse.Namespace) -> int:
    manifest_path, runs_paths = _locate(args)
    result = _check(args)
    if not result.ok:
        print(result.diagnostics.render(), file=sys.stderr)
        return EXIT_FAILED
    _report_warnings(result)

    if result.manifest.pipeline.pre_run:
        print(
            "dagnet: warning: runs launched from the Dagster UI do NOT pass through "
            f"dagnet, so the {len(result.manifest.pipeline.pre_run)} [pipeline].pre_run "
            "hook(s) will not run for them",
            file=sys.stderr,
        )

    home = dagster_home(result.manifest, manifest_path, args.dagster_home)
    defs_path = _write_defs_module(home, manifest_path, runs_paths)

    with open_instance(home) as instance:
        if result.manifest.pools and not sync_pools(instance, result.manifest.pools):
            print("dagnet: warning: [pools] limits are not enforced here", file=sys.stderr)

    env = dict(os.environ, DAGSTER_HOME=str(home))
    command = ["dagster", "dev", "-f", str(defs_path)]
    print(f"dagnet: DAGSTER_HOME={home}\ndagnet: {' '.join(command)}", file=sys.stderr)
    return subprocess.call(command, env=env)


def cmd_graph(args: argparse.Namespace) -> int:
    # Node functions need not be importable to draw the map.
    result = _check(args, import_functions=False)
    if result.manifest is None:
        print(result.diagnostics.render(), file=sys.stderr)
        return EXIT_FAILED

    diagram = to_mermaid(result.manifest, PipelineGraph.build(result.manifest), args.direction)
    if args.output:
        Path(args.output).write_text(diagram)
    else:
        print(diagram, end="")
    return EXIT_OK if result.ok else EXIT_FAILED


# --- shared plumbing -------------------------------------------------------


def _check(args: argparse.Namespace, import_functions: bool = True) -> CheckResult:
    manifest_path, runs_paths = _locate(args)
    return check(manifest_path, runs_paths, import_functions=import_functions)


def _locate(args: argparse.Namespace) -> tuple[Path, list[Path]]:
    """Find the manifest and run presets, explicitly or by convention."""
    if args.manifest:
        manifest_path = Path(args.manifest)
        if not manifest_path.exists():
            raise UsageError(f"no manifest at {manifest_path}")
    else:
        manifest_path = _first_existing(Path.cwd(), MANIFEST_NAMES)
        if manifest_path is None:
            raise UsageError(
                f"no manifest found in {Path.cwd()} "
                f"(looked for {', '.join(MANIFEST_NAMES)}); pass --manifest"
            )

    if args.runs:
        runs_paths = [Path(p) for p in args.runs]
        for path in runs_paths:
            if not path.exists():
                raise UsageError(f"no runs file or folder at {path}")
    else:
        found = _first_existing(manifest_path.parent, RUNS_NAMES)
        runs_paths = [found] if found is not None else []
    return manifest_path, runs_paths


def _first_existing(directory: Path, names: tuple[str, ...]) -> Path | None:
    for name in names:
        candidate = directory / name
        if candidate.exists():
            return candidate
    return None


def _report_warnings(result: CheckResult) -> None:
    for diagnostic in result.diagnostics.warnings:
        print(diagnostic.render(), file=sys.stderr)


def _pre_run_refuses(
    args: argparse.Namespace,
    result: CheckResult,
    manifest_path: Path,
    runs_paths: list[Path],
    reexecution: Any,
) -> bool:
    """Run the `pre_run` hooks. True means: do not launch.

    An ERROR from any hook aborts; WARNINGs print and the run proceeds. This is
    dagnet's launch path only — a run started from the Dagster UI's launchpad
    never passes through here (DESIGN §8).
    """
    from dagnet.compile import build_job, run_config
    from dagnet.prerun import PreRunContext, run_hooks

    job = build_job(
        str(manifest_path),
        [str(p) for p in runs_paths],
        run_name=args.run_name,
        select=args.select,
        store_root=args.store_root,
        executor="in_process",
    )
    asset_keys = sorted(key.to_user_string() for key in job.asset_layer.selected_asset_keys)
    context = PreRunContext(
        manifest_path=manifest_path.resolve(),
        run_name=args.run_name,
        selection=args.select,
        node_names=tuple(_nodes_for(result, asset_keys)),
        asset_keys=tuple(asset_keys),
        run_config=run_config(result.manifest, result, args.run_name),
        is_resume=reexecution is not None,
        parent_run_id=reexecution.parent_run_id if reexecution is not None else None,
    )

    diagnostics = run_hooks(result.manifest.pipeline.pre_run, context)
    if diagnostics.items:
        print(diagnostics.render(), file=sys.stderr)
    return bool(diagnostics.errors)


def _nodes_for(result: CheckResult, asset_keys: list[str]) -> list[str]:
    """Which nodes produce these asset keys, sorted and deduplicated."""
    owners = {
        "/".join(asset_key(result.manifest, node_name, output)): node_name
        for node_name, node in result.manifest.nodes.items()
        for output in node.outputs
    }
    return sorted({owners[key] for key in asset_keys if key in owners})


def _reexecution_options(args: argparse.Namespace, instance):
    """`--from-failure` resumes a run, skipping steps that already succeeded.

    Spike (e) confirmed this reaches individual ops inside graph-backed assets,
    which is what replaces a hand-rolled `skip_if_done` resume.
    """
    if not args.from_failure:
        return None
    import dagster as dg

    run_id = args.from_failure
    if run_id == "last":
        runs = instance.get_runs(limit=1)
        if not runs:
            raise UsageError("no previous run to resume from")
        run_id = runs[0].run_id
    return dg.ReexecutionOptions.from_failure(run_id, instance)


def _write_defs_module(home: Path, manifest_path: Path, runs_paths: list[Path]) -> Path:
    """The three-line `defs.py` DESIGN §4 promises, generated rather than hand-kept."""
    home.mkdir(parents=True, exist_ok=True)
    defs_path = home / "defs.py"
    runs = ", ".join(repr(str(p)) for p in runs_paths)
    defs_path.write_text(
        "# Generated by `dagnet dev`. Edit pipeline.toml, not this file.\n"
        "import dagnet\n\n"
        f"defs = dagnet.build({str(manifest_path)!r}, [{runs}])\n"
    )
    return defs_path


# --- argument parsing ------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="dagnet",
        description="A thin declarative wrapper around Dagster: the pipeline is the map file.",
    )
    parser.add_argument("--version", action="version", version=f"dagnet {__version__}")
    subparsers = parser.add_subparsers(dest="command", required=True)

    def add_common(sub: argparse.ArgumentParser) -> None:
        sub.add_argument(
            "--manifest", help=f"the map file (default: {' or '.join(MANIFEST_NAMES)} in cwd)"
        )
        sub.add_argument(
            "--runs",
            action="append",
            metavar="PATH",
            help="a runs file or folder; repeatable (default: runs.toml or runs/ "
            "next to the manifest)",
        )

    checker = subparsers.add_parser("check", help="validate the manifest and run presets")
    add_common(checker)
    checker.set_defaults(handler=cmd_check)

    runner = subparsers.add_parser("run", help="materialize the pipeline")
    add_common(runner)
    runner.add_argument("run_name", nargs="?", help="a run preset; omit to use declared defaults")
    runner.add_argument(
        "--select", help="Dagster asset selection, e.g. '+db/drugs' for it and everything upstream"
    )
    runner.add_argument(
        "--ephemeral",
        action="store_true",
        help="leave no instance state behind; implies in-process execution and "
        "unenforced pool limits",
    )
    runner.add_argument("--dagster-home", help="override [pipeline].dagster_home")
    runner.add_argument("--store-root", help="override where file artifacts resolve")
    runner.add_argument(
        "--from-failure",
        metavar="RUN_ID",
        help="resume a failed run, skipping successful steps ('last' for the most recent)",
    )
    runner.set_defaults(handler=cmd_run)

    dev = subparsers.add_parser("dev", help="serve the Dagster UI for this pipeline")
    add_common(dev)
    dev.add_argument("--dagster-home", help="override [pipeline].dagster_home")
    dev.set_defaults(handler=cmd_dev)

    graph = subparsers.add_parser("graph", help="export the pipeline as a Mermaid diagram")
    add_common(graph)
    graph.add_argument("--output", "-o", help="write to a file instead of stdout")
    graph.add_argument("--direction", default="LR", choices=["LR", "TB", "RL", "BT"])
    graph.set_defaults(handler=cmd_graph)

    return parser


if __name__ == "__main__":
    raise SystemExit(main())
