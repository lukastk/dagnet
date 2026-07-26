"""Compiling a validated manifest to Dagster `Definitions` (DESIGN §8).

The mapping, in one table:

| manifest            | Dagster                                                |
|---------------------|--------------------------------------------------------|
| node                | one `multi_asset`, built by calling the decorator       |
| output              | an asset keyed `node/output`, or its bound artifact key |
| `inputs` value ref  | `AssetIn` with a key mapping (this is the rename case)  |
| `inputs` artifact   | a non-argument dep; dagnet injects the location itself  |
| `after`             | non-argument deps on every output of the named node     |
| `pool`              | the `pool=` tag; `[pools]` limits go on the instance    |
| `retries`           | `RetryPolicy`                                           |
| `checks`            | `asset_check` definitions                               |
| `[vars]` + runs     | per-node `config_schema` + per-run job config           |
| `group`/`description` | asset group and metadata                              |

Two things are load-bearing and not obvious:

*Nodes are built by calling `multi_asset(...)` as a function*, not with decorator
syntax, and each generated body carries a synthetic `__signature__` — Dagster
reads the compute function's parameters to bind inputs, so `**kwargs` alone is
not enough.

*An output bound to an artifact declares `dagster_type=Nothing` and yields
`Output(None)`*: the node wrote the artifact itself, so there is no value for the
IO manager to store, but the asset is still materialized and appears in the
catalog with its declared location.
"""

from __future__ import annotations

import asyncio
import inspect
from pathlib import Path
from typing import Any, Callable, Iterator

import dagster as dg
from msgspec import UNSET

from dagnet.check import CheckResult, check
from dagnet.context import ArtifactLocation, NodeContext, resolve_artifact
from dagnet.diagnostics import DagnetError
from dagnet.graph import NodeRef, PipelineGraph, asset_key
from dagnet.nodefn import NodeFunction, describe, import_object
from dagnet.runs import declaration_for, resolve_run
from dagnet.schema import Manifest, Node

#: The resource key carrying run-wide information into every node.
RESOURCE_KEY = "dagnet"
#: The job `dagnet run` builds when no run preset applies.
DEFAULT_JOB_NAME = "dagnet"

_DAGSTER_CONFIG_TYPE = {"str": str, "int": int, "float": float, "bool": bool}


class CompileError(DagnetError):
    """The manifest is valid but could not be turned into Dagster definitions."""


class NodeReturnError(DagnetError):
    """A node function returned something its declared outputs don't describe."""


class ArtifactNotWritten(DagnetError):
    """A node claimed to produce a file artifact but the file isn't there."""


class CheckReturnError(DagnetError):
    """A check function returned something dagnet can't turn into a result."""


class DagnetRun(dg.ConfigurableResource):
    """Run-wide values that aren't per-node. Configurable, so the UI launchpad shows it."""

    #: empty when a run is launched ad hoc from the UI rather than from a preset.
    run_name: str = ""


def build(
    manifest_path: str | Path,
    runs_paths: list[str | Path] | None = None,
    *,
    store_root: str | Path | None = None,
) -> dg.Definitions:
    """`defs = dagnet.build("pipeline.toml")` — the entry point a `defs.py` calls.

    Validates first and raises with the full diagnostic set: definitions are never
    built from a manifest that hasn't passed `check`.
    """
    result = check(manifest_path, runs_paths)
    result.diagnostics.raise_if_errors()
    return compile_definitions(result, Path(manifest_path), store_root=store_root)


def compile_definitions(
    result: CheckResult,
    manifest_path: Path,
    *,
    store_root: str | Path | None = None,
    executor: str = "multiprocess",
) -> dg.Definitions:
    """Turn a passed `CheckResult` into `Definitions`, with one job per run preset."""
    manifest = result.manifest
    graph = result.graph
    root = Path(store_root) if store_root is not None else manifest_path.parent
    artifacts = {key: resolve_artifact(art, root) for key, art in manifest.artifacts.items()}

    assets: list[dg.AssetsDefinition] = []
    checks: list[dg.AssetChecksDefinition] = []
    for node_name in sorted(manifest.nodes):
        node = manifest.nodes[node_name]
        if not node.asset:
            raise CompileError(
                f"node '{node_name}' sets `asset = false`, which the compiler does not support yet"
            )
        assets.append(_compile_node(manifest, graph, node_name, result.functions, artifacts))
        checks.extend(_compile_checks(manifest, node_name, artifacts))

    preset_names = sorted(result.runs.runs) if result.runs is not None else []
    jobs = [_compile_run_job(manifest, result, name, executor) for name in preset_names]
    return dg.Definitions(
        assets=assets,
        asset_checks=checks,
        jobs=jobs,
        resources={RESOURCE_KEY: DagnetRun()},
    )


# --- nodes -----------------------------------------------------------------


def _compile_node(
    manifest: Manifest,
    graph: PipelineGraph,
    node_name: str,
    functions: dict[str, NodeFunction],
    artifacts: dict[str, ArtifactLocation],
) -> dg.AssetsDefinition:
    node = manifest.nodes[node_name]
    described = functions.get(node_name) or describe(_must_import(node.fn))

    outs: dict[str, dg.AssetOut] = {}
    for output in node.outputs:
        key = dg.AssetKey(list(asset_key(manifest, node_name, output)))
        bound = node.artifacts.get(output)
        # `is_required=False` is what lets `can_subset=True` work: an unselected
        # output is simply not yielded. It is not a licence for a node to skip an
        # output it was asked for — `_unpack_return` still demands every one.
        if bound is None:
            outs[output] = dg.AssetOut(key=key, is_required=False)
        else:
            outs[output] = dg.AssetOut(
                key=key,
                is_required=False,
                # The node writes the artifact itself, so nothing crosses the IO
                # manager; the asset is still materialized (see module docstring).
                dagster_type=dg.Nothing,
                metadata={"dagnet/artifact": bound, "dagnet/location": str(artifacts[bound])},
            )

    ins: dict[str, dg.AssetIn] = {}
    artifact_params: dict[str, str] = {}
    deps: list[dg.AssetKey] = []
    for param in node.inputs:
        ref = graph.references[node_name][param]
        if isinstance(ref, NodeRef):
            ins[param] = dg.AssetIn(
                key=dg.AssetKey(list(asset_key(manifest, ref.node, ref.output)))
            )
        else:
            artifact_params[param] = ref.key
            deps.append(dg.AssetKey(list(ref.key.split("/"))))

    for target in node.after:
        for output in manifest.nodes[target].outputs:
            deps.append(dg.AssetKey(list(asset_key(manifest, target, output))))

    compute = _compile_body(manifest, node_name, described, artifacts, artifact_params)
    return dg.multi_asset(
        name=node_name,
        outs=outs,
        ins=ins or None,
        deps=_dedupe_deps(deps, ins) or None,
        description=node.description or None,
        group_name=node.group,
        pool=node.pool,
        retry_policy=_retry_policy(node),
        config_schema=_config_schema(manifest, node_name) or None,
        required_resource_keys={RESOURCE_KEY},
        # Selecting one output of a multi-output node must not be an error, so the
        # node advertises subsetting: it still runs whole (a node is atomic), and
        # the body yields only the outputs Dagster asked for.
        can_subset=True,
    )(compute)


def _compile_body(
    manifest: Manifest,
    node_name: str,
    described: NodeFunction,
    artifacts: dict[str, ArtifactLocation],
    artifact_params: dict[str, str],
) -> Callable[..., Any]:
    """Wrap a plain node function as a Dagster compute function."""
    node = manifest.nodes[node_name]
    value_outputs = [o for o in node.outputs if o not in node.artifacts]
    artifact_outputs = {o: key for o, key in node.artifacts.items()}

    def compute(context: dg.AssetExecutionContext, **kwargs: Any) -> Iterator[dg.Output]:
        ctx = NodeContext(
            vars=context.op_execution_context.op_config or {},
            run_name=getattr(context.resources, RESOURCE_KEY).run_name,
            artifacts=artifacts,
        )
        call_kwargs = dict(kwargs)
        for param, key in artifact_params.items():
            call_kwargs[param] = ctx.artifact(key)

        for key in artifact_outputs.values():
            _prepare_artifact_location(artifacts[key])

        returned = described.fn(ctx, **call_kwargs)
        if described.is_async:
            returned = asyncio.run(returned)

        values = _unpack_return(node_name, returned, value_outputs)
        # A node is atomic — it always computes everything — but `--select` may
        # have asked for only some of its outputs, and Dagster requires that only
        # the selected ones are yielded.
        selected = context.op_execution_context.selected_output_names
        for output in node.outputs:
            if output not in selected:
                continue
            bound = artifact_outputs.get(output)
            if bound is None:
                yield dg.Output(values[output], output)
            else:
                _assert_artifact_written(node_name, bound, artifacts[bound])
                yield dg.Output(None, output, metadata={"location": str(artifacts[bound])})

    compute.__name__ = node_name
    # Dagster binds inputs by inspecting the signature, so it must name exactly the
    # parameters Dagster is responsible for: the context and the value inputs.
    # Artifact inputs are injected above and must not appear here.
    compute.__signature__ = inspect.Signature(
        [inspect.Parameter("context", inspect.Parameter.POSITIONAL_OR_KEYWORD)]
        + [
            inspect.Parameter(param, inspect.Parameter.POSITIONAL_OR_KEYWORD)
            for param in node.inputs
            if param not in artifact_params
        ]
    )
    return compute


def _unpack_return(node_name: str, returned: Any, value_outputs: list[str]) -> dict[str, Any]:
    """Check a node's return against its declared outputs, loudly (DESIGN §7 rule 2)."""
    if not value_outputs:
        if returned not in (None, {}):
            raise NodeReturnError(
                f"node '{node_name}' has only artifact outputs so it must return nothing, "
                f"but returned {type(returned).__name__}"
            )
        return {}

    if not isinstance(returned, dict):
        raise NodeReturnError(
            f"node '{node_name}' must return a dict with keys "
            f"{value_outputs}, but returned {type(returned).__name__}"
        )
    missing = [o for o in value_outputs if o not in returned]
    extra = [k for k in returned if k not in value_outputs]
    if missing or extra:
        parts = []
        if missing:
            parts.append(f"missing {missing}")
        if extra:
            parts.append(f"unexpected {extra}")
        raise NodeReturnError(
            f"node '{node_name}' returned keys that do not match its declared value "
            f"outputs {value_outputs}: {', and '.join(parts)}"
        )
    return returned


def _prepare_artifact_location(location: ArtifactLocation) -> None:
    """Make sure a file artifact's directory exists before the node writes to it."""
    if isinstance(location, Path):
        location.parent.mkdir(parents=True, exist_ok=True)


def _assert_artifact_written(node_name: str, key: str, location: ArtifactLocation) -> None:
    """A node that declares a file artifact must actually have written it."""
    if isinstance(location, Path) and not location.exists():
        raise ArtifactNotWritten(
            f"node '{node_name}' declares artifact '{key}' as an output but nothing "
            f"exists at {location}"
        )


def _dedupe_deps(deps: list[dg.AssetKey], ins: dict[str, dg.AssetIn]) -> list[dg.AssetKey]:
    """Dagster rejects a dep that is also an input, and rejects duplicates."""
    taken = {i.key for i in ins.values()}
    out: list[dg.AssetKey] = []
    for key in deps:
        if key in taken:
            continue
        taken.add(key)
        out.append(key)
    return out


def _retry_policy(node: Node) -> dg.RetryPolicy | None:
    if node.retries is None:
        return None
    return dg.RetryPolicy(max_retries=node.retries.max, delay=node.retries.wait_s)


def _config_schema(manifest: Manifest, node_name: str) -> dict[str, dg.Field]:
    """Every variable visible to a node becomes a config field, so `ctx.vars` is it."""
    node = manifest.nodes[node_name]
    names = list(manifest.vars) + [v for v in node.vars if v not in manifest.vars]
    fields: dict[str, dg.Field] = {}
    for name in names:
        decl = declaration_for(manifest, node_name, name)
        config_type = _DAGSTER_CONFIG_TYPE[decl.type]
        if decl.default is UNSET:
            # No default means no value to fall back on: Dagster must demand it.
            fields[name] = dg.Field(config_type, is_required=True, description=decl.description)
        else:
            fields[name] = dg.Field(
                config_type, default_value=decl.default, description=decl.description
            )
    return fields


# --- checks ----------------------------------------------------------------


def _compile_checks(
    manifest: Manifest, node_name: str, artifacts: dict[str, ArtifactLocation]
) -> list[dg.AssetChecksDefinition]:
    node = manifest.nodes[node_name]
    built: list[dg.AssetChecksDefinition] = []
    for output, paths in node.checks.items():
        key = dg.AssetKey(list(asset_key(manifest, node_name, output)))
        bound = node.artifacts.get(output)
        for path in paths:
            built.append(_compile_check(manifest, node_name, output, key, path, bound, artifacts))
    return built


def _compile_check(
    manifest: Manifest,
    node_name: str,
    output: str,
    key: dg.AssetKey,
    path: str,
    bound: str | None,
    artifacts: dict[str, ArtifactLocation],
) -> dg.AssetChecksDefinition:
    fn = _must_import(path)
    name = path.rpartition(".")[2]

    def run_check(context: dg.AssetCheckExecutionContext, **kwargs: Any) -> dg.AssetCheckResult:
        ctx = NodeContext(
            vars=context.op_execution_context.op_config or {},
            run_name=getattr(context.resources, RESOURCE_KEY).run_name,
            artifacts=artifacts,
        )
        # A value output arrives loaded through the IO manager; an artifact-bound
        # output has no value, so the check gets its location instead.
        subject = ctx.artifact(bound) if bound is not None else next(iter(kwargs.values()))
        return _as_check_result(name, fn(ctx, subject))

    run_check.__name__ = name
    params = [inspect.Parameter("context", inspect.Parameter.POSITIONAL_OR_KEYWORD)]
    if bound is None:
        params.append(inspect.Parameter(output, inspect.Parameter.POSITIONAL_OR_KEYWORD))
    run_check.__signature__ = inspect.Signature(params)

    return dg.asset_check(
        asset=key,
        name=name,
        description=f"{path} on {node_name}.{output}",
        required_resource_keys={RESOURCE_KEY},
        config_schema=_config_schema(manifest, node_name) or None,
    )(run_check)


def _as_check_result(name: str, returned: Any) -> dg.AssetCheckResult:
    """A check returns a bool, or a dict with `passed` and optional `metadata`."""
    if isinstance(returned, bool):
        return dg.AssetCheckResult(passed=returned)
    if isinstance(returned, dict) and isinstance(returned.get("passed"), bool):
        return dg.AssetCheckResult(
            passed=returned["passed"], metadata=returned.get("metadata") or {}
        )
    raise CheckReturnError(
        f"check '{name}' must return a bool or {{'passed': bool, 'metadata': {{...}}}}, "
        f"but returned {type(returned).__name__}"
    )


# --- jobs ------------------------------------------------------------------


def _compile_run_job(
    manifest: Manifest, result: CheckResult, run_name: str, executor: str
) -> dg.JobDefinition:
    """One job per run preset, with its resolved variables baked in as config.

    This is what makes DESIGN §6's "visible/editable in the launchpad" true: each
    named run shows up in the UI as a job whose config is already filled in.
    """
    return dg.define_asset_job(
        name=run_name,
        selection="*",
        config=run_config(manifest, result, run_name),
        executor_def=_executor(executor),
    )


def run_config(manifest: Manifest, result: CheckResult, run_name: str | None) -> dict[str, Any]:
    """The Dagster run config for a run preset: per-node variables plus the run name."""
    if run_name is None:
        return {"resources": {RESOURCE_KEY: {"config": {"run_name": ""}}}}
    resolved = resolve_run(manifest, result.runs, run_name)
    return {
        "ops": {
            node_name: {"config": dict(values)}
            for node_name, values in resolved.per_node.items()
            if values
        },
        "resources": {RESOURCE_KEY: {"config": {"run_name": run_name}}},
    }


def _executor(executor: str) -> Any:
    if executor == "multiprocess":
        return dg.multiprocess_executor
    if executor == "in_process":
        return dg.in_process_executor
    raise CompileError(f"unknown executor '{executor}'")


def build_job(
    manifest: str | Path,
    runs: list[str] | None = None,
    run_name: str | None = None,
    select: str | None = None,
    store_root: str | None = None,
    executor: str = "multiprocess",
) -> dg.JobDefinition:
    """Build the single job `dagnet run` executes.

    Every argument is JSON-serializable on purpose: multiprocess execution rebuilds
    the job in each step subprocess through `dagnet._reconstruct`, and
    `build_reconstructable_job` can only carry JSON (see FINDINGS.md, spike (b)).
    """
    manifest_path = Path(manifest)
    result = check(manifest_path, list(runs or []))
    result.diagnostics.raise_if_errors()

    if run_name is not None and run_name not in result.runs.runs:
        known = ", ".join(sorted(result.runs.runs)) or "none"
        raise CompileError(f"no run named '{run_name}' (known runs: {known})")

    defs = compile_definitions(result, manifest_path, store_root=store_root, executor=executor)
    job = dg.define_asset_job(
        name=DEFAULT_JOB_NAME,
        selection=select or "*",
        config=run_config(result.manifest, result, run_name),
        executor_def=_executor(executor),
    )
    return dg.Definitions(
        assets=defs.assets,
        asset_checks=defs.asset_checks,
        jobs=[job],
        resources=defs.resources,
    ).resolve_job_def(DEFAULT_JOB_NAME)


def _must_import(path: str) -> Any:
    from dagnet.nodefn import ImportFailure

    obj = import_object(path)
    if isinstance(obj, ImportFailure):
        raise CompileError(f"{path}: {obj.detail}")
    return obj
