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
from dagnet.graph import ArtifactRef, NodeRef, PipelineGraph, asset_key
from dagnet.nodefn import NodeFunction, describe, import_object
from dagnet.partition import Cluster, config_path, partition
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

    clusters = partition(manifest, graph)
    _reject_name_collisions(manifest, clusters)

    assets: list[dg.AssetsDefinition] = []
    checks: list[dg.AssetChecksDefinition] = []
    for cluster in clusters:
        if cluster.is_graph_backed:
            assets.append(_compile_cluster(manifest, graph, cluster, result.functions, artifacts))
        else:
            assets.append(
                _compile_node(manifest, graph, cluster.assets[0], result.functions, artifacts)
            )
        for node_name in cluster.assets:
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


# --- clusters that fold in `asset = false` nodes ---------------------------


def _reject_name_collisions(manifest: Manifest, clusters: list[Cluster]) -> None:
    for cluster in clusters:
        if cluster.is_graph_backed and cluster.name in manifest.nodes:
            raise CompileError(
                f"the graph generated for {', '.join(cluster.assets)} would be called "
                f"'{cluster.name}', which is also a node name; rename that node"
            )


def _compile_cluster(
    manifest: Manifest,
    graph: PipelineGraph,
    cluster: Cluster,
    functions: dict[str, NodeFunction],
    artifacts: dict[str, ArtifactLocation],
) -> dg.AssetsDefinition:
    """Build the graph-backed asset for a cluster with op-nodes folded in.

    Ordering-only dependencies can't be expressed with `deps=` here — `from_graph`
    has no such parameter — so they arrive as `Nothing`-typed graph inputs mapped
    to the upstream asset key, which is the same thing by another route.
    """
    inside = set(cluster.members)
    # External dependencies, deduplicated by asset key so two ops consuming the
    # same upstream asset share one graph input.
    value_inputs: dict[tuple[str, ...], str] = {}
    ordering_inputs: dict[tuple[str, ...], str] = {}

    for member in cluster.members:
        node = manifest.nodes[member]
        for param in node.inputs:
            ref = graph.references[member][param]
            if isinstance(ref, NodeRef):
                if ref.node in inside:
                    continue
                key = asset_key(manifest, ref.node, ref.output)
                value_inputs.setdefault(key, _graph_input_name("in", key))
            else:
                producer = graph.producer_of(ref)
                if producer is None or producer in inside:
                    continue
                for key in _keys_of(manifest, producer):
                    ordering_inputs.setdefault(key, _graph_input_name("dep", key))
        for target in node.after:
            if target in inside:
                continue
            for key in _keys_of(manifest, target):
                ordering_inputs.setdefault(key, _graph_input_name("dep", key))

    ops = {
        member: _compile_op(manifest, graph, member, cluster, functions, artifacts, value_inputs)
        for member in cluster.members
    }

    graph_outs = {
        _graph_output_name(node_name, output): dg.GraphOut()
        for node_name in cluster.assets
        for output in manifest.nodes[node_name].outputs
    }
    body = _compile_graph_body(manifest, graph, cluster, ops, value_inputs, ordering_inputs, inside)
    graph_def = dg.graph(name=cluster.name, out=graph_outs)(body)

    keys_by_input_name = {
        name: dg.AssetKey(list(key))
        for key, name in list(value_inputs.items()) + list(ordering_inputs.items())
    }
    keys_by_output_name = {
        _graph_output_name(node_name, output): dg.AssetKey(
            list(asset_key(manifest, node_name, output))
        )
        for node_name in cluster.assets
        for output in manifest.nodes[node_name].outputs
    }
    groups = {node.group for node in (manifest.nodes[n] for n in cluster.assets)}
    return dg.AssetsDefinition.from_graph(
        graph_def,
        keys_by_input_name=keys_by_input_name,
        keys_by_output_name=keys_by_output_name,
        group_name=groups.pop() if len(groups) == 1 else None,
        # A graph-backed asset cannot subset: its body wires a fixed topology, so
        # every output is produced together. Selecting one output of such a node
        # pulls all of them.
        can_subset=False,
    )


def _compile_op(
    manifest: Manifest,
    graph: PipelineGraph,
    node_name: str,
    cluster: Cluster,
    functions: dict[str, NodeFunction],
    artifacts: dict[str, ArtifactLocation],
    value_inputs: dict[tuple[str, ...], str],
) -> dg.OpDefinition:
    """One member of a cluster, as an op rather than an asset."""
    node = manifest.nodes[node_name]
    described = functions.get(node_name) or describe(_must_import(node.fn))

    artifact_params = {
        param: ref.key
        for param, ref in graph.references[node_name].items()
        if isinstance(ref, ArtifactRef)
    }
    value_params = [p for p in node.inputs if p not in artifact_params]
    ordering_params = _ordering_params(manifest, graph, node_name, cluster)

    ins: dict[str, dg.In] = {param: dg.In() for param in value_params}
    ins.update({param: dg.In(dg.Nothing) for param in ordering_params})
    outs = {
        output: (dg.Out(dg.Nothing) if output in node.artifacts else dg.Out())
        for output in node.outputs
    }

    compute = _compile_op_body(manifest, node_name, described, artifacts, artifact_params)
    # A `Nothing` input carries no value, so Dagster forbids it as a parameter: it
    # is declared in `ins` and supplied only when the op is invoked in the graph.
    compute.__signature__ = inspect.Signature(
        [inspect.Parameter("context", inspect.Parameter.POSITIONAL_OR_KEYWORD)]
        + [inspect.Parameter(p, inspect.Parameter.POSITIONAL_OR_KEYWORD) for p in value_params]
    )
    return dg.op(
        name=node_name,
        ins=ins,
        out=outs,
        description=node.description or None,
        pool=node.pool,
        retry_policy=_retry_policy(node),
        config_schema=_config_schema(manifest, node_name) or None,
        required_resource_keys={RESOURCE_KEY},
    )(compute)


def _ordering_params(
    manifest: Manifest, graph: PipelineGraph, node_name: str, cluster: Cluster
) -> dict[str, tuple[str, str] | tuple[str, ...]]:
    """The `Nothing` inputs an op needs: `after` targets and artifact producers.

    Maps the generated parameter name to what satisfies it — a `(node, output)`
    pair when the source is inside the cluster, or an asset key when it is not.
    """
    node = manifest.nodes[node_name]
    inside = set(cluster.members)
    params: dict[str, tuple[str, str] | tuple[str, ...]] = {}

    def add(target: str) -> None:
        if target in inside:
            first_output = manifest.nodes[target].outputs[0]
            params[f"_after_{target}"] = (target, first_output)
        else:
            for key in _keys_of(manifest, target):
                params[_graph_input_name("dep", key)] = key

    for param, ref in graph.references[node_name].items():
        if isinstance(ref, ArtifactRef):
            producer = graph.producer_of(ref)
            if producer is not None and producer != node_name:
                add(producer)
    for target in node.after:
        if target in manifest.nodes:
            add(target)
    return params


def _compile_graph_body(
    manifest: Manifest,
    graph: PipelineGraph,
    cluster: Cluster,
    ops: dict[str, dg.OpDefinition],
    value_inputs: dict[tuple[str, ...], str],
    ordering_inputs: dict[tuple[str, ...], str],
    inside: set[str],
) -> Callable[..., Any]:
    """A closure that invokes each op in topological order and wires the handles."""
    input_names = list(value_inputs.values()) + list(ordering_inputs.values())

    def body(**graph_inputs: Any) -> Any:
        produced: dict[tuple[str, str], Any] = {}
        for member in cluster.members:
            node = manifest.nodes[member]
            kwargs: dict[str, Any] = {}
            for param in node.inputs:
                ref = graph.references[member][param]
                if not isinstance(ref, NodeRef):
                    continue  # artifact inputs are injected inside the op body
                if ref.node in inside:
                    kwargs[param] = produced[(ref.node, ref.output)]
                else:
                    key = asset_key(manifest, ref.node, ref.output)
                    kwargs[param] = graph_inputs[value_inputs[key]]
            for param, source in _ordering_params(manifest, graph, member, cluster).items():
                if param.startswith("_after_"):
                    kwargs[param] = produced[source]
                else:
                    kwargs[param] = graph_inputs[ordering_inputs[source]]

            handles = ops[member](**kwargs)
            outputs = node.outputs
            if len(outputs) == 1:
                produced[(member, outputs[0])] = handles
            else:
                for output, handle in zip(outputs, handles):
                    produced[(member, output)] = handle

        return {
            _graph_output_name(node_name, output): produced[(node_name, output)]
            for node_name in cluster.assets
            for output in manifest.nodes[node_name].outputs
        }

    body.__name__ = cluster.name
    body.__signature__ = inspect.Signature(
        [inspect.Parameter(name, inspect.Parameter.POSITIONAL_OR_KEYWORD) for name in input_names]
    )
    return body


def _keys_of(manifest: Manifest, node_name: str) -> list[tuple[str, ...]]:
    return [asset_key(manifest, node_name, output) for output in manifest.nodes[node_name].outputs]


def _graph_input_name(prefix: str, key: tuple[str, ...]) -> str:
    return f"{prefix}_{'__'.join(key)}"


def _graph_output_name(node_name: str, output: str) -> str:
    return f"{node_name}__{output}"


def _invoke_node(
    manifest: Manifest,
    node_name: str,
    described: NodeFunction,
    artifacts: dict[str, ArtifactLocation],
    artifact_params: dict[str, str],
    op_config: dict[str, Any] | None,
    run_name: str,
    kwargs: dict[str, Any],
    selected: set[str] | None,
) -> Iterator[dg.Output]:
    """Call one node function and turn its return into Dagster outputs.

    Shared by the asset path and the op path — the only difference between them
    is where the config and the selection come from.
    """
    node = manifest.nodes[node_name]
    value_outputs = [o for o in node.outputs if o not in node.artifacts]

    ctx = NodeContext(vars=op_config or {}, run_name=run_name, artifacts=artifacts)
    call_kwargs = dict(kwargs)
    for param, key in artifact_params.items():
        call_kwargs[param] = ctx.artifact(key)
    for key in node.artifacts.values():
        _prepare_artifact_location(artifacts[key])

    returned = described.fn(ctx, **call_kwargs)
    if described.is_async:
        returned = asyncio.run(returned)

    values = _unpack_return(node_name, returned, value_outputs)
    for output in node.outputs:
        # A node is atomic — it always computes everything — but `--select` may
        # have asked for only some of its outputs, and Dagster requires that only
        # the selected ones are yielded.
        if selected is not None and output not in selected:
            continue
        bound = node.artifacts.get(output)
        if bound is None:
            yield dg.Output(values[output], output)
        else:
            _assert_artifact_written(node_name, bound, artifacts[bound])
            yield dg.Output(None, output, metadata={"location": str(artifacts[bound])})


def _compile_body(
    manifest: Manifest,
    node_name: str,
    described: NodeFunction,
    artifacts: dict[str, ArtifactLocation],
    artifact_params: dict[str, str],
) -> Callable[..., Any]:
    """Wrap a plain node function as the compute function of a `multi_asset`."""

    def compute(context: dg.AssetExecutionContext, **kwargs: Any) -> Iterator[dg.Output]:
        inner = context.op_execution_context
        yield from _invoke_node(
            manifest,
            node_name,
            described,
            artifacts,
            artifact_params,
            inner.op_config,
            getattr(context.resources, RESOURCE_KEY).run_name,
            kwargs,
            set(inner.selected_output_names),
        )

    compute.__name__ = node_name
    # Dagster binds inputs by inspecting the signature, so it must name exactly the
    # parameters Dagster is responsible for: the context and the value inputs.
    # Artifact inputs are injected above and must not appear here.
    compute.__signature__ = inspect.Signature(
        [inspect.Parameter("context", inspect.Parameter.POSITIONAL_OR_KEYWORD)]
        + [
            inspect.Parameter(param, inspect.Parameter.POSITIONAL_OR_KEYWORD)
            for param in manifest.nodes[node_name].inputs
            if param not in artifact_params
        ]
    )
    return compute


def _compile_op_body(
    manifest: Manifest,
    node_name: str,
    described: NodeFunction,
    artifacts: dict[str, ArtifactLocation],
    artifact_params: dict[str, str],
) -> Callable[..., Any]:
    """Wrap a plain node function as the compute function of an op inside a graph.

    A graph-backed asset does not subset, so every declared output is always
    yielded — hence `selected=None`.
    """

    def compute(context: dg.OpExecutionContext, **kwargs: Any) -> Iterator[dg.Output]:
        yield from _invoke_node(
            manifest,
            node_name,
            described,
            artifacts,
            artifact_params,
            context.op_config,
            getattr(context.resources, RESOURCE_KEY).run_name,
            kwargs,
            None,
        )

    compute.__name__ = node_name
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
    """The Dagster run config for a run preset: per-node variables plus the run name.

    A node folded into a graph-backed asset sits one level deeper in the config
    tree (`ops.<graph>.ops.<node>.config`), so the paths come from the partition
    rather than being assumed flat.
    """
    config: dict[str, Any] = {"resources": {RESOURCE_KEY: {"config": {"run_name": run_name or ""}}}}
    if run_name is None:
        return config

    resolved = resolve_run(manifest, result.runs, run_name)
    for cluster in partition(manifest, result.graph):
        for node_name in cluster.members:
            values = resolved.per_node.get(node_name)
            if not values:
                continue
            _set_path(config, config_path(cluster, node_name), dict(values))
    return config


def _set_path(tree: dict[str, Any], path: list[str], value: Any) -> None:
    for segment in path[:-1]:
        tree = tree.setdefault(segment, {})
    tree[path[-1]] = value


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
