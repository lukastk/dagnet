"""`dagnet check` — aggregated, located validation (DESIGN §7b).

Every pass here appends to one `Diagnostics` collector and keeps going. Nothing
fails fast, because the point of the map file is that a person can fix all of it
in one edit rather than discovering the next problem on the next run.

The checklist, in the order the passes run:

- pools referenced by nodes exist, and their limits are usable;
- outputs exist, are unique per node, and don't collide as asset keys;
- `artifacts` bindings name a declared artifact and one of the node's outputs;
- every `inputs` reference resolves to exactly one producer;
- `after` names real nodes, and the whole dependency graph is acyclic;
- every `asset = false` node reaches a downstream asset node, and none of them
  claim asset-only features (checks, artifacts);
- every `fn` and check import path imports, and each signature matches the
  manifest's declared interface;
- annotations on wired output/input pairs agree (warning only);
- run presets name declared variables of matching types.
"""

from __future__ import annotations

import difflib
import re
from dataclasses import dataclass, field
from pathlib import Path

from msgspec import UNSET

from dagnet.diagnostics import Diagnostics, Location
from dagnet.graph import (
    ARTIFACT_SEPARATOR,
    ArtifactRef,
    NodeRef,
    PipelineGraph,
    RefError,
    asset_key,
    resolve_reference,
)
from dagnet.loader import RunRegistry, load_manifest
from dagnet.nodefn import (
    ImportFailure,
    ImportProblem,
    NodeFunction,
    ReturnKind,
    describe,
    import_object,
)
from dagnet.runs import validate_runs, value_matches
from dagnet.schema import DuckDBTableArtifact, FileArtifact, Manifest, as_check_decl

#: The parameter every node function takes first (DESIGN §7 rule 1).
CTX_PARAMETER = "ctx"

_IMPORT_CODES = {
    ImportProblem.NO_MODULE: "not-importable",
    ImportProblem.NO_ATTRIBUTE: "missing-attribute",
    ImportProblem.NOT_CALLABLE: "not-callable",
    ImportProblem.RAISED: "import-error",
    ImportProblem.MALFORMED_PATH: "malformed-path",
}


@dataclass
class CheckResult:
    """Everything a successful check produces, plus the diagnostics either way."""

    diagnostics: Diagnostics
    manifest: Manifest | None = None
    runs: RunRegistry | None = None
    graph: PipelineGraph | None = None
    #: node name -> its imported function, when `import_functions` was on.
    functions: dict[str, NodeFunction] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return self.diagnostics.ok


def check(
    manifest_path: str | Path,
    runs_paths: list[str | Path] | None = None,
    *,
    import_functions: bool = True,
) -> CheckResult:
    """Validate a manifest and its run presets. Never raises on a manifest problem."""
    diags = Diagnostics()
    manifest = load_manifest(manifest_path, diags)
    if manifest is None:
        return CheckResult(diagnostics=diags)

    registry = RunRegistry()
    for path in runs_paths or []:
        registry.load(path, diags)

    root = Location(file=Path(manifest_path))
    graph = PipelineGraph.build(manifest)

    _check_names(manifest, registry, root, diags)
    _check_vars(manifest, root, diags)
    _check_retries(manifest, root, diags)
    _check_pools(manifest, root, diags)
    _check_outputs(manifest, root, diags)
    _check_artifact_bindings(manifest, graph, root, diags)
    _check_asset_key_collisions(manifest, root, diags)
    _check_inputs(manifest, graph, root, diags)
    _check_after(manifest, root, diags)
    _check_cycles(graph, root, diags)
    _check_op_nodes(manifest, graph, root, diags)

    functions: dict[str, NodeFunction] = {}
    if import_functions:
        functions = _check_functions(manifest, root, diags)
        _check_wired_annotations(manifest, graph, functions, root, diags)

    validate_runs(manifest, registry, diags)

    return CheckResult(
        diagnostics=diags, manifest=manifest, runs=registry, graph=graph, functions=functions
    )


# --- names ----------------------------------------------------------------

#: Dagster op, output, check and job names must be plain identifiers. Catching
#: this here means a bad name is a located diagnostic rather than a traceback out
#: of `Definitions`.
_DAGSTER_NAME = re.compile(r"^[A-Za-z0-9_]+$")


def _check_names(
    manifest: Manifest, registry: RunRegistry, root: Location, diags: Diagnostics
) -> None:
    for node_name, node in manifest.nodes.items():
        _require_name(node_name, "node name", root.child("nodes", node_name), diags)
        for index, output in enumerate(node.outputs):
            _require_name(
                output,
                f"output name of node '{node_name}'",
                root.child("nodes", node_name, "outputs").child(index),
                diags,
            )

    for key in manifest.artifacts:
        parts = key.split(ARTIFACT_SEPARATOR)
        loc = root.child("artifacts", key)
        if not all(parts):
            diags.error(
                "invalid-name",
                f"artifact key '{key}' has an empty path component",
                loc,
                hint="write it as 'namespace/name'",
            )
            continue
        for part in parts:
            _require_name(part, f"component of artifact key '{key}'", loc, diags)

    for run_name, loc in registry.run_sources.items():
        _require_name(run_name, "run name", loc, diags)


def _require_name(name: str, what: str, loc: Location, diags: Diagnostics) -> None:
    if not _DAGSTER_NAME.match(name):
        diags.error(
            "invalid-name",
            f"{what} '{name}' is not usable: names must be letters, digits and underscores only",
            loc,
        )


# --- variables and retries -------------------------------------------------

#: POSIX environment variable names. Anything else is unreachable from a shell.
_ENV_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _check_vars(manifest: Manifest, root: Location, diags: Diagnostics) -> None:
    """`env` names must be usable, and a declared default must match its type."""
    scopes = [(manifest.vars, root.child("vars"))]
    for node_name, node in manifest.nodes.items():
        scopes.append((node.vars, root.child("nodes", node_name, "vars")))

    for declarations, scope in scopes:
        for var, decl in declarations.items():
            loc = scope.child(var)
            if decl.env is not None and not _ENV_NAME.match(decl.env):
                diags.error(
                    "invalid-env-name",
                    f"variable '{var}' names environment variable '{decl.env}', which is "
                    f"not a usable name",
                    loc.child("env"),
                    hint="letters, digits and underscores, not starting with a digit",
                )
            if decl.default is not UNSET and not value_matches(decl.default, decl.type):
                diags.error(
                    "var-type-mismatch",
                    f"variable '{var}' is declared as {decl.type} but its default is "
                    f"{type(decl.default).__name__} ({decl.default!r})",
                    loc.child("default"),
                )


def _check_retries(manifest: Manifest, root: Location, diags: Diagnostics) -> None:
    """A retry count is a number of *extra* attempts, so it cannot be negative."""
    policies = [(manifest.pipeline.retries, root.child("pipeline", "retries"))]
    for node_name, node in manifest.nodes.items():
        policies.append((node.retries, root.child("nodes", node_name, "retries")))

    for retries, loc in policies:
        if retries is None:
            continue
        if retries.max < 0:
            diags.error(
                "invalid-retries",
                f"`max = {retries.max}` is not a number of retries",
                loc.child("max"),
                hint="0 means never retry; omit `retries` entirely for the same effect",
            )
        if retries.wait_s < 0:
            diags.error(
                "invalid-retries",
                f"`wait_s = {retries.wait_s}` is not a delay",
                loc.child("wait_s"),
            )


# --- pools ----------------------------------------------------------------


def _check_pools(manifest: Manifest, root: Location, diags: Diagnostics) -> None:
    for name, limit in manifest.pools.items():
        if limit < 1:
            diags.error(
                "invalid-pool-limit",
                f"pool '{name}' has limit {limit}; a pool must allow at least one step",
                root.child("pools", name),
            )

    used: set[str] = set()
    for node_name, node in manifest.nodes.items():
        if node.pool is None:
            continue
        used.add(node.pool)
        if node.pool not in manifest.pools:
            diags.error(
                "unknown-pool",
                f"node '{node_name}' uses pool '{node.pool}', which is not declared",
                root.child("nodes", node_name, "pool"),
                hint=_closest(node.pool, manifest.pools) or "add it to [pools]",
            )

    for name in manifest.pools:
        if name not in used:
            diags.warning(
                "unused-pool",
                f"pool '{name}' is declared but no node uses it",
                root.child("pools", name),
            )


# --- outputs and artifacts -------------------------------------------------


def _check_outputs(manifest: Manifest, root: Location, diags: Diagnostics) -> None:
    for node_name, node in manifest.nodes.items():
        loc = root.child("nodes", node_name)
        if not node.outputs:
            diags.error(
                "no-outputs",
                f"node '{node_name}' declares no outputs",
                loc,
                hint="every node produces at least one output; use `after` for pure ordering",
            )
        seen: set[str] = set()
        for index, output in enumerate(node.outputs):
            if output in seen:
                diags.error(
                    "duplicate-output",
                    f"node '{node_name}' declares output '{output}' twice",
                    loc.child("outputs").child(index),
                )
            seen.add(output)


def _check_artifact_bindings(
    manifest: Manifest, graph: PipelineGraph, root: Location, diags: Diagnostics
) -> None:
    for node_name, node in manifest.nodes.items():
        loc = root.child("nodes", node_name, "artifacts")
        for output, key in node.artifacts.items():
            if output not in node.outputs:
                diags.error(
                    "unknown-output",
                    f"node '{node_name}' binds output '{output}' to artifact '{key}', "
                    f"but declares no such output",
                    loc.child(output),
                    hint=_closest(output, node.outputs),
                )
            if key not in manifest.artifacts:
                diags.error(
                    "unknown-artifact",
                    f"artifact '{key}' is not declared in [artifacts]",
                    loc.child(output),
                    hint=_closest(key, manifest.artifacts),
                )

        for output in node.checks:
            if output not in node.outputs:
                diags.error(
                    "unknown-output",
                    f"node '{node_name}' attaches checks to output '{output}', "
                    f"but declares no such output",
                    root.child("nodes", node_name, "checks", output),
                    hint=_closest(output, node.outputs),
                )

    _check_artifact_databases(manifest, root, diags)

    for key, producers in graph.artifact_producers.items():
        if len(producers) > 1:
            where = ", ".join(f"{n}.{o}" for n, o in sorted(producers))
            diags.error(
                "duplicate-artifact-producer",
                f"artifact '{key}' is produced by more than one output: {where}",
                root.child("artifacts", key),
                hint="an artifact has exactly one producer",
            )


def _check_artifact_databases(manifest: Manifest, root: Location, diags: Diagnostics) -> None:
    """A table artifact's `database` must name a declared *file* artifact (§5.4)."""
    files = [k for k, a in manifest.artifacts.items() if isinstance(a, FileArtifact)]
    for key, artifact in manifest.artifacts.items():
        if not isinstance(artifact, DuckDBTableArtifact):
            continue
        loc = root.child("artifacts", key, "database")
        target = manifest.artifacts.get(artifact.database)
        if target is None:
            diags.error(
                "unknown-artifact",
                f"artifact '{key}' names database '{artifact.database}', "
                f"which is not declared in [artifacts]",
                loc,
                hint=_closest(artifact.database, files),
            )
        elif not isinstance(target, FileArtifact):
            diags.error(
                "database-not-a-file",
                f"artifact '{key}' names database '{artifact.database}', which is a "
                f"{target.__struct_config__.tag} artifact, not a file",
                loc,
                hint='a DuckDB database is a file on disk; declare it with kind = "file"',
            )


def _check_asset_key_collisions(manifest: Manifest, root: Location, diags: Diagnostics) -> None:
    """Two outputs must not compile to the same Dagster asset key."""
    owners: dict[tuple[str, ...], list[str]] = {}
    for node_name, node in manifest.nodes.items():
        for output in node.outputs:
            owners.setdefault(asset_key(manifest, node_name, output), []).append(
                f"{node_name}.{output}"
            )
    for key, holders in owners.items():
        if len(holders) > 1:
            diags.error(
                "asset-key-collision",
                f"{' and '.join(sorted(holders))} both compile to asset key '{'/'.join(key)}'",
                root.child("nodes"),
            )


# --- wiring ----------------------------------------------------------------


def _check_inputs(
    manifest: Manifest, graph: PipelineGraph, root: Location, diags: Diagnostics
) -> None:
    for node_name, node in manifest.nodes.items():
        for param, ref in node.inputs.items():
            loc = root.child("nodes", node_name, "inputs", param)
            resolved = resolve_reference(manifest, ref)
            if isinstance(resolved, RefError):
                _report_ref_error(manifest, node_name, param, ref, resolved, loc, diags)
                continue
            if isinstance(resolved, ArtifactRef):
                _check_artifact_input(graph, ref, resolved, loc, diags)
            elif resolved.node == node_name:
                diags.error(
                    "self-input",
                    f"node '{node_name}' takes its own output '{ref}' as an input",
                    loc,
                )


def _report_ref_error(
    manifest: Manifest,
    node_name: str,
    param: str,
    ref: str,
    error: RefError,
    loc: Location,
    diags: Diagnostics,
) -> None:
    if error is RefError.NOT_A_REFERENCE:
        diags.error(
            "unresolved-input",
            f"input '{param}' of node '{node_name}' references '{ref}', which is "
            f"neither <node>.<output> nor a declared artifact key",
            loc,
            hint=_closest(ref, manifest.artifacts),
        )
        return
    if error is RefError.AMBIGUOUS:
        diags.error(
            "ambiguous-input",
            f"'{ref}' is both a declared artifact key and a node output",
            loc,
            hint="rename one of them; a reference must have exactly one meaning",
        )
        return

    producer, _, output = ref.partition(".")
    if error is RefError.NO_SUCH_NODE:
        diags.error(
            "unresolved-input",
            f"input '{param}' of node '{node_name}' references node '{producer}', "
            f"which does not exist",
            loc,
            hint=_closest(producer, manifest.nodes),
        )
        return
    diags.error(
        "unresolved-input",
        f"input '{param}' of node '{node_name}' references output '{output}' of node "
        f"'{producer}', which declares no such output",
        loc,
        hint=_closest(output, manifest.nodes[producer].outputs),
    )


def _check_artifact_input(
    graph: PipelineGraph, ref: str, resolved: ArtifactRef, loc: Location, diags: Diagnostics
) -> None:
    """DESIGN §5.5: a reference must resolve to exactly one producer."""
    producers = graph.artifact_producers.get(resolved.key, [])
    if not producers:
        diags.error(
            "unproduced-input",
            f"artifact '{ref}' is consumed but no node produces it",
            loc,
            hint='bind it to a node output with `artifacts = { <output> = "' + ref + '" }',
        )


def _check_after(manifest: Manifest, root: Location, diags: Diagnostics) -> None:
    for node_name, node in manifest.nodes.items():
        for index, target in enumerate(node.after):
            loc = root.child("nodes", node_name, "after").child(index)
            if target not in manifest.nodes:
                diags.error(
                    "unknown-after",
                    f"node '{node_name}' declares `after = \"{target}\"`, which is not a node",
                    loc,
                    hint=_closest(target, manifest.nodes),
                )


def _check_cycles(graph: PipelineGraph, root: Location, diags: Diagnostics) -> None:
    for cycle in graph.find_cycles():
        diags.error(
            "cycle",
            f"nodes form a dependency cycle: {' -> '.join(cycle)} -> {cycle[0]}",
            root.child("nodes", cycle[0]),
            hint="dependencies come from `inputs` and `after`; both are directed edges",
        )


def _check_op_nodes(
    manifest: Manifest, graph: PipelineGraph, root: Location, diags: Diagnostics
) -> None:
    """`asset = false` nodes: transient plumbing, with the constraints that implies."""
    for node_name, node in manifest.nodes.items():
        if node.asset:
            continue
        loc = root.child("nodes", node_name)

        if node.checks:
            diags.error(
                "op-node-checks",
                f"node '{node_name}' has `asset = false` but declares checks",
                loc.child("checks"),
                hint="asset checks need an asset; set asset = true or drop the checks",
            )
        if node.artifacts:
            diags.error(
                "op-node-artifacts",
                f"node '{node_name}' has `asset = false` but binds a durable artifact",
                loc.child("artifacts"),
                hint="an artifact is durable by definition; set asset = true",
            )

        reached = graph.nearest_asset_consumers(node_name)
        if not reached:
            diags.error(
                "orphan-op-node",
                f"node '{node_name}' has `asset = false` but no asset node consumes it",
                loc,
                hint="transient work nothing durable consumes is dead code; "
                "set asset = true or wire it into a downstream node's inputs",
            )
        elif len(reached) > 1:
            diags.warning(
                "op-node-merges-assets",
                f"node '{node_name}' feeds {len(reached)} asset nodes "
                f"({', '.join(sorted(reached))}), which will be merged into one "
                f"multi-asset that always materializes together",
                loc,
                hint="set asset = true to keep them independent",
            )


# --- functions -------------------------------------------------------------


def _check_functions(
    manifest: Manifest, root: Location, diags: Diagnostics
) -> dict[str, NodeFunction]:
    functions: dict[str, NodeFunction] = {}
    for node_name, node in manifest.nodes.items():
        loc = root.child("nodes", node_name, "fn")
        imported = import_object(node.fn)
        if isinstance(imported, ImportFailure):
            diags.error(
                f"fn-{_IMPORT_CODES[imported.problem]}",
                f"node '{node_name}': {imported.detail}",
                loc,
            )
        else:
            described = describe(imported)
            functions[node_name] = described
            _check_signature(node_name, node, described, root, diags)

        for output, entries in node.checks.items():
            for index, entry in enumerate(entries):
                path = as_check_decl(entry).fn
                check_loc = root.child("nodes", node_name, "checks", output).child(index)
                result = import_object(path)
                if isinstance(result, ImportFailure):
                    diags.error(
                        f"check-{_IMPORT_CODES[result.problem]}",
                        f"check on '{node_name}.{output}': {result.detail}",
                        check_loc,
                    )
    return functions


def _check_signature(
    node_name: str, node, described: NodeFunction, root: Location, diags: Diagnostics
) -> None:
    loc = root.child("nodes", node_name, "fn")

    if described.var_params:
        diags.error(
            "signature-var-params",
            f"node '{node_name}' function takes "
            f"{', '.join('*' + p for p in described.var_params)}; the manifest must "
            f"describe the full interface, so variadic parameters are not allowed",
            loc,
        )

    if described.first_param != CTX_PARAMETER:
        found = described.first_param or "no parameters"
        diags.error(
            "missing-ctx-parameter",
            f"node '{node_name}' function must take '{CTX_PARAMETER}' first, got {found}",
            loc,
            hint=f"def {node.fn.rpartition('.')[2]}({CTX_PARAMETER}, ...)",
        )
        # Without ctx the remaining parameters are offset by one; comparing them
        # would produce a second, misleading error.
        return

    declared = list(node.inputs)
    actual = described.params
    missing = [p for p in declared if p not in actual]
    extra = [p for p in actual if p not in declared]

    for param in missing:
        diags.error(
            "signature-missing-parameter",
            f"node '{node_name}' declares input '{param}' but its function has no such parameter",
            root.child("nodes", node_name, "inputs", param),
            hint=_closest(param, actual),
        )
    for param in extra:
        diags.error(
            "signature-extra-parameter",
            f"node '{node_name}' function takes parameter '{param}', which the "
            f"manifest does not declare as an input",
            loc,
            hint=_closest(param, declared) or "add it to `inputs`",
        )

    _check_return_annotation(node_name, node, described, root, diags)


def _check_return_annotation(
    node_name: str, node, described: NodeFunction, root: Location, diags: Diagnostics
) -> None:
    """The dict-shaped annotation is optional; when present it must be right (§7 rule 2)."""
    loc = root.child("nodes", node_name, "outputs")
    #: outputs bound to artifacts are written by the node, not returned.
    value_outputs = [o for o in node.outputs if o not in node.artifacts]

    if described.return_kind is ReturnKind.ABSENT:
        return
    if described.return_kind is ReturnKind.OTHER:
        diags.warning(
            "return-annotation-unrecognised",
            f"node '{node_name}' has a return annotation dagnet cannot compare "
            f"against its declared outputs",
            loc,
            hint="the checked form is `-> {'output_name': type, ...}`",
        )
        return
    if described.return_kind is ReturnKind.NONE:
        if value_outputs:
            diags.error(
                "return-annotation-mismatch",
                f"node '{node_name}' is annotated `-> None` but declares value "
                f"output(s) {', '.join(value_outputs)}",
                loc,
                hint="bind them to artifacts, or return them",
            )
        return

    annotated = list(described.return_outputs)
    if annotated != value_outputs:
        diags.error(
            "return-annotation-mismatch",
            f"node '{node_name}' declares value outputs "
            f"[{', '.join(value_outputs)}] but its return annotation names "
            f"[{', '.join(annotated)}]",
            loc,
            hint="outputs bound to artifacts are written by the node, not returned",
        )


def _check_wired_annotations(
    manifest: Manifest,
    graph: PipelineGraph,
    functions: dict[str, NodeFunction],
    root: Location,
    diags: Diagnostics,
) -> None:
    """Compare annotations across an edge when both sides have one (warning only)."""
    for node_name, refs in graph.references.items():
        consumer = functions.get(node_name)
        if consumer is None:
            continue
        for param, ref in refs.items():
            if not isinstance(ref, NodeRef):
                continue
            producer = functions.get(ref.node)
            if producer is None:
                continue
            upstream = producer.return_outputs.get(ref.output)
            downstream = consumer.param_annotations.get(param)
            if upstream is None or downstream is None or upstream == downstream:
                continue
            diags.warning(
                "type-mismatch",
                f"'{ref}' is annotated {upstream} but arrives at input '{param}' of "
                f"node '{node_name}' annotated {downstream}",
                root.child("nodes", node_name, "inputs", param),
            )


def _closest(name: str, candidates) -> str | None:
    matches = difflib.get_close_matches(name, list(candidates), n=3, cutoff=0.6)
    if matches:
        return f"did you mean {', '.join(repr(m) for m in matches)}?"
    options = sorted(candidates)
    if not options:
        return None
    return f"available: {', '.join(options[:10])}"
