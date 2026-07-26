"""Run presets: validating them against the declarations, and resolving `ctx.vars`.

DESIGN §6 fixes the file format and the merge (`[defaults]` merged with
`[runs.<name>]`, run wins) but not the full precedence order once node-local
declarations and per-node overrides are in play. The order used here, highest
first:

1. the run's per-node override      `[runs.<run>.<node>] v = ...`
2. `[defaults]`' per-node override  `[defaults.<node>] v = ...`
3. the run's global value           `[runs.<run>] v = ...`
4. `[defaults]`' global value       `[defaults] v = ...`
5. the node-local declared default  `[nodes.<node>.vars] v = { default = ... }`
6. the global declared default      `[vars] v = { default = ... }`

That is: values set by a run always beat declared defaults; among values, more
specific beats less specific and the run beats the defaults section; among
declared defaults, node-local beats global (DESIGN §5.3: "a node-level
declaration with the same name simply overrides the value for that node").
"""

from __future__ import annotations

import difflib
from dataclasses import dataclass

from msgspec import UNSET

from dagnet.diagnostics import Diagnostics, Location
from dagnet.loader import RunRegistry
from dagnet.schema import Manifest, Scalar, VarDecl, split_run_body


@dataclass(frozen=True)
class ResolvedRun:
    """A run preset resolved against a manifest: what every node will see."""

    name: str
    #: the global variables, after merging defaults and the run.
    globals: dict[str, Scalar]
    #: node name -> the complete variable mapping that node's `ctx.vars` exposes.
    per_node: dict[str, dict[str, Scalar]]


def declaration_for(manifest: Manifest, node_name: str | None, var: str) -> VarDecl | None:
    """The declaration governing `var` at `node_name` — node-local shadows global."""
    if node_name is not None:
        node = manifest.nodes.get(node_name)
        if node is not None and var in node.vars:
            return node.vars[var]
    return manifest.vars.get(var)


def resolve_run(manifest: Manifest, registry: RunRegistry, name: str) -> ResolvedRun:
    """Merge declarations, `[defaults]` and one run preset into per-node variables.

    Assumes `check` has already passed: unknown names and type mismatches are
    diagnostics, not exceptions, and are not re-reported here.
    """
    default_globals, default_nodes = split_run_body(registry.defaults)
    run_globals, run_nodes = split_run_body(registry.runs.get(name, {}))

    globals_: dict[str, Scalar] = {}
    for var, decl in manifest.vars.items():
        if decl.default is not UNSET:
            globals_[var] = decl.default
    globals_.update(default_globals)
    globals_.update(run_globals)

    per_node: dict[str, dict[str, Scalar]] = {}
    for node_name, node in manifest.nodes.items():
        values = dict(globals_)
        for var, decl in node.vars.items():
            # A node-local declaration shadows the global *declaration*, but not a
            # value a run actually set — hence only applying it where the global
            # contribution was itself just a declared default.
            if decl.default is not UNSET and var not in default_globals and var not in run_globals:
                values[var] = decl.default
        values.update(default_nodes.get(node_name, {}))
        values.update(run_nodes.get(node_name, {}))
        per_node[node_name] = values

    return ResolvedRun(name=name, globals=globals_, per_node=per_node)


def unfilled_variables(manifest: Manifest, resolved: ResolvedRun) -> list[tuple[str | None, str]]:
    """Declared variables with no default that the run never set.

    Returns `(node_name_or_None, var_name)` — None meaning a global declaration.
    """
    missing: list[tuple[str | None, str]] = []
    for var, decl in manifest.vars.items():
        if decl.default is UNSET and var not in resolved.globals:
            missing.append((None, var))
    for node_name, node in manifest.nodes.items():
        for var, decl in node.vars.items():
            if decl.default is UNSET and var not in resolved.per_node[node_name]:
                missing.append((node_name, var))
    return missing


def value_matches(value: Scalar, var_type: str) -> bool:
    """Does a run value match a declared type? `int` widens to `float`, `bool` never does."""
    if isinstance(value, bool):
        return var_type == "bool"
    if var_type == "int":
        return isinstance(value, int)
    if var_type == "float":
        return isinstance(value, (int, float))
    if var_type == "str":
        return isinstance(value, str)
    if var_type == "bool":
        return False
    return False


def validate_runs(manifest: Manifest, registry: RunRegistry, diags: Diagnostics) -> None:
    """Every run key must name a declared variable (or a node), with a matching type."""
    bodies = [(registry.defaults, registry.default_sources, None)]
    for name, body in registry.runs.items():
        source = registry.run_sources.get(name)
        bodies.append((body, None, source))

    for body, key_sources, base in bodies:
        for key, value in body.items():
            loc = _key_location(key, key_sources, base)
            if isinstance(value, dict):
                _validate_node_overrides(manifest, key, value, loc, diags)
            else:
                _validate_value(manifest, None, key, value, loc, diags)

    for name in registry.runs:
        resolved = resolve_run(manifest, registry, name)
        for node_name, var in unfilled_variables(manifest, resolved):
            where = f"[nodes.{node_name}.vars]" if node_name else "[vars]"
            diags.error(
                "unfilled-var",
                f"run '{name}' leaves required variable '{var}' unset",
                registry.run_sources[name],
                hint=f"{var} is declared in {where} with no default, so every run must set it",
            )


def _validate_node_overrides(
    manifest: Manifest, node_name: str, values: dict, loc: Location, diags: Diagnostics
) -> None:
    if node_name not in manifest.nodes:
        diags.error(
            "unknown-run-node",
            f"'{node_name}' is not a node, so it cannot hold per-node variables",
            loc,
            hint=_closest(node_name, manifest.nodes),
        )
        return
    for var, value in values.items():
        _validate_value(manifest, node_name, var, value, loc.child(var), diags)


def _validate_value(
    manifest: Manifest,
    node_name: str | None,
    var: str,
    value: Scalar,
    loc: Location,
    diags: Diagnostics,
) -> None:
    decl = declaration_for(manifest, node_name, var)
    if decl is None:
        scope = f"node '{node_name}'" if node_name else "the pipeline"
        diags.error(
            "unknown-var",
            f"'{var}' is not a variable declared for {scope}",
            loc,
            hint=_closest(var, _visible_vars(manifest, node_name)),
        )
        return
    if not value_matches(value, decl.type):
        diags.error(
            "var-type-mismatch",
            f"'{var}' is declared as {decl.type} but the run sets "
            f"{type(value).__name__} ({value!r})",
            loc,
        )


def _visible_vars(manifest: Manifest, node_name: str | None) -> list[str]:
    names = list(manifest.vars)
    if node_name is not None and node_name in manifest.nodes:
        names += list(manifest.nodes[node_name].vars)
    return names


def _key_location(key: str, sources: dict | None, base: Location | None) -> Location:
    if sources is not None and key in sources:
        return sources[key]
    if base is not None:
        return base.child(key)
    raise AssertionError(f"no location recorded for run key '{key}'")


def _closest(name: str, candidates) -> str | None:
    matches = difflib.get_close_matches(name, list(candidates), n=3, cutoff=0.6)
    if matches:
        return f"did you mean {', '.join(repr(m) for m in matches)}?"
    options = sorted(candidates)
    if not options:
        return None
    return f"declared: {', '.join(options[:10])}"
