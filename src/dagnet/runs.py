"""Run presets: validating them against the declarations, and resolving `ctx.vars`.

DESIGN §6 fixes the file format and the merge (`[defaults]` merged with
`[runs.<name>]`, run wins) but not the full precedence order once node-local
declarations and per-node overrides are in play. The order used here, highest
first:

1. the run's per-node override      `[runs.<run>.<node>] v = ...`
2. `[defaults]`' per-node override  `[defaults.<node>] v = ...`
3. the run's global value           `[runs.<run>] v = ...`
4. `[defaults]`' global value       `[defaults] v = ...`
5. the environment                  `v = { env = "NAME" }`, when NAME is set
6. the node-local declared default  `[nodes.<node>.vars] v = { default = ... }`
7. the global declared default      `[vars] v = { default = ... }`

That is: values set by a run always beat everything a declaration can supply;
among values, more specific beats less specific and the run beats the defaults
section; a declaration's environment source beats its own default (the default is
the fallback for when the environment is silent); among declarations, node-local
beats global (DESIGN §5.3: "a node-level declaration with the same name simply
overrides the value for that node").

Levels 5 and 7 both come from the *governing* declaration for that node, so a
node-local declaration's `env` shadows the global declaration's.
"""

from __future__ import annotations

import difflib
import os
from dataclasses import dataclass
from typing import Mapping

from msgspec import UNSET, UnsetType

from dagnet.diagnostics import DagnetError, Diagnostics, Location
from dagnet.loader import RunRegistry
from dagnet.schema import Manifest, Scalar, VarDecl, split_run_body

_TRUE = frozenset({"1", "true", "yes", "on"})
_FALSE = frozenset({"0", "false", "no", "off"})


class BadEnvironmentValue(DagnetError):
    """An environment variable held something the declared type can't accept."""


class UnresolvedVariable(DagnetError):
    """A variable nothing supplied: not the run, not the environment, no default."""


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


def _declared_value(decl: VarDecl, env: Mapping[str, str], var: str) -> Scalar | UnsetType:
    """What a declaration alone can supply: the environment, then the default.

    An `env`-sourced value beats the declared default (the default is the
    fallback for when the environment is silent), and both lose to anything a run
    preset sets.
    """
    if decl.env is not None and decl.env in env:
        return coerce_env_value(env[decl.env], decl, var)
    return decl.default


def resolve_run(
    manifest: Manifest,
    registry: RunRegistry,
    name: str,
    env: Mapping[str, str] | None = None,
) -> ResolvedRun:
    """Merge declarations, the environment, `[defaults]` and one run preset.

    Assumes `check` has already passed: unknown names and type mismatches in the
    *runs files* are diagnostics, not exceptions, and are not re-reported here. A
    malformed **environment** value does raise, since nothing earlier could have
    seen it.

    `env` defaults to the real process environment; tests and `check` pass their
    own so resolution stays deterministic.
    """
    env = os.environ if env is None else env
    default_globals, default_nodes = split_run_body(registry.defaults)
    run_globals, run_nodes = split_run_body(registry.runs.get(name, {}))

    globals_: dict[str, Scalar] = {}
    for var, decl in manifest.vars.items():
        value = _declared_value(decl, env, var)
        if value is not UNSET:
            globals_[var] = value
    globals_.update(default_globals)
    globals_.update(run_globals)

    per_node: dict[str, dict[str, Scalar]] = {}
    for node_name, node in manifest.nodes.items():
        values = dict(globals_)
        for var, decl in node.vars.items():
            # A node-local declaration shadows the global *declaration*, but not a
            # value a run actually set — hence only applying it where the global
            # contribution was itself just a declaration.
            if var in default_globals or var in run_globals:
                continue
            value = _declared_value(decl, env, var)
            if value is not UNSET:
                values[var] = value
        values.update(default_nodes.get(node_name, {}))
        values.update(run_nodes.get(node_name, {}))
        per_node[node_name] = values

    return ResolvedRun(name=name, globals=globals_, per_node=per_node)


@dataclass(frozen=True)
class MissingVariable:
    """A variable nothing could supply. `node` is None for a global declaration."""

    node: str | None
    var: str
    env: str | None

    def describe(self) -> str:
        where = f"node '{self.node}'" if self.node else "the pipeline"
        out = f"variable '{self.var}' of {where} has no value"
        if self.env is not None:
            out += f"; set it in the run preset, or export {self.env}"
        else:
            out += "; set it in the run preset"
        return out


def unfilled_variables(manifest: Manifest, resolved: ResolvedRun) -> list[MissingVariable]:
    """Declared variables that neither a run, the environment, nor a default filled."""
    missing: list[MissingVariable] = []
    for var, decl in manifest.vars.items():
        if var not in resolved.globals:
            missing.append(MissingVariable(None, var, decl.env))
    for node_name, node in manifest.nodes.items():
        for var, decl in node.vars.items():
            if var not in resolved.per_node[node_name]:
                missing.append(MissingVariable(node_name, var, decl.env))
    return missing


def coerce_env_value(raw: str, decl: VarDecl, var: str) -> Scalar:
    """Environment variables are strings; the declared type says what they mean."""
    if decl.type == "str":
        return raw
    if decl.type == "bool":
        lowered = raw.strip().lower()
        if lowered in _TRUE:
            return True
        if lowered in _FALSE:
            return False
        raise BadEnvironmentValue(
            f"{decl.env}={raw!r} cannot be read as a bool for variable '{var}'; "
            f"use one of {', '.join(sorted(_TRUE | _FALSE))}"
        )
    converter = int if decl.type == "int" else float
    try:
        return converter(raw.strip())
    except ValueError:
        raise BadEnvironmentValue(
            f"{decl.env}={raw!r} cannot be read as {decl.type} for variable '{var}'"
        ) from None


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
        # Resolve against an *empty* environment: whether some machine happens to
        # export a variable must not change what `dagnet check` says. A variable
        # that declares `env` is therefore treated as satisfiable here, and its
        # absence becomes a launch error instead (DESIGN §5.3).
        resolved = resolve_run(manifest, registry, name, env={})
        for missing in unfilled_variables(manifest, resolved):
            if missing.env is not None:
                continue
            where = f"[nodes.{missing.node}.vars]" if missing.node else "[vars]"
            diags.error(
                "unfilled-var",
                f"run '{name}' leaves required variable '{missing.var}' unset",
                registry.run_sources[name],
                hint=f"{missing.var} is declared in {where} with no default and no "
                f"`env`, so every run must set it",
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
