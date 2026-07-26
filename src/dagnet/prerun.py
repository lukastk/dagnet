"""Hooks that run before any materialization: `pre_run` and `pre_execute`.

Both are plain callables named in the map like everything else
(`module.path:callable`) and both receive the same `PreRunContext` describing the
launch about to happen. They differ on exactly one axis — **side effects** — and
everything else about them follows from that:

| | `pre_run` | `pre_execute` |
|---|---|---|
| purpose | validate; refuse a bad launch | set up; the launch is committed |
| side effects | **must not** have any | this is the slot for them |
| when | first, before anything | after every `pre_run` hook passed |
| on failure | all hooks still run, then abort | stop at the first failure |
| a refused launch | reaches them | never reaches them |

`pre_run` aggregates because a person should see every objection in one pass, and
that is only safe because those hooks change nothing — several of them will run
on a launch that is already doomed. `pre_execute` cannot aggregate for the same
reason reversed: running the next side effect on top of a half-applied one is
worse than an incomplete report.

Either kind returns a `Diagnostics` or raises. Any ERROR-severity diagnostic
aborts before a single step executes, with the same located output `dagnet check`
produces; WARNING prints and the run proceeds.

The point of `pre_run` is refusal *before* work starts — an advisory command a
careful person remembers to run is no protection against the person who doesn't
think to run it.

**Scope, stated plainly:** both kinds run on dagnet's own launch paths —
`dagnet run`, with or without `--select`, and `--from-failure`. A run launched
from the Dagster UI's launchpad **bypasses them entirely**, because that launch
never passes through dagnet. Covering the UI too would mean compiling a hook step
into the graph upstream of everything; that was considered and deferred
(DESIGN §12).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Mapping

from dagnet.diagnostics import DagnetError, Diagnostics, Location
from dagnet.nodefn import ImportFailure, import_entry_point


class PreRunReturnError(DagnetError):
    """A hook returned something that is not a `Diagnostics`."""


class PreRunHookError(DagnetError):
    """A hook could not be imported when it was needed."""


@dataclass(frozen=True)
class PreRunContext:
    """What a `pre_run` hook is told about the launch it may refuse."""

    #: Absolute path of the manifest this launch was compiled from.
    manifest_path: Path
    #: The **effective** store root for this launch, absolute: the `--store-root`
    #: override when one was given, otherwise `[pipeline] store_root`, otherwise
    #: the default. Use this rather than re-deriving it from the manifest — under
    #: `--store-root` the two differ, and a hook that resolved durable locations
    #: itself would act on a different database from the one the run writes to,
    #: with nothing in the manifest to reveal the discrepancy.
    #:
    #: Deliberately has no default: a hook must never silently receive a
    #: plausible-looking wrong location, which is the whole failure this field
    #: exists to close.
    store_root: Path
    #: The run preset's name, or None when launched without one.
    run_name: str | None
    #: The `--select` expression exactly as given, or None meaning everything.
    #: `is_everything` is the readable form of that test.
    selection: str | None
    #: Node names this launch will run, sorted.
    node_names: tuple[str, ...] = ()
    #: Asset keys this launch will materialize, in `namespace/name` form, sorted.
    asset_keys: tuple[str, ...] = ()
    #: The resolved Dagster run config: variables per node, plus the run name.
    run_config: Mapping[str, Any] = field(default_factory=dict)
    #: True when this launch is a `--from-failure` resume rather than a fresh run.
    #: A resume re-executes a *subset* of the selection below — the steps that
    #: failed and what depends on them — so a guard that would refuse a narrow
    #: selection must not refuse a legitimate resume, and anything that clears
    #: state before writing must not clear it again for steps that already
    #: succeeded.
    is_resume: bool = False
    #: The run being resumed, when `is_resume`. None otherwise.
    parent_run_id: str | None = None

    @property
    def is_everything(self) -> bool:
        """True when no `--select` was given, i.e. the whole pipeline is in scope.

        Note this describes the *selection*, not the set of steps that will end up
        executing: a resume re-executes a subset of it. Check `is_resume` before
        concluding anything about what will actually run.
        """
        return self.selection is None

    def location(self, path: str | None = None) -> Location:
        """A `Location` in this manifest, for a hook building its own diagnostics."""
        return Location(file=self.manifest_path, path=path)


def load_hooks(paths: list[str]) -> list[tuple[str, Callable[..., Any]]]:
    """Import every declared hook. Raises rather than launching with a broken one."""
    loaded: list[tuple[str, Callable[..., Any]]] = []
    for path in paths:
        hook = import_entry_point(path)
        if isinstance(hook, ImportFailure):
            raise PreRunHookError(f"pre_run hook '{path}': {hook.detail}")
        loaded.append((path, hook))
    return loaded


def run_hooks(paths: list[str], context: PreRunContext) -> Diagnostics:
    """Run every declared `pre_run` hook and collect what they all say.

    Every hook runs even if an earlier one objected: the point of aggregated
    diagnostics is that a person sees all of it in one pass. That is only safe
    because `pre_run` hooks are required to be side-effect-free — several of them
    will run on a launch that is already doomed. A hook that raises becomes an
    ERROR diagnostic naming the hook, so one broken hook cannot let a launch
    through and the output stays uniform.
    """
    diagnostics = Diagnostics()
    for path, hook in load_hooks(paths):
        try:
            returned = hook(context)
        except Exception as exc:
            diagnostics.error(
                "pre-run-refused",
                f"pre_run hook '{path}' refused this run: {exc}",
                context.location("pipeline.pre_run"),
                hint=f"raised {type(exc).__name__}",
            )
            continue
        diagnostics.extend(_as_diagnostics(path, returned, context))
    return diagnostics


def run_setup_hooks(paths: list[str], context: PreRunContext) -> Diagnostics:
    """Run the `pre_execute` hooks: side effects, once the launch is committed.

    Deliberately *not* aggregated. These hooks change things — dagnet-db clears
    tables here — so they run in declared order and stop at the first failure.
    Carrying on past a hook that failed would mean running the next side effect
    on top of a half-applied one, and reporting a tidy list of everything that
    went wrong is worth much less than not making the mess.
    """
    diagnostics = Diagnostics()
    for path, hook in load_hooks(paths):
        try:
            returned = hook(context)
        except Exception as exc:
            diagnostics.error(
                "pre-execute-failed",
                f"pre_execute hook '{path}' failed: {exc}",
                context.location("pipeline.pre_execute"),
                hint=f"raised {type(exc).__name__}",
            )
            return diagnostics
        diagnostics.extend(_as_diagnostics(path, returned, context))
        if diagnostics.errors:
            return diagnostics
    return diagnostics


def _as_diagnostics(path: str, returned: Any, context: PreRunContext) -> Diagnostics:
    """A hook returns a `Diagnostics`, or None when it has nothing to say."""
    if returned is None:
        return Diagnostics()
    if isinstance(returned, Diagnostics):
        return returned
    raise PreRunReturnError(
        f"pre_run hook '{path}' must return a Diagnostics (or None), "
        f"but returned {type(returned).__name__}"
    )
