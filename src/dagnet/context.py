"""`ctx` — the small dagnet-owned object every node function receives (DESIGN §7 rule 3).

Deliberately *not* Dagster's context: node code stays framework-agnostic, and this
is the whole surface it sees. Three things, no more: the resolved variables, the
resolved location of any declared artifact, and the run name.
"""

from __future__ import annotations

import difflib
from pathlib import Path
from types import MappingProxyType
from typing import Mapping

from dagnet.diagnostics import DagnetError
from dagnet.schema import Artifact, DuckDBTableArtifact, FileArtifact, Scalar

#: What `ctx.artifact(key)` hands back: a filesystem path, or a table name.
ArtifactLocation = Path | str


class UnknownArtifact(DagnetError):
    """`ctx.artifact()` was called with a key the manifest does not declare."""


def resolve_artifact(artifact: Artifact, store_root: Path) -> ArtifactLocation:
    """Turn a declared artifact into the location its node will read or write."""
    if isinstance(artifact, FileArtifact):
        return store_root / artifact.path
    if isinstance(artifact, DuckDBTableArtifact):
        return artifact.table
    raise AssertionError(f"unhandled artifact kind: {type(artifact).__name__}")


class NodeContext:
    """What a node function's `ctx` parameter is."""

    __slots__ = ("run_name", "vars", "_artifacts")

    def __init__(
        self,
        *,
        vars: Mapping[str, Scalar],
        run_name: str,
        artifacts: Mapping[str, ArtifactLocation],
    ):
        #: this node's resolved variables: globals overridden by its own.
        self.vars: Mapping[str, Scalar] = MappingProxyType(dict(vars))
        self.run_name = run_name
        self._artifacts = artifacts

    def artifact(self, key: str) -> ArtifactLocation:
        """The resolved location of a declared artifact — a `Path`, or a table name."""
        try:
            return self._artifacts[key]
        except KeyError:
            raise UnknownArtifact(
                f"no artifact '{key}' is declared in [artifacts]{_suggest(key, self._artifacts)}"
            ) from None

    def __repr__(self) -> str:
        return f"NodeContext(run_name={self.run_name!r}, vars={dict(self.vars)!r})"


def _suggest(key: str, known: Mapping[str, ArtifactLocation]) -> str:
    matches = difflib.get_close_matches(key, list(known), n=3, cutoff=0.6)
    if matches:
        return f"; did you mean {', '.join(repr(m) for m in matches)}?"
    if known:
        return f"; declared: {', '.join(sorted(known))}"
    return ""
