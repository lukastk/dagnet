"""`ctx` — the small dagnet-owned object every node function receives (DESIGN §7 rule 3).

Deliberately *not* Dagster's context: node code stays framework-agnostic, and this
is the whole surface it sees. Three things, no more: the resolved variables, the
resolved location of any declared artifact, and the run name.
"""

from __future__ import annotations

import difflib
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Mapping

from dagnet.diagnostics import DagnetError
from dagnet.schema import DuckDBTableArtifact, FileArtifact, Manifest, Scalar


@dataclass(frozen=True)
class TableLocation:
    """Where a `duckdb_table` artifact lives: a table, in a particular database.

    Frozen, because a node resolving a location must not be able to move it.
    """

    table: str
    database: Path

    def __str__(self) -> str:
        return f"{self.database}:{self.table}"


#: What `ctx.artifact(key)` hands back — a filesystem path, or a table handle.
ArtifactLocation = Path | TableLocation


class UnknownArtifact(DagnetError):
    """`ctx.artifact()` was called with a key the manifest does not declare."""


def resolve_artifacts(manifest: Manifest, store_root: Path) -> dict[str, ArtifactLocation]:
    """Resolve every declared artifact to the location its nodes read or write.

    **Public API** (exported from `dagnet`), because `dagnet-db` needs artifact
    locations to answer questions like "which database files does this pipeline
    use?". Re-deriving that resolution in another package would recreate the
    shadow path registry `[artifacts]` exists to abolish — the same knowledge in
    two places, drifting. Better an explicit dependency than a second copy.

    Files resolve first, because a table artifact's `database` names one of them.
    A `database` that doesn't resolve is a `check` error, so by the time this runs
    the reference is known good.
    """
    locations: dict[str, ArtifactLocation] = {
        key: store_root / artifact.path
        for key, artifact in manifest.artifacts.items()
        if isinstance(artifact, FileArtifact)
    }
    for key, artifact in manifest.artifacts.items():
        if isinstance(artifact, DuckDBTableArtifact):
            locations[key] = TableLocation(
                table=artifact.table, database=locations[artifact.database]
            )
    return locations


class NodeContext:
    """What a node function's `ctx` parameter is."""

    __slots__ = ("run_name", "vars", "_artifacts", "_manifest_path", "_node_name")

    def __init__(
        self,
        *,
        vars: Mapping[str, Scalar],
        run_name: str,
        artifacts: Mapping[str, ArtifactLocation],
        node_name: str,
        manifest_path: Path,
    ):
        #: this node's resolved variables: globals overridden by its own.
        self.vars: Mapping[str, Scalar] = MappingProxyType(dict(vars))
        self.run_name = run_name
        self._artifacts = artifacts
        self._node_name = node_name
        self._manifest_path = manifest_path

    @property
    def node_name(self) -> str:
        """The manifest name of the node currently executing.

        Read-only: a node does not get to decide which node it is. For an asset
        check, this is the node that produced the output being checked.
        """
        return self._node_name

    @property
    def manifest_path(self) -> Path:
        """Absolute path of the manifest this pipeline was compiled from.

        Read-only. The manifest is a pipeline's discovery root, so a library
        helper a node opts into can locate anything relative to it rather than
        guessing from the working directory — which is not stable across
        executors, since each step of a multiprocess run is its own process.
        """
        return self._manifest_path

    def artifact(self, key: str) -> ArtifactLocation:
        """The resolved location of a declared artifact — a `Path`, or a table name."""
        try:
            return self._artifacts[key]
        except KeyError:
            raise UnknownArtifact(
                f"no artifact '{key}' is declared in [artifacts]{_suggest(key, self._artifacts)}"
            ) from None

    def __repr__(self) -> str:
        return (
            f"NodeContext(node_name={self._node_name!r}, run_name={self.run_name!r}, "
            f"vars={dict(self.vars)!r})"
        )


def _suggest(key: str, known: Mapping[str, ArtifactLocation]) -> str:
    matches = difflib.get_close_matches(key, list(known), n=3, cutoff=0.6)
    if matches:
        return f"; did you mean {', '.join(repr(m) for m in matches)}?"
    if known:
        return f"; declared: {', '.join(sorted(known))}"
    return ""
