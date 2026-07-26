"""The manifest and runs-file schema as msgspec structs (DESIGN §5, §6).

Pure data: these structs describe what a `pipeline.toml` / `pipeline.json` and a
`runs.toml` / `runs.json` may contain, and nothing else. No resolution, no
validation beyond structure and types, no Dagster. Loading lives in `loader.py`,
semantic validation in `check.py`, and the mapping to Dagster in `compile.py`.

`forbid_unknown_fields=True` everywhere is deliberate: a mistyped field in the
map must be a loud error, never a silently ignored key.
"""

from __future__ import annotations

from typing import Any, Literal, Union

import msgspec
from msgspec import UNSET, UnsetType

#: The types a variable may be declared as. Deliberately scalars only for v0 —
#: both real consumers' run defs are scalar (DESIGN §6).
VarType = Literal["str", "int", "float", "bool"]

#: A value a variable may hold. `bool` first so msgspec doesn't widen it to int.
Scalar = Union[bool, int, float, str]

PYTHON_TYPE_FOR_VAR: dict[str, type] = {
    "str": str,
    "int": int,
    "float": float,
    "bool": bool,
}


class Base(msgspec.Struct, forbid_unknown_fields=True, kw_only=True):
    pass


class Retries(Base):
    """`retries = { max = 3, wait_s = 10 }` (DESIGN §5.1, §5.5)."""

    max: int
    wait_s: float = 0.0


class Pipeline(Base):
    """`[pipeline]` — metadata (DESIGN §5.1)."""

    name: str
    description: str = ""
    #: Where Dagster keeps instance state. Resolved relative to the manifest file
    #: by `locations.dagster_home`; a `--dagster-home` flag overrides it.
    dagster_home: str = ".dagster"
    #: Where file artifacts resolve. Same resolution rule; `--store-root` overrides.
    store_root: str = "."
    #: The default retry policy for every node. A node's own `retries` replaces
    #: this **entirely** — an override is the whole policy, not a field-wise
    #: merge. Absent here and on the node means no retry.
    retries: Retries | None = None
    #: Validation hooks run before any materialization, as `module.path:callable`
    #: (DESIGN §8). Side-effect-free: all of them run even after one objects, so
    #: they must be safe to run on a launch that is about to be refused.
    pre_run: list[str] = []
    #: Setup hooks run once the launch is committed — after every `pre_run` hook
    #: has passed, still before any step executes (DESIGN §8). This is the slot
    #: for side effects; they run in declared order and stop at the first failure.
    pre_execute: list[str] = []


class VarDecl(Base):
    """One entry of `[vars]` or `[nodes.<n>.vars]` (DESIGN §5.3).

    `default is UNSET` means the variable has no default — distinct from a
    declared default that happens to be falsy.
    """

    type: VarType
    default: Union[Any, UnsetType] = UNSET
    #: An environment variable to take the value from when no run preset sets it.
    #: Naming it here is the point: configuration stays discoverable from the map
    #: rather than being orphan knowledge in someone's shell profile.
    env: str | None = None
    description: str = ""

    @property
    def required(self) -> bool:
        """True when nothing but a run preset can supply this variable."""
        return self.default is UNSET and self.env is None


class FileArtifact(Base, tag="file", tag_field="kind"):
    """`kind = "file"` — a durable file at a declared path (DESIGN §5.4)."""

    path: str
    description: str = ""


class DuckDBTableArtifact(Base, tag="duckdb_table", tag_field="kind"):
    """`kind = "duckdb_table"` — a table in a DuckDB database (DESIGN §5.4).

    `database` is required and names a declared `file`-kind artifact, so the
    database's location is in the map too rather than in a variable or a
    constant — the whole point of the `[artifacts]` section.
    """

    table: str
    database: str
    description: str = ""


Artifact = Union[FileArtifact, DuckDBTableArtifact]


class CheckDecl(Base):
    """The long form of a `checks` entry: `{ fn = "...", blocking = false }`.

    A check blocks by default: a violated contract must fail the run rather than
    being recorded and exited-zero over. `blocking = false` makes one advisory —
    still executed, still recorded and visible, but the run continues.
    """

    fn: str
    blocking: bool = True


#: A `checks` entry is a bare import path (blocking) or the long form above.
CheckEntry = Union[str, CheckDecl]


def as_check_decl(entry: CheckEntry) -> CheckDecl:
    """Normalise either form of a `checks` entry to the long one."""
    if isinstance(entry, str):
        return CheckDecl(fn=entry)
    return entry


class Node(Base):
    """One `[nodes.<name>]` table (DESIGN §5.5)."""

    fn: str
    description: str = ""
    #: parameter name -> reference: either `<node>.<output>` or an artifact key.
    inputs: dict[str, str] = {}
    #: the node's named outputs; authoritative, validated against the signature.
    outputs: list[str] = []
    #: output name -> artifact key: "this output IS that durable artifact".
    artifacts: dict[str, str] = {}
    #: node names this node must run after, with no data passed.
    after: list[str] = []
    pool: str | None = None
    retries: Retries | None = None
    #: output name -> check declarations, each a bare import path or a long form.
    checks: dict[str, list[CheckEntry]] = {}
    #: False makes the outputs transient ops folded into a downstream asset.
    asset: bool = True
    group: str | None = None
    vars: dict[str, VarDecl] = {}


class Manifest(Base):
    """A whole `pipeline.toml` / `pipeline.json` (DESIGN §5)."""

    pipeline: Pipeline
    #: pool name -> max concurrent steps in that pool.
    pools: dict[str, int] = {}
    vars: dict[str, VarDecl] = {}
    artifacts: dict[str, Artifact] = {}
    nodes: dict[str, Node] = {}


#: A run preset's body: scalar keys are global variables, table keys are per-node
#: overrides named after a node (DESIGN §6).
RunBody = dict[str, Union[Scalar, dict[str, Scalar]]]


class RunsFile(Base):
    """A whole `runs.toml` / `runs.json` (DESIGN §6)."""

    defaults: RunBody = {}
    runs: dict[str, RunBody] = {}


def split_run_body(body: RunBody) -> tuple[dict[str, Scalar], dict[str, dict[str, Scalar]]]:
    """Separate a run body into (global variables, per-node variable overrides)."""
    globals_: dict[str, Scalar] = {}
    per_node: dict[str, dict[str, Scalar]] = {}
    for key, value in body.items():
        if isinstance(value, dict):
            per_node[key] = value
        else:
            globals_[key] = value
    return globals_, per_node
