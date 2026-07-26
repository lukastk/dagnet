"""Parsing `pipeline.toml`/`.json` and `runs.toml`/`.json` into the schema structs.

One schema, two formats: `msgspec` decodes TOML and JSON to the same raw tree, and
everything below this line is format-blind.

Structural validation is done **item by item** rather than by converting the whole
document at once. msgspec raises on the first type error, so a single malformed
node would otherwise hide the other thirty-seven; converting each node, artifact,
var and pool separately means `dagnet check` still reports them all (DESIGN §7b).
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, TypeVar

import msgspec

from dagnet.diagnostics import Diagnostics, Location
from dagnet.schema import (
    Artifact,
    Manifest,
    Node,
    Pipeline,
    RunBody,
    RunsFile,
    Scalar,
    VarDecl,
)

T = TypeVar("T")

#: msgspec appends the offending path as " - at `$.some.path`"; we splice it onto
#: the item's own location so the diagnostic points at the exact key.
_AT_SUFFIX = re.compile(r"^(?P<message>.*) - at `\$(?P<path>[^`]*)`$", re.DOTALL)

_MANIFEST_SECTIONS = frozenset(Manifest.__struct_fields__)
_RUNS_SECTIONS = frozenset(RunsFile.__struct_fields__)


def decode_document(path: Path, diags: Diagnostics) -> dict[str, Any] | None:
    """Read a TOML or JSON file into a raw dict, or report why we couldn't."""
    loc = Location(file=path)
    suffix = path.suffix.lower()
    if suffix not in (".toml", ".json"):
        diags.error(
            "unsupported-format",
            f"unsupported file extension '{path.suffix}'",
            loc,
            hint="dagnet reads .toml and .json",
        )
        return None
    try:
        data = path.read_bytes()
    except OSError as exc:
        diags.error("unreadable-file", f"cannot read file: {exc.strerror}", loc)
        return None

    decoder = msgspec.toml.decode if suffix == ".toml" else msgspec.json.decode
    try:
        raw = decoder(data)
    except msgspec.DecodeError as exc:
        diags.error("malformed-file", f"could not parse {suffix[1:]}: {exc}", loc)
        return None

    if not isinstance(raw, dict):
        diags.error("not-a-table", f"top level must be a table, got {type(raw).__name__}", loc)
        return None
    return raw


def load_manifest(path: str | Path, diags: Diagnostics | None = None) -> Manifest | None:
    """Parse a manifest file. Returns None if it could not be parsed at all.

    Only *structural* problems are reported here (bad types, unknown fields).
    Semantic validation — references, cycles, signatures — is `check.py`.
    """
    own = diags is None
    diags = diags if diags is not None else Diagnostics()
    manifest = _load_manifest(Path(path), diags)
    if own:
        diags.raise_if_errors()
    return manifest


def _load_manifest(path: Path, diags: Diagnostics) -> Manifest | None:
    raw = decode_document(path, diags)
    if raw is None:
        return None
    root = Location(file=path)
    _reject_unknown_sections(raw, _MANIFEST_SECTIONS, root, diags)

    if "pipeline" not in raw:
        diags.error(
            "missing-section",
            "manifest has no [pipeline] section",
            root,
            hint="add [pipeline] with at least a name",
        )
        pipeline = None
    else:
        pipeline = _convert(raw["pipeline"], Pipeline, root.child("pipeline"), diags)
    if pipeline is None:
        return None

    return Manifest(
        pipeline=pipeline,
        pools=_convert_mapping(raw.get("pools"), int, root.child("pools"), diags),
        vars=_convert_mapping(raw.get("vars"), VarDecl, root.child("vars"), diags),
        artifacts=_convert_mapping(raw.get("artifacts"), Artifact, root.child("artifacts"), diags),
        nodes=_convert_mapping(raw.get("nodes"), Node, root.child("nodes"), diags),
    )


class RunRegistry:
    """Named run presets accumulated from one or more files (DESIGN §6).

    `load` is callable repeatedly; the `runs/`-folder form is just a loop over it.
    A run name defined in two sources is a loud error, and so is a `[defaults]`
    key defined in two sources — with several files there is no defensible
    winner, so we refuse to pick one.
    """

    def __init__(self) -> None:
        self.defaults: RunBody = {}
        self.runs: dict[str, RunBody] = {}
        #: where each name came from, so a duplicate can name both files.
        self.run_sources: dict[str, Location] = {}
        self.default_sources: dict[str, Location] = {}

    def load(self, path: str | Path, diags: Diagnostics) -> None:
        """Load one file, or every `*.toml`/`*.json` in a folder."""
        path = Path(path)
        if path.is_dir():
            for child in sorted(path.iterdir()):
                if child.suffix.lower() in (".toml", ".json") and child.is_file():
                    self._load_file(child, diags)
            return
        self._load_file(path, diags)

    def _load_file(self, path: Path, diags: Diagnostics) -> None:
        raw = decode_document(path, diags)
        if raw is None:
            return
        root = Location(file=path)
        _reject_unknown_sections(raw, _RUNS_SECTIONS, root, diags)

        defaults_loc = root.child("defaults")
        defaults = _convert_run_body(raw.get("defaults", {}), defaults_loc, diags)
        for key, value in defaults.items():
            previous = self.default_sources.get(key)
            if previous is not None:
                diags.error(
                    "duplicate-default",
                    f"default '{key}' is set in two sources",
                    defaults_loc.child(key),
                    hint=f"already set at {previous}",
                )
                continue
            self.default_sources[key] = defaults_loc.child(key)
            self.defaults[key] = value

        runs_loc = root.child("runs")
        raw_runs = raw.get("runs", {})
        if not isinstance(raw_runs, dict):
            diags.error(
                "not-a-table",
                f"[runs] must be a table, got {type(raw_runs).__name__}",
                runs_loc,
            )
            return
        for name, raw_body in raw_runs.items():
            loc = runs_loc.child(name)
            previous = self.run_sources.get(name)
            if previous is not None:
                diags.error(
                    "duplicate-run",
                    f"run '{name}' is defined in two sources",
                    loc,
                    hint=f"already defined at {previous}",
                )
                continue
            self.run_sources[name] = loc
            body = _convert_run_body(raw_body, loc, diags)
            self.runs[name] = body


def load_runs(
    path: str | Path, diags: Diagnostics | None = None, registry: RunRegistry | None = None
) -> RunRegistry:
    """Load run presets from a file or a folder, accumulating into `registry`."""
    own = diags is None
    diags = diags if diags is not None else Diagnostics()
    registry = registry if registry is not None else RunRegistry()
    registry.load(path, diags)
    if own:
        diags.raise_if_errors()
    return registry


# --- conversion helpers ---------------------------------------------------


def _reject_unknown_sections(
    raw: dict[str, Any], known: frozenset[str], loc: Location, diags: Diagnostics
) -> None:
    for key in raw:
        if key not in known:
            diags.error(
                "unknown-section",
                f"unknown top-level section '{key}'",
                loc.child(key),
                hint=f"known sections: {', '.join(sorted(known))}",
            )


def _convert(raw: Any, type_: type[T] | Any, loc: Location, diags: Diagnostics) -> T | None:
    """Convert one raw value, turning a msgspec error into a located diagnostic."""
    try:
        return msgspec.convert(raw, type_)
    except msgspec.ValidationError as exc:
        message, at = _split_msgspec_error(str(exc))
        hint = _field_hint(type_) if "unknown field" in message else None
        diags.error("invalid-value", message, _splice(loc, at), hint)
        return None


def _convert_mapping(
    raw: Any, value_type: type[T] | Any, loc: Location, diags: Diagnostics
) -> dict[str, T]:
    """Convert every entry of a table independently, keeping the ones that work."""
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        diags.error("not-a-table", f"expected a table, got {type(raw).__name__}", loc)
        return {}
    out: dict[str, T] = {}
    for key, value in raw.items():
        converted = _convert(value, value_type, loc.child(key), diags)
        if converted is not None:
            out[key] = converted
    return out


def _convert_run_body(raw: Any, loc: Location, diags: Diagnostics) -> RunBody:
    """Convert a `[defaults]` or `[runs.<name>]` body key by key.

    Run bodies have dynamic keys, so converting the whole table at once would
    make msgspec report `runs.one[...]` — useless when a runs file names a dozen
    nodes. Descending manually keeps the offending key in the location.
    """
    if not isinstance(raw, dict):
        diags.error("not-a-table", f"expected a table, got {type(raw).__name__}", loc)
        return {}
    body: RunBody = {}
    for key, value in raw.items():
        if isinstance(value, dict):
            body[key] = _convert_mapping(value, Scalar, loc.child(key), diags)
        else:
            converted = _convert(value, Scalar, loc.child(key), diags)
            if converted is not None:
                body[key] = converted
    return body


def _split_msgspec_error(text: str) -> tuple[str, str | None]:
    match = _AT_SUFFIX.match(text)
    if match is None:
        return text, None
    return match.group("message"), match.group("path")


def _splice(loc: Location, at: str | None) -> Location:
    """Append msgspec's internal `$.a.b[0]` path onto the item's own location."""
    if not at:
        return loc
    return Location(file=loc.file, path=f"{loc.path}{at}" if loc.path else at.lstrip("."))


def _field_hint(type_: Any) -> str | None:
    fields = getattr(type_, "__struct_fields__", None)
    if not fields:
        return None
    return f"known fields: {', '.join(sorted(fields))}"
