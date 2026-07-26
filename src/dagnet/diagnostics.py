"""Aggregated, located diagnostics — the loud-errors half of dagnet.

Everything that inspects a manifest reports through a `Diagnostics` collector
rather than raising on the first problem, so `dagnet check` can print every
problem in one pass (DESIGN §7b).

A `Location` is a file plus a *logical path* into the document
(`nodes.rerank_candidates.inputs.ad_ids`) — the same dotted path in TOML and in
JSON, and unique because the manifest is a keyed document. `line` is carried in
the type but not yet populated; neither `tomllib` nor `msgspec` reports source
positions, so resolving one is a separate job (see `_dev/experiments/FINDINGS.md`).
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from pathlib import Path


class Severity(enum.Enum):
    ERROR = "error"
    WARNING = "warning"


@dataclass(frozen=True)
class Location:
    """Where in a manifest or runs file a diagnostic points."""

    file: Path
    path: str | None = None
    line: int | None = None

    def child(self, *parts: str | int) -> Location:
        """A location one or more steps deeper, e.g. `loc.child("inputs", "ad_ids")`."""
        segments = [self.path] if self.path else []
        for part in parts:
            if isinstance(part, int):
                segments[-1] = f"{segments[-1]}[{part}]"
            else:
                segments.append(_quote_segment(part))
        return Location(file=self.file, path=".".join(segments) or None)

    def __str__(self) -> str:
        out = str(self.file)
        if self.line is not None:
            out += f":{self.line}"
        if self.path:
            out += f":{self.path}"
        return out


def _quote_segment(segment: str) -> str:
    """TOML-quote a key that isn't a bare identifier, so paths round-trip readably."""
    if segment and all(c.isalnum() or c in "_-" for c in segment):
        return segment
    return f'"{segment}"'


@dataclass(frozen=True)
class Diagnostic:
    severity: Severity
    code: str
    message: str
    location: Location
    hint: str | None = None

    def render(self) -> str:
        out = f"{self.location}: {self.severity.value}[{self.code}]: {self.message}"
        if self.hint:
            out += f"\n    hint: {self.hint}"
        return out


@dataclass
class Diagnostics:
    """A collector. Append freely; inspect `errors` at the end."""

    items: list[Diagnostic] = field(default_factory=list)

    def error(self, code: str, message: str, location: Location, hint: str | None = None) -> None:
        self.items.append(Diagnostic(Severity.ERROR, code, message, location, hint))

    def warning(self, code: str, message: str, location: Location, hint: str | None = None) -> None:
        self.items.append(Diagnostic(Severity.WARNING, code, message, location, hint))

    def extend(self, other: Diagnostics) -> None:
        self.items.extend(other.items)

    @property
    def errors(self) -> list[Diagnostic]:
        return [d for d in self.items if d.severity is Severity.ERROR]

    @property
    def warnings(self) -> list[Diagnostic]:
        return [d for d in self.items if d.severity is Severity.WARNING]

    @property
    def ok(self) -> bool:
        return not self.errors

    def codes(self) -> list[str]:
        """Every code in order — the handle tests assert on."""
        return [d.code for d in self.items]

    def render(self) -> str:
        """Compiler-style listing, errors first, then a summary line."""
        ordered = self.errors + self.warnings
        lines = [d.render() for d in ordered]
        if ordered:
            lines.append("")
        lines.append(
            f"{len(self.errors)} error(s), {len(self.warnings)} warning(s)"
            if ordered
            else "no problems found"
        )
        return "\n".join(lines)

    def raise_if_errors(self) -> None:
        if self.errors:
            raise CheckFailed(self)


class DagnetError(Exception):
    """Base for every error dagnet raises deliberately."""


class CheckFailed(DagnetError):
    """Validation found errors. Carries the full diagnostic set, not just the first."""

    def __init__(self, diagnostics: Diagnostics):
        self.diagnostics = diagnostics
        super().__init__(diagnostics.render())
