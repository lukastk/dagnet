"""Where the manifest says things live.

Two paths are declared in `[pipeline]` and resolved the same way: **relative to
the manifest file**, with an absolute path taken as-is and a CLI flag overriding
both (DESIGN §5.1). Keeping them together means the rule is written once.

- `dagster_home` — Dagster's instance state (run history, event log, pool
  bookkeeping). Default `.dagster/` next to the manifest.
- `store_root` — where file artifacts resolve. Default `.`, i.e. the manifest's
  own directory.
"""

from __future__ import annotations

from pathlib import Path

from dagnet.schema import Manifest


def resolve_declared_path(declared: str, manifest_path: Path, override: str | None) -> Path:
    """`--flag` beats the manifest field, which is relative to the manifest file."""
    if override is not None:
        return Path(override).expanduser().resolve()
    path = Path(declared).expanduser()
    if path.is_absolute():
        return path
    return (manifest_path.parent / path).resolve()


def dagster_home(manifest: Manifest, manifest_path: Path, override: str | None = None) -> Path:
    return resolve_declared_path(manifest.pipeline.dagster_home, manifest_path, override)


def store_root(manifest: Manifest, manifest_path: Path, override: str | None = None) -> Path:
    """Where file artifacts resolve, per `[pipeline] store_root` (DESIGN §5.1).

    **Public API** (exported from `dagnet`), for the same reason as
    `resolve_artifacts`: it is half of the answer to "where does this pipeline
    keep its things", and a consumer that recomputed it would be maintaining a
    second copy of a rule that lives in the manifest.
    """
    return resolve_declared_path(manifest.pipeline.store_root, manifest_path, override)
