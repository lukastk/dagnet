"""Where Dagster keeps its state, and getting the manifest's pools onto it.

Two findings from the spikes shape this module (`_dev/experiments/FINDINGS.md`):

- pool *limits* are instance state, not definition state. `pool="heavy"` on an
  asset is only a tag; the limit has to be written to the instance, so `dagnet run`
  and `dagnet dev` sync `[pools]` on every invocation — otherwise editing a limit
  in the manifest silently does nothing;
- an ephemeral instance cannot run the multiprocess executor at all, and reports
  `supports_global_concurrency_limits = False`, so `--ephemeral` means in-process
  execution with no pool enforcement.
"""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import dagster as dg

from dagnet.schema import Manifest

#: Written when a DAGSTER_HOME has no config yet. `granularity: op` is what makes
#: a pool limit apply per step rather than per run.
DEFAULT_INSTANCE_CONFIG = "concurrency:\n  pools:\n    granularity: op\n"


def dagster_home(manifest: Manifest, manifest_path: Path, override: str | None = None) -> Path:
    """DESIGN §5.1: `dagster_home` is resolved relative to the manifest file."""
    if override is not None:
        return Path(override).expanduser().resolve()
    declared = Path(manifest.pipeline.dagster_home).expanduser()
    if declared.is_absolute():
        return declared
    return (manifest_path.parent / declared).resolve()


def prepare_home(home: Path) -> None:
    """Create the instance directory, adding a default config only if there is none."""
    home.mkdir(parents=True, exist_ok=True)
    config = home / "dagster.yaml"
    if not config.exists():
        config.write_text(DEFAULT_INSTANCE_CONFIG)


def pool_granularity_is_op(home: Path) -> bool:
    """Whether a user-authored `dagster.yaml` sets the granularity pools need."""
    config = home / "dagster.yaml"
    if not config.exists():
        return False
    text = config.read_text()
    return "granularity: op" in text or "granularity: 'op'" in text or 'granularity: "op"' in text


def sync_pools(instance: dg.DagsterInstance, pools: dict[str, int]) -> bool:
    """Write the manifest's pool limits onto the instance. False if unsupported."""
    storage = instance.event_log_storage
    if not storage.supports_global_concurrency_limits:
        return False
    for name, limit in pools.items():
        storage.set_concurrency_slots(name, limit)
    return True


@contextmanager
def open_instance(home: Path | None) -> Iterator[dg.DagsterInstance]:
    """A persistent instance at `home`, or an ephemeral one when `home` is None."""
    if home is None:
        with dg.DagsterInstance.ephemeral() as instance:
            yield instance
        return
    prepare_home(home)
    with dg.DagsterInstance.from_config(str(home)) as instance:
        yield instance
