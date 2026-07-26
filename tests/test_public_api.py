"""What `import dagnet` promises.

`__all__` is a contract with sibling packages — `dagnet-db` imports from it — so
a name disappearing from it should break a test here rather than a downstream
build.
"""

from __future__ import annotations

from pathlib import Path

import msgspec
import pytest

import dagnet
from dagnet.schema import Manifest

#: Consumed by `dagnet-db`. Removing one of these is a breaking change.
CONSUMED_BY_DAGNET_DB = ("resolve_artifacts", "store_root", "PreRunContext", "NodeContext")


def test_everything_in_all_actually_exists():
    missing = [name for name in dagnet.__all__ if not hasattr(dagnet, name)]
    assert missing == []


def test_all_is_sorted_so_diffs_stay_readable():
    assert list(dagnet.__all__) == sorted(dagnet.__all__)


@pytest.mark.parametrize("name", CONSUMED_BY_DAGNET_DB)
def test_the_names_dagnet_db_depends_on_are_public(name):
    assert name in dagnet.__all__, f"{name} is part of dagnet-db's contract"
    assert getattr(dagnet, name) is not None


def test_artifact_resolution_is_usable_straight_off_the_package(tmp_path):
    """The path dagnet-db takes: manifest -> store root -> resolved locations.

    Duplicating this resolution downstream would rebuild the shadow path registry
    `[artifacts]` exists to abolish, which is why it is exported at all.
    """
    manifest_path = tmp_path / "pipeline.toml"
    manifest_path.write_text(
        '[pipeline]\nname = "p"\nstore_root = "build"\n\n'
        '[artifacts."db/warehouse"]\nkind = "file"\npath = "w.duckdb"\n\n'
        '[artifacts."db/facts"]\nkind = "duckdb_table"\ntable = "facts"\n'
        'database = "db/warehouse"\n'
    )
    manifest: Manifest = msgspec.toml.decode(manifest_path.read_bytes(), type=Manifest)

    root = dagnet.store_root(manifest, manifest_path)
    assert root == (tmp_path / "build").resolve()

    locations = dagnet.resolve_artifacts(manifest, root)
    assert locations["db/warehouse"] == root / "w.duckdb"
    assert locations["db/facts"].table == "facts"
    assert locations["db/facts"].database == root / "w.duckdb"
    assert isinstance(locations["db/warehouse"], Path)
