from __future__ import annotations

import itertools
import textwrap
from dataclasses import dataclass, field
from pathlib import Path

import pytest

#: Module names must be unique across the whole session: `sys.modules` outlives
#: any one test's tmp_path, so a reused name would import the previous file.
_IDS = itertools.count()

MANIFEST_HEADER = '[pipeline]\nname = "p"\n\n'


@dataclass
class Project:
    """A throwaway dagnet project on disk, with its node module importable."""

    manifest_path: Path
    runs_paths: list[Path] = field(default_factory=list)
    module: str = ""
    root: Path = Path(".")


@pytest.fixture
def write(tmp_path: Path):
    """Write a file under tmp_path and return its path."""

    def _write(name: str, text: str) -> Path:
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
        return path

    return _write


@pytest.fixture
def project(tmp_path, monkeypatch):
    """Write a manifest, an optional node module, and optional runs files.

    `MOD` in the manifest is replaced with the generated module's name, so tests
    can write `fn = "MOD.extract"` without caring what it is called.
    """
    monkeypatch.syspath_prepend(str(tmp_path))

    def _project(manifest: str, module: str | None = None, runs: str | None = None) -> Project:
        suffix = next(_IDS)
        name = f"nodes_{suffix}"
        if module is not None:
            (tmp_path / f"{name}.py").write_text(textwrap.dedent(module))
        manifest_path = tmp_path / f"pipeline_{suffix}.toml"
        manifest_path.write_text(MANIFEST_HEADER + textwrap.dedent(manifest).replace("MOD", name))
        runs_paths = []
        if runs is not None:
            runs_path = tmp_path / f"runs_{suffix}.toml"
            runs_path.write_text(textwrap.dedent(runs))
            runs_paths.append(runs_path)
        return Project(
            manifest_path=manifest_path, runs_paths=runs_paths, module=name, root=tmp_path
        )

    return _project
