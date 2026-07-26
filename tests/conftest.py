from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture
def write(tmp_path: Path):
    """Write a file under tmp_path and return its path."""

    def _write(name: str, text: str) -> Path:
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
        return path

    return _write
