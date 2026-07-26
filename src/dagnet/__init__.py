"""dagnet: a thin declarative wrapper around Dagster.

The whole pipeline is a map file (pipeline.toml / pipeline.json); the only
code you write is each node's body. See _dev/DESIGN.md for the design.
"""

__version__ = "0.0.1"

from dagnet.check import CheckResult, check
from dagnet.compile import build, build_job
from dagnet.context import NodeContext, resolve_artifacts
from dagnet.diagnostics import CheckFailed, DagnetError, Diagnostic, Diagnostics, Severity
from dagnet.loader import RunRegistry, load_manifest, load_runs
from dagnet.locations import store_root
from dagnet.prerun import PreRunContext

__all__ = [
    "CheckFailed",
    "CheckResult",
    "DagnetError",
    "Diagnostic",
    "Diagnostics",
    "NodeContext",
    "PreRunContext",
    "RunRegistry",
    "Severity",
    "__version__",
    "build",
    "build_job",
    "check",
    "load_manifest",
    "load_runs",
    "resolve_artifacts",
    "store_root",
]
