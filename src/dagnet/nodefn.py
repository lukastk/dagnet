"""Importing node functions and describing their signatures.

The manifest is authoritative (AGENTS.md): nothing here *derives* a node's
interface from code. This module only reports what a function looks like so
`check.py` can hold the two in lockstep, and so `compile.py` knows whether to
await it.

The one awkward corner is netrun's dict-shaped return annotation
(`-> {'successful_ad_ids': list[int]}`), which is an arbitrary expression rather
than a type. Depending on whether the node module uses
`from __future__ import annotations` it reaches us as a real dict or as source
text, so both forms are normalised to `{output_name: annotation_source}`.
"""

from __future__ import annotations

import ast
import importlib
import inspect
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable


class ImportProblem(Enum):
    """Why an import path did not yield a callable."""

    NO_MODULE = "no-module"
    NO_ATTRIBUTE = "no-attribute"
    NOT_CALLABLE = "not-callable"
    RAISED = "raised"
    MALFORMED_PATH = "malformed-path"


@dataclass(frozen=True)
class ImportFailure:
    problem: ImportProblem
    detail: str


class ReturnKind(Enum):
    ABSENT = "absent"
    #: `-> None`: the node writes artifacts and returns nothing.
    NONE = "none"
    #: `-> {'name': type}`: the documented, validated form.
    DICT = "dict"
    #: anything else — we can say it isn't the contract, but not what it means.
    OTHER = "other"


@dataclass
class NodeFunction:
    """What a node function looks like from the outside."""

    fn: Callable[..., Any]
    is_async: bool
    #: first parameter name, whatever it is — the contract requires `ctx`.
    first_param: str | None
    #: parameter names after the first, in order. These must match `inputs`.
    params: list[str]
    #: parameter name -> normalised annotation source, for annotated parameters only.
    param_annotations: dict[str, str] = field(default_factory=dict)
    #: `*args` / `**kwargs` names, which the contract forbids.
    var_params: list[str] = field(default_factory=list)
    return_kind: ReturnKind = ReturnKind.ABSENT
    #: output name -> normalised annotation source, when `return_kind is DICT`.
    return_outputs: dict[str, str] = field(default_factory=dict)


#: `pre_run` hooks are written entry-point style, so the module/attribute split
#: is explicit rather than inferred from the last dot.
ENTRY_POINT_SEPARATOR = ":"


def import_object(path: str) -> Any | ImportFailure:
    """Import `pkg.module.attribute`. Never guesses; every failure mode is distinct."""
    module_path, _, attribute = path.rpartition(".")
    if not module_path or not attribute:
        return ImportFailure(
            ImportProblem.MALFORMED_PATH,
            f"'{path}' is not a dotted path to a function",
        )
    return _import_attribute(module_path, attribute, path)


def import_entry_point(path: str) -> Any | ImportFailure:
    """Import `pkg.module:attribute` — the explicit form `pre_run` hooks use."""
    module_path, separator, attribute = path.partition(ENTRY_POINT_SEPARATOR)
    if not separator or not module_path or not attribute:
        suggestion = ""
        if ENTRY_POINT_SEPARATOR not in path and "." in path:
            module, _, attr = path.rpartition(".")
            suggestion = f"; did you mean '{module}{ENTRY_POINT_SEPARATOR}{attr}'?"
        return ImportFailure(
            ImportProblem.MALFORMED_PATH,
            f"'{path}' is not of the form 'module.path{ENTRY_POINT_SEPARATOR}callable'{suggestion}",
        )
    return _import_attribute(module_path, attribute, path)


def _import_attribute(module_path: str, attribute: str, path: str) -> Any | ImportFailure:
    try:
        module = importlib.import_module(module_path)
    except ModuleNotFoundError as exc:
        # A ModuleNotFoundError for some *other* module means the target module
        # exists but its own imports are broken — a different problem entirely.
        if exc.name is not None and (
            module_path == exc.name or module_path.startswith(f"{exc.name}.")
        ):
            return ImportFailure(ImportProblem.NO_MODULE, f"no module named '{exc.name}'")
        return ImportFailure(ImportProblem.RAISED, f"importing '{module_path}' raised {exc!r}")
    except Exception as exc:
        return ImportFailure(ImportProblem.RAISED, f"importing '{module_path}' raised {exc!r}")

    try:
        obj = getattr(module, attribute)
    except AttributeError:
        return ImportFailure(
            ImportProblem.NO_ATTRIBUTE,
            f"module '{module_path}' has no attribute '{attribute}'",
        )
    if not callable(obj):
        return ImportFailure(
            ImportProblem.NOT_CALLABLE,
            f"'{path}' is a {type(obj).__name__}, not a callable",
        )
    return obj


def describe(fn: Callable[..., Any]) -> NodeFunction:
    """Introspect a node function's parameters and return annotation."""
    signature = inspect.signature(fn)
    positional: list[str] = []
    var_params: list[str] = []
    annotations: dict[str, str] = {}

    for name, param in signature.parameters.items():
        if param.kind in (param.VAR_POSITIONAL, param.VAR_KEYWORD):
            var_params.append(name)
            continue
        positional.append(name)
        if param.annotation is not inspect.Signature.empty:
            annotations[name] = normalise_annotation(param.annotation)

    kind, outputs = _describe_return(signature.return_annotation)
    return NodeFunction(
        fn=fn,
        is_async=inspect.iscoroutinefunction(fn),
        first_param=positional[0] if positional else None,
        params=positional[1:],
        param_annotations=annotations,
        var_params=var_params,
        return_kind=kind,
        return_outputs=outputs,
    )


def _describe_return(annotation: Any) -> tuple[ReturnKind, dict[str, str]]:
    if annotation is inspect.Signature.empty:
        return ReturnKind.ABSENT, {}
    if annotation is None or annotation is type(None):
        return ReturnKind.NONE, {}

    if isinstance(annotation, dict):
        if all(isinstance(key, str) for key in annotation):
            return ReturnKind.DICT, {
                key: normalise_annotation(value) for key, value in annotation.items()
            }
        return ReturnKind.OTHER, {}

    if isinstance(annotation, str):
        return _parse_return_source(annotation)

    return ReturnKind.OTHER, {}


def _parse_return_source(source: str) -> tuple[ReturnKind, dict[str, str]]:
    """Read a stringified annotation without evaluating it.

    `from __future__ import annotations` turns the dict-shaped annotation into
    source text. Parsing it as an AST reads the output names off the literal keys
    while never executing whatever the values name.
    """
    if source == "None":
        return ReturnKind.NONE, {}
    try:
        expression = ast.parse(source, mode="eval").body
    except SyntaxError:
        return ReturnKind.OTHER, {}
    if not isinstance(expression, ast.Dict):
        return ReturnKind.OTHER, {}

    outputs: dict[str, str] = {}
    for key, value in zip(expression.keys, expression.values):
        if not isinstance(key, ast.Constant) or not isinstance(key.value, str):
            return ReturnKind.OTHER, {}
        outputs[key.value] = ast.unparse(value)
    return ReturnKind.DICT, outputs


def normalise_annotation(annotation: Any) -> str:
    """A comparable source-like string for an annotation in either form."""
    if isinstance(annotation, str):
        try:
            return ast.unparse(ast.parse(annotation, mode="eval").body)
        except SyntaxError:
            return annotation
    if annotation is None or annotation is type(None):
        return "None"
    if isinstance(annotation, type):
        return annotation.__qualname__
    return repr(annotation).replace("typing.", "")
