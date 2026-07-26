"""Importing node functions and reading their signatures."""

from __future__ import annotations

import textwrap

import pytest

from dagnet.nodefn import (
    ImportFailure,
    ImportProblem,
    ReturnKind,
    describe,
    import_object,
    normalise_annotation,
)


@pytest.fixture
def node_module(tmp_path, monkeypatch):
    """Write a module onto sys.path and return its import prefix."""
    monkeypatch.syspath_prepend(str(tmp_path))

    def _write(name: str, source: str) -> str:
        (tmp_path / f"{name}.py").write_text(textwrap.dedent(source))
        return name

    return _write


def test_imports_a_dotted_path(node_module):
    node_module("nodes_ok", "def main(ctx):\n    return {}\n")
    assert callable(import_object("nodes_ok.main"))


def test_missing_module_is_distinct_from_a_broken_one(node_module):
    node_module("nodes_broken", "import a_module_that_does_not_exist\n")
    assert import_object("no_such_module.main") == ImportFailure(
        ImportProblem.NO_MODULE, "no module named 'no_such_module'"
    )
    broken = import_object("nodes_broken.main")
    assert broken.problem is ImportProblem.RAISED
    assert "a_module_that_does_not_exist" in broken.detail


def test_import_side_effects_that_raise_are_reported_not_swallowed(node_module):
    node_module("nodes_boom", "raise RuntimeError('kaboom')\n")
    failure = import_object("nodes_boom.main")
    assert failure.problem is ImportProblem.RAISED
    assert "kaboom" in failure.detail


def test_missing_attribute_and_non_callable_are_distinct(node_module):
    node_module("nodes_attrs", "value = 3\n")
    assert import_object("nodes_attrs.main").problem is ImportProblem.NO_ATTRIBUTE
    assert import_object("nodes_attrs.value").problem is ImportProblem.NOT_CALLABLE


def test_a_bare_name_is_a_malformed_path():
    assert import_object("main").problem is ImportProblem.MALFORMED_PATH


def test_describes_parameters_and_marks_async(node_module):
    node_module(
        "nodes_sig",
        """
        async def main(ctx, ad_ids: list[int], flag):
            return {}
        """,
    )
    described = describe(import_object("nodes_sig.main"))
    assert described.is_async
    assert described.first_param == "ctx"
    assert described.params == ["ad_ids", "flag"]
    assert described.param_annotations == {"ad_ids": "list[int]"}
    assert described.var_params == []


def test_var_args_are_reported_so_check_can_reject_them(node_module):
    node_module("nodes_var", "def main(ctx, *args, **kwargs):\n    return {}\n")
    described = describe(import_object("nodes_var.main"))
    assert described.var_params == ["args", "kwargs"]


@pytest.mark.parametrize("future_import", ["", "from __future__ import annotations\n"])
def test_dict_shaped_return_annotation_reads_the_same_either_way(node_module, future_import):
    """netrun's `-> {'name': type}` arrives as a real dict or as source text."""
    name = f"nodes_ret_{bool(future_import)}"
    node_module(
        name,
        future_import
        + textwrap.dedent(
            """
            async def main(ctx, ad_ids: list[int]) -> {'successful_ad_ids': list[int]}:
                return {'successful_ad_ids': ad_ids}
            """
        ),
    )
    described = describe(import_object(f"{name}.main"))
    assert described.return_kind is ReturnKind.DICT
    assert described.return_outputs == {"successful_ad_ids": "list[int]"}


def test_none_return_annotation_is_its_own_kind(node_module):
    node_module("nodes_none", "def main(ctx) -> None:\n    return None\n")
    assert describe(import_object("nodes_none.main")).return_kind is ReturnKind.NONE


def test_absent_and_unrecognised_return_annotations_are_distinguished(node_module):
    node_module(
        "nodes_other",
        "def bare(ctx):\n    return {}\ndef odd(ctx) -> list[int]:\n    return []\n",
    )
    assert describe(import_object("nodes_other.bare")).return_kind is ReturnKind.ABSENT
    assert describe(import_object("nodes_other.odd")).return_kind is ReturnKind.OTHER


def test_a_stringified_dict_annotation_is_never_evaluated(node_module):
    """Only the literal keys are read, so an unimportable value type is harmless."""
    node_module(
        "nodes_lazy",
        "from __future__ import annotations\n"
        "def main(ctx) -> {'out': SomeTypeThatDoesNotExist}:\n    return {'out': 1}\n",
    )
    described = describe(import_object("nodes_lazy.main"))
    assert described.return_outputs == {"out": "SomeTypeThatDoesNotExist"}


@pytest.mark.parametrize(
    "annotation,expected",
    [(int, "int"), (list[int], "list[int]"), ("list[ int ]", "list[int]"), (None, "None")],
)
def test_annotations_normalise_to_comparable_strings(annotation, expected):
    assert normalise_annotation(annotation) == expected
