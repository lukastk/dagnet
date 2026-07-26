"""The derived graph view: reference resolution, deps, cycles, asset keys."""

from __future__ import annotations

import msgspec
import pytest

from dagnet.graph import (
    ArtifactRef,
    NodeRef,
    PipelineGraph,
    RefError,
    asset_key,
    resolve_reference,
)
from dagnet.schema import Manifest


def manifest(text: str) -> Manifest:
    return msgspec.toml.decode(('[pipeline]\nname = "p"\n' + text).encode(), type=Manifest)


CHAIN = manifest("""
[nodes.extract]
fn = "m.extract"
outputs = ["rows"]

[nodes.transform]
fn = "m.transform"
inputs = { rows = "extract.rows" }
outputs = ["clean", "rejects"]

[nodes.load]
fn = "m.load"
inputs = { data = "transform.clean" }
outputs = ["done"]
after = ["extract"]
""")


def test_resolves_a_node_output_reference():
    assert resolve_reference(CHAIN, "extract.rows") == NodeRef("extract", "rows")


def test_resolves_an_artifact_reference():
    m = manifest(
        '[artifacts."db/file"]\nkind = "file"\npath = "w.duckdb"\n'
        '[artifacts."db/drugs"]\nkind = "duckdb_table"\ntable = "drugs"\ndatabase = "db/file"\n'
    )
    assert resolve_reference(m, "db/drugs") == ArtifactRef("db/drugs")


@pytest.mark.parametrize(
    "ref,expected",
    [
        ("nosuch.rows", RefError.NO_SUCH_NODE),
        ("extract.nosuch", RefError.NO_SUCH_OUTPUT),
        ("just_a_name", RefError.NOT_A_REFERENCE),
    ],
)
def test_unresolvable_references_say_why(ref, expected):
    assert resolve_reference(CHAIN, ref) is expected


def test_a_reference_that_is_both_an_artifact_and_a_node_output_is_ambiguous():
    m = manifest("""
[artifacts."extract.rows"]
kind = "file"
path = "x.json"

[nodes.extract]
fn = "m.extract"
outputs = ["rows"]
""")
    assert resolve_reference(m, "extract.rows") is RefError.AMBIGUOUS


def test_deps_come_from_inputs_and_after_but_consumers_only_from_inputs():
    g = PipelineGraph.build(CHAIN)
    assert g.data_deps["load"] == {"transform"}
    assert g.all_deps["load"] == {"transform", "extract"}
    assert g.consumers["extract"] == {"transform"}


def test_topological_order_puts_dependencies_first():
    order = PipelineGraph.build(CHAIN).topological_order()
    assert order.index("extract") < order.index("transform") < order.index("load")


def test_asset_key_is_node_slash_output_by_default():
    assert asset_key(CHAIN, "transform", "clean") == ("transform", "clean")


def test_an_artifact_bound_output_takes_the_artifact_key():
    m = manifest("""
[artifacts."db/file"]
kind = "file"
path = "w.duckdb"

[artifacts."db/drugs"]
kind = "duckdb_table"
table = "drugs"
database = "db/file"

[nodes.load_drugs]
fn = "m.load"
outputs = ["drugs"]
artifacts = { drugs = "db/drugs" }
""")
    assert asset_key(m, "load_drugs", "drugs") == ("db", "drugs")


def test_finds_a_two_node_cycle_once():
    m = manifest("""
[nodes.a]
fn = "m.a"
inputs = { x = "b.out" }
outputs = ["out"]

[nodes.b]
fn = "m.b"
inputs = { x = "a.out" }
outputs = ["out"]
""")
    assert PipelineGraph.build(m).find_cycles() == [["a", "b"]]


def test_finds_a_self_cycle():
    m = manifest('[nodes.a]\nfn = "m.a"\noutputs = ["out"]\nafter = ["a"]\n')
    assert PipelineGraph.build(m).find_cycles() == [["a"]]


def test_finds_a_cycle_created_by_after_alone():
    m = manifest("""
[nodes.a]
fn = "m.a"
outputs = ["out"]
after = ["b"]

[nodes.b]
fn = "m.b"
inputs = { x = "a.out" }
outputs = ["out"]
""")
    assert PipelineGraph.build(m).find_cycles() == [["a", "b"]]


def test_an_acyclic_pipeline_has_no_cycles():
    assert PipelineGraph.build(CHAIN).find_cycles() == []


def test_a_deep_chain_does_not_blow_the_recursion_limit():
    """3000 nodes deep, named so the *first* node visited is the deepest."""
    depth = 3000
    lines = [f'[nodes.n{depth - 1:04d}]\nfn = "m.n"\noutputs = ["out"]\n']
    for i in range(depth - 1):
        lines.append(
            f'[nodes.n{i:04d}]\nfn = "m.n"\n'
            f'inputs = {{ x = "n{i + 1:04d}.out" }}\noutputs = ["out"]\n'
        )
    g = PipelineGraph.build(manifest("\n".join(lines)))
    assert g.find_cycles() == []
    order = g.topological_order()
    assert len(order) == depth
    assert order.index(f"n{depth - 1:04d}") < order.index("n0000")


OP_CHAIN = manifest("""
[nodes.src]
fn = "m.src"
outputs = ["out"]

[nodes.mid]
fn = "m.mid"
inputs = { x = "src.out" }
outputs = ["out"]
asset = false

[nodes.sink_a]
fn = "m.a"
inputs = { x = "mid.out" }
outputs = ["out"]

[nodes.sink_b]
fn = "m.b"
inputs = { x = "sink_a.out" }
outputs = ["out"]
""")


def test_nearest_asset_consumers_stops_at_the_first_asset_node():
    g = PipelineGraph.build(OP_CHAIN)
    assert g.nearest_asset_consumers("mid") == {"sink_a"}
    assert g.reachable_consumers("mid") == {"sink_a", "sink_b"}


def test_nearest_asset_consumers_sees_through_a_chain_of_op_nodes():
    m = manifest("""
[nodes.src]
fn = "m.src"
outputs = ["out"]

[nodes.op1]
fn = "m.op1"
inputs = { x = "src.out" }
outputs = ["out"]
asset = false

[nodes.op2]
fn = "m.op2"
inputs = { x = "op1.out" }
outputs = ["out"]
asset = false

[nodes.sink]
fn = "m.sink"
inputs = { x = "op2.out" }
outputs = ["out"]
""")
    assert PipelineGraph.build(m).nearest_asset_consumers("op1") == {"sink"}
