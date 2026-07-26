"""Grouping nodes into what Dagster executes: clusters (DESIGN §5.5)."""

from __future__ import annotations

import msgspec

from dagnet.graph import PipelineGraph
from dagnet.partition import config_path, partition
from dagnet.schema import Manifest


def clusters(text: str):
    manifest = msgspec.toml.decode(('[pipeline]\nname = "p"\n' + text).encode(), type=Manifest)
    return manifest, partition(manifest, PipelineGraph.build(manifest))


def test_every_asset_node_is_its_own_cluster_when_there_are_no_op_nodes():
    _, result = clusters("""
    [nodes.a]
    fn = "m.a"
    outputs = ["out"]

    [nodes.b]
    fn = "m.b"
    inputs = { x = "a.out" }
    outputs = ["out"]
    """)
    assert [(c.name, c.assets, c.ops) for c in result] == [
        ("a", ("a",), ()),
        ("b", ("b",), ()),
    ]
    assert not any(c.is_graph_backed for c in result)


def test_an_op_node_joins_the_cluster_of_its_downstream_asset():
    _, result = clusters("""
    [nodes.src]
    fn = "m.s"
    outputs = ["out"]

    [nodes.mid]
    fn = "m.m"
    inputs = { x = "src.out" }
    outputs = ["out"]
    asset = false

    [nodes.sink]
    fn = "m.k"
    inputs = { x = "mid.out" }
    outputs = ["out"]
    """)
    by_name = {c.name: c for c in result}
    assert set(by_name) == {"src", "sink_graph"}
    folded = by_name["sink_graph"]
    assert folded.assets == ("sink",)
    assert folded.ops == ("mid",)
    assert folded.members == ("mid", "sink")
    assert folded.is_graph_backed


def test_a_chain_of_op_nodes_all_folds_into_the_same_cluster():
    _, result = clusters("""
    [nodes.src]
    fn = "m.s"
    outputs = ["out"]

    [nodes.op1]
    fn = "m.1"
    inputs = { x = "src.out" }
    outputs = ["out"]
    asset = false

    [nodes.op2]
    fn = "m.2"
    inputs = { x = "op1.out" }
    outputs = ["out"]
    asset = false

    [nodes.sink]
    fn = "m.k"
    inputs = { x = "op2.out" }
    outputs = ["out"]
    """)
    folded = next(c for c in result if c.is_graph_backed)
    assert folded.ops == ("op1", "op2")
    assert folded.members == ("op1", "op2", "sink")


def test_an_op_node_feeding_two_assets_merges_them_into_one_cluster():
    """DESIGN §5.5: the two assets then always materialize together."""
    _, result = clusters("""
    [nodes.src]
    fn = "m.s"
    outputs = ["out"]

    [nodes.shared]
    fn = "m.sh"
    inputs = { x = "src.out" }
    outputs = ["out"]
    asset = false

    [nodes.left]
    fn = "m.l"
    inputs = { x = "shared.out" }
    outputs = ["out"]

    [nodes.right]
    fn = "m.r"
    inputs = { x = "shared.out" }
    outputs = ["out"]
    """)
    merged = next(c for c in result if c.is_graph_backed)
    assert merged.assets == ("left", "right")
    assert merged.name == "left_right_graph"
    assert set(merged.members) == {"shared", "left", "right"}


def test_config_paths_are_flat_for_assets_and_nested_for_folded_nodes():
    _, result = clusters("""
    [nodes.src]
    fn = "m.s"
    outputs = ["out"]

    [nodes.mid]
    fn = "m.m"
    inputs = { x = "src.out" }
    outputs = ["out"]
    asset = false

    [nodes.sink]
    fn = "m.k"
    inputs = { x = "mid.out" }
    outputs = ["out"]
    """)
    by_name = {c.name: c for c in result}
    assert config_path(by_name["src"], "src") == ["ops", "src", "config"]
    assert config_path(by_name["sink_graph"], "mid") == [
        "ops",
        "sink_graph",
        "ops",
        "mid",
        "config",
    ]
