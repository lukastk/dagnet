"""`dagnet graph`: the Mermaid export, drawn from the manifest alone."""

from __future__ import annotations

import msgspec

from dagnet.graph import PipelineGraph
from dagnet.mermaid import to_mermaid
from dagnet.schema import Manifest


def diagram(text: str, **kwargs) -> str:
    manifest = msgspec.toml.decode(('[pipeline]\nname = "p"\n' + text).encode(), type=Manifest)
    return to_mermaid(manifest, PipelineGraph.build(manifest), **kwargs)


def test_a_value_edge_is_labelled_with_its_output():
    out = diagram("""
    [nodes.a]
    fn = "m.a"
    outputs = ["rows"]

    [nodes.b]
    fn = "m.b"
    inputs = { rows = "a.rows" }
    outputs = ["out"]
    """)
    assert '    n_a -->|"rows"| n_b' in out


def test_a_renamed_edge_shows_both_names():
    out = diagram("""
    [nodes.a]
    fn = "m.a"
    outputs = ["successful_ad_ids"]

    [nodes.b]
    fn = "m.b"
    inputs = { ad_ids = "a.successful_ad_ids" }
    outputs = ["out"]
    """)
    assert '-->|"successful_ad_ids → ad_ids"| n_b' in out


def test_after_is_a_dotted_edge_because_no_data_flows():
    out = diagram("""
    [nodes.a]
    fn = "m.a"
    outputs = ["out"]

    [nodes.b]
    fn = "m.b"
    outputs = ["out"]
    after = ["a"]
    """)
    assert "    n_a -.-> n_b" in out


def test_artifacts_are_their_own_boxes_between_producer_and_consumer():
    out = diagram("""
    [artifacts."db/file"]
    kind = "file"
    path = "w.duckdb"

    [artifacts."db/drugs"]
    kind = "duckdb_table"
    table = "drugs"
    database = "db/file"

    [artifacts."raw/ndc"]
    kind = "file"
    path = "raw/ndc.json"

    [nodes.extract]
    fn = "m.e"
    outputs = ["ndc"]
    artifacts = { ndc = "raw/ndc" }

    [nodes.load]
    fn = "m.l"
    inputs = { ndc = "raw/ndc" }
    outputs = ["drugs"]
    artifacts = { drugs = "db/drugs" }
    """)
    assert 'a_raw_ndc[/"raw/ndc' in out
    assert 'a_db_drugs[("db/drugs' in out
    assert "    n_extract --> a_raw_ndc" in out
    assert '    a_raw_ndc -->|"ndc"| n_load' in out


def test_groups_become_subgraphs():
    out = diagram("""
    [nodes.a]
    fn = "m.a"
    outputs = ["out"]
    group = "extract"
    """)
    assert "    subgraph extract[extract]" in out
    assert out.rstrip().endswith("end")


def test_transient_nodes_are_drawn_differently():
    out = diagram("""
    [nodes.a]
    fn = "m.a"
    outputs = ["out"]

    [nodes.t]
    fn = "m.t"
    inputs = { x = "a.out" }
    outputs = ["out"]
    asset = false

    [nodes.b]
    fn = "m.b"
    inputs = { x = "t.out" }
    outputs = ["out"]
    """)
    assert 'n_t(["t' in out
    assert "class n_t transient" in out


def test_direction_is_configurable():
    assert diagram('[nodes.a]\nfn = "m.a"\noutputs = ["o"]\n', direction="TB").startswith(
        "flowchart TB"
    )


def test_an_unresolvable_reference_simply_draws_no_edge():
    """`graph` must still render a broken manifest — `check` is what complains."""
    out = diagram("""
    [nodes.b]
    fn = "m.b"
    inputs = { x = "ghost.out" }
    outputs = ["out"]
    """)
    assert "n_b" in out
    assert "ghost" not in out
