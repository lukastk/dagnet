"""Sample 09's topology really is the AI-exposure-index topology.

`sample_projects/09_ai_index/_netrun_reference.json` is a slice of the real
public `config/netrun.json` — node names, which are netrun factory nodes, the
edges, and the pools. This module derives what the dagnet manifest *should* look
like from that reference, mechanically, and compares it to the manifest actually
checked in. Mistranscribe one edge and this fails.

The three collapses under test are the whole point of the sample:

- a `__signal_epoch_finished__` -> `__control_start_epoch__` edge pair becomes one
  `after` entry;
- the `broadcast` factory node disappears, its five out-edges becoming `after`
  entries on the five targets;
- the `join` factory node disappears, its four in-edges becoming four `inputs` on
  the node that consumed the join's output.
"""

from __future__ import annotations

import json
from pathlib import Path

import msgspec
import pytest

from dagnet.schema import Manifest

SAMPLE = Path(__file__).parent.parent / "sample_projects" / "09_ai_index"

SIGNAL_PREFIX = "__signal_"
CONTROL_PREFIX = "__control_"


@pytest.fixture(scope="module")
def reference() -> dict:
    return json.loads((SAMPLE / "_netrun_reference.json").read_text())


@pytest.fixture(scope="module")
def manifest() -> Manifest:
    return msgspec.toml.decode((SAMPLE / "pipeline.toml").read_bytes(), type=Manifest)


def factories(reference: dict, kind: str) -> set[str]:
    return {n["name"] for n in reference["nodes"] if n["factory"] == kind}


def is_ordering(edge: dict) -> bool:
    return edge["source_port"].startswith(SIGNAL_PREFIX) or edge["target_port"].startswith(
        CONTROL_PREFIX
    )


def test_every_real_node_survives_and_every_factory_node_disappears(reference, manifest):
    real = {n["name"] for n in reference["nodes"] if n["factory"] == "from_function"}
    assert set(manifest.nodes) == real

    gone = factories(reference, "broadcast") | factories(reference, "join")
    assert gone == {"broadcast_onet_ready", "join_scores"}
    assert not gone & set(manifest.nodes)


def test_ordering_edges_and_the_broadcast_node_become_exactly_these_after_entries(
    reference, manifest
):
    """Every signal/control edge, with the broadcast node seen through."""
    broadcast = factories(reference, "broadcast")
    ordering = [e for e in reference["edges"] if is_ordering(e)]
    assert len(ordering) == 8

    # source -> targets, resolving any edge that passes through a broadcast node.
    into_broadcast = {
        e["target_node"]: e["source_node"] for e in ordering if e["target_node"] in broadcast
    }
    expected: dict[str, set[str]] = {}
    for edge in ordering:
        if edge["target_node"] in broadcast:
            continue
        source = edge["source_node"]
        source = into_broadcast.get(source, source)
        expected.setdefault(edge["target_node"], set()).add(source)

    actual = {name: set(node.after) for name, node in manifest.nodes.items() if node.after}
    assert actual == expected
    assert expected == {
        "prepare_onet_targets": {"fetch_onet"},
        "sample_ads": {"fetch_adzuna"},
        "embed_onet": {"prepare_onet_targets"},
        "score_presence": {"prepare_onet_targets"},
        "score_felten": {"prepare_onet_targets"},
        "score_task_exposure": {"prepare_onet_targets"},
        "score_task_exposure_bt": {"prepare_onet_targets"},
    }


def test_data_edges_survive_with_their_ports_and_the_join_node_seen_through(reference, manifest):
    join = factories(reference, "join")
    data = [e for e in reference["edges"] if not is_ordering(e)]

    # What fed the join, and what the join fed.
    into_join = [e for e in data if e["target_node"] in join]
    out_of_join = [e for e in data if e["source_node"] in join]
    assert len(into_join) == 4 and len(out_of_join) == 1
    consumer = out_of_join[0]["target_node"]

    expected: dict[str, dict[str, str]] = {}
    for edge in data:
        if edge["source_node"] in join or edge["target_node"] in join:
            continue
        expected.setdefault(edge["target_node"], {})[edge["target_port"]] = (
            f"{edge['source_node']}.{edge['source_port']}"
        )
    # The join's four inputs land on the node that consumed the join's output,
    # keeping the join's own port names as parameter names.
    for edge in into_join:
        expected.setdefault(consumer, {})[edge["target_port"]] = (
            f"{edge['source_node']}.{edge['source_port']}"
        )

    actual = {name: dict(node.inputs) for name, node in manifest.nodes.items() if node.inputs}
    assert actual == expected


def test_the_rename_that_motivated_input_naming_is_preserved(manifest):
    """`llm_filter_candidates.successful_ad_ids` arrives as `rerank_candidates`' `ad_ids`."""
    assert manifest.nodes["rerank_candidates"].inputs == {
        "ad_ids": "llm_filter_candidates.successful_ad_ids"
    }


def test_the_join_became_one_node_with_four_inputs(manifest):
    assert manifest.nodes["combine_onet_exposure"].inputs == {
        "presence": "score_presence.out",
        "felten": "score_felten.out",
        "task_exposure": "score_task_exposure.out",
        "task_exposure_bt": "score_task_exposure_bt.out",
    }


def test_pool_membership_matches(reference, manifest):
    assert manifest.pools == reference["pools"] == {"main": 4, "heavy": 1}
    expected_heavy = {n["name"] for n in reference["nodes"] if "heavy" in n["pools"]}
    actual_heavy = {name for name, node in manifest.nodes.items() if node.pool == "heavy"}
    assert (
        actual_heavy
        == expected_heavy
        == {
            "embed_ads",
            "score_task_exposure",
            "score_task_exposure_bt",
        }
    )


def test_edge_count_is_fully_accounted_for(reference):
    """21 netrun edges: 8 ordering, 5 through the join, 8 plain data edges."""
    join = factories(reference, "join")
    edges = reference["edges"]
    ordering = [e for e in edges if is_ordering(e)]
    through_join = [
        e
        for e in edges
        if not is_ordering(e) and (e["source_node"] in join or e["target_node"] in join)
    ]
    plain = [e for e in edges if e not in ordering and e not in through_join]
    assert (len(edges), len(ordering), len(through_join), len(plain)) == (21, 8, 5, 8)
