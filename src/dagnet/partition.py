"""Splitting the flat manifest into what Dagster actually executes (DESIGN §5.5).

A node with `asset = false` has no asset identity: it becomes an op folded into
the graph backing the nearest downstream asset node. This module works out which
ops go with which assets, while the manifest stays a flat graph.

The unit is a **cluster**: one or more asset nodes plus the op-nodes that feed
them. Almost every cluster is a single asset node on its own. Clusters grow in
one case only — an op-node feeding two asset nodes forces those assets into one
cluster, because the op cannot be in two graphs at once. That is exactly the
"merged into one multi-asset that always materializes together" consequence
DESIGN §5.5 warns about at check time.
"""

from __future__ import annotations

from dataclasses import dataclass

from dagnet.graph import PipelineGraph
from dagnet.schema import Manifest

#: Suffix for the generated graph wrapping a cluster that folds in op-nodes.
GRAPH_SUFFIX = "_graph"


@dataclass(frozen=True)
class Cluster:
    """One unit of Dagster execution: a plain asset, or a graph-backed one."""

    #: the op/graph name this cluster gets in the job — the key run config uses.
    name: str
    #: asset nodes (`asset = true`) in the cluster, sorted.
    assets: tuple[str, ...]
    #: op nodes (`asset = false`) folded in, in topological order.
    ops: tuple[str, ...]
    #: every member node, dependencies before dependents.
    members: tuple[str, ...]

    @property
    def is_graph_backed(self) -> bool:
        return bool(self.ops)


def partition(manifest: Manifest, graph: PipelineGraph) -> list[Cluster]:
    """Group nodes into clusters. Assumes `check` has passed."""
    asset_nodes = [n for n, node in manifest.nodes.items() if node.asset]
    op_nodes = [n for n, node in manifest.nodes.items() if not node.asset]

    merge = _DisjointSet(asset_nodes)
    targets_by_op: dict[str, set[str]] = {}
    for op_name in op_nodes:
        targets = graph.nearest_asset_consumers(op_name)
        targets_by_op[op_name] = targets
        ordered = sorted(targets)
        for other in ordered[1:]:
            merge.union(ordered[0], other)

    grouped_assets: dict[str, list[str]] = {}
    for name in asset_nodes:
        grouped_assets.setdefault(merge.find(name), []).append(name)

    grouped_ops: dict[str, list[str]] = {}
    for op_name, targets in targets_by_op.items():
        if not targets:
            # `check` reports this as an orphan op-node; nothing to compile.
            continue
        grouped_ops.setdefault(merge.find(next(iter(sorted(targets)))), []).append(op_name)

    order = {name: index for index, name in enumerate(graph.topological_order())}
    clusters: list[Cluster] = []
    for root, assets in grouped_assets.items():
        ops = grouped_ops.get(root, [])
        members = sorted(assets + ops, key=lambda n: order[n])
        clusters.append(
            Cluster(
                name=_cluster_name(sorted(assets), bool(ops)),
                assets=tuple(sorted(assets)),
                ops=tuple(sorted(ops, key=lambda n: order[n])),
                members=tuple(members),
            )
        )
    return sorted(clusters, key=lambda c: c.name)


def config_path(cluster: Cluster, node_name: str) -> list[str]:
    """Where a node's config lives in run config.

    A plain asset is `ops.<node>.config`; a node folded into a graph is one level
    deeper, at `ops.<graph>.ops.<node>.config`.
    """
    if not cluster.is_graph_backed:
        return ["ops", cluster.name, "config"]
    return ["ops", cluster.name, "ops", node_name, "config"]


def _cluster_name(assets: list[str], graph_backed: bool) -> str:
    if not graph_backed:
        return assets[0]
    return "_".join(assets) + GRAPH_SUFFIX


class _DisjointSet:
    """Union-find over asset node names."""

    def __init__(self, names: list[str]):
        self._parent = {name: name for name in names}

    def find(self, name: str) -> str:
        root = name
        while self._parent[root] != root:
            root = self._parent[root]
        while self._parent[name] != root:
            self._parent[name], name = root, self._parent[name]
        return root

    def union(self, left: str, right: str) -> None:
        left_root, right_root = self.find(left), self.find(right)
        if left_root != right_root:
            self._parent[max(left_root, right_root)] = min(left_root, right_root)
