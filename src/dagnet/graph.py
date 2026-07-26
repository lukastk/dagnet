"""The derived, framework-free view of a manifest.

Everything here is a pure function of the schema structs: what an input reference
points at, which nodes depend on which, what asset key an output gets, where the
cycles are. `check.py` phrases the problems it finds as diagnostics and `compile.py`
turns the same structure into Dagster definitions — neither re-derives it.

Nothing in this module imports Dagster or the user's node functions.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field

from dagnet.schema import Manifest, Node

#: Separator between a node and one of its outputs in an `inputs` reference.
REF_SEPARATOR = "."
#: Separator inside an artifact key, which becomes a namespaced Dagster asset key.
ARTIFACT_SEPARATOR = "/"


@dataclass(frozen=True)
class NodeRef:
    """An input wired to the *value* of another node's output."""

    node: str
    output: str

    def __str__(self) -> str:
        return f"{self.node}{REF_SEPARATOR}{self.output}"


@dataclass(frozen=True)
class ArtifactRef:
    """An input wired to a declared artifact; the function receives its location."""

    key: str

    def __str__(self) -> str:
        return self.key


Reference = NodeRef | ArtifactRef


class RefError(enum.Enum):
    """Why a reference did not resolve. `check.py` turns these into messages."""

    NO_SUCH_NODE = "no-such-node"
    NO_SUCH_OUTPUT = "no-such-output"
    NOT_A_REFERENCE = "not-a-reference"
    AMBIGUOUS = "ambiguous"


def resolve_reference(manifest: Manifest, ref: str) -> Reference | RefError:
    """Resolve one `inputs` value to the artifact or node output it names.

    An artifact key wins only if it is *unambiguous*: a string that is both a
    declared artifact key and a real `<node>.<output>` is refused rather than
    silently preferred one way.
    """
    as_artifact = ArtifactRef(ref) if ref in manifest.artifacts else None
    as_node = _resolve_node_ref(manifest, ref)

    if as_artifact is not None and isinstance(as_node, NodeRef):
        return RefError.AMBIGUOUS
    if as_artifact is not None:
        return as_artifact
    return as_node


def _resolve_node_ref(manifest: Manifest, ref: str) -> NodeRef | RefError:
    node_name, sep, output = ref.partition(REF_SEPARATOR)
    if not sep:
        return RefError.NOT_A_REFERENCE
    node = manifest.nodes.get(node_name)
    if node is None:
        return RefError.NO_SUCH_NODE
    if output not in node.outputs:
        return RefError.NO_SUCH_OUTPUT
    return NodeRef(node_name, output)


def asset_key(manifest: Manifest, node_name: str, output: str) -> tuple[str, ...]:
    """The Dagster asset key an output gets: its bound artifact key, else `node/output`.

    Returned as a tuple of path components so `compile.py` can hand it straight to
    `AssetKey` and `check.py` can compare keys for collisions.
    """
    node = manifest.nodes[node_name]
    bound = node.artifacts.get(output)
    if bound is not None:
        return tuple(bound.split(ARTIFACT_SEPARATOR))
    return (node_name, output)


@dataclass
class PipelineGraph:
    """Node-level dependency structure derived from `inputs` and `after`."""

    manifest: Manifest
    #: node -> the references its inputs resolve to, by parameter name. Only
    #: resolvable references appear; `check.py` reports the rest.
    references: dict[str, dict[str, Reference]] = field(default_factory=dict)
    #: node -> upstream node names, through `inputs` only (data dependencies).
    data_deps: dict[str, set[str]] = field(default_factory=dict)
    #: node -> upstream node names, through `inputs` *and* `after`.
    all_deps: dict[str, set[str]] = field(default_factory=dict)
    #: node -> downstream node names, through `inputs` only. Inverse of data_deps.
    consumers: dict[str, set[str]] = field(default_factory=dict)
    #: artifact key -> the (node, output) pairs bound to it. More than one is a
    #: check error; the graph records them all so `check.py` can name both.
    artifact_producers: dict[str, list[tuple[str, str]]] = field(default_factory=dict)

    @classmethod
    def build(cls, manifest: Manifest) -> PipelineGraph:
        graph = cls(manifest=manifest)
        for name, node in manifest.nodes.items():
            graph.references[name] = {}
            graph.data_deps[name] = set()
            graph.all_deps[name] = set()
            graph.consumers.setdefault(name, set())
            for output, key in node.artifacts.items():
                graph.artifact_producers.setdefault(key, []).append((name, output))

        for name, node in manifest.nodes.items():
            for param, ref in node.inputs.items():
                resolved = resolve_reference(manifest, ref)
                if isinstance(resolved, RefError):
                    continue
                graph.references[name][param] = resolved
                producer = graph.producer_of(resolved)
                # An artifact input depends on whichever node writes that artifact
                # (DESIGN §8: artifact ref -> dep + location injection), so the
                # edge exists even though no value flows through the IO manager.
                if producer is not None and producer != name:
                    graph.data_deps[name].add(producer)
                    graph.consumers[producer].add(name)
            graph.all_deps[name] = set(graph.data_deps[name])
            graph.all_deps[name].update(a for a in node.after if a in manifest.nodes)
        return graph

    def producer_of(self, ref: Reference) -> str | None:
        """The single node that produces a reference, or None if not exactly one."""
        if isinstance(ref, NodeRef):
            return ref.node
        producers = self.artifact_producers.get(ref.key, [])
        return producers[0][0] if len(producers) == 1 else None

    def node(self, name: str) -> Node:
        return self.manifest.nodes[name]

    def find_cycles(self) -> list[list[str]]:
        """Every cyclic group in the dependency graph, each reported once.

        Tarjan's strongly-connected components: a component of more than one node
        is a cycle, and so is a node that depends on itself.
        """
        return _strongly_connected_cycles(self.all_deps)

    def topological_order(self) -> list[str]:
        """Dependencies before dependents.

        Iterative depth-first: a pipeline is allowed to be thousands of nodes deep
        and Python's recursion limit is 1000. A node inside a cycle still comes out
        somewhere sensible — `find_cycles` is what reports the cycle.
        """
        order: list[str] = []
        emitted: set[str] = set()
        started: set[str] = set()
        for root in sorted(self.all_deps):
            if root in emitted:
                continue
            stack: list[tuple[str, list[str]]] = [(root, sorted(self.all_deps[root]))]
            started.add(root)
            while stack:
                name, pending = stack[-1]
                if pending:
                    dep = pending.pop()
                    if dep not in emitted and dep not in started:
                        started.add(dep)
                        stack.append((dep, sorted(self.all_deps[dep])))
                    continue
                stack.pop()
                if name not in emitted:
                    emitted.add(name)
                    order.append(name)
        return order

    def reachable_consumers(self, start: str) -> set[str]:
        """Every node downstream of `start` through `inputs` edges, transitively."""
        out: set[str] = set()
        frontier = list(self.consumers[start])
        while frontier:
            name = frontier.pop()
            if name in out:
                continue
            out.add(name)
            frontier.extend(self.consumers[name])
        return out

    def nearest_asset_consumers(self, start: str) -> set[str]:
        """The first `asset = true` nodes reached downstream of an op-node.

        Search stops at each asset node rather than continuing through it, so an
        op-node feeding one asset that feeds another reports only the first
        (DESIGN §5.5: op-nodes fold into the *nearest* downstream asset).
        """
        out: set[str] = set()
        seen: set[str] = set()
        frontier = list(self.consumers[start])
        while frontier:
            name = frontier.pop()
            if name in seen:
                continue
            seen.add(name)
            if self.node(name).asset:
                out.add(name)
            else:
                frontier.extend(self.consumers[name])
        return out


def _strongly_connected_cycles(deps: dict[str, set[str]]) -> list[list[str]]:
    """Tarjan's SCC, returning only the components that are actually cyclic."""
    index: dict[str, int] = {}
    low: dict[str, int] = {}
    on_stack: set[str] = set()
    stack: list[str] = []
    counter = 0
    cycles: list[list[str]] = []

    def strongconnect(root: str) -> None:
        nonlocal counter
        # Iterative, so a deep pipeline can't blow the Python stack.
        work: list[tuple[str, list[str]]] = [(root, sorted(deps.get(root, ())))]
        index[root] = low[root] = counter
        counter += 1
        stack.append(root)
        on_stack.add(root)

        while work:
            name, pending = work[-1]
            if pending:
                dep = pending.pop()
                if dep not in index:
                    index[dep] = low[dep] = counter
                    counter += 1
                    stack.append(dep)
                    on_stack.add(dep)
                    work.append((dep, sorted(deps.get(dep, ()))))
                elif dep in on_stack:
                    low[name] = min(low[name], index[dep])
                continue

            work.pop()
            if work:
                parent = work[-1][0]
                low[parent] = min(low[parent], low[name])
            if low[name] == index[name]:
                component: list[str] = []
                while True:
                    member = stack.pop()
                    on_stack.discard(member)
                    component.append(member)
                    if member == name:
                        break
                if len(component) > 1 or name in deps.get(name, ()):
                    cycles.append(sorted(component))

    for name in sorted(deps):
        if name not in index:
            strongconnect(name)
    return cycles
