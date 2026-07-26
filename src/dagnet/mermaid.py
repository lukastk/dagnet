"""`dagnet graph` — a Mermaid flowchart straight from the manifest (DESIGN §8).

Deliberately Dagster-free: this reads the map file and nothing else, so it works
in a README, in CI, and on a manifest whose node functions aren't installed.

The rendering distinguishes the three kinds of edge the manifest can express, so
a reader can see at a glance which dependencies actually carry data:

- a solid labelled arrow — a value flowing from an output to a named parameter;
- an arrow through an artifact node — a durable object, drawn as its own box;
- a dotted arrow — `after`, ordering with no data.
"""

from __future__ import annotations

from dagnet.graph import ArtifactRef, NodeRef, PipelineGraph
from dagnet.schema import DuckDBTableArtifact, FileArtifact, Manifest


def to_mermaid(manifest: Manifest, graph: PipelineGraph, direction: str = "LR") -> str:
    lines = [f"flowchart {direction}"]
    lines.extend(_artifact_nodes(manifest))
    lines.extend(_node_boxes(manifest))
    lines.extend(_edges(manifest, graph))
    lines.extend(_classes(manifest))
    return "\n".join(lines) + "\n"


def _node_boxes(manifest: Manifest) -> list[str]:
    """Nodes, wrapped in a subgraph per `group` so the UI grouping shows up here too."""
    lines: list[str] = []
    ungrouped = [n for n, node in manifest.nodes.items() if node.group is None]
    groups: dict[str, list[str]] = {}
    for name, node in manifest.nodes.items():
        if node.group is not None:
            groups.setdefault(node.group, []).append(name)

    for name in ungrouped:
        lines.append(f"    {_node_id(name)}{_node_shape(manifest, name)}")
    for group, names in groups.items():
        lines.append(f"    subgraph {_safe(group)}[{group}]")
        for name in names:
            lines.append(f"        {_node_id(name)}{_node_shape(manifest, name)}")
        lines.append("    end")
    return lines


def _node_shape(manifest: Manifest, name: str) -> str:
    node = manifest.nodes[name]
    label = f"{name}<br/><small>{', '.join(node.outputs)}</small>" if node.outputs else name
    #: `asset = false` nodes are transient plumbing; draw them with a lighter shape.
    return f'(["{label}"])' if not node.asset else f'["{label}"]'


def _artifact_nodes(manifest: Manifest) -> list[str]:
    lines: list[str] = []
    for key, artifact in manifest.artifacts.items():
        if isinstance(artifact, FileArtifact):
            lines.append(f'    {_artifact_id(key)}[/"{key}<br/><small>{artifact.path}</small>"/]')
        elif isinstance(artifact, DuckDBTableArtifact):
            label = f"{key}<br/><small>{artifact.database}:{artifact.table}</small>"
            lines.append(f'    {_artifact_id(key)}[("{label}")]')
    return lines


def _edges(manifest: Manifest, graph: PipelineGraph) -> list[str]:
    lines: list[str] = []
    for name, node in manifest.nodes.items():
        for output, key in node.artifacts.items():
            lines.append(f"    {_node_id(name)} --> {_artifact_id(key)}")
        for param, ref in graph.references[name].items():
            if isinstance(ref, NodeRef):
                label = ref.output if ref.output == param else f"{ref.output} → {param}"
                lines.append(f'    {_node_id(ref.node)} -->|"{label}"| {_node_id(name)}')
            elif isinstance(ref, ArtifactRef):
                lines.append(f'    {_artifact_id(ref.key)} -->|"{param}"| {_node_id(name)}')
        for target in node.after:
            if target in manifest.nodes:
                lines.append(f"    {_node_id(target)} -.-> {_node_id(name)}")
    return lines


def _classes(manifest: Manifest) -> list[str]:
    lines: list[str] = []
    transient = [n for n, node in manifest.nodes.items() if not node.asset]
    if transient:
        lines.append("    classDef transient stroke-dasharray: 4 3")
        lines.append(f"    class {','.join(_node_id(n) for n in transient)} transient")
    return lines


def _node_id(name: str) -> str:
    return f"n_{name}"


def _artifact_id(key: str) -> str:
    return f"a_{_safe(key)}"


def _safe(text: str) -> str:
    return "".join(c if c.isalnum() or c == "_" else "_" for c in text)
