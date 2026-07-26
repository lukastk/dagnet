"""Loading manifests and runs files: both formats, aggregated structural errors."""

from __future__ import annotations

import pytest

from dagnet.diagnostics import CheckFailed, Diagnostics
from dagnet.loader import RunRegistry, load_manifest, load_runs
from dagnet.schema import FileArtifact

MINIMAL = '[pipeline]\nname = "p"\n'


def test_loads_toml_and_json_to_the_same_manifest(write):
    toml = write(
        "pipeline.toml",
        '[pipeline]\nname = "p"\n\n[nodes.a]\nfn = "m.a"\noutputs = ["out"]\n',
    )
    json = write(
        "pipeline.json",
        '{"pipeline": {"name": "p"}, "nodes": {"a": {"fn": "m.a", "outputs": ["out"]}}}',
    )
    assert load_manifest(toml) == load_manifest(json)


def test_missing_pipeline_section_is_an_error(write):
    diags = Diagnostics()
    assert load_manifest(write("pipeline.toml", '[nodes.a]\nfn = "m.a"\n'), diags) is None
    assert diags.codes() == ["missing-section"]


def test_unknown_top_level_section_is_an_error(write):
    diags = Diagnostics()
    load_manifest(write("pipeline.toml", MINIMAL + "\n[poolz]\nmain = 1\n"), diags)
    assert diags.codes() == ["unknown-section"]
    assert "poolz" in diags.errors[0].message


def test_every_bad_node_is_reported_not_just_the_first(write):
    """The point of item-by-item conversion (DESIGN §7b)."""
    path = write(
        "pipeline.toml",
        MINIMAL
        + """
[nodes.a]
fn = "m.a"
outputs = [1]

[nodes.b]
fn = "m.b"
outpots = ["x"]

[nodes.c]
fn = 42

[nodes.d]
fn = "m.d"
outputs = ["fine"]
""",
    )
    diags = Diagnostics()
    manifest = load_manifest(path, diags)

    assert len(diags.errors) == 3, diags.render()
    paths = [d.location.path for d in diags.errors]
    assert paths == ["nodes.a.outputs[0]", "nodes.b", "nodes.c.fn"]
    # the good node still survives, so later passes can keep working
    assert list(manifest.nodes) == ["d"]


def test_unknown_field_error_hints_at_the_real_field_names(write):
    diags = Diagnostics()
    load_manifest(
        write("pipeline.toml", MINIMAL + '\n[nodes.a]\nfn = "m.a"\noutpots = []\n'), diags
    )
    assert "outputs" in diags.errors[0].hint


def test_locations_quote_keys_that_are_not_bare_identifiers(write):
    diags = Diagnostics()
    load_manifest(
        write("pipeline.toml", MINIMAL + '\n[artifacts."db/drugs"]\nkind = "duckdb_table"\n'),
        diags,
    )
    assert diags.errors[0].location.path == 'artifacts."db/drugs"'


def test_malformed_toml_is_one_located_error(write):
    diags = Diagnostics()
    assert load_manifest(write("pipeline.toml", "[pipeline\nname = "), diags) is None
    assert diags.codes() == ["malformed-file"]


def test_unsupported_extension_is_an_error(write):
    diags = Diagnostics()
    assert load_manifest(write("pipeline.yaml", "pipeline:\n  name: p\n"), diags) is None
    assert diags.codes() == ["unsupported-format"]


def test_load_manifest_without_a_collector_raises_with_every_diagnostic(write):
    path = write("pipeline.toml", MINIMAL + "\n[nodes.a]\nfn = 1\n\n[nodes.b]\nfn = 2\n")
    with pytest.raises(CheckFailed) as excinfo:
        load_manifest(path)
    assert len(excinfo.value.diagnostics.errors) == 2


def test_artifact_kind_selects_the_struct(write):
    manifest = load_manifest(
        write(
            "pipeline.toml",
            MINIMAL + '\n[artifacts."a/b"]\nkind = "file"\npath = "x.json"\n',
        )
    )
    assert manifest.artifacts["a/b"] == FileArtifact(path="x.json")


# --- runs files -----------------------------------------------------------


def test_runs_folder_loads_every_file_and_accumulates(write):
    write("runs/a.toml", "[defaults]\nsample_n = 1\n\n[runs.test_api]\nsample_n = 10\n")
    write("runs/b.toml", "[runs.production]\nsample_n = 5000000\n")
    write("runs/notes.md", "ignored")
    registry = load_runs(
        write("runs/a.toml", "[defaults]\nsample_n = 1\n\n[runs.test_api]\nsample_n = 10\n").parent
    )
    assert registry.defaults == {"sample_n": 1}
    assert set(registry.runs) == {"test_api", "production"}


def test_load_runs_accumulates_across_calls(write):
    diags = Diagnostics()
    registry = RunRegistry()
    load_runs(write("a.toml", "[runs.one]\nn = 1\n"), diags, registry)
    load_runs(write("b.toml", "[runs.two]\nn = 2\n"), diags, registry)
    assert set(registry.runs) == {"one", "two"}
    assert diags.ok


def test_duplicate_run_name_across_files_is_a_loud_error(write):
    diags = Diagnostics()
    registry = RunRegistry()
    load_runs(write("a.toml", "[runs.one]\nn = 1\n"), diags, registry)
    load_runs(write("b.toml", "[runs.one]\nn = 2\n"), diags, registry)
    assert diags.codes() == ["duplicate-run"]
    assert "a.toml" in diags.errors[0].hint
    assert registry.runs["one"] == {"n": 1}


def test_duplicate_default_key_across_files_is_a_loud_error(write):
    diags = Diagnostics()
    registry = RunRegistry()
    load_runs(write("a.toml", "[defaults]\nn = 1\n"), diags, registry)
    load_runs(write("b.toml", "[defaults]\nn = 2\n"), diags, registry)
    assert diags.codes() == ["duplicate-default"]


def test_run_body_rejects_a_doubly_nested_table(write):
    """Subtables name a node; a node's variables are scalars, not more tables."""
    diags = Diagnostics()
    load_runs(write("r.toml", "[runs.one.node_a.deeper]\nn = 1\n"), diags)
    assert diags.codes() == ["invalid-value"]
    assert diags.errors[0].location.path == "runs.one.node_a.deeper"
