"""The schema structs themselves: pure data, both formats, one definition."""

from __future__ import annotations

import msgspec
import pytest
from msgspec import UNSET

from dagnet.schema import (
    DuckDBTableArtifact,
    FileArtifact,
    Manifest,
    Node,
    RunsFile,
    VarDecl,
    split_run_body,
)

MANIFEST_TOML = """
[pipeline]
name = "ai_index"
description = "Match job ads to O*NET occupations."
dagster_home = ".dagster"

[pools]
main = 4
heavy = 1

[vars]
run_name = { type = "str" }
sample_n = { type = "int", default = 1000 }

[artifacts."openfda/drug_ndc"]
kind = "file"
path = "extracted/openfda_drug_ndc.json"

[artifacts."db/warehouse"]
kind = "file"
path = "w.duckdb"

[artifacts."db/drugs"]
kind = "duckdb_table"
table = "drugs"
database = "db/warehouse"

[nodes.rerank_candidates]
fn = "ai_index.nodes.rerank_candidates.main"
inputs = { ad_ids = "llm_filter_candidates.successful_ad_ids" }
outputs = ["ad_ids"]
pool = "heavy"
retries = { max = 3, wait_s = 10 }

[nodes.rerank_candidates.vars]
chunk_size = { type = "int", default = 512 }

[nodes.load_drugs]
fn = "healthcare_pipeline.nodes.load_drugs.main"
inputs = { drug_ndc = "openfda/drug_ndc" }
outputs = ["drugs"]
artifacts = { drugs = "db/drugs" }
checks = { drugs = ["healthcare_pipeline.checks.drugs_vocab"] }
after = ["rerank_candidates"]
asset = false
group = "load"
"""


def test_full_manifest_decodes_from_toml():
    m = msgspec.toml.decode(MANIFEST_TOML.encode(), type=Manifest)

    assert m.pipeline.name == "ai_index"
    assert m.pipeline.dagster_home == ".dagster"
    assert m.pools == {"main": 4, "heavy": 1}

    assert m.vars["run_name"].required
    assert not m.vars["sample_n"].required
    assert m.vars["sample_n"].default == 1000

    assert isinstance(m.artifacts["openfda/drug_ndc"], FileArtifact)
    assert m.artifacts["openfda/drug_ndc"].path == "extracted/openfda_drug_ndc.json"
    assert isinstance(m.artifacts["db/drugs"], DuckDBTableArtifact)
    assert m.artifacts["db/drugs"].table == "drugs"

    rerank = m.nodes["rerank_candidates"]
    assert rerank.inputs == {"ad_ids": "llm_filter_candidates.successful_ad_ids"}
    assert rerank.outputs == ["ad_ids"]
    assert rerank.pool == "heavy"
    assert rerank.retries.max == 3 and rerank.retries.wait_s == 10.0
    assert rerank.vars["chunk_size"].default == 512
    assert rerank.asset is True

    load = m.nodes["load_drugs"]
    assert load.artifacts == {"drugs": "db/drugs"}
    assert load.checks == {"drugs": ["healthcare_pipeline.checks.drugs_vocab"]}
    assert load.after == ["rerank_candidates"]
    assert load.asset is False
    assert load.group == "load"


def test_toml_and_json_produce_the_same_manifest():
    """One schema, two formats — DESIGN §5."""
    from_toml = msgspec.toml.decode(MANIFEST_TOML.encode(), type=Manifest)
    as_json = msgspec.json.encode(from_toml)
    from_json = msgspec.json.decode(as_json, type=Manifest)
    assert from_json == from_toml


def test_unknown_field_is_rejected():
    with pytest.raises(msgspec.ValidationError, match="unknown field"):
        msgspec.convert({"fn": "a.b", "outpots": ["x"]}, Node)


def test_unknown_artifact_kind_is_rejected():
    with pytest.raises(msgspec.ValidationError):
        msgspec.convert({"kind": "parquet_dir", "path": "x"}, FileArtifact)


def test_var_without_default_is_unset_not_none():
    """`default = 0` and "no default" must stay distinguishable."""
    assert msgspec.convert({"type": "int"}, VarDecl).default is UNSET
    assert msgspec.convert({"type": "int", "default": 0}, VarDecl).default == 0


def test_node_defaults_are_not_shared_between_instances():
    a = msgspec.convert({"fn": "a.b"}, Node)
    b = msgspec.convert({"fn": "c.d"}, Node)
    a.outputs.append("leaked")
    assert b.outputs == []


RUNS_TOML = """
[defaults]
sample_n = 1000
llm_model = "qwen-0.5b"
[defaults.rerank_candidates]
chunk_size = 512

[runs.test_api]
sample_n = 10
llm_model = "gpt-5.2"

[runs.production_5m]
sample_n = 5000000
"""


def test_runs_file_decodes_and_splits_into_globals_and_node_overrides():
    runs = msgspec.toml.decode(RUNS_TOML.encode(), type=RunsFile)
    globals_, per_node = split_run_body(runs.defaults)
    assert globals_ == {"sample_n": 1000, "llm_model": "qwen-0.5b"}
    assert per_node == {"rerank_candidates": {"chunk_size": 512}}
    assert set(runs.runs) == {"test_api", "production_5m"}
    assert split_run_body(runs.runs["test_api"])[0] == {
        "sample_n": 10,
        "llm_model": "gpt-5.2",
    }


def test_bools_survive_the_scalar_union():
    """`Scalar` lists bool first so msgspec doesn't widen True to 1."""
    runs = msgspec.toml.decode(b"[runs.x]\nflag = true\nn = 1\n", type=RunsFile)
    assert runs.runs["x"]["flag"] is True
    assert runs.runs["x"]["n"] == 1
