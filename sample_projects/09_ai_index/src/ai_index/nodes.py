"""Stub bodies shaped like the real AI-exposure-index nodes.

Every function here is instant and dependency-free: plain lists and dicts, no
numpy, no pandas, no network, no models. The *shapes* are real — `sample_ads`
emits a list of ad ids sized by `ctx.vars["sample_n"]`, the scorers emit small
score tables keyed by occupation, `combine_onet_exposure` merges four of them —
because the point of this sample is the topology and the wiring, not the maths.

Like every other sample: zero framework imports. `ctx` is the whole surface.
"""

#: Stand-in for the O*NET occupation catalogue the real pipeline downloads.
OCCUPATIONS = [
    "11-1011 Chief Executives",
    "15-1252 Software Developers",
    "25-2021 Elementary School Teachers",
    "29-1141 Registered Nurses",
    "35-3023 Fast Food Workers",
    "41-2031 Retail Salespersons",
    "43-4051 Customer Service Representatives",
    "47-2111 Electricians",
    "53-3032 Heavy Truck Drivers",
    "27-1024 Graphic Designers",
]

#: Stand-in for the geography lookup `aggregate_geo` joins against.
DISTRICTS = ["E06000001", "E06000002", "E08000003", "S12000033", "W06000015"]


def _score_table(occupations, offset: float, spread: float) -> dict:
    """A deterministic little score table, so runs are reproducible."""
    return {
        occ: round((offset + spread * (i % 7) / 7.0) % 1.0, 4) for i, occ in enumerate(occupations)
    }


# --- ingest ----------------------------------------------------------------


def fetch_onet(ctx) -> {"onet_db": dict}:
    print(f"[{ctx.run_name}] fetching O*NET ({len(OCCUPATIONS)} occupations)")
    return {"onet_db": {"occupations": list(OCCUPATIONS), "version": "30.0"}}


def fetch_adzuna(ctx) -> {"ads": int}:
    if ctx.vars["skip_fetch"]:
        print(f"skip_fetch is set; using the archive already at {ctx.vars['adzuna_s3_prefix']}")
    total = 250_000
    print(f"ad archive covers years={ctx.vars['fetch_years']}, {total} ads available")
    return {"ads": total}


def sample_ads(ctx) -> {"ad_ids": list[int]}:
    """`after = ["fetch_adzuna"]`: waits for the fetch, takes no value from it."""
    n = ctx.vars["sample_n"]
    # Cap the stub so a 5M-row production preset still finishes instantly.
    taken = min(n, 500) if n >= 0 else 500
    print(f"sampling {taken} of a requested {n} ads (seed={ctx.vars['sample_seed']})")
    return {"ad_ids": list(range(taken))}


def prepare_onet_targets(ctx) -> {"targets": list[dict]}:
    top_n = ctx.vars["onet_top_n"]
    occupations = OCCUPATIONS[:top_n]
    if ctx.vars["onet_exclude_public_sector"]:
        occupations = [o for o in occupations if "Teachers" not in o]
    print(f"prepared {len(occupations)} O*NET targets")
    return {"targets": [{"code": o.split()[0], "text": o} for o in occupations]}


# --- the matching chain ----------------------------------------------------


def embed_ads(ctx, ad_ids: list[int]) -> {"ad_ids": list[int]}:
    print(
        f"embedding {len(ad_ids)} ads with {ctx.vars['embedding_model']} "
        f"in chunks of {ctx.vars['chunk_size']}"
    )
    return {"ad_ids": list(ad_ids)}


def embed_onet(ctx) -> {"out": dict}:
    print(f"embedding O*NET targets with {ctx.vars['embedding_model']}")
    return {"out": {"model": ctx.vars["embedding_model"], "n": len(OCCUPATIONS)}}


def cosine_candidates(ctx, ad_ids: list[int], onet_done: dict) -> {"ad_ids": list[int]}:
    """Two inputs: real ad ids, and a completion handshake from `embed_onet`."""
    print(
        f"top-{ctx.vars['cosine_topk']} candidates for {len(ad_ids)} ads "
        f"against {onet_done['n']} occupations"
    )
    return {"ad_ids": list(ad_ids)}


def llm_filter_candidates(ctx, ad_ids: list[int]) -> {"successful_ad_ids": list[int]}:
    """Some ads fail the LLM filter; the output port says so, and is renamed downstream."""
    successful = [i for i in ad_ids if i % 10 != 0]
    print(f"[{ctx.vars['llm_model']}] filter kept {len(successful)} of {len(ad_ids)}")
    return {"successful_ad_ids": successful}


def rerank_candidates(ctx, ad_ids: list[int]) -> {"ad_ids": list[int]}:
    """Receives `llm_filter_candidates.successful_ad_ids` as `ad_ids`."""
    print(f"reranking {len(ad_ids)} with {ctx.vars['rerank_model']}")
    return {"ad_ids": sorted(ad_ids, reverse=True)}


# --- scoring ---------------------------------------------------------------


def score_presence(ctx) -> {"out": dict}:
    return {"out": _score_table(OCCUPATIONS, 0.10, 0.9)}


def score_felten(ctx) -> {"out": dict}:
    print(f"felten scenario={ctx.vars['felten_scenario']} alpha={ctx.vars['felten_alpha']}")
    return {"out": _score_table(OCCUPATIONS, ctx.vars["felten_alpha"], 0.5)}


def score_task_exposure(ctx) -> {"out": dict}:
    # This node's `llm_model` is overridden per-node in the runs file.
    print(
        f"task exposure via {ctx.vars['llm_model']} "
        f"(max_new_tokens={ctx.vars['llm_max_new_tokens']})"
    )
    return {"out": _score_table(OCCUPATIONS, 0.30, 0.7)}


def score_task_exposure_bt(ctx) -> {"out": dict}:
    rounds = [
        ctx.vars["comparisons_per_item_r1"],
        ctx.vars["comparisons_per_item_r2"],
        ctx.vars["comparisons_per_item_r3"],
    ][: ctx.vars["n_rounds"]]
    print(f"pairwise BT via {ctx.vars['llm_model']}, comparisons per round: {rounds}")
    return {"out": _score_table(OCCUPATIONS, 0.55, 0.4)}


# --- combine ---------------------------------------------------------------


def combine_onet_exposure(
    ctx, presence: dict, felten: dict, task_exposure: dict, task_exposure_bt: dict
) -> {"out": dict}:
    """Four inputs. netrun needed a `join` factory node to express this."""
    combined = {
        occ: {
            "presence": presence[occ],
            "felten": felten[occ],
            "task_exposure": task_exposure[occ],
            "task_exposure_bt": task_exposure_bt[occ],
        }
        for occ in OCCUPATIONS
    }
    print(f"combined 4 score tables over {len(combined)} occupations")
    return {"out": combined}


def compute_job_ad_exposure(
    ctx, exposure_scores: dict, ad_ids: list[int]
) -> {"ad_ids": list[dict]}:
    """The two halves of the pipeline meet here: occupation scores and ad ids."""
    occupations = list(exposure_scores)
    scored = [
        {
            "ad_id": ad_id,
            "occupation": occupations[ad_id % len(occupations)],
            "exposure": exposure_scores[occupations[ad_id % len(occupations)]]["task_exposure"],
            "lad": DISTRICTS[ad_id % len(DISTRICTS)],
        }
        for ad_id in ad_ids
    ]
    print(f"scored {len(scored)} ads in chunks of {ctx.vars['exposure_chunk_size']}")
    return {"ad_ids": scored}


def aggregate_geo(ctx, ad_ids: list[dict]) -> {"by_lad": dict}:
    totals: dict[str, list[float]] = {}
    for row in ad_ids:
        totals.setdefault(row["lad"], []).append(row["exposure"])
    by_lad = {
        lad: {"ads": len(values), "mean_exposure": round(sum(values) / len(values), 4)}
        for lad, values in sorted(totals.items())
    }
    print(f"aggregated {len(ad_ids)} ads into {len(by_lad)} districts")
    return {"by_lad": by_lad}
