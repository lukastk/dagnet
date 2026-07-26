"""Nodes read their parameters from `ctx.vars` — a plain mapping, already resolved.

A node never merges anything itself: by the time it runs, the declared defaults,
`[defaults]`, the chosen run, and any per-node override have been collapsed into
one mapping holding exactly the variables that node can see.
"""


def sample(ctx) -> {"rows": list[int]}:
    n = ctx.vars["sample_n"]
    print(f"[{ctx.run_name}] sampling {n} rows with {ctx.vars['llm_model']}")
    return {"rows": list(range(n))}


def classify(ctx, rows: list[int]) -> {"labels": dict}:
    # `sample_n` here is the node-local declaration, not the global one.
    print(
        f"[{ctx.run_name}] classify sees sample_n={ctx.vars['sample_n']}, "
        f"chunk_size={ctx.vars['chunk_size']}, model={ctx.vars['llm_model']}"
    )
    chunks = max(1, -(-len(rows) // ctx.vars["chunk_size"]))
    return {"labels": {"chunks": chunks, "n": len(rows), "model": ctx.vars["llm_model"]}}


def report(ctx, labels: dict) -> {"summary": dict}:
    summary = dict(labels, run=ctx.run_name, dry_run=ctx.vars["dry_run"])
    print(f"summary: {summary}")
    return {"summary": summary}
