from dagnet.diagnostics import Diagnostics
def a(ctx):
    return {"rows": [1, 2]}
def guard(ctx, rows):
    return {"passed": len(rows) == ctx.vars["expected"]}
