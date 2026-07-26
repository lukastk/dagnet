# Open design questions from building v0 — **all resolved**

*Raised 2026-07-26 while implementing DESIGN §11 step 1. Decided by Lukas
2026-07-27; every decision is now recorded in `_dev/DESIGN.md`, which stays the
authoritative document. This file is kept as the record of what was asked and
what was answered.*

**Status: 11 of 11 resolved. Nothing here is open.**

---

## 1. What is the "store root"? (§5.4) — **RESOLVED**

§5.4 said file-artifact paths were "relative to the store root"; no section
defined a store root. v0 resolved them relative to the manifest's directory.

**Decision:** add an optional `[pipeline] store_root` (default `"."`), resolved
relative to the manifest file like `dagster_home`. Precedence `--store-root` >
manifest field > default. *Rationale: the map should carry its own store location
rather than it existing only as a CLI flag.*

→ Recorded in **DESIGN §5.1**. Implemented in `locations.py`; demonstrated by
sample 03 (`store_root = "build"`).

## 2. A DuckDB table artifact doesn't say which database it is in — **RESOLVED**

`kind = "duckdb_table"` carried only `table`, so the database's location lived
outside the manifest — the `paths.py` problem in miniature.

**Decision:** `database` becomes a **required** field naming a declared
`file`-kind artifact. `check` validates the reference resolves *and* that the
target is a file. `ctx.artifact()` returns a `Path` for file artifacts
(unchanged) and a small frozen handle with `.table: str` / `.database: Path` for
table artifacts.

→ Recorded in **DESIGN §5.4** and §7b. New diagnostic code `database-not-a-file`.
Sample 03 rewritten around it.

## 3. Should a failing asset check fail the run? — **RESOLVED**

v0 followed Dagster's default: recorded, visible, exit 0.

**Decision: checks are blocking by default** — a failure stops the assets
downstream and fails the run with a nonzero exit. *Rationale: exit 0 on a
violated schema contract is precisely the silent failure this project's
principles forbid.* Per-check opt-out via the long form
`{ fn = "...", blocking = false }`: advisory, recorded and visible at WARN
severity, run continues.

→ Recorded in **DESIGN §5.5**. Sample 07 demonstrates both, including the
nonzero exit (`bad` → exit 1 with `aggregate` skipped; `noisy` → exit 0).

## 4. The check-function contract was undefined — **RESOLVED**

**Decision:** accepted exactly as implemented — `(ctx, subject) -> bool |
{"passed", "metadata"}`, raising counts as a failure, anything else raises
`CheckReturnError`; `subject` is the loaded value, or the resolved location for
an artifact-bound output.

→ Recorded in **DESIGN §7 rule 6**.

## 5. Variable precedence beyond defaults-vs-run — **RESOLVED**

**Decision:** accepted exactly as implemented — the six-level order (run-per-node
> defaults-per-node > run-global > defaults-global > node-local declared default
> global declared default).

→ Recorded in **DESIGN §6**.

## 6. `--ephemeral` cannot be multiprocess at all — **RESOLVED**

**Decision:** keep the **warning**, not an error. *Rationale: the mode is an
explicit opt-in and the warning names the inert pools; erroring would make
ephemeral unusable for any pooled pipeline.*

→ Recorded in **DESIGN §8**, along with the mandatory pool-limit sync onto the
instance that spike (a) turned up.

## 7. Seven additions v0 made that DESIGN didn't mention — **ALL ACCEPTED**

Optional run name; `dagnet run --from-failure`; check-time `unfilled-var`;
duplicate-`[defaults]`-key error; at-least-one-output; identifier-only names.

**One standing instruction attached to at-least-one-output:** if a real pipeline
surfaces a genuine pure-side-effect terminal node, **raise it** — do not quietly
invent a token output. That is the netrun `"done"` disease coming back.

→ Recorded in **DESIGN §7b** and **§8**.

## 8. Variable types are scalars only — **RESOLVED**

**Decision:** stay scalar for now. Widen to typed lists (`list[str]`, `list[int]`)
only when a real consumer actually forces it — deliberately, not preemptively.

→ Recorded in **DESIGN §6**.

## 9. Diagnostic locations are logical paths, not line numbers — **RESOLVED**

**Decision:** dotted paths are enough for v0. Keep the dormant `line` field so
adding real positions later changes no call sites.

→ Recorded in **DESIGN §12**.

## 10. Two consequences worth knowing about — **ACKNOWLEDGED**

- **A graph-backed asset does not subset** — a node folding in `asset = false`
  op-nodes loses per-output selectability. Documented in sample 06's README.
- **The dict-shaped return annotation trips type-aware tooling.**
  **Decision:** keep the annotation as designed and keep it optional; document the
  recommended per-file lint ignore (and the option of simply omitting the
  annotation) in the README. No alternative form for now.

→ Recorded in **DESIGN §12**; the ignore is documented in the top-level README.

## 11. Size — **ACCEPTED**

~1,635 lines of code against the §1 estimate of 500–800.

**Decision:** justified by content — `check.py`'s diagnostic quality is the
product. **Standing checkpoint: if `compile.py` approaches ~1,000 lines, raise it
before it gets there.** (At the time of writing it is ~830.)

→ Recorded in **DESIGN §12**.
