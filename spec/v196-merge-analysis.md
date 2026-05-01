# v1.9.6 Merge Analysis

**Last merge**: `cdfa00b` (v1.9.5 / upstream commit `5240e7a`) — 2026-04-16
**Upstream HEAD**: `5ade9f6` (v1.9.6) — 2026-04-25 release
**Commits behind**: 182 raw / ~30 real fixes after filtering CI noise

---

## 1. Files Changed in Upstream

| File | Insertions | Deletions | Significance |
|---|---|---|---|
| `livysession.py` | ~2583 | ~2400 | Massive — hardened retries, 429/430 handling |
| `connections.py` | ~968 | ~900 | retry_all + retryable keywords |
| `credentials.py` | ~471 | ~440 | Mostly cosmetic + 1 new field |
| `impl.py` | ~1453 | ~1400 | Mostly cosmetic + 1 small fix |
| `relation.py` | +13 / -7 | small | New `create()` + `_identifier_prefix` |
| `mlv_api.py` | ~259 | ~220 | Capacity throttling hardening |
| `macros/adapters/relation.sql` | +5 / -3 | small | Materialized view drop, db gate |
| `macros/adapters/schema.sql` | +1 / -1 | small | `ensure_database_exists` guard |
| `macros/seed.sql` | +6 | small | Drop stale catalog before seed |
| `macros/snapshot.sql` | +1 / -1 | small | Use `model.database` |
| `macros/materialized_lake_view.sql` | +1 / -1 | small | Type string fix |

---

## 2. Real Fixes Worth Taking (~13)

### Resilience / hardening (the v1.9.6 headline theme)

| Commit | What | File |
|---|---|---|
| `d5f6878` | Livy session creation: MAX_RETRIES 2→5, shard staggering, 404/5xx retryable | livysession.py |
| `0a0b877` | POLL_TIMEOUT 600s→1800s, HTTP 429 retry POST + jitter, `_parse_retry_after()` helper | livysession.py |
| `5028a6f` | `retry_all` honors statement execution + new retryable keywords | connections.py |
| `823e423` | Retry transient `Failed to get database metadata` Spark errors | livysession.py |
| `ed2fbad` | MLV API hardened against 429 throttling | mlv_api.py |
| `af831ee` | Livy statement 404 retry resilience | livysession.py |
| `fa26275` | Fabric API timeout 30s→120s | livysession.py |
| `441c5c0` | `connect_retries` default 5 (was 1) | credentials.py |
| `b2e8d37` | `statement_timeout` 3600s→43200s (12h, for long-running models) | credentials.py |

### Bug fixes

| Commit | What | File |
|---|---|---|
| `7958901` | Fix single quote escaping in seed values (`_fix_binding`) | livysession.py |
| `01172b1` | Refactor `_fix_binding` for readability | livysession.py |
| `4c56378` | Snapshot uses `model.database` for cross-lakehouse | snapshot.sql |
| `d6c1a43` | `FabricSparkQuotePolicy.database=True` (mixed-case lakehouse names) | relation.py |
| `1ed0c97` | Handle `materialized_view` type in `drop_relation` | relation.sql |
| `d975930` | Match `MATERIALIZED_LAKE_VIEW` type string | impl.py |
| `8fb4cca` | Drop stale catalog entry before seed creation (DELTA_METADATA_ABSENT) | seed.sql |
| `6efd728` | Schema-enabled regression fix (ensure_database_exists guard) | schema.sql |

### New features

| Commit | What |
|---|---|
| `d6c1a43`+`relation.py` | **`identifier_prefix` config** — `_identifier_prefix` ClassVar + `create()` method that auto-prepends to all identifiers (skips CTEs) |

### Infrastructure (mostly skip)

| What | Take? |
|---|---|
| `azure-cli` → optional `[cli]` extra (`37a9ed8`) | **Yes** — lighter install |
| Python model FAQ docs | Skip |
| Dependabot skill, nudge robot, nx.json | Skip — their CI infra |
| ~80 sentinel commits | Skip |

---

## 3. Conflicts With Our Fork

### `relation.py` — **MEDIUM CONFLICT**

**Upstream added** a `create()` classmethod that auto-prepends `_identifier_prefix`:
```python
@classmethod
def create(cls, database=None, schema=None, identifier=None, **kwargs):
    skip_prefix = kwargs.pop("_skip_prefix", False)
    prefix = cls._identifier_prefix
    if prefix and identifier and not skip_prefix:
        # ... prepend logic ...
    return super().create(...)
```

**Our fork has** a `create()` classmethod for cross-workspace 4-part naming:
```python
@classmethod
def create(cls, database=None, schema=None, identifier=None, type=None, workspace=None, **kwargs):
    if workspace:
        include_policy = FabricSparkFourPartIncludePolicy()
    elif database and schema and database != schema:
        include_policy = FabricSparkThreePartIncludePolicy()
    else:
        include_policy = FabricSparkIncludePolicy(database=cls._schemas_enabled, ...)
    # ... include policy logic ...
    return super().create(...)
```

**Resolution**: **Merge both** — combine prefix logic + workspace include-policy logic in a single `create()`. Both features are valid.

**Upstream also flipped** `FabricSparkQuotePolicy.database` from `False` → `True`. Our fork has `False`. Need to **adopt upstream's `True`** for the mixed-case lakehouse fix.

### `credentials.py` — **LOW CONFLICT**

Upstream rewrote the file but the **only NEW field** is `identifier_prefix: Optional[str] = ""`.

Other changes:
- `statement_timeout`: 3600 → 43200 (we have 3600, take upstream's 43200)
- `connect_retries`: 1 → 5 (we have 1, take upstream's 5? Or keep 0 per our recommendation?)

**Our fork-unique fields to preserve**:
- `workspace_name`
- `vdstudio_oauth_endpoint_url`

**Resolution**: Take upstream as base + keep our fork-unique fields + add new `identifier_prefix`.

### `livysession.py` — **MEDIUM CONFLICT**

Upstream's resilience improvements DON'T conflict with our `cross_process_session_lock` (we added it independently for parallel-process scenarios). They're complementary:
- Upstream's `_parse_retry_after()` + 429/430 handling = better individual request reliability
- Our `cross_process_session_lock` = prevents N parallel processes from each creating a session

**Our fork-unique pieces to preserve**:
- `cross_process_session_lock()` context manager
- `get_vdstudio_oauth_access_token()`
- `get_env_access_token()` (env var token)
- `vdstudio_oauth` and `env_oauth_access_token` auth methods in `get_headers()`
- Three-level state checking (`fabricSessionStateInfo.state`)
- Defensive `.get()` access on all API responses

**Upstream's better-than-ours pieces to take**:
- `_parse_retry_after()` (proper Retry-After header parsing)
- POLL_TIMEOUT 600s → 1800s
- 429 retry with exponential backoff + jitter on session creation
- Shard staggering (5s × shard index)
- 404 retry on session creation POST
- Single quote escaping in `_fix_binding`
- `retry_all` for statement execution

**Resolution**: Strategy is upstream-base + layer fork additions. Same approach as v1.9.5 merge.

### `impl.py` — **NO CONFLICT**

Only upstream change: `Type: MATERIALIZED_LAKE_VIEW` string match in `_build_spark_relation_list`. Our cache-key fix and `_normalize_workspace_database` are in different methods.

**Resolution**: Take upstream's MATERIALIZED_LAKE_VIEW match + keep our cache normalization.

### Macros — **NO CONFLICT**

| File | Upstream change | Our fork change | Action |
|---|---|---|---|
| `relation.sql` | `drop_relation` handles materialized_view; `database` gated by `include_policy.database` | We took upstream's version in v1.9.5 merge | Take upstream |
| `schema.sql` | `ensure_database_exists` guard | We have `generate_schema_name` + custom logic | **MERGE** — both changes are compatible |
| `seed.sql` | Drop stale catalog | None | Take upstream |
| `snapshot.sql` | `database=model.database` | None | Take upstream |
| `materialized_lake_view.sql` | Type string fix | None | Take upstream |

---

## 4. Strategy

Same as v1.9.5 merge:

1. `git merge upstream/main` (creates conflicts)
2. **Take upstream wholesale** for: connections.py, snapshot.sql, seed.sql, schema.sql, relation.sql, impl.py, materialized_lake_view.sql
3. **Merge carefully** for: livysession.py, relation.py, credentials.py
4. **Keep fork** for: notebook/, cross_workspace.sql, integration tests, FORK.md

### File-by-file plan

| File | Strategy |
|---|---|
| `connections.py` | Upstream wholesale (their retry_all logic is better than our nothing) |
| `livysession.py` | Upstream base + layer our: `cross_process_session_lock`, vdstudio_oauth, env_oauth, 3-level state checking, defensive `.get()` |
| `credentials.py` | Upstream base + add our `workspace_name` + `vdstudio_oauth_endpoint_url` + new upstream `identifier_prefix` |
| `relation.py` | Upstream's `_identifier_prefix` and `database=True` + merge our 4-part workspace `create()` logic into upstream's `create()` |
| `impl.py` | Upstream wholesale (our cache fix and helpers are in different methods, will preserve) |
| Macros | Upstream wholesale |
| `__version__.py` | Bump to 1.9.16 (one minor up from upstream 1.9.6) |
| `pyproject.toml` | Take upstream (azure-cli optional extra) |

---

## 5. Breaking Changes / User-facing

| Change | Impact |
|---|---|
| `azure-cli` is now an optional extra | Users need `pip install vd-dbt-fabricspark[cli]` if they want CLI auth — or ensure az is installed elsewhere. We can keep it as required for now to avoid friction |
| `connect_retries` default 1 → 5 | Each retry creates a new session. Not great. **Recommend keep 0** in our fork docs |
| `statement_timeout` default 3600 → 43200 (12h) | Better for long-running models, harmless |
| New `identifier_prefix` config | Optional, no impact unless set |
| `FabricSparkQuotePolicy.database = True` | Fixes mixed-case lakehouse names. Mostly transparent |

---

## 6. Estimated Effort

- **Merge + conflict resolution**: ~30 min (smaller than v1.9.5 merge — fewer conflict surface)
- **Test verification**: 195 unit tests should still pass
- **Integration test**: ~30 min on real Fabric (need user trigger)

Same approach as last time — take upstream as base, layer fork on top.
