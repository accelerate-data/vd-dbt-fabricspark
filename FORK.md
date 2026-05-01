# Fork Feature Tracker — vd-dbt-fabricspark

Tracks all features added by the accelerate-data fork that are NOT in
upstream microsoft/dbt-fabricspark. Helps with future upstream merges.

**Upstream**: microsoft/dbt-fabricspark (v1.9.6 as of 2026-04-25)
**Fork**: accelerate-data/vd-dbt-fabricspark (v1.9.16)

## Recent Sync History

| Fork ver | Upstream | Date | Spec |
|---|---|---|---|
| 1.9.14 | v1.9.5 | 2026-04-17 | spec/fork-merge-changelog.md |
| 1.9.16 | v1.9.6 | 2026-04-25 | spec/v196-merge-analysis.md |

---

## Active Fork Features

### 1. Cross-workspace 4-part naming
**Files**: `relation.py`, `credentials.py`, `cross_workspace.sql`
**Status**: Active — working
**Why**: Fabric Spark supports `workspace.lakehouse.schema.table` for cross-workspace queries.
**How it works**:
- `FabricSparkRelation` has a `workspace` field and custom `render()` for 4-part output
- `FabricSparkFourPartIncludePolicy` / `FabricSparkThreePartIncludePolicy` classes
- Models can set `database: "workspace_name.lakehouse_name"` to target another workspace
- `relation.py` splits dotted database names and renders them as separate parts

> **Review needed**: `workspace_name` profile field (credentials.py line 40) globally
> prepends workspace to ALL relations — this is heavy-handed. The per-model
> `database: "ws.lakehouse"` approach is better for most use cases. Consider
> deprecating `workspace_name` from profiles and only supporting the per-model
> database config. The `is_current_workspace()` helper may still be useful.

### 2. Notebook module (run dbt inside Fabric notebooks)
**Files**: `notebook/__init__.py`, `notebook/environment.py`, `notebook/repo.py`, `notebook/runner.py`
**Status**: Active — working
**Why**: Allows running dbt from within Fabric notebooks with full lifecycle management.
**Features**:
- `DbtJobConfig` with multi-command support (sequential execution)
- GitHub App auth via Azure Key Vault for private repo cloning
- Public repo clone fallback (no credentials needed)
- Log/artifact persistence to lakehouse Files area
- Environment variable setup for profiles.yml

### 3. Cross-schema cache-key normalization
**Files**: `impl.py` (`_relations_cache_for_schemas`, `list_relations`, `_normalize_workspace_database`)
**Status**: Active — fixes real bug
**Why**: Without this, Elementary's `on-run-end` hooks fail silently. Relations get cached under
the short database name (`salesforce`) while the schemas-cache is keyed by the full manifest
database (`sampledata.salesforce`), causing `get_relation()` to return None.

### 4. Cross-process session lock
**Files**: `livysession.py` (`cross_process_session_lock`, `_connect_fabric_fresh`)
**Status**: Active — fixes real bug
**Why**: When multiple dbt processes start simultaneously (e.g. Claude Code sub-agents running
7 parallel `dbt show` commands), each created its own Spark session. The file lock
(`fcntl.flock`) ensures only one process creates the session; others wait and reuse it.
**Note**: `reuse_session=True` recommended for parallel workloads (prevents atexit deletion).

### 5. Fork-specific auth methods
**Files**: `livysession.py` (`get_vdstudio_oauth_access_token`, `get_env_access_token`)
**Status**: Active
**Why**: Support for vd-studio local OAuth and environment variable token injection.
- `authentication: vdstudio_oauth` — gets token from local vd-studio OAuth endpoint
- `authentication: env_oauth_access_token` — reads token from `FABRIC_LAKEHOUSE_ACCESS_TOKEN` env var
- `authentication: fabric_notebook` — gets token from `notebookutils` (Fabric runtime)

### 6. Elementary schema fix (generate_schema_name)
**Files**: `macros/adapters/schema.sql` (`fabricspark__generate_schema_name`)
**Status**: Active — fixes real bug
**Why**: Upstream called `generate_schema_name_for_env()` which prepends `target.schema_` to
custom schema names (e.g. `dbo_elementary` instead of `elementary`). Fabric lakehouses have
real schemas so the custom name should be used directly.

### 7. Fabric API robustness (defensive response parsing)
**Files**: `livysession.py` (`create_session`, `wait_for_session_start`, `execute`)
**Status**: Active — fixes real production bugs
**Why**: Fabric's Livy API has a fragile contract. Per the official Swagger spec
(`microsoft/fabric-samples/.../Livy-API-swagger/swagger.yaml`):
- **All fields in `SessionResponse` are optional** — `state`, `livyInfo`, `id` are
  NOT required. Any client doing `res["state"]` violates the spec.
- **`state` values are open-ended strings**, not an enforced enum. New values can
  appear without spec changes.
- **Error responses (4xx/5xx) use a different schema** (`ErrorResponse` with
  `errorCode` + `message`) — no `state`, no `livyInfo`, no `id`.
- **HTTP 430** (non-standard) returned by Fabric for rate limiting ("too many
  concurrent Spark sessions"). Not in any HTTP spec but Fabric uses it.

**What we hardened** (upstream code had `res["state"]` → KeyError):
1. **Zero direct dict access on API responses** — all fields use `.get()` with
   fallback. Every missing field raises an error with the full response dumped.
2. **HTTP status check before parsing** — `wait_for_session_start` checks
   `status_code >= 400` and parses as `ErrorResponse` (not `SessionResponse`).
3. **Three-level state checking** — `state` (top-level) + `livyInfo.currentState`
   (Livy-side) + `fabricSessionStateInfo.state` (Fabric acquisition-side, earliest
   signal for provisioning failures like `error`/`cancelled`).
4. **HTTP 429/430 handling** in `create_session` — clear error message instead of
   cryptic `KeyError('state')`.
5. **`errorInfo` extraction** — surfaces Fabric's error code and message (e.g.
   `LIVY_JOB_TIMED_OUT`) in the error instead of generic "failed to connect".

**Real bugs this fixes**:
- User got `failed to connect: 'state'` — Fabric returned 430 (rate limit), old code
  tried `res["state"]` on the `ErrorResponse` body → KeyError.
- Session polling looped silently for 10 minutes then timed out — `fabricSessionStateInfo`
  showed `error` but code only checked `state` and `livyInfo`.
- Statement execution failed with `KeyError: 'output'` — intermittent Fabric API response
  missing the `output` field.

### 8. Integration test suite
**Files**: `tests/integration/` (dbt project with table/view/incremental/elementary/defer tests)
**Status**: Active
**Why**: End-to-end smoke test against real Fabric workspace. Not in upstream.
Includes:
- `run_integration_test.sh` — table/view/incremental + elementary (26 steps)
- `run_defer_test.sh` — defer + clone + state:modified (26 steps, all passing)
- dbt unit tests (7 tests for SQL logic)
- Verification macros + compiled SQL checks

---

## Design Decisions

### workspace_name profile field
**Files**: `credentials.py` (field + `__post_init__` composition)
**Status**: Active — required for cross-workspace `--defer`
**Why**: When using `dbt run --defer --state prod-artifacts/` and prod is in a different
Fabric workspace, dbt needs 4-part names to reference prod tables from the dev workspace.
Setting `workspace_name` in the prod profile makes all prod relations render as
`workspace.lakehouse.schema.table`, so deferred refs resolve correctly across workspaces.

**Example** (profiles.yml):
```yaml
prod:
  workspace_name: "vd-dbt-fabricspark-prod"   # enables 4-part naming for defer
  lakehouse: "kuruma_prod_lake"
  schema: "dbo"
  # → database becomes "vd-dbt-fabricspark-prod.kuruma_prod_lake"
  # → relations render as: vd-dbt-fabricspark-prod.kuruma_prod_lake.dbo.my_model

dev:
  # No workspace_name — standard 3-part naming for own workspace
  lakehouse: "kuruma_dev_lake"
  schema: "dbo"
  # → relations render as: kuruma_dev_lake.dbo.my_model
```

**When to use**:
- Set `workspace_name` on **prod profile only** (or any profile used as defer target from another workspace)
- Do NOT set on dev profile if dev runs in its own workspace (unnecessary overhead)
- For per-model cross-workspace reads (without defer), use `database: "ws.lakehouse"` in model config instead

---

## Upstream Features Gained (v1.9.5 merge — 2026-04-17)

These came from upstream and are NOT fork-specific:
- Materialized Lake View (MLV) support
- Local Livy mode (Docker Spark)
- Thread-safe session management + session reuse
- Schema auto-detection via Fabric REST API
- Security validations (UUID, HTTPS endpoint)
- `make_temp_relation` fix (strips db/schema for temp views)
- OneLake type parsing (MANAGED/EXTERNAL/MLV)
- Exponential backoff retry
- CI/CD modernization

## Upstream Features Gained (v1.9.6 merge — 2026-04-25)

Theme: **Resilience hardening + new identifier_prefix feature**.

### Resilience improvements

- `_parse_retry_after()` helper — proper Retry-After header parsing for 429s
- POLL_TIMEOUT 600s → 1800s (sessions have 30 min to start under contention)
- HTTP 429 retry on session creation POST (5 retries with exponential backoff + jitter)
- Shard staggering (5s × shard index) for concurrent session creation
- 404 retry on session creation POST (Livy endpoint warmup)
- `retry_all` honored for statement execution + new retryable keywords
- MLV API hardened against 429 throttling
- Network exception handling (SSLError/ConnectionError/Timeout) in submit + poll
- HTTP 404 retry-with-backoff for just-submitted statements
- Fabric API timeout 30s → 120s (avoids transient ReadTimeoutError)
- `connect_retries` default 1 → 5 (kept upstream default)
- `statement_timeout` default 3600s → 43200s (12h, supports long-running models)
- `poll_statement_wait` default `5` → `0.5` (faster MLV polling)

### Bug fixes

- `_fix_binding`: single-quote escaping in seed values
- `FabricSparkQuotePolicy.database = True` — fixes mixed-case lakehouse `ApproximateMatchError`
- `MATERIALIZED_LAKE_VIEW` type string match in `_build_spark_relation_list`
- `drop_relation` macro handles `materialized_view` type
- `ensure_database_exists` macro guard for schema-enabled
- Seed materialization drops stale catalog entry first (fixes DELTA_METADATA_ABSENT)
- Snapshot uses `model.database` for cross-lakehouse writes

### New features

- `identifier_prefix` config field — auto-prepend prefix to all identifiers (skips CTEs)
- `_identifier_prefix` ClassVar on `FabricSparkRelation`
- `_skip_prefix` kwarg in `create()` for opt-out

### Infrastructure

- `azure-cli` moved to optional `[cli]` extra (lighter install)

## Breaking Changes from v1.9.14 → v1.9.16

| v1.9.14 | v1.9.16 | Action |
|---|---|---|
| `quote_policy.database = False` | `quote_policy.database = True` | Database identifiers now backtick-quoted in rendered SQL |
| `statement_timeout: int = 3600` | `statement_timeout: int = 43200` | 12h default supports long-running models |
| `poll_statement_wait: int = 5` | `poll_statement_wait: float = 0.5` | Faster polling, type changed to float |
| `azure-cli` always installed | `azure-cli` optional `[cli]` extra | Users needing CLI auth: `pip install vd-dbt-fabricspark[cli]` |
