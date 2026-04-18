# Fork Feature Tracker — vd-dbt-fabricspark

Tracks all features added by the accelerate-data fork that are NOT in
upstream microsoft/dbt-fabricspark. Helps with future upstream merges.

**Upstream**: microsoft/dbt-fabricspark (v1.9.5 as of 2026-04-17)
**Fork**: accelerate-data/vd-dbt-fabricspark (v1.9.14)

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

### 7. Integration test suite
**Files**: `tests/integration/` (dbt project with table/view/incremental/elementary tests)
**Status**: Active
**Why**: End-to-end smoke test against real Fabric workspace. Not in upstream.

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

## Upstream Features Gained (v1.9.5 merge)

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
