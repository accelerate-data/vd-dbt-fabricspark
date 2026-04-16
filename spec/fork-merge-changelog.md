# Fork Merge Changelog — upstream/main into accelerate-data/vd-dbt-fabricspark

**Date**: 2026-04-16
**Upstream**: microsoft/dbt-fabricspark @ `deeba6f` (v1.9.5)
**Fork**: accelerate-data/vd-dbt-fabricspark @ `e79125b` (v1.9.13)
**Common ancestor**: `7fbc583` (Merge PR #40 — v1.9.0)
**Result version**: 1.9.14

---

## Features gained from upstream (41 commits)

| Feature | Files | Notes |
|---|---|---|
| **Materialized Lake View (MLV)** support | `mlv_api.py` (new), `connections.py`, `impl.py`, macros | Full lifecycle: create, refresh, schedule, validation |
| **Local Livy mode** | `credentials.py`, `connections.py`, `livysession.py` | `livy_mode: "local"`, Docker Spark support |
| **Thread-safe session management** | `livysession.py` | Global lock, atexit cleanup, session reuse |
| **Session reuse across runs** | `credentials.py`, `livysession.py` | `reuse_session`, `session_id_file`, `session_idle_timeout` |
| **Security validations** | `credentials.py` | UUID validation, HTTPS endpoint check, `__repr__` masking |
| **Schema auto-detection** | `credentials.py`, `connections.py` | `lakehouse_schemas_enabled` via REST API (init=False) |
| **Cross-lakehouse WRITE** | `schema.sql`, `impl.py` | `generate_database_name`/`generate_schema_name` with parse-time fallback |
| **Exponential backoff retry** | `connections.py` | Sleep: `min(5 * 2^(attempt-1), 60)` seconds |
| **OneLake type parsing** | `impl.py` | `ONELAKE_TYPE_REGEX` for MANAGED/EXTERNAL/MLV types |
| **Snapshot staging as Delta table** | `snapshot.sql` | OneLake doesn't support VIEWs |
| **CI/CD modernization** | `.github/workflows/` | `main.yml` → `ci.yml`, expanded integration tests |
| **Bootstrapper scripts** | `contrib/` | Dev environment setup (PS1/Bash) |
| **CHANGELOG.md** | root | Upstream changelog |
| **Seed materialization** | `macros/materializations/seeds/seed.sql` | New |
| **HTTP timeout configs** | `credentials.py` | `http_timeout`, `session_start_timeout`, `statement_timeout`, `poll_wait`, `poll_statement_wait` |

## Fork-unique features preserved

| Feature | Files | Decision rationale |
|---|---|---|
| **Cross-WORKSPACE support (4-part naming)** | `relation.py`, `credentials.py`, `cross_workspace.sql` | Upstream only has cross-lakehouse (3-part). Our 4-part `workspace.lakehouse.schema.table` support is unique. |
| **Notebook module** | `notebook/__init__.py`, `environment.py`, `repo.py`, `runner.py` | Entirely fork-only. Runs dbt inside Fabric notebooks. |
| **GitHub App auth via Key Vault** | `notebook/repo.py` | Fork-only integration for private repo cloning. |
| **Public repo clone fallback** | `notebook/repo.py` | Graceful degradation when no credentials provided. |
| **Multi-command dbt jobs** | `notebook/__init__.py` | `DbtJobConfig.command` accepts string or list. |
| **Cross-schema cache-key normalization** | `impl.py` (`_relations_cache_for_schemas`) | Fixes Elementary's `on-run-end` cache misses. Upstream doesn't have this. |
| **`_normalize_workspace_database`/`_normalize_schema_parts`** | `impl.py` | Helpers for 4-part naming decomposition. |
| **`list_relations` override** | `impl.py` | `cache_database` vs `query_database` normalization for relation cache. |
| **`get_relation` override** | `impl.py` | Uses `RelationReturnedMultipleResultsError`. |
| **`workspace_name` field** | `credentials.py` | For cross-workspace ref resolution. |
| **`vdstudio_oauth` auth** | `livysession.py`, `credentials.py` | vd-studio local OAuth token endpoint. |
| **`env_oauth_access_token` auth** | `livysession.py` | Reads token from `FABRIC_LAKEHOUSE_ACCESS_TOKEN` env var. |
| **`FabricSparkRelation.render()`** | `relation.py` | Custom SQL identifier rendering for 2/3/4-part naming. |

## Fork features replaced by upstream

| Fork feature | Replaced by upstream | Reason |
|---|---|---|
| `reuse_livy_session` field | `reuse_session` | Unified naming with upstream |
| `livy_session_path` field | `session_id_file` | Unified naming with upstream |
| `database: Optional[str] = None` | `field(default=None, init=False)` | Upstream's derived-from-lakehouse approach is cleaner |
| `lakehouse_schemas_enabled: bool = True` | `field(default=False, init=False)` | Upstream auto-detects via REST API |
| Fork's `__pre_deserialize__` (db/lh sync) | Upstream's simpler strip approach | Upstream's `init=False` design makes sync unnecessary |
| Fork's inline type detection (`MaterializedView`, `VIEW`) | `_parse_relation_type()` with regex | Upstream's approach handles all OneLake types properly |
| Fork's `_build_spark_relation_list` with `norm_database` | Upstream's `schema_relation` parameter passing | Upstream's approach is the standard dbt pattern |
| Fork's retry message (`5 seconds`) | Upstream's `min(5 * 2^(attempt-1), 60)` | Fork had wrong message (didn't match actual sleep) |
| Fork's snapshot staging as `type='table', database=none` | Upstream's `type='view', database=target.database` | Upstream handles this correctly |

## Conflict resolution summary

| File | Conflicts | Strategy |
|---|---|---|
| `.gitignore` | 1 | Merged both — upstream's `.temp/*` + fork's scratch ignores |
| `__version__.py` | 1 | Took upstream (1.9.5), will bump to 1.9.14 |
| `connections.py` | 1 | Took upstream wholesale (MLV, local mode, exponential backoff) |
| `credentials.py` | 7 | Upstream base + fork's `workspace_name`, `vdstudio_oauth_endpoint_url`, `is_current_workspace()` |
| `impl.py` | 4 | Upstream base + fork's cache-key normalization, workspace helpers, `get_relation` override |
| `livysession.py` | 7 | Upstream base + fork's `vdstudio_oauth`, `env_access_token`, `notebook_access_token` auth |
| `relation.py` | 3 | Upstream's `_schemas_enabled` ClassVar + fork's 4-part naming, `render()`, `create()` |
| `relation.sql` | 1 | Took upstream (database prefix for schema-enabled mode) |
| `schema.sql` | 1 | Took upstream (generate_database_name/schema_name with parse-time fallback) |
| `snapshot.sql` | 1 | Took upstream (Delta staging table with database) |
| `pyproject.toml` | 1 | Took upstream |
| `uv.lock` | 60 | Took upstream (will regenerate) |
