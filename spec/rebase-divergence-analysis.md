# Rebase Divergence Analysis

**Common ancestor**: `7fbc583` — "Merge pull request #40 from microsoft/v1.9.0"

**Upstream**: `microsoft/dbt-fabricspark` (main) — 41 commits ahead, version `1.9.5`
**Fork**: `accelerate-data/vd-dbt-fabricspark` (main) — 5 commits ahead, version `1.9.13`

Both branches diverged and independently added features in overlapping areas (cross-lakehouse, schema support, Livy session management, local mode). The conflict complexity stems from two teams solving similar problems with different designs.

---

## 1. Upstream Changes (41 commits)

### 1.1 Major Features

**Materialized Lake View (MLV) support**
- New file: `src/dbt/adapters/fabricspark/mlv_api.py` (485 lines)
- New materialization: `materialized_lake_view.sql`
- New tests: `test_materialized_lake_view.py`, `test_mlv_api.py`
- Hooks into `connections.py` (prereq check at connect time) and `impl.py` (`@available` adapter methods: `mlv_run_on_demand`, `mlv_create_or_update_schedule`, `mlv_resolve_lakehouse_id`, `mlv_validate_prerequisites`, `mlv_validate_delta_sources`)

**Local Livy mode**
- `livy_mode: Literal["fabric", "local"]` field in credentials
- `is_local_mode` property; local mode relaxes Fabric-specific validations
- Local mode uses `livy_url` instead of Fabric API endpoint

**Livy session stability & reuse (session sharing across runs)**
- `reuse_session` flag, `session_idle_timeout`, `session_id_file`
- Thread-safe session creation via global lock
- `environmentId` field for custom Spark environments
- HTTP/session/statement timeouts: `http_timeout`, `session_start_timeout`, `statement_timeout`, `poll_wait`, `poll_statement_wait`
- Major expansion of `livysession.py` (+809 lines)
- Atexit-registered session cleanup

**Fabric auth support** (separate from CLI auth)
- `authentication: str = "CLI"` (was `"az_cli"`)
- Upstream supports ServicePrincipal via `client_id/client_secret/tenant_id`

**Security validations**
- `_validate_uuid()` for workspaceid/lakehouseid (prevents path traversal)
- `_validate_endpoint()` — ensures HTTPS and known Fabric domain
- `__repr__` masks sensitive fields (`client_secret`, `accessToken`)

**Schema-enabled lakehouse auto-detection**
- `lakehouse_schemas_enabled: bool = field(default=False, init=False)` — auto-detected via Fabric REST API at connection open
- `apply_lakehouse_properties()` sets flag based on `defaultSchema` presence
- `FabricSparkRelation._schemas_enabled` ClassVar toggles two-part vs three-part naming
- `from_dict` sanitizes invalid `type` values (Jinja Undefined → None)

**Cross-lakehouse WRITE support** (upstream's approach)
- Three-part naming: `lakehouse.schema.table`
- `generate_database_name` returns `target.lakehouse`
- `generate_schema_name` with parse-time fallback (compares target.schema vs target.lakehouse)
- `database` field is `init=False` — always derived from `lakehouse` name
- Adjusted macros (`relation.sql`, `schema.sql`) to prefix database when needed

**Retry-message fix**
- Exponential backoff message now reflects actual sleep duration: `min(5 * (2 ** (attempt - 1)), 60)`

**Relation/catalog improvements**
- `_parse_relation_type` handles OneLake types (MANAGED/EXTERNAL/MATERIALIZED_LAKE_VIEW/VIEW)
- `ONELAKE_TYPE_REGEX` replaces string-match checks
- `list_relations_without_caching` uses schema_relation for DB/schema carrying
- `_get_columns_for_catalog` ensures `table_database` matches manifest's `CatalogKey`

**OneLake view-compat fixes**
- Snapshot staging table built as Delta table instead of VIEW (OneLake doesn't support standard Spark VIEWs)
- `fabricspark__make_temp_relation` uses `include(database=false, schema=false)` for temp views

### 1.2 Infrastructure

- **Build**: Migration to `uv`, `ruff`, semver
- **CI/CD**: Refactored workflows (`main.yml` → `ci.yml`, updated `integration.yml`, `release.yml`)
- **Bootstrapper scripts**: `contrib/bootstrap-dev-env.{sh,ps1}`
- **CHANGELOG.md** added
- **Dependabot** config
- **README** significantly expanded

### 1.3 Tests Cleanup
- Removed many older skipped/obsolete functional tests (-18 to -36 lines per file)
- Added MLV unit + functional tests
- Added livysession unit tests
- Expanded credentials tests (+248 lines)

---

## 2. Fork Changes (5 commits)

Commits (oldest → newest):
1. `ec9895c` — Add cross-workspace support, relation enhancements, update gitignore
2. `163052d` — v1.9.7: Fix clone materialization, sync database/lakehouse, add notebook module
3. `e57e526` — v1.9.8: Cross-workspace support, relation enhancements, gitignore cleanup
4. `afdbdf0` — v1.9.9: Fetch all GitHub App credentials from Key Vault
5. `e79125b` — v1.9.13: Multi-command support, public repo clone, cross-schema cache fix

### 2.1 Fork-unique features (NOT in upstream)

**Cross-WORKSPACE support — four-part naming**
- `workspace.lakehouse.schema.table` (cross-workspace queries, not just cross-lakehouse)
- `workspace_name` field on credentials
- `workspace` field on `FabricSparkRelation`
- `FabricSparkFourPartIncludePolicy`, `FabricSparkThreePartIncludePolicy`
- `is_current_workspace()` helper
- Custom `render()` method on relation to emit 4-part identifiers
- New macro file: `cross_workspace.sql` (116 lines) — likely defines source() / ref() wrappers
- Properties: `is_cross_workspace`, `is_four_part`, `is_three_part`, `is_two_part`
- Helpers: `with_workspace()`, `without_workspace()`
- `credentials.lakehouse_endpoint` special-cases local mode

**Notebook module** — run dbt inside a Fabric notebook
- `src/dbt/adapters/fabricspark/notebook/__init__.py` — `DbtJobConfig`, `run_dbt_job`
- `notebook/environment.py` — `ConnectionConfig`, `setup_environment`
- `notebook/repo.py` — `RepoConfig`, `clone_repo` (GitHub App auth via Key Vault, public repo fallback)
- `notebook/runner.py` — executes `dbt` subprocess with logging + artifact persistence
- Multi-command support: `DbtJobConfig.command` accepts string or list; runs sequentially

**Cross-schema cache-key fix** (v1.9.13)
- `_relations_cache_for_schemas` normalizes relation database to match manifest's key (e.g. `sampledata.salesforce` vs bare `salesforce`)
- Required so Elementary's `on-run-end` `get_relation()` calls hit the cache

**Clone materialization fix** (v1.9.7)
- `clone.sql` patched

**Key Vault GitHub App credentials**
- `repo.py::_resolve_token` fetches `github_app_id`, `github_installation_id`, and PEM key from Azure Key Vault

### 2.2 Overlapping features (different implementations)

| Feature | Upstream | Fork |
|---|---|---|
| Local livy mode | `livy_mode: Literal` enum | similar (likely compatible) |
| Schema-enabled lakehouse | auto-detect via API; `init=False` | default `True`; database/lakehouse synonyms |
| Session reuse | `reuse_session`, `session_id_file`, `session_idle_timeout` | `reuse_livy_session`, `livy_session_path`, `session_id_file` (alias) |
| Cross-lakehouse | three-part naming via `generate_database_name` | `workspace_name` + four-part, plus three-part |
| `lakehouse_schemas_enabled` | `bool = field(default=False, init=False)` | `bool = True` |
| `database` field | `init=False`, derived from lakehouse | regular field, synced via `__pre_deserialize__` |
| `connection_keys` | excludes sensitive fields | includes `workspace_name` |
| Retry message | exponential backoff string | fixed 5s string (BUG — fork has wrong msg) |

### 2.3 Fork-specific fields on credentials

- `workspace_name` (unique to fork)
- `reuse_livy_session` / `livy_session_path` (fork's equivalent of upstream's `reuse_session` / `session_id_file`)
- `vdstudio_oauth_endpoint_url` (fork-specific OAuth integration)

---

## 3. Conflicting Files — Resolution Strategy

Conflicts are grouped by severity. Strategy for each file reflects the guiding principle: **keep upstream as the base** (it's the more actively maintained branch with proper testing/CI), then **layer fork-unique features on top**.

### 3.1 Auto-generated (regenerate, don't merge)

| File | Action |
|---|---|
| `uv.lock` | Accept upstream version, then `uv lock` to regenerate after code settles |

### 3.2 Keep upstream (our fork's changes were independent fixes to same problem, upstream's are cleaner/tested)

| File | Reasoning |
|---|---|
| `connections.py` (retry msg only) | Upstream's message matches actual exponential backoff; fork's is a bug |
| `src/dbt/include/fabricspark/macros/adapters/relation.sql` | Upstream adds `database.` prefix — needed for schema-enabled mode |
| `src/dbt/include/fabricspark/macros/adapters/schema.sql` | Upstream's `generate_database_name`/`generate_schema_name` with parse-time fallback is more robust |
| `src/dbt/include/fabricspark/macros/materializations/snapshots/snapshot.sql` | Upstream correctly uses Delta table (not view) with `database=target.database` — matches OneLake constraints |
| `__version__.py` | First conflict: choose upstream `1.9.5` base; final commit bumps to fork version |

### 3.3 Keep fork (unique features not in upstream)

| File | Reasoning |
|---|---|
| All files under `src/dbt/adapters/fabricspark/notebook/` | Fork-only module, no upstream version |
| `src/dbt/include/fabricspark/macros/adapters/cross_workspace.sql` | Fork-only |
| `.gitignore` | Merge — keep both upstream's `.temp/*` and fork's `ANALYSIS.md`, `node_modules/`, etc. |

### 3.4 Merge carefully (both sides made complementary changes)

#### `src/dbt/adapters/fabricspark/credentials.py`
**Base**: upstream (it has more security, validations, init=False fields)
**Add from fork**:
- `workspace_name: Optional[str] = None` field
- `vdstudio_oauth_endpoint_url: Optional[str] = None` field
- `is_current_workspace()` method
- `workspace_name` in `_connection_keys()` tuple
- Rename/alias: keep fork's `reuse_livy_session` → use upstream's `reuse_session` name (breaking change, but unify)

**Conflicts to drop from fork** (because upstream has a better solution):
- Fork's `database: Optional[str] = None` → use upstream's `field(default=None, init=False)`
- Fork's `lakehouse_schemas_enabled: bool = True` → use upstream's `field(default=False, init=False)`
- Fork's schema-enabled validation in `__post_init__` → use upstream's `apply_lakehouse_properties`

#### `src/dbt/adapters/fabricspark/impl.py`
**Base**: upstream (MLV support, `_parse_relation_type`, `ONELAKE_TYPE_REGEX`)
**Add from fork**:
- `_normalize_workspace_database()` helper (for cache-key fix)
- `_normalize_schema_parts()` helper (four-part support)
- Cross-schema cache-key normalization in `_relations_cache_for_schemas` (tuple-based futures tracking with cache_schema retained)
- `list_relations` override with `cache_database` vs `query_database` normalization
- `get_relation` override with `RelationReturnedMultipleResultsError` (but upstream already imports it — keep fork's override logic)

**Keep upstream**:
- All MLV adapter methods (`mlv_run_on_demand`, etc.)
- `is_local_mode`, `is_lakehouse_schemas_enabled` (may need to merge if fork diverged)
- `_parse_relation_type` over fork's inline type-detection
- Upstream's `_get_columns_for_catalog` that uses `relation.database` directly (fork's `or self.config.credentials.lakehouse` fallback is already covered by upstream's init=False design)

#### `src/dbt/adapters/fabricspark/relation.py`
**Base**: upstream (has `_schemas_enabled` ClassVar, `from_dict` sanitization, `include_policy` factory)
**Add from fork**:
- `FabricSparkThreePartIncludePolicy`, `FabricSparkFourPartIncludePolicy` classes
- `workspace: Optional[str] = None` field
- Custom `render()` method (handles 4-part and `database="ws.lakehouse"` patterns)
- `create()` override that selects include policy based on workspace/database/schema
- `with_workspace()`, `without_workspace()` helpers
- Properties: `is_table`, `is_view`, `is_cross_workspace`, `is_four_part`, `is_three_part`, `is_two_part`
- `get_three_part_include_policy`, `get_four_part_include_policy` classmethods
- Backwards-compat alias: `FabricSparkSchemaEnabledIncludePolicy = FabricSparkThreePartIncludePolicy`

**Keep upstream**:
- `_schemas_enabled: ClassVar[bool]`
- `incorporate` override (fork's `incorporate` is debug-print + type sanitization — upstream's `from_dict` approach is cleaner; fork's sanitization can be kept but remove the `print()`)
- `_VALID_RELATION_TYPES` set + `from_dict` sanitization

#### `src/dbt/adapters/fabricspark/livysession.py`
**Base**: upstream (much more extensive — threading, session reuse, atexit, better error handling)
**Merge carefully**:
- Fork's `reuse_livy_session`/`livy_session_path` references should map to upstream's `reuse_session`/`session_id_file` — simplify by keeping upstream naming
- Fork-specific `vdstudio_oauth_endpoint_url` auth path — add if fork relies on it
- Fork's `get_lakehouse_properties` function — needed by upstream's `connections.py` (already imported there)

**Strategy**: for each conflict block, compare side-by-side and pick upstream unless fork has a unique feature (e.g. vdstudio auth).

### 3.5 No-code surfacing decisions for the user

1. **Version bump**: After rebase, fork's head commit bumps version. We'll set version to `1.9.14` (next after current fork `1.9.13`) to reflect the rebase+merge.
2. **`reuse_livy_session` vs `reuse_session`**: Adopt upstream's naming. Any fork YAML configs will need migration. (Confirm with user before committing.)
3. **Elementary `on-run-end` cache fix** (v1.9.13): Keep as a patch on top of upstream's `_relations_cache_for_schemas` since upstream's version doesn't handle the Elementary-specific case.
4. **Notebook module**: keep entirely — fork-unique.
5. **`#_bootstrap_elementary` commented-out line** in `runner.py` (v1.9.13) — temporary; leave commented as-is during rebase.

---

## 4. Rebase Execution Plan

The rebase will replay 5 fork commits on top of upstream's 41 commits. Because the first fork commit (`ec9895c`) creates most conflicts (it originally diverged from upstream just before upstream's big cross-lakehouse/schema work), the strategy is:

1. **`ec9895c`** (first commit to rebase) — resolve all 10 conflicts per 3.x above. This is the biggest one.
2. **`163052d`** (notebook module + db/lakehouse sync) — notebook module should apply cleanly (new files). The `database/lakehouse` sync logic in `__pre_deserialize__` conflicts with upstream's `init=False` design; **drop fork's sync code** and rely on upstream's derived `database`.
3. **`e57e526`** (gitignore cleanup, cross-workspace enhancements) — minor; likely resolves after step 1.
4. **`afdbdf0`** (Key Vault credentials) — touches only `notebook/repo.py`; should apply cleanly.
5. **`e79125b`** (our new v1.9.13) — applies cleanly if step 1 preserved fork's `_relations_cache_for_schemas` logic.

### Git-level plan

```bash
git rebase upstream/main
# Resolve conflicts per 3.x, `git add` files, `git rebase --continue`
# Repeat for each commit
```

### Post-rebase validation

1. `uv lock` to regenerate `uv.lock`
2. `uv sync` + `ruff check` + `ruff format` to verify no syntax errors
3. Run unit tests: `pytest tests/unit/`
4. Confirm with user before `git push --force-with-lease`

---

## 5. User Decisions (locked in 2026-04-15)

1. **Naming unification**: ✅ Adopt upstream's `reuse_session`/`session_id_file` names. Fork's `reuse_livy_session`/`livy_session_path` are removed. This is a **breaking change** for profiles that used the fork fields.
2. **Version bump**: ✅ `2.0.0-rc1` — signals the breaking change + major upstream merge.
3. **Push strategy**: ✅ Do not push after rebase; user will review first.
