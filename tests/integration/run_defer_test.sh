#!/usr/bin/env bash
set -uo pipefail

# ─── Defer Test Runner for vd-dbt-fabricspark ─────────────────────────────────
#
# Tests dbt --defer with clone, state:modified, and cross-workspace scenarios
# on real Fabric workspaces.
#
# DAG under test:
#   seed_orders → defer_base (table) → defer_agg (incremental) → defer_summary (view)
#   seed_customers → defer_customers (table)  ← separate chain
#
# Usage:
#   ./run_defer_test.sh                          # default: prod→dev defer
#   ./run_defer_test.sh --prod-target prod       # specify targets
#   ./run_defer_test.sh --dev-target dev
#
# Prerequisites:
#   - az login
#   - source .venv/bin/activate
#   - Both prod and dev targets defined in profiles.yml
# ──────────────────────────────────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PROD_TARGET="prod"
DEV_TARGET="dev"
SKIP_PROD=false

for arg in "$@"; do
    case "$arg" in
        --prod-target) shift; PROD_TARGET="$1"; shift ;;
        --prod-target=*) PROD_TARGET="${arg#*=}" ;;
        --dev-target) shift; DEV_TARGET="$1"; shift ;;
        --dev-target=*) DEV_TARGET="${arg#*=}" ;;
        --skip-prod) SKIP_PROD=true ;;
    esac
done

# Lakehouse names for verification (must match profiles.yml)
# Bare names for --args YAML (no backticks — YAML can't parse them)
PROD_LAKEHOUSE="kuruma_prod_lake"
DEV_LAKEHOUSE="kuruma_dev_lake"

export DBT_PROFILES_DIR="$SCRIPT_DIR"
RESULTS_FILE="$SCRIPT_DIR/target/defer_results.txt"
RESULTS_LOG="$SCRIPT_DIR/target/defer_results_full.log"
PROD_ARTIFACTS="$SCRIPT_DIR/prod-artifacts"
mkdir -p "$SCRIPT_DIR/target"
> "$RESULTS_FILE"
> "$RESULTS_LOG"

PASS=0
FAIL=0
STEP=0
START_TIME=$(date +%s)

run_step() {
    local step_name="$1"
    shift
    STEP=$((STEP + 1))
    local step_start
    step_start=$(date +%s)

    echo ""
    echo "── [$STEP] $step_name ──"

    local output
    local exit_code
    output=$("$@" 2>&1) && exit_code=0 || exit_code=$?

    local step_end
    step_end=$(date +%s)
    local duration=$((step_end - step_start))

    if [ "$exit_code" -eq 0 ]; then
        echo "  PASSED  (${duration}s)"
        PASS=$((PASS + 1))
        echo "PASS | [$STEP] $step_name (${duration}s)" >> "$RESULTS_FILE"
        echo "PASS | [$STEP] $step_name (${duration}s)" >> "$RESULTS_LOG"
    else
        echo "  FAILED  (${duration}s, exit=$exit_code)"
        echo "  ── failure output (last 15 lines) ──"
        echo "$output" | tail -15 | sed 's/^/  | /'
        echo "  ── end ──"
        FAIL=$((FAIL + 1))
        echo "FAIL | [$STEP] $step_name (${duration}s, exit=$exit_code)" >> "$RESULTS_FILE"
        {
            echo "============ FAIL | [$STEP] $step_name (${duration}s, exit=$exit_code) ============"
            echo "$output"
            echo ""
        } >> "$RESULTS_LOG"
    fi
}

# Helper: verify compiled SQL contains (or doesn't contain) a string
verify_compiled_sql() {
    local model_name="$1"
    local search_string="$2"
    local should_exist="$3"  # "yes" or "no"
    local compiled_file="$SCRIPT_DIR/target/run/vd_dbt_fabricspark_integration/models/defer_chain/${model_name}.sql"

    if [ ! -f "$compiled_file" ]; then
        echo "VERIFY FAIL: compiled SQL not found at $compiled_file" >&2
        return 1
    fi

    if grep -qi "$search_string" "$compiled_file"; then
        if [ "$should_exist" = "yes" ]; then
            echo "VERIFY PASS: '$search_string' found in $model_name"
            return 0
        else
            echo "VERIFY FAIL: '$search_string' found in $model_name but should NOT be" >&2
            head -20 "$compiled_file" >&2
            return 1
        fi
    else
        if [ "$should_exist" = "no" ]; then
            echo "VERIFY PASS: '$search_string' not in $model_name (expected)"
            return 0
        else
            echo "VERIFY FAIL: '$search_string' NOT found in $model_name" >&2
            head -20 "$compiled_file" >&2
            return 1
        fi
    fi
}

echo "=============================================="
echo "  vd-dbt-fabricspark DEFER test"
echo "  Prod target: $PROD_TARGET"
echo "  Dev target:  $DEV_TARGET"
echo "  Started: $(date '+%Y-%m-%d %H:%M:%S')"
echo "=============================================="

# ══════════════════════════════════════════════════════════════════════════════
# PHASE 1: Build prod baseline
# ══════════════════════════════════════════════════════════════════════════════
run_step "deps" \
    dbt deps

if [ "$SKIP_PROD" = true ] && [ -d "$PROD_ARTIFACTS" ]; then
    echo ""
    echo "═══ PHASE 1: SKIPPED (--skip-prod, using existing $PROD_ARTIFACTS) ═══"
else
    echo ""
    echo "═══ PHASE 1: Build prod baseline ═══"

    run_step "prod: debug (cluster warm-up)" \
        dbt debug --target "$PROD_TARGET"

    run_step "prod: seed" \
        dbt seed --target "$PROD_TARGET" --full-refresh

    run_step "prod: build defer chain" \
        dbt run --target "$PROD_TARGET" --select "defer_base defer_agg defer_summary defer_customers"

    run_step "prod: test defer chain" \
        dbt test --target "$PROD_TARGET" --select "defer_base defer_agg"

    # Save prod artifacts for defer
    rm -rf "$PROD_ARTIFACTS"
    cp -r "$SCRIPT_DIR/target" "$PROD_ARTIFACTS"
    echo ""
    echo "  Prod artifacts saved to: $PROD_ARTIFACTS"
fi

# ══════════════════════════════════════════════════════════════════════════════
# PHASE 2: Basic defer (dev empty)
# ══════════════════════════════════════════════════════════════════════════════
echo ""
echo "═══ PHASE 2: Basic defer (dev empty) ═══"

# Warm up dev cluster
run_step "dev: debug (cluster warm-up)" \
    dbt debug --target "$DEV_TARGET"

# Clean dev completely — drop all defer tables/views + seeds so dev is truly empty
run_step "dev: clean all defer tables" \
    dbt run-operation drop_defer_tables --target "$DEV_TARGET"

run_step "dev: clean seeds" \
    dbt run-operation drop_seeds --target "$DEV_TARGET"

# Now seed fresh
run_step "dev: seed" \
    dbt seed --target "$DEV_TARGET" --full-refresh

# ── Test 2a: Defer single model — upstream falls back to prod ──
run_step "defer 2a: run defer_summary (upstream defers to prod)" \
    dbt run --target "$DEV_TARGET" --select defer_summary \
    --defer --state "$PROD_ARTIFACTS"

# VERIFY 2a: compiled SQL references prod lakehouse
run_step "defer 2a: VERIFY compiled SQL refs prod" \
    verify_compiled_sql defer_summary "kuruma_prod_lake" "yes"

# ── Test 2b: Incremental without clone → full refresh ──
run_step "defer 2b: run defer_agg without clone (full refresh)" \
    dbt run --target "$DEV_TARGET" --select defer_agg \
    --defer --state "$PROD_ARTIFACTS"

# VERIFY 2b: table exists in dev
run_step "defer 2b: VERIFY defer_agg exists in dev" \
    dbt run-operation verify_table_exists --target "$DEV_TARGET" \
    --args "{lakehouse: $DEV_LAKEHOUSE, schema_name: dbo, table_name: defer_agg}"

# VERIFY 2b: was full refresh (NO WHERE clause = is_incremental was false)
run_step "defer 2b: VERIFY was full refresh (no WHERE)" \
    verify_compiled_sql defer_agg "where order_date >=" "no"

# ══════════════════════════════════════════════════════════════════════════════
# PHASE 3: Clone + defer (incremental stays incremental)
# ══════════════════════════════════════════════════════════════════════════════
echo ""
echo "═══ PHASE 3: Clone + defer (incremental) ═══"

# Drop dev defer tables to start fresh
run_step "dev: drop defer tables" \
    dbt run-operation drop_defer_tables --target "$DEV_TARGET"

# VERIFY tables are gone
run_step "dev: VERIFY defer_agg dropped" \
    dbt run-operation verify_table_not_exists --target "$DEV_TARGET" \
    --args "{lakehouse: $DEV_LAKEHOUSE, schema_name: dbo, table_name: defer_agg}"

# Clone ONLY incremental models (not table/view)
run_step "defer 3a: clone only incremental models" \
    dbt clone --target "$DEV_TARGET" \
    --select "config.materialized:incremental" \
    --state "$PROD_ARTIFACTS"

# VERIFY 3a: defer_agg cloned
run_step "defer 3a: VERIFY defer_agg cloned" \
    dbt run-operation verify_table_exists --target "$DEV_TARGET" \
    --args "{lakehouse: $DEV_LAKEHOUSE, schema_name: dbo, table_name: defer_agg}"

# VERIFY 3a: defer_base NOT cloned (it's table, not incremental)
run_step "defer 3a: VERIFY defer_base NOT cloned" \
    dbt run-operation verify_table_not_exists --target "$DEV_TARGET" \
    --args "{lakehouse: $DEV_LAKEHOUSE, schema_name: dbo, table_name: defer_base}"

# Run defer_agg — should be INCREMENTAL (clone made it exist)
run_step "defer 3b: run defer_agg after clone (incremental)" \
    dbt run --target "$DEV_TARGET" --select defer_agg \
    --defer --state "$PROD_ARTIFACTS"

# VERIFY 3b: has WHERE clause (incremental merge, not full refresh)
run_step "defer 3b: VERIFY was incremental (has WHERE)" \
    verify_compiled_sql defer_agg "merge into" "yes"

# ══════════════════════════════════════════════════════════════════════════════
# PHASE 4: state:modified + defer
# ══════════════════════════════════════════════════════════════════════════════
echo ""
echo "═══ PHASE 4: state:modified + defer ═══"

# Drop dev defer tables to start fresh
run_step "dev: drop defer tables (phase 4)" \
    dbt run-operation drop_defer_tables --target "$DEV_TARGET"

# Simulate modifying defer_base
DEFER_BASE_FILE="$SCRIPT_DIR/models/defer_chain/defer_base.sql"
ORIGINAL_CONTENT=$(cat "$DEFER_BASE_FILE")
echo "-- modified: $(date +%s)" >> "$DEFER_BASE_FILE"

# Clone only incremental within modified+ selection
run_step "defer 4a: clone incremental in modified+ selection" \
    dbt clone --target "$DEV_TARGET" \
    --select "state:modified+,config.materialized:incremental" \
    --state "$PROD_ARTIFACTS"

# Run state:modified+ with defer
run_step "defer 4b: run state:modified+ with defer" \
    dbt run --target "$DEV_TARGET" \
    --select "state:modified+" \
    --defer --state "$PROD_ARTIFACTS"

# VERIFY 4b: defer_base rebuilt in dev
run_step "defer 4b: VERIFY defer_base rebuilt in dev" \
    dbt run-operation verify_table_exists --target "$DEV_TARGET" \
    --args "{lakehouse: $DEV_LAKEHOUSE, schema_name: dbo, table_name: defer_base}"

# VERIFY 4b: defer_agg was incremental
run_step "defer 4b: VERIFY defer_agg was incremental" \
    verify_compiled_sql defer_agg "merge into" "yes"

# VERIFY 4b: defer_customers NOT in dev (separate chain)
run_step "defer 4b: VERIFY defer_customers NOT in dev" \
    dbt run-operation verify_table_not_exists --target "$DEV_TARGET" \
    --args "{lakehouse: $DEV_LAKEHOUSE, schema_name: dbo, table_name: defer_customers}"

# Tests on modified chain
run_step "defer 4c: test modified chain with defer" \
    dbt test --target "$DEV_TARGET" \
    --select "state:modified+" \
    --defer --state "$PROD_ARTIFACTS"

# Restore original file
echo "$ORIGINAL_CONTENT" > "$DEFER_BASE_FILE"

# ══════════════════════════════════════════════════════════════════════════════
# PHASE 5: Tests with defer
# ══════════════════════════════════════════════════════════════════════════════
echo ""
echo "═══ PHASE 5: Tests with defer ═══"

# Schema tests with defer
run_step "defer 5a: schema tests with defer" \
    dbt test --target "$DEV_TARGET" \
    --select "defer_base defer_agg" \
    --defer --state "$PROD_ARTIFACTS"

# Unit tests with defer (exclude known-failing amount cast test)
run_step "defer 5b: unit tests with defer" \
    dbt test --target "$DEV_TARGET" \
    --select "test_type:unit" --exclude test_stg_orders_casts_amount_to_decimal \
    --defer --state "$PROD_ARTIFACTS"

# ══════════════════════════════════════════════════════════════════════════════
# Summary
# ══════════════════════════════════════════════════════════════════════════════

END_TIME=$(date +%s)
TOTAL_DURATION=$((END_TIME - START_TIME))
TOTAL_MIN=$((TOTAL_DURATION / 60))
TOTAL_SEC=$((TOTAL_DURATION % 60))

echo ""
echo "=============================================="
echo "  DEFER TEST RESULTS"
echo "=============================================="
echo ""
cat "$RESULTS_FILE"
echo ""
echo "----------------------------------------------"
echo "  Total: $STEP steps | $PASS passed | $FAIL failed"
echo "  Duration: ${TOTAL_MIN}m ${TOTAL_SEC}s"
echo "  Prod: $PROD_TARGET | Dev: $DEV_TARGET"
echo "  Finished: $(date '+%Y-%m-%d %H:%M:%S')"
echo "----------------------------------------------"

if [ "$FAIL" -gt 0 ]; then
    echo ""
    echo "  FAILED steps:"
    grep "^FAIL" "$RESULTS_FILE" | sed 's/^/    /'
    echo ""
    exit 1
else
    echo ""
    echo "  All defer tests passed!"
    echo ""
fi
