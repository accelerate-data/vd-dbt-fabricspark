#!/usr/bin/env bash
set -uo pipefail
# NOTE: no `set -e` — we handle errors ourselves via run_step()

# ─── Integration test runner for vd-dbt-fabricspark ───────────────────────────
# Runs a complete dbt lifecycle against a real Fabric workspace.
#
# Usage:
#   ./run_integration_test.sh              # uses default 'dev' target
#   ./run_integration_test.sh --target prod
#   ./run_integration_test.sh --skip-elementary
#
# Prerequisites:
#   - az login (Azure CLI authenticated)
#   - source .venv/bin/activate
# ──────────────────────────────────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

TARGET="dev"
SKIP_ELEMENTARY=false

for arg in "$@"; do
    case "$arg" in
        --target) shift; TARGET="$1"; shift ;;
        --target=*) TARGET="${arg#*=}" ;;
        --skip-elementary) SKIP_ELEMENTARY=true ;;
    esac
done

export DBT_PROFILES_DIR="$SCRIPT_DIR"
RESULTS_FILE="$SCRIPT_DIR/target/integration_results.txt"
RESULTS_LOG="$SCRIPT_DIR/target/integration_results_full.log"
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
        # Show last 15 lines of output for quick diagnosis
        echo "  ── failure output (last 15 lines) ──"
        echo "$output" | tail -15 | sed 's/^/  | /'
        echo "  ── end ──"
        FAIL=$((FAIL + 1))
        echo "FAIL | [$STEP] $step_name (${duration}s, exit=$exit_code)" >> "$RESULTS_FILE"
        # Write full failure output to log
        {
            echo "============ FAIL | [$STEP] $step_name (${duration}s, exit=$exit_code) ============"
            echo "$output"
            echo ""
        } >> "$RESULTS_LOG"
    fi
}

echo "=============================================="
echo "  vd-dbt-fabricspark integration test"
echo "  Target: $TARGET"
echo "  Started: $(date '+%Y-%m-%d %H:%M:%S')"
echo "=============================================="

# ── Core pipeline ──

run_step "dbt debug" \
    dbt debug --target "$TARGET"

run_step "dbt deps" \
    dbt deps

run_step "dbt unit test" \
    dbt test --target "$TARGET" --select "test_type:unit"

run_step "dbt seed (full-refresh)" \
    dbt seed --target "$TARGET" --full-refresh

run_step "dbt run (full build)" \
    dbt run --target "$TARGET"

run_step "dbt test (schema tests)" \
    dbt test --target "$TARGET"

run_step "dbt run (incremental re-run)" \
    dbt run --target "$TARGET"

run_step "dbt test (post-incremental)" \
    dbt test --target "$TARGET"

# ── Elementary ──

if [ "$SKIP_ELEMENTARY" = false ]; then
    run_step "elementary: initial build" \
        dbt run --target "$TARGET" --select elementary

    run_step "elementary: verify tables exist" \
        dbt test --target "$TARGET" --select verify_elementary_tables_exist

    run_step "elementary: verify invocations" \
        dbt test --target "$TARGET" --select verify_elementary_invocations

    run_step "elementary: verify run results" \
        dbt test --target "$TARGET" --select verify_elementary_run_results

    # Drop entire elementary schema and rebuild from scratch
    run_step "elementary: drop schema" \
        dbt run-operation drop_elementary_schema --target "$TARGET"

    run_step "elementary: rebuild after drop" \
        dbt run --target "$TARGET" --select elementary

    run_step "elementary: verify tables recreated" \
        dbt test --target "$TARGET" --select verify_elementary_tables_exist
fi

# ── Summary ──

END_TIME=$(date +%s)
TOTAL_DURATION=$((END_TIME - START_TIME))
TOTAL_MIN=$((TOTAL_DURATION / 60))
TOTAL_SEC=$((TOTAL_DURATION % 60))

echo ""
echo "=============================================="
echo "  RESULTS SUMMARY"
echo "=============================================="
echo ""
cat "$RESULTS_FILE"
echo ""
echo "----------------------------------------------"
echo "  Total: $STEP steps | $PASS passed | $FAIL failed"
echo "  Duration: ${TOTAL_MIN}m ${TOTAL_SEC}s"
echo "  Target: $TARGET"
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
    echo "  All steps passed!"
    echo ""
fi
