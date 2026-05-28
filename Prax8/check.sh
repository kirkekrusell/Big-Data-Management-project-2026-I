#!/usr/bin/env bash
# Self-check for Maria's PRs.
#
# Runs each phase's test file inside airflow-scheduler and prints PASS / FAIL /
# NOT ATTEMPTED. On FAIL, points the participant at the first hint; on
# NOT ATTEMPTED, tells them which file to start in.
set -u

GREEN="\033[0;32m"
RED="\033[0;31m"
YELLOW="\033[0;33m"
DIM="\033[2m"
BOLD="\033[1m"
RESET="\033[0m"

PHASE1_LABEL="Phase 1 (Factory — orders + events)"
PHASE2_LABEL="Phase 2 (Stateful merge)"
PHASE3_LABEL="Phase 3 (Circuit breaker + Slack)"

# How many of the three phases passed.
COMPLETED=0

_pad() {
    # Left-align `$1`, pad with dots to column 56 so the status sticks at the
    # same place across all three lines.
    local label="$1"
    printf "  %s " "$label"
    local len=${#label}
    local target=54
    while [ $len -lt $target ]; do
        printf "."
        len=$((len + 1))
    done
    printf " "
}

# Phase status detector. Echoes one of: NOT_ATTEMPTED PASS FAIL.
phase_status() {
    local phase="$1"
    case "$phase" in
        1)
            # Phase 1 = YAML expanded to 3 tables. Use the config loader to
            # be tolerant of whitespace / comment changes.
            local count
            count=$(docker compose exec -T airflow-scheduler python -c "
from internal_etl_package.config_loader import load_tables
print(len(load_tables('/opt/airflow/config/tables.yaml')))
" 2>/dev/null | tr -d '[:space:]')
            if [ "$count" = "1" ]; then
                echo NOT_ATTEMPTED
                return
            fi
            ;;
        2)
            if docker compose exec -T airflow-scheduler bash -c \
                'grep -q "# TODO" /opt/airflow/internal_etl_package/merger.py' 2>/dev/null; then
                echo NOT_ATTEMPTED
                return
            fi
            ;;
        3)
            if docker compose exec -T airflow-scheduler bash -c \
                'grep -q "# TODO" /opt/airflow/internal_etl_package/quality.py' 2>/dev/null; then
                echo NOT_ATTEMPTED
                return
            fi
            ;;
    esac

    # Phase has been "attempted" — run the matching test file.
    if docker compose exec -T airflow-scheduler bash -c \
        "cd /opt/airflow && pytest tests/test_phase${phase}_*.py -q --no-header" \
        >/dev/null 2>&1; then
        echo PASS
    else
        echo FAIL
    fi
}

print_phase() {
    local label="$1"
    local status="$2"
    local hint_file="$3"
    local short_hint="$4"

    _pad "$label"
    case "$status" in
        PASS)
            printf "${GREEN}✅ PASS${RESET}\n"
            COMPLETED=$((COMPLETED + 1))
            ;;
        FAIL)
            printf "${RED}❌ FAIL${RESET}\n"
            printf "      ${DIM}Hint: %s${RESET}\n" "$short_hint"
            printf "      ${DIM}Run \`make ${hint_file}\` for more.${RESET}\n"
            ;;
        NOT_ATTEMPTED)
            printf "${YELLOW}⏸  NOT ATTEMPTED${RESET}\n"
            printf "      ${DIM}Run \`make ${hint_file}\` for a starting nudge.${RESET}\n"
            ;;
    esac
}

printf "\n${BOLD}🔍 Checking your progress on Maria's PRs...${RESET}\n\n"

s1=$(phase_status 1)
s2=$(phase_status 2)
s3=$(phase_status 3)

print_phase "$PHASE1_LABEL" "$s1" "hint-phase1-1" \
    "Did you add prd.orders and prd.events to config/tables.yaml?"
print_phase "$PHASE2_LABEL" "$s2" "hint-phase2-1" \
    "Did you replace the \`None\` in merger.py with a Ledger lookup?"
print_phase "$PHASE3_LABEL" "$s3" "hint-phase3-1" \
    "Does post_audit count duplicates and raise on > 0?"

printf "\n"
if [ "$COMPLETED" -eq 3 ]; then
    printf "${GREEN}All three phases green. 💚 You're done.${RESET}\n\n"
else
    printf "${BOLD}You have completed %d of 3 phases.${RESET} 💚\n\n" "$COMPLETED"
fi
