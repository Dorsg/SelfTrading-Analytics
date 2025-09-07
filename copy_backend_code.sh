#!/bin/bash

set -euo pipefail

# Script: copy_backend_code.sh
# Purpose: Collect backend project tree and source files (excluding libs/vendor/venvs)
#          and append the last 100 lines of each log file under /logs.

OUT_FILE="backend_project_code.txt"
> "$OUT_FILE"

PROJECT_ROOT="$(pwd)"

echo "📁 Collecting BACKEND code and logs (last 50 lines, 300 for error/warn; excluding lib/vendor/venv)..."

# small helper: print separator header into output
print_sep() {
    printf "\n===== %s =====\n\n" "$1" >> "$OUT_FILE"
}

# --- Write logs first (unique files, last 50 lines) ---
shopt -s nullglob
print_sep "LOGS (last 50 lines each)"
declare -A seen
LOG_DIRS=("$PROJECT_ROOT/logs" "/logs" "./logs")
for d in "${LOG_DIRS[@]}"; do
    if [ -d "$d" ]; then
        echo "Scanning logs in: $d"
        while IFS= read -r -d '' logfile; do
            # resolve to absolute path when possible to avoid duplicates
            rp="$(realpath "$logfile" 2>/dev/null || printf '%s' "$logfile")"
            if [ -z "${seen[$rp]:-}" ]; then
                seen[$rp]=1
                print_sep "$rp"
                printf "FILE: %s\n\n" "$rp" >> "$OUT_FILE"
                basef="$(basename "$rp" | tr 'A-Z' 'a-z')"
                # default lines to tail
                lines=50
                # if filename suggests errors or warnings, increase to 300
                case "$basef" in
                    *error*|*errors*|*warn*|*warning*) lines=300 ;;
                esac
                if [ "$lines" -eq 300 ]; then
                    head -n 300 "$rp" >> "$OUT_FILE" || true
                else
                    tail -n "$lines" "$rp" >> "$OUT_FILE" || true
                fi
                printf "\n" >> "$OUT_FILE"
            fi
        done < <(find "$d" -type f -print0)
    fi
done
shopt -u nullglob || true

add_file() {
    local file_path="$1"
    local description="$2"
    if [ -f "$file_path" ]; then
        # resolve real path to avoid duplicates with logs
        rp="$(realpath "$file_path" 2>/dev/null || printf '%s' "$file_path")"
        if [ -n "${seen[$rp]:-}" ]; then
            echo "Skipping already-included file: $rp"
            return
        fi
        echo "Adding: $file_path"
        print_sep "$file_path"
        printf "DESCRIPTION: %s\n\n" "$description" >> "$OUT_FILE"
        cat "$file_path" >> "$OUT_FILE"
        printf "\n" >> "$OUT_FILE"
    else
        echo "Warning: $file_path not found"
    fi
}

# Only include files under backend, logs (already processed), and top-level .env
# (omit printing a project tree and exclude scripts, Dockerfiles, and other top-level files)

# Collect source files under backend only (excluding vendor/lib/venv)
# Note: api_gateway is included (not pruned)
while IFS= read -r -d '' file; do
    add_file "$file" "Source file"
done < <(find backend \
    -path "*/lib" -prune -o -path "*/lib/*" -prune -o \
    -path "*/vendor" -prune -o -path "*/vendor/*" -prune -o \
    -path "*/node_modules" -prune -o -path "*/node_modules/*" -prune -o \
    -path "*/.venv" -prune -o -path "*/.venv/*" -prune -o \
    -path "*/venv" -prune -o -path "*/venv/*" -prune -o \
    -path "*/__pycache__" -prune -o -path "*/__pycache__/*" -prune -o \
    -path "*/strategies" -prune -o -path "*/strategies/*" -prune -o \
    -type f -print0 2>/dev/null)

# Also include only top-level .env if present
TOP_FILES=(".env")
for tf in "${TOP_FILES[@]}"; do
    if [ -f "$PROJECT_ROOT/$tf" ]; then
        add_file "$PROJECT_ROOT/$tf" "Top-level project file"
    fi
done


echo "✅ Backend bundle saved to: $OUT_FILE"

# Usage instructions
if [ "${BASH_SOURCE[0]}" = "$0" ]; then
    echo "\nUsage: $0"
    echo "Run this script from the project root. It will write to: $OUT_FILE"
fi


