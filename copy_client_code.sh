#!/bin/bash

# Create a file to hold client code + logs
OUT_FILE="client_project_code.txt"
> "$OUT_FILE"

echo "📁 Collecting CLIENT (frontend) code and last 50 lines of logs..."

add_file() {
    local file_path="$1"
    local description="$2"
    if [ -f "$file_path" ]; then
        echo "Adding: $description"
        echo "" >> "$OUT_FILE"
        echo "=" >> "$OUT_FILE"
        echo "FILE: $file_path" >> "$OUT_FILE"
        echo "DESCRIPTION: $description" >> "$OUT_FILE"
        echo "=" >> "$OUT_FILE"
        cat "$file_path" >> "$OUT_FILE"
        echo "" >> "$OUT_FILE"
    else
        echo "Warning: $file_path not found"
    fi
}

# Frontend files
add_file "client-ui-naive/src/App.vue" "Main Vue application component"
add_file "client-ui-naive/src/main.js" "Vue application entry point"
add_file "client-ui-naive/src/router/index.js" "Vue router configuration"
add_file "client-ui-naive/src/components/tabs/ProgressTab.vue" "Progress tab component"
add_file "client-ui-naive/src/components/tabs/ResultsTab.vue" "Results tab component"

# Include last 50 lines of client logs
shopt -s nullglob
LOG_DIRS=("./logs" "/root/projects/SelfTrading Analytics/logs" "/app/logs")
for d in "${LOG_DIRS[@]}"; do
    if [ -d "$d" ]; then
        for f in "$d"/*-strategy.log "$d"/basic-strategy.log "$d"/chatgpt-5-strategy.log; do
            if [ -f "$f" ]; then
                echo "Adding log: $f"
                echo "" >> "$OUT_FILE"
                echo "=" >> "$OUT_FILE"
                echo "FILE: $f" >> "$OUT_FILE"
                echo "DESCRIPTION: Last 100 lines of frontend/strategy log" >> "$OUT_FILE"
                echo "=" >> "$OUT_FILE"
                tail -n 50 "$f" >> "$OUT_FILE"
                echo "" >> "$OUT_FILE"
            fi
        done
    fi
done
shopt -u nullglob || true

echo "✅ Client bundle saved to: $OUT_FILE"


