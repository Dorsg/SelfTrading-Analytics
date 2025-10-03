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

# Helper: add all files in a directory (exclude any 'lib' folders)
add_dir() {
    local dir_path="$1"
    local description="$2"
    if [ -d "$dir_path" ]; then
        echo "Adding files from: $dir_path"
        # find files excluding any path that contains /lib/
        while IFS= read -r -d '' file; do
            echo "" >> "$OUT_FILE"
            echo "=" >> "$OUT_FILE"
            echo "FILE: $file" >> "$OUT_FILE"
            echo "DESCRIPTION: $description" >> "$OUT_FILE"
            echo "=" >> "$OUT_FILE"
            cat "$file" >> "$OUT_FILE"
            echo "" >> "$OUT_FILE"
        done < <(find "$dir_path" -type f ! -path "*/lib/*" -print0)
    else
        echo "Warning: directory $dir_path not found"
    fi
}

# Frontend: add a short curated list of files useful for debugging
CLIENT_FILES=(
    "client-ui-naive/src/main.js"
    "client-ui-naive/src/App.vue"
    "client-ui-naive/src/router/index.js"
    "client-ui-naive/src/pages/Dashboard.vue"
    "client-ui-naive/src/views/SimulationView.vue"
    "client-ui-naive/src/views/ResultsView.vue"
    "client-ui-naive/src/components/SimulationControls.vue"
    "client-ui-naive/src/components/ImportStatus.vue"
    "client-ui-naive/src/components/LogsPanel.vue"
    "client-ui-naive/src/components/ResultsBreakdown.vue"
    "client-ui-naive/src/components/BestStocksTable.vue"
    "client-ui-naive/src/components/tabs/ProgressTab.vue"
    "client-ui-naive/src/components/tabs/ResultsTab.vue"
    "client-ui-naive/src/stores/simulation.js"
    "client-ui-naive/src/stores/results.js"
    "client-ui-naive/src/stores/runners.js"
    "client-ui-naive/src/services/api.js"
    "client-ui-naive/src/services/dataManager.js"
    "client-ui-naive/Dockerfile.client-ui-naive"
    "client-ui-naive/nginx.conf"
)

for cf in "${CLIENT_FILES[@]}"; do
    add_file "$cf" "Client file: $cf"
done

# From project root: include a short set of root files (env + Docker files)
shopt -s nullglob
ROOT_FILES=( ".env" "Dockerfile" "Dockerfile.client-ui-naive" "docker-compose.yml" "docker-compose.yaml" "docker-compose.prod.yml" )
for rf in "${ROOT_FILES[@]}"; do
    if [ -f "$rf" ]; then
        add_file "$rf" "Root project file: $rf"
    fi
done
shopt -u nullglob || true

# Include a small selection of backend/api_gateway files (only root python files)
API_GATEWAY_DIR="/root/projects/SelfTrading Analytics/backend/api_gateway"
if [ -d "$API_GATEWAY_DIR" ]; then
    while IFS= read -r -d '' apif; do
        add_file "$apif" "API gateway file: $apif"
    done < <(find "$API_GATEWAY_DIR" -maxdepth 1 -type f -name '*.py' -print0)
fi

# Include a short set of important logs (trimmed to last 50 lines)
shopt -s nullglob
LOG_FILES=("logs/errors_warnings.log" "logs/app.log" "logs/api_gateway.log" "logs/runner-service.log" "logs/chatgpt-5-strategy.log")
for lf in "${LOG_FILES[@]}"; do
    # try multiple possible locations
    for p in "$lf" "/root/projects/SelfTrading Analytics/$lf" "/app/$lf"; do
        if [ -f "$p" ]; then
            echo "Adding log: $p"
            echo "" >> "$OUT_FILE"
            echo "=" >> "$OUT_FILE"
            echo "FILE: $p" >> "$OUT_FILE"
            echo "DESCRIPTION: Last 50 lines of important log" >> "$OUT_FILE"
            echo "=" >> "$OUT_FILE"
            tail -n 50 "$p" >> "$OUT_FILE"
            echo "" >> "$OUT_FILE"
            break
        fi
    done
done
shopt -u nullglob || true

echo "✅ Client bundle saved to: $OUT_FILE"


