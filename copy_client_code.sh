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

# Frontend: add entire client-ui-naive tree excluding lib folders
add_dir "client-ui-naive" "All client-ui-naive source files (excluding lib folders)"

# From project root: include .env and Docker files if present
shopt -s nullglob
ROOT_DOCKER_FILES=( ".env" "Dockerfile" "docker-compose.yml" "docker-compose.yaml" "docker-compose.prod.yml" )
for rf in "${ROOT_DOCKER_FILES[@]}" Dockerfile.* docker-compose.*; do
    if [ -f "$rf" ]; then
        add_file "$rf" "Root project file: $rf"
    fi
done
shopt -u nullglob || true

# Include backend api_gateway files
add_dir "/root/projects/SelfTrading Analytics/backend/api_gateway" "Backend api_gateway files"

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


