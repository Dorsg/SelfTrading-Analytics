#!/bin/bash

# Create a file in the root project directory to hold all the code
TEMP_FILE="project_code.txt"

# Clear the file
> "$TEMP_FILE"

echo "📁 Copying SelfTrading Analytics project code to clipboard..."
echo ""

# Function to add a file to the collection
add_file() {
    local file_path="$1"
    local description="$2"
    
    if [ -f "$file_path" ]; then
        echo "Adding: $description"
        echo "" >> "$TEMP_FILE"
        echo "=" >> "$TEMP_FILE"
        echo "FILE: $file_path" >> "$TEMP_FILE"
        echo "DESCRIPTION: $description" >> "$TEMP_FILE"
        echo "=" >> "$TEMP_FILE"
        cat "$file_path" >> "$TEMP_FILE"
        echo "" >> "$TEMP_FILE"
    else
        echo "Warning: $file_path not found"
    fi
}

# Backend API Routes
add_file "backend/api_gateway/routes/analytics_routes.py" "Main analytics API routes with progress, simulation, and logs endpoints"

# Frontend Components
add_file "client-ui-naive/src/components/tabs/ProgressTab.vue" "Progress tab component with simulation controls and auto-advance"
add_file "client-ui-naive/src/components/tabs/ResultsTab.vue" "Results tab component with monthly summary and partial results"

# Simulation Core
add_file "backend/analytics/sim_scheduler.py" "Simulation scheduler that processes historical data"
add_file "backend/analytics/mock_broker.py" "Mock broker for simulation (if exists)"

# Database Models (only the models, not data)
add_file "database/models.py" "Database models for analytics tables"

# Docker Configuration
add_file "docker-compose.yml" "Production Docker Compose configuration"
add_file "docker-compose.dev.yml" "Development Docker Compose configuration"
add_file "Dockerfile.scheduler" "Scheduler Dockerfile"

# Main Application Files
add_file "client-ui-naive/src/App.vue" "Main Vue application component"
add_file "client-ui-naive/src/main.js" "Vue application entry point"
add_file "client-ui-naive/src/router/index.js" "Vue router configuration"

# Configuration and Environment
add_file ".env.example" "Environment variables example (if exists)"
add_file "requirements.txt" "Python dependencies"

# Add a summary header
{
    echo "# SelfTrading Analytics - Complete Project Code"
    echo ""
    echo "## Project Structure:"
    echo "- Backend: FastAPI with analytics simulation"
    echo "- Frontend: Vue.js with naive-ui components"
    echo "- Database: PostgreSQL with historical data"
    echo "- Simulation: Historical data processing with mock broker"
    echo ""
    echo "## Key Features:"
    echo "- Historical data simulation"
    echo "- Strategy backtesting"
    echo "- Real-time progress tracking"
    echo "- Auto-advance simulation"
    echo "- Results aggregation"
    echo ""
    echo "## Files Included:"
    echo "- API routes for simulation control"
    echo "- Frontend components for progress and results"
    echo "- Simulation scheduler and mock broker"
    echo "- Database models"
    echo "- Docker configuration"
    echo "- Application entry points"
    echo ""
    echo "## Note:"
    echo "This is the complete codebase excluding:"
    echo "- Library files (node_modules, __pycache__)"
    echo "- Database data files"
    echo "- Log files"
    echo "- Configuration files with secrets"
    echo ""
    echo "Generated: $(date)"
    echo ""
} | cat - "$TEMP_FILE" > "$TEMP_FILE.tmp" && mv "$TEMP_FILE.tmp" "$TEMP_FILE"

# Try to copy to clipboard
if command -v xclip >/dev/null 2>&1; then
    cat "$TEMP_FILE" | xclip -selection clipboard
    echo "✅ Project code copied to clipboard using xclip"
elif command -v pbcopy >/dev/null 2>&1; then
    cat "$TEMP_FILE" | pbcopy
    echo "✅ Project code copied to clipboard using pbcopy"
else
    echo "❌ No clipboard tool found"
    echo "📄 Content saved to: $TEMP_FILE"
    echo "📋 Please copy manually:"
    echo ""
    echo "File size: $(du -h "$TEMP_FILE" | cut -f1)"
    echo "Lines: $(wc -l < "$TEMP_FILE")"
    echo ""
    echo "To copy manually, run: cat $TEMP_FILE"
    echo "Or open the file: $TEMP_FILE"
fi

echo ""
echo "🎯 All relevant project code has been collected!"
echo "📁 Excluded: libraries, database files, logs, and configuration secrets"
echo "📄 File saved to: $TEMP_FILE in the project root"
