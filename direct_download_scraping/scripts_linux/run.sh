#!/bin/bash
# Run the Docker container
# Usage: ./run.sh [output_path] [logs_path]

OUTPUT_PATH=${1:-./.yearbook_downloads}
LOGS_PATH=${2:-./.scraper_logs}
IMAGE=${3:-yearbook-scraper:latest}

# Create directories if they don't exist
mkdir -p "$OUTPUT_PATH"
mkdir -p "$LOGS_PATH"

# Get absolute paths
OUTPUT_ABS=$(cd "$OUTPUT_PATH" && pwd)
LOGS_ABS=$(cd "$LOGS_PATH" && pwd)

echo "Running Docker container..."
echo "Output folder: $OUTPUT_ABS"
echo "Logs folder: $LOGS_ABS"
echo ""

docker run \
    --name yearbook-scraper-run \
    -v "$OUTPUT_ABS:/app/yearbook_downloads" \
    -v "$LOGS_ABS:/app/scraper_logs" \
    --rm \
    "$IMAGE"

if [ $? -eq 0 ]; then
    echo ""
    echo "✓ Scraper completed successfully!"
    echo "Check output at: $OUTPUT_ABS"
    echo "Check logs at: $LOGS_ABS"
else
    echo ""
    echo "✗ Scraper failed. Check logs at: $LOGS_ABS"
    exit 1
fi
