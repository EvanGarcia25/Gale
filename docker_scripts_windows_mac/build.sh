#!/bin/bash
# Build the Docker image (macOS version)
# Usage: ./build.sh [tag]

TAG=${1:-yearbook-scraper:latest}

echo "Building Docker image: $TAG"
docker build -t $TAG .

if [ $? -eq 0 ]; then
    echo "✓ Build successful!"
    echo ""
    echo "To run the scraper:"
    echo "  docker run -v /path/to/output:/app/yearbook_downloads -v /path/to/logs:/app/scraper_logs $TAG"
else
    echo "✗ Build failed"
    exit 1
fi
