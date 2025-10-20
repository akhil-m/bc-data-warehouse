#!/bin/bash
set -e

# Usage: ./run-docker.sh [LIMIT]
# Example: ./run-docker.sh 5    # Test with 5 datasets
#          ./run-docker.sh       # Process all datasets

if [ "$1" = "--help" ] || [ "$1" = "-h" ]; then
    echo "Usage: ./run-docker.sh [LIMIT]"
    echo ""
    echo "Rebuilds Docker image and runs StatsCan pipeline"
    echo ""
    echo "Options:"
    echo "  LIMIT    Number of datasets to process (optional)"
    echo ""
    echo "Examples:"
    echo "  ./run-docker.sh 5     # Test with 5 datasets"
    echo "  ./run-docker.sh       # Process all datasets"
    exit 0
fi

LIMIT=${1:-}

echo "🔨 Rebuilding Docker image..."
docker compose -f docker/docker-compose.yml build

echo ""
echo "🚀 Starting pipeline..."
if [ -n "$LIMIT" ]; then
    echo "   LIMIT: $LIMIT datasets"
    LIMIT=$LIMIT docker compose -f docker/docker-compose.yml up
else
    echo "   LIMIT: (all datasets)"
    docker compose -f docker/docker-compose.yml up
fi
