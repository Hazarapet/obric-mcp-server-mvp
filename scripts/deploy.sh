#!/bin/bash
# Deployment script for cloud deployment

set -e

# Check if remote server IP is provided
if [ -z "$1" ]; then
    echo "Error: Remote server IP is required"
    echo "Usage: $0 <remote_server_ip> [remote_user]"
    exit 1
fi

REMOTE_IP="$1"
REMOTE_USER="root"  # Use second argument or default to current user
REMOTE_PATH="development/obric-mcp-server-mvp"

echo "Deploying Obric MCP Server to $REMOTE_USER@$REMOTE_IP..."

# Get the project root directory (parent of scripts/)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
SRC_DIR="$PROJECT_ROOT/src"
REQ_FILE="$PROJECT_ROOT/requirements.txt"

# Sync the src folder to the remote server, excluding Python cache files
echo "Syncing src folder to $REMOTE_USER@$REMOTE_IP:$REMOTE_PATH/ (excluding __pycache__ and *.pyc)..."
rsync -avz \
    --exclude="__pycache__" \
    --exclude="*.pyc" \
    --exclude="*.pyo" \
    "$SRC_DIR/" "$REMOTE_USER@$REMOTE_IP:$REMOTE_PATH/src/"

echo ""

# Copy requirements.txt as well
if [ -f "$REQ_FILE" ]; then
    echo "Copying requirements.txt to $REMOTE_USER@$REMOTE_IP:$REMOTE_PATH/"
    scp "$REQ_FILE" "$REMOTE_USER@$REMOTE_IP:$REMOTE_PATH/"
else
    echo "Warning: requirements.txt not found at $REQ_FILE"
fi

echo "\nDeployment complete!"

