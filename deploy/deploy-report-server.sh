#!/usr/bin/env bash
# Deploy seeknal report server to remote host
set -euo pipefail

REMOTE_HOST="exedev@reportkami.exe.xyz"
REMOTE_DIR="/home/exedev/seeknal-report-server"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
SSH_OPTS="-o ConnectTimeout=30 -o StrictHostKeyChecking=no -o GSSAPIAuthentication=no -o ServerAliveInterval=10"

echo "=== Seeknal Report Server Deployment ==="
echo "  Target: $REMOTE_HOST:$REMOTE_DIR"
echo ""

# 1. Create remote directory structure
echo "[1/4] Creating remote directories..."
ssh $SSH_OPTS "$REMOTE_HOST" "mkdir -p $REMOTE_DIR"

# 2. Package and copy seeknal source
echo "[2/4] Packaging and copying seeknal source..."
cd "$REPO_ROOT"
tar czf /tmp/seeknal-report-src.tar.gz \
    --exclude='.git' \
    --exclude='__pycache__' \
    --exclude='.claude' \
    --exclude='node_modules' \
    --exclude='.venv' \
    --exclude='target' \
    --exclude='qa' \
    --exclude='.worktrees' \
    --exclude='.omc' \
    --exclude='state' \
    -C "$REPO_ROOT" .
scp $SSH_OPTS /tmp/seeknal-report-src.tar.gz "$REMOTE_HOST:$REMOTE_DIR/seeknal-report-src.tar.gz"
ssh $SSH_OPTS "$REMOTE_HOST" "mkdir -p $REMOTE_DIR/seeknal-src && tar xzf $REMOTE_DIR/seeknal-report-src.tar.gz -C $REMOTE_DIR/seeknal-src"

# 3. Copy deployment files
echo "[3/4] Copying deployment configs..."
scp $SSH_OPTS \
    "$SCRIPT_DIR/docker-compose.report-server.yml" \
    "$REMOTE_HOST:$REMOTE_DIR/docker-compose.yml"

# Copy .env if it exists locally
if [ -f "$SCRIPT_DIR/.env.report-server" ]; then
    echo "  Copying .env.report-server..."
    scp $SSH_OPTS "$SCRIPT_DIR/.env.report-server" "$REMOTE_HOST:$REMOTE_DIR/.env"
fi

# 4. Build and start services
echo "[4/4] Building and starting services..."
ssh $SSH_OPTS "$REMOTE_HOST" "cd $REMOTE_DIR && docker compose down --remove-orphans 2>/dev/null; docker compose up -d --build"

# Wait for health check
echo ""
echo "Waiting for health check..."
sleep 5
HEALTH=$(ssh $SSH_OPTS "$REMOTE_HOST" "curl -sf http://localhost:8787/healthz 2>/dev/null || echo 'UNREACHABLE'")
echo "  Health: $HEALTH"

echo ""
echo "=== Deployment Complete ==="
echo "  Report Server: http://reportkami.exe.xyz:8787"
echo "  Health Check:  http://reportkami.exe.xyz:8787/healthz"
echo "  Reports at:    http://reportkami.exe.xyz:8787/r/{slug}"
echo ""
echo "To publish reports, set:"
echo "  export SEEKNAL_PUBLISH_SERVER=http://reportkami.exe.xyz:8787"
echo ""
