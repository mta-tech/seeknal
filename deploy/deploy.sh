#!/usr/bin/env bash
# Deploy seeknal temporal gateway + chat UI to remote host
set -euo pipefail

REMOTE_HOST="fitra@172.19.0.9"
REMOTE_DIR="/home/fitra/seeknal-temporal"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
SSH_OPTS="-o ConnectTimeout=30 -o StrictHostKeyChecking=no -o GSSAPIAuthentication=no -o ServerAliveInterval=10"

echo "=== Seeknal Temporal Gateway Deployment ==="

# 1. Create remote directory structure
echo "[1/5] Creating remote directories..."
ssh $SSH_OPTS "$REMOTE_HOST" "mkdir -p $REMOTE_DIR/{static,ecommerce}"

# 2. Copy seeknal source (tarball for speed)
echo "[2/5] Packaging and copying seeknal source..."
cd "$REPO_ROOT"
tar czf /tmp/seeknal-src.tar.gz \
    --exclude='.git' \
    --exclude='__pycache__' \
    --exclude='.claude' \
    --exclude='node_modules' \
    --exclude='.venv' \
    --exclude='target' \
    --exclude='qa' \
    --exclude='.worktrees' \
    -C "$REPO_ROOT" .
scp $SSH_OPTS /tmp/seeknal-src.tar.gz "$REMOTE_HOST:$REMOTE_DIR/seeknal-src.tar.gz"
ssh $SSH_OPTS "$REMOTE_HOST" "mkdir -p $REMOTE_DIR/seeknal-src && tar xzf $REMOTE_DIR/seeknal-src.tar.gz -C $REMOTE_DIR/seeknal-src"

# 3. Copy ecommerce project
echo "[3/5] Copying ecommerce project..."
scp -r $SSH_OPTS "$REPO_ROOT/qa/runs/ecommerce/." "$REMOTE_HOST:$REMOTE_DIR/ecommerce/"

# 4. Copy deployment files
echo "[4/5] Copying deployment configs..."
scp $SSH_OPTS \
    "$SCRIPT_DIR/docker-compose.yml" \
    "$SCRIPT_DIR/docker-compose.worker.yml" \
    "$SCRIPT_DIR/nginx.conf" \
    "$REMOTE_HOST:$REMOTE_DIR/"

# Copy .env from ecommerce project
scp $SSH_OPTS "$REPO_ROOT/qa/runs/ecommerce/.env" "$REMOTE_HOST:$REMOTE_DIR/.env"

# Copy chat UI
scp $SSH_OPTS "$REPO_ROOT/src/seeknal/ask/gateway/static/chat.html" "$REMOTE_HOST:$REMOTE_DIR/static/"

# 5. Build and start services
echo "[5/5] Building and starting services..."
ssh $SSH_OPTS "$REMOTE_HOST" "cd $REMOTE_DIR && docker compose down --remove-orphans 2>/dev/null; SEEKNAL_BUILD_CONTEXT=./seeknal-src docker compose up -d --build"

echo ""
echo "=== Deployment Complete ==="
echo "  Chat UI:      http://172.19.0.9:3000"
echo "  Gateway API:  http://172.19.0.9:8000"
echo "  Temporal UI:  http://172.19.0.9:8233"
echo ""
echo "Test: curl http://172.19.0.9:8000/health"
