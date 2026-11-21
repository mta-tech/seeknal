#!/usr/bin/env bash
# Deploy seeknal report server to remote host
# NOTE: Report server listens on plain HTTP; API keys travel over plain HTTP unless TLS is terminated in front of the server (e.g. by a TLS reverse proxy or gateway).
set -euo pipefail

REMOTE_HOST="exedev@reportkami.exe.xyz"
REMOTE_DIR="/home/exedev/seeknal-report-server"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
SSH_OPTS="-o ConnectTimeout=30 -o StrictHostKeyChecking=accept-new -o GSSAPIAuthentication=no -o ServerAliveInterval=10"

echo "=== Seeknal Report Server Deployment ==="
echo "  Target: $REMOTE_HOST:$REMOTE_DIR"
echo ""

ENV_FILE="$SCRIPT_DIR/.env.report-server"
if [ ! -f "$ENV_FILE" ]; then
    echo "ERROR: $ENV_FILE not found. Configure SEEKNAL_REPORT_SERVER_KEYS (or SEEKNAL_REPORT_SERVER_AUTH_MODE=open) in deploy/.env.report-server before deploying." >&2
    exit 1
fi

read_env_var() {
    local var_name="$1"
    local val
    val="$(grep -E "^[[:space:]]*(export[[:space:]]+)?${var_name}=" "$ENV_FILE" | tail -n 1 | sed -E "s/^[[:space:]]*(export[[:space:]]+)?${var_name}=//" || true)"
    val="${val%\"}"
    val="${val#\"}"
    val="${val%\'}"
    val="${val#\'}"
    printf '%s' "$val"
}

AUTH_MODE="$(read_env_var SEEKNAL_REPORT_SERVER_AUTH_MODE)"
AUTH_MODE="${AUTH_MODE:-api_key}"
RAW_KEYS="$(read_env_var SEEKNAL_REPORT_SERVER_KEYS)"
BIND_ADDRESS="$(read_env_var SEEKNAL_REPORT_SERVER_BIND_ADDRESS)"
BIND_ADDRESS="${BIND_ADDRESS:-127.0.0.1}"

if [ "$AUTH_MODE" = "open" ]; then
    echo "WARNING: SEEKNAL_REPORT_SERVER_AUTH_MODE=open is set in deploy/.env.report-server; deploying without API-key authentication." >&2
elif [ -z "$RAW_KEYS" ] || [ -z "$(printf '%s' "$RAW_KEYS" | tr -d '[:space:],')" ]; then
    echo "ERROR: SEEKNAL_REPORT_SERVER_KEYS must be set in deploy/.env.report-server before deploying (or set SEEKNAL_REPORT_SERVER_AUTH_MODE=open explicitly for unauthenticated mode)." >&2
    exit 1
fi

# 1. Create remote directory structure
echo "[1/4] Creating remote directories..."
ssh $SSH_OPTS "$REMOTE_HOST" "mkdir -p $REMOTE_DIR"

# 2. Package and copy seeknal source
echo "[2/4] Packaging and copying seeknal source..."
cd "$REPO_ROOT"
TMP_BUNDLE="$(mktemp "${TMPDIR:-/tmp}/seeknal-report-src.XXXXXX")"
trap 'rm -f "$TMP_BUNDLE"' EXIT
tar czf "$TMP_BUNDLE" \
    --exclude='.git' \
    --exclude='.env' \
    --exclude='.env.*' \
    --exclude='deploy/.env.report-server' \
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
scp $SSH_OPTS "$TMP_BUNDLE" "$REMOTE_HOST:$REMOTE_DIR/seeknal-report-src.tar.gz"
ssh $SSH_OPTS "$REMOTE_HOST" "rm -rf $REMOTE_DIR/seeknal-src && mkdir -p $REMOTE_DIR/seeknal-src && tar xzf $REMOTE_DIR/seeknal-report-src.tar.gz -C $REMOTE_DIR/seeknal-src && rm -f $REMOTE_DIR/seeknal-src/deploy/.env.report-server"

# 3. Copy deployment files
echo "[3/4] Copying deployment configs..."
scp $SSH_OPTS \
    "$SCRIPT_DIR/docker-compose.report-server.yml" \
    "$REMOTE_HOST:$REMOTE_DIR/docker-compose.yml"

echo "  Copying .env.report-server..."
ssh $SSH_OPTS "$REMOTE_HOST" "umask 077; cat > $REMOTE_DIR/.env" < "$ENV_FILE"

# 4. Build and start services
echo "[4/4] Building and starting services..."
ssh $SSH_OPTS "$REMOTE_HOST" "cd $REMOTE_DIR && docker compose down --remove-orphans 2>/dev/null; docker compose up -d --build"

# Poll /healthz with a bounded timeout (30 x 2s = 60s)
echo ""
echo "Waiting for health check..."
HEALTH="UNREACHABLE"
for _attempt in $(seq 1 30); do
    HEALTH=$(ssh $SSH_OPTS "$REMOTE_HOST" "curl -sf http://localhost:8787/healthz 2>/dev/null || true")
    if [ -n "$HEALTH" ]; then
        break
    fi
    sleep 2
done

if [ -z "$HEALTH" ] || [ "$HEALTH" = "UNREACHABLE" ]; then
    echo "ERROR: Report server failed health check at http://localhost:8787/healthz after 60 seconds." >&2
    exit 1
fi
echo "  Health: $HEALTH"

echo ""
echo "=== Deployment Complete ==="
if [ "$BIND_ADDRESS" = "127.0.0.1" ] || [ "$BIND_ADDRESS" = "localhost" ]; then
    echo "  Bind Address:  $BIND_ADDRESS:8787 (loopback-only; set SEEKNAL_REPORT_SERVER_BIND_ADDRESS in deploy/.env.report-server to expose externally)"
    echo "  Report Server: http://127.0.0.1:8787 (on $REMOTE_HOST)"
    echo "  Health Check:  http://127.0.0.1:8787/healthz"
    echo "  Reports at:    http://127.0.0.1:8787/r/{slug}"
else
    echo "  Bind Address:  $BIND_ADDRESS:8787"
    echo "  Report Server: http://reportkami.exe.xyz:8787"
    echo "  Health Check:  http://reportkami.exe.xyz:8787/healthz"
    echo "  Reports at:    http://reportkami.exe.xyz:8787/r/{slug}"
    echo ""
    echo "To publish reports, set:"
    echo "  export SEEKNAL_PUBLISH_SERVER=http://reportkami.exe.xyz:8787"
fi
echo ""
