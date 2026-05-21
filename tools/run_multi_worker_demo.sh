#!/bin/bash
# Multi-worker demo in tmux: 1 gateway + 3 workers (N=2 each) + 1 dispatcher
# submitting 12 work items. Watches 6 items get processed in parallel across
# the 3 workers' 2-slot semaphores.
#
# Robust against tmux base-index / pane-base-index settings (uses dynamic
# pane IDs captured at creation time rather than positional addressing).
set -e

SESSION="${SEEKNAL_DEMO_SESSION:-seeknal-demo}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORKER_N="${WORKER_N:-2}"          # per-worker concurrency
NUM_ITEMS="${NUM_ITEMS:-12}"       # total items to submit
NUM_WORKERS="${NUM_WORKERS:-3}"

tmux kill-session -t "$SESSION" 2>/dev/null || true

# Start session — first pane is the gateway. Capture its pane ID.
tmux new-session -d -s "$SESSION" -c "$ROOT" -x 220 -y 50 \
    "PYTHONPATH=src python tools/demo_gateway.py 2>&1"

# Helper: split the most recently created pane and capture the new pane ID.
# Returns the new pane ID on stdout.
new_pane() {
    local target_pane="$1"
    local direction="$2"   # -h or -v
    local cmd="$3"
    tmux split-window -P -F '#{pane_id}' "$direction" -t "$target_pane" \
        -c "$ROOT" "$cmd"
}

# Get the initial (gateway) pane ID.
GW_PANE=$(tmux list-panes -t "$SESSION" -F '#{pane_id}' | head -1)

# Right column: 3 workers stacked vertically (split off the gateway pane).
W1_PANE=$(new_pane "$GW_PANE" "-h" \
    "sleep 2 && PYTHONPATH=src python tools/demo_worker.py 1 $WORKER_N 2>&1")
W2_PANE=$(new_pane "$W1_PANE" "-v" \
    "sleep 2 && PYTHONPATH=src python tools/demo_worker.py 2 $WORKER_N 2>&1")
W3_PANE=$(new_pane "$W2_PANE" "-v" \
    "sleep 2 && PYTHONPATH=src python tools/demo_worker.py 3 $WORKER_N 2>&1")

# Left column bottom: dispatcher (splits gateway pane vertically).
DISP_PANE=$(new_pane "$GW_PANE" "-v" \
    "sleep 6 && PYTHONPATH=src python tools/demo_dispatcher.py $NUM_ITEMS 2>&1")

tmux select-layout -t "$SESSION" tiled

echo "Demo running in tmux session: $SESSION"
echo "  Gateway pane:    $GW_PANE"
echo "  Worker 1 pane:   $W1_PANE"
echo "  Worker 2 pane:   $W2_PANE"
echo "  Worker 3 pane:   $W3_PANE"
echo "  Dispatcher pane: $DISP_PANE"
echo
echo "Commands:"
echo "  tmux attach -t $SESSION                  # watch live"
echo "  tmux capture-pane -t $DISP_PANE -p       # capture dispatcher output"
echo "  tmux kill-session -t $SESSION            # stop"
