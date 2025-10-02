#!/bin/bash
set -e

BASE="/Users/songsungkyu/my-ai/stock"
VENV="$BASE/.venv/bin/python"
PY="$BASE/collector_v1.py"
LOG="$BASE/collector.log"
LOCKDIR="$BASE/.collector_lock"

# 잠금: 이미 실행 중이면 종료
if mkdir "$LOCKDIR" 2>/dev/null; then
  trap 'rmdir "$LOCKDIR"' EXIT
else
  echo "$(date '+%F %T') another run detected, exiting." >> "$LOG"
  exit 0
fi

cd "$BASE"
echo "$(date '+%F %T') START collector" >> "$LOG"
"$VENV" "$PY" >> "$LOG" 2>&1
echo "$(date '+%F %T') END collector" >> "$LOG"
