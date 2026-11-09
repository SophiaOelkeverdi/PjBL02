#!/usr/bin/env bash
# Launch a PySpark script on Linux and macOS, from any terminal or IDE.
# Inside Flatpak, run on the host so Java and Spark are visible.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
FILE="${1:-pjbl02_spark.py}"
if [[ "$FILE" != /* ]]; then
  FILE="$ROOT/$FILE"
fi
shift || true

if [[ -f /.flatpak-info ]] && command -v flatpak-spawn >/dev/null; then
  exec flatpak-spawn --host python3 "$FILE" "$@"
fi

if [[ -x "$ROOT/.venv/bin/python" ]]; then
  PYTHON="$ROOT/.venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
  PYTHON="$(command -v python3)"
else
  PYTHON="$(command -v python)"
fi

exec "$PYTHON" "$FILE" "$@"
