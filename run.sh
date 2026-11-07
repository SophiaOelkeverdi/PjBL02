#!/usr/bin/env bash
# Run a PySpark script on the host, even when started from Flatpak VS Code.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
FILE="${1:-pjBL2.py}"
if [[ "$FILE" != /* ]]; then
  FILE="$ROOT/$FILE"
fi
shift || true

HOST_ENV=(
  JAVA_HOME=/usr/lib/jvm/java-21-openjdk
  SPARK_HOME=/usr/lib/spark
  PYTHONPATH=/usr/lib/spark/python:/usr/lib/spark/python/lib/py4j-0.10.9.9-src.zip
  PYSPARK_PYTHON=python3
  PYSPARK_DRIVER_PYTHON=python3
)

if [[ -f /.flatpak-info ]] && command -v flatpak-spawn >/dev/null; then
  exec flatpak-spawn --host env "${HOST_ENV[@]}" python3 "$FILE" "$@"
fi

export "${HOST_ENV[@]}"
exec python3 "$FILE" "$@"
