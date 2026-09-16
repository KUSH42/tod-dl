#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
exec python3 "$root_dir/src/tod-dl.py" \
    --destination "$root_dir/downloaded_files" \
    --state "$root_dir/download-state" \
    --queue "$root_dir/urls.txt" \
    "$@"
