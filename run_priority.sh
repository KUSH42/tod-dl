#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
exec python3 "$root_dir/download_priority.py" \
    --destination "$root_dir/downloaded_files" \
    --state "$root_dir/download-state" \
    --queue "$root_dir/urls_1_priority.txt" \
    --queue "$root_dir/urls_2_priority.txt" \
    --queue "$root_dir/urls_3_priority.txt" \
    "$@"
