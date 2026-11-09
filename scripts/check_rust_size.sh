#!/usr/bin/env bash
set -u

status=0
while IFS= read -r -d '' file; do
    lines=$(wc -l < "$file")
    if [ "$lines" -gt 500 ]; then
        printf '%s: %s (limit 500)\n' "$file" "$lines" >&2
        status=1
    fi
done < <(find src-tauri/src -type f -name '*.rs' -print0 | sort -z)

exit "$status"
