#!/usr/bin/env bash
set -u

cd "$(dirname "$0")/.."

failed=0
for probe in roundtrip batch wait launcher recovery; do
    echo "=== ${probe} ==="
    if python3 tools/incident_probe.py "${probe}"; then
        echo "${probe}: OK"
    else
        echo "${probe}: FAILED"
        failed=1
    fi
done

exit "${failed}"
