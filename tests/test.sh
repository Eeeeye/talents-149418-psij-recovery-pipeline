#!/usr/bin/env bash
set -u

mkdir -p /logs/verifier
export PYTHONDONTWRITEBYTECODE=1
export PYTHONWARNINGS=ignore::ResourceWarning

status=0
python3 /tests/test_task.py || status=$?

if [ "${status}" -eq 0 ]; then
    printf '1\n' > /logs/verifier/reward.txt
else
    printf '0\n' > /logs/verifier/reward.txt
fi

exit "${status}"
