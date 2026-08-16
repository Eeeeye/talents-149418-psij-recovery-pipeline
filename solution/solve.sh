#!/bin/bash
set -euo pipefail

test -d /workspace/src/psij
cp -a /solution/files/src/psij/. /workspace/src/psij/

python3 /workspace/scripts/healthcheck.py
