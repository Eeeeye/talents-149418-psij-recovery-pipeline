#!/bin/bash
set -euo pipefail

test -d /workspace/src/psij
cp -a /solution/files/src/psij/. /workspace/src/psij/

PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=/workspace/src python3 -c 'import psij'
