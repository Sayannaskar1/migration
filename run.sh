#!/bin/bash
cd "$(dirname "$0")" || exit 1

# Try project-local venv, then parent-relative (legacy), then system python
if [ -f ".venv/bin/python3" ]; then
    PYTHON=".venv/bin/python3"
elif [ -f "../.venv/bin/python3" ]; then
    PYTHON="../.venv/bin/python3"
else
    PYTHON="python3"
fi

echo "Using: $PYTHON"
"$PYTHON" -m uvicorn app:app --host 0.0.0.0 --port 8001 --timeout-keep-alive 600 --reload
