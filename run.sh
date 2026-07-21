#!/bin/bash
cd "$(dirname "$0")" || exit 1

# Try project-local venv first, then parent-relative (legacy), then system python
if [ -f ".venv/bin/python3" ]; then
    PYTHON=".venv/bin/python3"
elif [ -f "../.venv/bin/python3" ]; then
    PYTHON="../.venv/bin/python3"
else
    PYTHON="python3"
fi

echo "Using: $PYTHON"
echo "Create a venv with: python3 -m venv .venv && .venv/bin/pip install -r requirements.txt"
"$PYTHON" -m uvicorn app:app --host 0.0.0.0 --port 8001 --timeout-keep-alive 600 --reload
