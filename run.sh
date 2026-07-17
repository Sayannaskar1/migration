#!/bin/bash
cd "$(dirname "$0")"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
VENV_PYTHON="$(dirname "$SCRIPT_DIR")/.venv/bin/python3"
"$VENV_PYTHON" -m uvicorn app:app --host 0.0.0.0 --port 8001 --timeout-keep-alive 600 --reload
