#!/bin/bash
PID=$(lsof -t -i :8000)
if [ -n "$PID" ]; then
  kill "$PID"
  echo "Server on port 8000 terminated (PID: $PID)"
else
  echo "No server running on port 8000"
fi
