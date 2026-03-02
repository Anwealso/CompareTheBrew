#!/bin/bash
# Check if venv exists
if [ -d "venv" ]; then
    ./venv/bin/python3 app.py
else
    echo "Virtual environment not found. Please run bash setup.sh first."
    exit 1
fi
