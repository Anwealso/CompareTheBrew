#!/bin/bash

# Create a virtual environment
python3 -m venv venv

# Activate the virtual environment and install requirements
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

echo "Setup complete. To activate the virtual environment, run: source venv/bin/activate"
