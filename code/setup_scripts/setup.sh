#!/bin/bash
python3 -m venv ../parserEnv
source ../parserEnv/Scripts/activate
pip install -r ../requirements.txt
python ./setup.py
python ../api_entry.py