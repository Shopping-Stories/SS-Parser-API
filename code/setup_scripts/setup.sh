#!/bin/bash
cd setup_scripts
python3 -m venv ../parserEnv
source ../parserEnv/bin/activate
pip install -r ../requirements.txt
python ./setup.py
python ../api_entry.py