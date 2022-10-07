cd setup_scripts
python -m venv ..\parserEnv
call ..\parserEnv\Scripts\activate.bat
pip install -r ..\requirements.txt
python .\setup.py
python ..\api_entry.py