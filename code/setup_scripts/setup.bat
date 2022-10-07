cd setup_scripts
python -m venv ..\parserEnv
call ..\parserEnv\Scripts\activate.bat
pip install -r ..\requirements.txt
python .\setup.py
cd ..
python ..\api_entry.py