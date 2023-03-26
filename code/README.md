# Shopping Stories V4 Python API  
Please note that requirements.txt contains all requirements for the api without the parser, and parser_requirements.txt should contain everything necessary to run the parser.  
To build and run the api, run the following:  
```bash
chmod +x ./setup_scripts/setup.sh
./setup_scripts/setup.sh
```  

To just run the api (if you've already built it), run the following:
```bash
source ./parserEnv/Scripts/activate
python api_entry.py
```  
Once you've sourced activate once, you can just run the api as follows:
```bash
python api_entry.py
```  

## On Windows:
Windows commands corresponding to the above linux commands:
```
.\setup_scripts\setup.bat
```
and
```
.\parserEnv\Scripts\activate.bat
python api_entry.py
```
and
```
python api_entry.py
```