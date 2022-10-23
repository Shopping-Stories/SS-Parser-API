# Sample endpoint at /parse/, if you make another file besides parse.py for other endpoints you need to put the following lines in ../api_entry.py:
# from .api import <yourFileName>
# incoming.include_router(<yourFileName>.router)
from decimal import ROUND_CEILING
from fastapi import APIRouter
from json import loads
from .ssParser.ss_parser import parse_file

router = APIRouter()

# Sample endpoint for basic parsing, try making request to /parse/C_1760_002_FINAL_2.xlsx
@router.get("/parse/{fileloc}", tags=["parse"])
async def return_basic_parse(fileloc: str):
    return parse_file(fileloc)