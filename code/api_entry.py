from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from api import simplesearch, stringlist, parser_endpoints, new_entry_manager, documentation_endpoints, fuzzysearch, advsearch
from api.ssParser import entry_upload, items, db_people
from api.new_parser.people import people_index_coro
import logging
import sys

logging.basicConfig(filename="parsing.log", encoding="utf-8", level=logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

incoming = FastAPI()

incoming.include_router(simplesearch.router)
incoming.include_router(stringlist.router)
incoming.include_router(entry_upload.router)
incoming.include_router(parser_endpoints.router)
incoming.include_router(new_entry_manager.router)
incoming.include_router(fuzzysearch.router)
incoming.include_router(advsearch.router)
incoming.include_router(documentation_endpoints.router)
incoming.include_router(items.router)
incoming.include_router(db_people.router)

incoming.add_middleware(
    CORSMiddleware,
    allow_origins="*",
    allow_methods="*",
    allow_headers="*"
)


@incoming.on_event("startup")
async def start_people_tasks():
    await people_index_coro()

@incoming.get("/")
async def main():
    return {"message": "The api is still working. API Version 1.6.3"}

if __name__ == "__main__":
    # If this file is run like python api_entry.py parser, run parser on all files in s3 bucket with prefix ParseMe
    # DO NOT CHANGE BEHAVAIOR UNLESS YOU KNOW WHAT YOU ARE DOING! WILL BREAK PARSER AS IT RUNS ON AWS ECS BY RUNNING THIS FILE!
    # Also do not move the imports that are in here. They are here so we don't have to install pytorch to run the basic api
    if len(sys.argv) > 1 and sys.argv[1] == "parser":
        from boto3 import client
        from api.new_parser.parser_manager import start_parse
        import asyncio
        # Make sure the people index gets created
        asyncio.run(people_index_coro())
        s3 = client("s3")
        start_parse(s3)
    else:
        uvicorn.run("api_entry:incoming", port=5050, log_level='info')
