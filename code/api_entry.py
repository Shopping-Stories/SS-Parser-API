from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from api import simplesearch, stringlist, parser_endpoints, new_entry_manager, documentation_endpoints, fuzzysearch, advsearch
from api.ssParser import entry_upload
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
    return {"message": "The api is still working. API Version 1.4.2"}

if __name__ == "__main__":
    uvicorn.run("api_entry:incoming", port=5050, log_level='info')