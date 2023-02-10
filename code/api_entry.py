from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from api import simplesearch, stringlist, parser_endpoints, new_entry_manager, fuzzysearch, advsearch
from api.ssParser import entry_upload
import logging
# import sys

logging.basicConfig(filename="parsing.log", encoding="utf-8", level=logging.INFO)
# logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

incoming = FastAPI()

incoming.include_router(simplesearch.router)
incoming.include_router(stringlist.router)
incoming.include_router(entry_upload.router)
incoming.include_router(parser_endpoints.router)
incoming.include_router(new_entry_manager.router)
incoming.include_router(fuzzysearch.router)
incoming.include_router(advsearch.router)

incoming.add_middleware(
    CORSMiddleware,
    allow_origins="*",
    allow_methods="*",
    allow_headers="*"
)

@incoming.get("/")
async def main():
    return {"message": "The api is still working. API Version 1.0.1"}

if __name__ == "__main__":
    uvicorn.run("api_entry:incoming", port=5050, log_level='info')