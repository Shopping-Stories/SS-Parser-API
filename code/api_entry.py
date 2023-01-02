from fastapi import FastAPI
import uvicorn
from api import simplesearch, stringlist, parser_endpoints, excelUploader, new_entry_manager
from api.ssParser import entry_upload

incoming = FastAPI()

incoming.include_router(simplesearch.router)
incoming.include_router(stringlist.router)
incoming.include_router(entry_upload.router)
incoming.include_router(parser_endpoints.router)
incoming.include_router(new_entry_manager.router)

@incoming.get("/")
async def main():
    return {"message": "Checking auto deployment on new github again."}

if __name__ == "__main__":
    uvicorn.run("api_entry:incoming", port=5050, log_level='info')