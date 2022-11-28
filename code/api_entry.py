from fastapi import FastAPI
import uvicorn
from api import parse, simplesearch, stringlist

incoming = FastAPI()

incoming.include_router(parse.router)
incoming.include_router(simplesearch.router)
incoming.include_router(stringlist.router)

@incoming.get("/")
async def main():
    return {"message": "Checking auto deployment on new github again."}

if __name__ == "__main__":
    uvicorn.run("api_entry:incoming", port=5050, log_level='info')