from fastapi import FastAPI
import uvicorn
from api import parse

incoming = FastAPI()

incoming.include_router(parse.router)

@incoming.get("/")
async def main():
    return {"message": "Checking auto deployment on new github again."}

if __name__ == "__main__":
    uvicorn.run("api_entry:incoming", port=5050, log_level='info')