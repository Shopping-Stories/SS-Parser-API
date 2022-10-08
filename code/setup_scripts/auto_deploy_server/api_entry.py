from fastapi import FastAPI
import uvicorn
import os

incoming = FastAPI()

@incoming.get("/")
async def main():
    os.system("./redeploy.sh")
    return {"message": "Redeploying..."}

if __name__ == "__main__":
    # uvicorn.run("api_entry:incoming", port=5050, log_level='info')
    pass