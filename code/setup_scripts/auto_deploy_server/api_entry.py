from fastapi import FastAPI
from subprocess import Popen

incoming = FastAPI()

@incoming.get("/")
async def main():
    Popen("./redeploy.sh", shell=True)
    return {"message": "Redeploying..."}

@incoming.post("/")
async def main():
    Popen("./redeploy.sh", shell=True)
    return {"message": "Redeploying..."}


if __name__ == "__main__":
    # uvicorn.run("api_entry:incoming", port=5050, log_level='info')
    pass