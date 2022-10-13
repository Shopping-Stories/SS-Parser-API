from fastapi import FastAPI
from subprocess import Popen

incoming = FastAPI()

@incoming.get("/")
async def gmain():
    Popen("./redeploy.sh", shell=True)
    return {"message": "Redeploying..."}

@incoming.post("/")
async def pmain():
    Popen("./redeploy.sh", shell=True)
    return {"message": "Redeploying..."}

@incoming.get("/website")
async def gweb():
    Popen("./redeploy_website.sh", shell=True)
    return {"message": "Redeploying..."}

@incoming.post("/website")
async def pweb():
    Popen("./redeploy_website.sh", shell=True)
    return {"message": "Redeploying..."}


if __name__ == "__main__":
    # uvicorn.run("api_entry:incoming", port=5050, log_level='info')
    pass