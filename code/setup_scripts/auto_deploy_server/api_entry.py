from fastapi import FastAPI, Request
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
async def pweb(request: Request):
    json = await request.json()
    if "ref" in json and "preprod" in json["ref"]: 
        Popen("./redeploy_website.sh", shell=True)
        return {"message": "Redeploying..."}
    else:
        return {"message": "Not redeploying, not preprod branch."}


if __name__ == "__main__":
    # uvicorn.run("api_entry:incoming", port=5050, log_level='info')
    pass