from fastapi import FastAPI, Request
from subprocess import run
import logging

logging.basicConfig(filename="adlogs.log", encoding='utf-8', level=logging.INFO)

incoming = FastAPI()

@incoming.get("/")
def gmain():
    a = run("./redeploy.sh", shell=True, capture_output=True, encoding="utf-8")
    logging.info("Ran Redeploy: ")
    logging.info(a.stdout)
    logging.info(a.stderr)
    return {"message": "Redeploying..."}

@incoming.post("/")
def pmain():
    a = run("./redeploy.sh", shell=True, capture_output=True, encoding="utf-8")
    logging.info("Ran Redeploy: ")
    logging.info(a.stdout)
    logging.info(a.stderr)
    return {"message": "Redeploying..."}

@incoming.get("/website")
def gweb():
    a = run("./redeploy_website.sh", shell=True, capture_output=True, encoding="utf-8")
    logging.info("Redeployed website: ")
    logging.info(a.stdout)
    logging.info(a.stderr)
    return {"message": "Redeploying..."}

@incoming.post("/website")
async def pweb(request: Request):
    json = await request.json()
    if "ref" in json and "preprod" in json["ref"]:
        a = run("./redeploy_website.sh", shell=True, capture_output=True, encoding="utf-8")
        logging.info("Redeployed website")
        logging.info(a.stdout)
        logging.info(a.stderr)
        return {"message": "Redeploying..."}
    else:
        return {"message": "Not redeploying, not preprod branch."}


if __name__ == "__main__":
    # uvicorn.run("api_entry:incoming", port=5050, log_level='info')
    pass