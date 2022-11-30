import pymongo
from .ssParser.database import db
from fastapi import APIRouter

router = APIRouter()

@router.get("/createStringList/{name}")
async def createStringList(name: str):
  global db
  collection = db["stringLists"]
  strList = {"name": name}
  collection.insert_one(strList)
  return {"Message": f"Successfully created string list with name {name}"}

@router.get("/addString/{name}")
async def addString(name: str, string: str):
  global db
  collection = db["stringLists"]
  collection.update_one({"name": name}, {"$addToSet": {"strings": string}})
  return {"Message": f"Successfully added string {string} to list {name}"}

@router.get("/removeString/{name}")
async def removeString(name: str, string: str):
  global db
  collection = db["stringLists"]
  collection.update_one({"name": name}, {"$pull": {"strings": string}})
  return {"Message": f"Successfully removed string {string} from list {name}."}

# returns "strings" array and contents as just the raw json returned from the search
@router.get("/getStrings/{name}")
async def getStrings(name: str):
  global db
  collection = db["stringLists"]
  strlist = collection.find_one({"name": name}, {"strings": 1, "_id": 0})
  return strlist