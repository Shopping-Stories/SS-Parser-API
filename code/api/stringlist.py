import pymongo
from .ssParser.parser import db
from fastapi import APIRouter

router = APIRouter()

@router.get("/createStringList/{name}")
async def createStringList(name: str):
  global db
  collection = db["stringLists"]
  strList = {"name": name}
  collection.insert_one(strList)

@router.get("/addString/")
async def addString(name: str, string: str):
  global db
  collection = db["stringLists"]
  collection.update_one({"name": name}, {"$addToSet": {"strings": string}})

@router.get("/removeString/")
async def removeString(name: str, string: str):
  global db
  collection = db["stringLists"]
  collection.update_one({"name": name}, {"$pull": {"strings": string}})

# returns "strings" array and contents as just the raw json returned from the search
@router.get("/getStrings/{name}")
async def getStrings(name: str):
  global db
  collection = db["stringLists"]
  strlist = collection.find_one({"name": name}, {"strings": 1, "_id": 0})
  return str(strlist)