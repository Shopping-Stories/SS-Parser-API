import pymongo
from .ssParser.parser import db
from fastapi import APIRouter

router = APIRouter()


def createStringList(name: str):
  global db
  collection = db["stringLists"]
  strList = {"name": name}
  collection.insert_one(strList)


def addString(name: str, string: str):
  global db
  collection = db["stringLists"]
  collection.update_one({"name": name}, {"$addToSet": {"strings": string}})


def removeString(name: str, string: str):
  global db
  collection = db["stringLists"]
  collection.update_one({"name": name}, {"$pull": {"strings": string}})

# didnt know how we wanted strings retrieved so i did both

# returns "strings" array and contents as just the raw json returned from the search
def getStrings(name: str):
  global db
  collection = db["stringLists"]
  strlist = collection.find_one({"name": name}, {"strings": 1, "_id": 0})
  return str(strlist)

# returns the different entries in the "strings" array as a python list
def getStringList(name: str):
  strings = getStrings(name)
  strings = strings[strings.index('[')+1:strings.index(']')]
  strings = strings.split(", ")
  strlist = []
  for item in strings:
    strlist.append(item[1:-1])
  return strlist