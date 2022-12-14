import pymongo
from .ssParser.database import db
from .api_types import Message, StringList
from fastapi import APIRouter
from typing import List

router = APIRouter()


@router.get("/createStringList/{name}", response_model=Message, tags=["String Lists"])
def createStringList(name: str) -> Message:
    """
    Creates a new string list with the given name.
    """
    global db
    collection = db["stringLists"]
    strList = {"name": name}
    collection.insert_one(strList)
    return Message(message=f"Successfully created list with name {name}.")


@router.get("/addString/", response_model=Message, tags=["String Lists"])
def addString(name: str, string: str):
    """
    Adds new string to string list with given name.
    """
    global db
    collection = db["stringLists"]
    collection.update_one({"name": name}, {"$addToSet": {"strings": string}})
    return Message(message=f"Successfully added string {string} to list {name}.")


@router.get("/removeString/", response_model=Message, tags=["String Lists"])
def removeString(name: str, string: str):
    """
    Deletes a given string from the string list with given name.
    """
    global db
    collection = db["stringLists"]
    collection.update_one({"name": name}, {"$pull": {"strings": string}})
    return Message(message=f"Successfully removed string {string} from list {name}.")

# returns "strings" array and contents as just the raw json returned from the search

@router.get("/getStrings/{name}", response_model=StringList, tags=["String Lists"])
def getStrings(name: str) -> List[str]:
    """
    Returns all the strings in a string list with given name.
    """
    global db
    collection = db["stringLists"]
    strlist = collection.find_one({"name": name}, {"strings": 1, "_id": 0})
    return StringList.parse_obj({"strings": strlist})
