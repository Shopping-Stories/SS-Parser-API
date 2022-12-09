from pymongo import MongoClient
from bson.json_util import dumps, loads
from .ssParser.parser import db
from fastapi import APIRouter

router = APIRouter()

# simple search function for ShoppingStories project
# "search" parameter is a string
# searches relevant string fields in entries
#
# plan to add support for:
# - detecting numeral and searching it (ex. from search "16" detecting int 16 and searching int fields)
# - detecting currency (ex. "16 shillings" and searching for entries where shillings = 16)
# - detecting month (ex. "august" and searching for entries where month = 8 since dates are stored numerically)
# - in general, anticipating and accounting for all manner of generic search entries that don't necessarily
#   match our database format, like the currency example

@router.get("/search/{search}", tags=["search"])

async def simple_search(search: str):
  # accessing db - replaced connection string with empty "getDatabase()" function
  global db
  cluster: MongoClient = db
  # db = cluster["shoppingStories"]
  entries = db["entries"]

  # trim search entry whitespace
  search = search.strip()

  # searches all string fields in "entries" collection
  results = dumps(entries.find({"$or": [
      {"accountHolder.prefix": {"$regex": search, "$options": 'i'}},
      {"accountHolder.accountFirstName": {"$regex": search, "$options": 'i'}},
      {"accountHolder.accountLastName": {"$regex": search, "$options": 'i'}},
      {"accountHolder.suffix": {"$regex": search, "$options": 'i'}},
      {"accountHolder.profession": {"$regex": search, "$options": 'i'}},
      {"accountHolder.location": {"$regex": search, "$options": 'i'}},
      {"accountHolder.reference": {"$regex": search, "$options": 'i'}},
      {"meta.owner": {"$regex": search, "$options": 'i'}},
      {"meta.store": {"$regex": search, "$options": 'i'}},
      {"itemEntries.itemsOrServices.item": {"$regex": search, "$options": 'i'}},
      {"itemEntries.itemsOrServices.category": {"$regex": search, "$options": 'i'}},
      {"itemEntries.itemsOrServices.subcategory": {"$regex": search, "$options": 'i'}},
      {"people.name": {"$regex": search, "$options": 'i'}},
      {"places.name": {"$regex": search, "$options": 'i'}}
  ]}))
  return results

if __name__ == "__main__":
  print(simple_search("Hat"))