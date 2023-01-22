from pymongo import MongoClient
from fuzzy import DMetaphone
import bson
from .ssParser.database import db
from fastapi import APIRouter
from .api_types import EntryList

router = APIRouter()

# creates necessary dmeta tokenizations for ALL entries in database in one go.
# it can take a few minutes to process a few hundred documents
#
# IN PRACTICE THIS SHOULD NEVER HAVE TO BE CALLED !!!
# therefore i am not routing it to anything API-wise so it isn't accidentally accessed
def createDmetasForAllEntries():
  global db
  entries = db['entries']
  ids = entries.find()
  for x in ids:
    createDmetas(x['_id'])

# create "dmeta" (fuzzy.DMetaphone) tokenizations of searchable fields in document
# this is what search terms will be compared to to determine matches
# input is an ObjectId corresponding to the document to create dmeta fields for
# 
# thoughts: maybe call this somewhere in the parser AFTER document has been
#   created and inserted into the database
@router.get("/createDmetas/{_id}", tags=["search"])
def createDmetas(id: str):
  """
  creates searchable fields in a document for use with ShoppingStories project's fuzzy search
  """
  global db
  entries = db['entries']
  dmeta = DMetaphone()
  _id = bson.objectid.ObjectId(id)

  # locate document if it has 'item' field
  entry = entries.find_one({"$and":[{'_id': _id},{'item': {"$exists": True}}]})
  # if it has 'item' field, process dmetas for it and add to db
  if(entry != None):
    item_name = entry['item'].split(" ")
    for x in item_name:
      if(x.lower() == "mr"):
        x = "mister"
      entries.update_one({'_id': _id}, {"$addToSet": {"item_dmetas": str(dmeta(x))}})
      entries.update_one({'_id': _id}, {"$addToSet": {"all_dmetas": str(dmeta(x))}})
  
  # process dmetas if it has a "people" field (which would be an array)
  entry = entries.find_one({"$and":[{'_id': _id}, {"people": {"$exists": True} }]})
  people = []
  people_dmetas = []
  if(entry != None):
    for person in entry['people']:
      person = person.strip()
      person = person.split(" ")
      for i in person:
        people.append(i)
    for x in people:
      if(x.lower() == "mr"):
        x = "mister"
      entries.update_one({'_id': _id}, {"$addToSet": {"people_dmetas": str(dmeta(x))}})
      entries.update_one({'_id': _id}, {"$addToSet": {"all_dmetas": str(dmeta(x))}})

  # locate document regardless of if it has items or people,
  # all documents to date have 'account_name' and 'store_owner' fields
  entry = entries.find_one({'_id': _id})
  # process dmetas for account_name and store_owner and add to db
  account_name = entry['account_name'].split(" ")
  for x in account_name:
    if(x.lower() == "mr"):
      x = "mister"
    entries.update_one({'_id': _id}, {"$addToSet": {"account_name_dmetas": str(dmeta(x))}})
    entries.update_one({'_id': _id}, {"$addToSet": {"all_dmetas": str(dmeta(x))}})

  store_owner = entry['store_owner'].split(" ")
  for x in store_owner:
    if(x.lower() == "mr"):
      x = "mister"
    entries.update_one({'_id': _id}, {"$addToSet": {"store_owner_dmetas": str(dmeta(x))}})
    entries.update_one({'_id': _id}, {"$addToSet": {"all_dmetas": str(dmeta(x))}})


@router.get("/fuzzysearch/{search}", tags=["search"], response_model=EntryList)
def fuzzy_search(search: str):
  """
  simple fuzzy search for ShoppingStories project
  """
  global db
  entries = db['entries']
  dmeta = DMetaphone()

  # split search terms, removing all whitespace
  search = search.strip()
  search = search.split(" ")
  search_terms = [i for i in search if i]

  # generate list of dmeta variations of search terms to compare to dmeta variations in db
  dmeta_terms = []
  for i in search_terms:
    if(i.lower() == "mr"):
      i = "mister"
    dmeta_terms.append(str(dmeta(i)))

  results = entries.find({"all_dmetas": {"$all": dmeta_terms}})

  ids = ["peopleID", "itemID", "accountHolderID", "entryID", "_id"]

  def bson_objectid_to_str(old_entry: dict):
      entry = {x: old_entry[x] for x in old_entry}
      for id in ids:
          if id in entry:
              entry[id] = str(entry[id])
      
      return entry

  return EntryList.parse_obj({"entries": [bson_objectid_to_str(x) for x in results]})