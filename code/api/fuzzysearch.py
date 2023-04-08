from jellyfish import metaphone as meta
import bson
from .ssParser.database import db
from fastapi import APIRouter
from .api_types import EntryList

router = APIRouter()

# creates necessary metaphone tokenizations for ALL entries in database in one go.
# it can take a few minutes to process a few hundred documents
#
# IN PRACTICE THIS SHOULD NEVER HAVE TO BE CALLED !!!
# therefore i am not routing it to anything API-wise so it isn't accidentally accessed
def createMetasForAllEntries():
  global db
  entries = db['entries']
  ids = entries.find()
  for x in ids:
    createMetas(x['_id'])

# creates and inserts necessary metphone tokenizations for listed entries
# input 'entrylist' is a list of document _id fields in string format
# ex. ["639b8552fcc5de9ec26b12ba", "639b8552fcc5de9ec26b12bc"]
def createMetasForEntries(entrylist: list):
  for x in entrylist:
    createMetas(x)

# create metaphone tokenizations of searchable fields in document
# this is what search terms will be compared to to determine matches
# input is an ObjectId corresponding to the document to create metaphone fields for
@router.get("/createMetas/{_id}", tags=["search"])
def createMetas(id: str):
  """
  creates searchable fields in a document for use with ShoppingStories project's fuzzy search
  """
  global db
  entries = db['entries']
  _id = bson.objectid.ObjectId(id)

  # locate document if it has 'item' field
  entry = entries.find_one({"$and":[{'_id': _id},{'item': {"$exists": True}}]})
  # if it has 'item' field, process metaphones for it and add to db
  if(entry != None):
    item_name = entry['item'].split(" ")
    for x in item_name:
      if(x.lower() == "mr"):
        x = "mister"
      entries.update_one({'_id': _id}, {"$addToSet": {"item_metas": str(meta(x))}})
      entries.update_one({'_id': _id}, {"$addToSet": {"all_metas": str(meta(x))}})
  
  # process metaphones if it has a "people" field (which would be an array)
  entry = entries.find_one({"$and":[{'_id': _id}, {"people": {"$exists": True} }]})
  people = []
  people_metas = []
  if(entry != None):
    for person in entry['people']:
      person = person.strip()
      person = person.split(" ")
      for i in person:
        people.append(i)
    for x in people:
      if(x.lower() == "mr"):
        x = "mister"
      entries.update_one({'_id': _id}, {"$addToSet": {"people_metas": str(meta(x))}})
      entries.update_one({'_id': _id}, {"$addToSet": {"all_metas": str(meta(x))}})

  # locate document regardless of if it has items or people,
  # all documents to date have 'account_name' and 'store_owner' fields
  entry = entries.find_one({'_id': _id})
  # process metaphones for account_name and store_owner and add to db
  account_name = entry['account_name'].split(" ")
  for x in account_name:
    if(x.lower() == "mr"):
      x = "mister"
    entries.update_one({'_id': _id}, {"$addToSet": {"account_name_metas": str(meta(x))}})
    entries.update_one({'_id': _id}, {"$addToSet": {"all_metas": str(meta(x))}})

  store_owner = entry['store_owner'].split(" ")
  for x in store_owner:
    if(x.lower() == "mr"):
      x = "mister"
    entries.update_one({'_id': _id}, {"$addToSet": {"store_owner_metas": str(meta(x))}})
    entries.update_one({'_id': _id}, {"$addToSet": {"all_metas": str(meta(x))}})


@router.get("/fuzzysearch/{search}", tags=["search"], response_model=EntryList)
def fuzzy_search(search: str):
  """
  simple fuzzy search for ShoppingStories project
  """
  global db
  entries = db['entries']

  # split search terms, removing all whitespace
  search = search.strip()
  search = search.split(" ")
  search_terms = [i for i in search if i]

  # generate list of metaphone variations of search terms to compare to metaphone variations in db
  meta_terms = []
  for i in search_terms:
    if(i.lower() == "mr"):
      i = "mister"
    meta_terms.append(str(meta(i)))

  query = []

  for i in meta_terms:
    query.append({"all_metas": {"$regex": i, "$options": 'i'}})

  res = entries.find({"$and": query})

  res_ids = []
  for entry in res:
    res_ids.append(entry['_id'])

  results = entries.aggregate([
    {"$match": {"_id": {"$in": res_ids}}},
    {"$lookup": {
      "from": "items",
      "localField": "itemID",
      "foreignField": "_id",
      "as": "related_items"
    }},
    {"$lookup": {
      "from": "people",
      "localField": "peopleID",
      "foreignField": "_id",
      "as": "related_people"
    }},
    {"$lookup": {
      "from": "people",
      "localField": "accountHolderID",
      "foreignField": "_id",
      "as": "accountHolder"
    }}
  ])

  ids = ["peopleID", "itemID", "accountHolderID", "entryID", "_id", "related_people", "related_items", "accountHolder"]

  def bson_objectid_to_str(old_entry: dict):
      entry = {x: old_entry[x] for x in old_entry}
      for id in ids:
          if id in entry:
              entry[id] = str(entry[id])
      
      return entry

  return EntryList.parse_obj({"entries": [bson_objectid_to_str(x) for x in results]})