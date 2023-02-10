from pymongo import MongoClient
from jellyfish import metaphone as meta
import bson
from .ssParser.database import db
from fastapi import APIRouter
from .api_types import EntryList

router = APIRouter()

# allows for fuzzy searching
@router.get("/itemsearch-fuzzy/", tags=["search"], response_model=EntryList)
def item_search(item:str = "", cat:str = "", subcat:str = "", amt:str = "", acc_name:str = "", person:str = "", co:str = "", year:str = "", page:int = -1):
  """
  fuzzy advanced search for items for shoppingStories project
  """
  items = db['items']
  categories = db['categories']
  entries = db['entries']
  _ids = []

  itemids = items.find({"item": {"$regex": item, "$options": 'i'}})
  for i in itemids:
    _ids.append(i['_id'])

  catids = categories.find({"$and": [
      {"item": {"$regex": item, "$options": 'i'}},
      {"category": {"$regex": cat, "$options": 'i'}},
      {"subcategory": {"$regex": subcat, "$options": 'i'}}
    ]})
  for i in catids:
    _ids.append(i['_id'])

  contents = [{"$or": [{"itemID": {"$in": _ids}}, {"item_metas": {"$in": [str(meta(item))]}}]}]

  if(amt!=""):
    contents.append({"amount": {"$regex": amt, "$options": 'i'}})
  if(acc_name!=""):
    contents.append({"account_name_metas": str(meta(acc_name))})
  if(person!=""):
    contents.append({"all_metas": str(meta(person))})
  if(co!=""):
    contents.append({"store_owner_metas": str(meta(co))})
  if(year!=""):
    contents.append({"ledger.folio_year": {"$regex": year}})
  if(page!=-1):
    contents.append({"ledger.folio_page": page})

  query = {"$and": contents}
  results = entries.find(query)

  ids = ["peopleID", "itemID", "accountHolderID", "entryID", "_id"]

  def bson_objectid_to_str(old_entry: dict):
      entry = {x: old_entry[x] for x in old_entry}
      for id in ids:
          if id in entry:
              entry[id] = str(entry[id])
      
      return entry

  return EntryList.parse_obj({"entries": [bson_objectid_to_str(x) for x in results]})

# case insensitive but otherwise requires exact matches
@router.get("/itemsearch/", tags=["search"], response_model=EntryList)
def item_search(item:str = "", cat:str = "", subcat:str = "", amt:str = "", acc_name:str = "", person:str = "", co:str = "", year:str = "", page:int = -1):
  """
  advanced search for items for shoppingStories project
  """
  items = db['items']
  categories = db['categories']
  entries = db['entries']
  _ids = []

  itemids = items.find({"item": {"$regex": item, "$options": 'i'}})
  for i in itemids:
    _ids.append(i['_id'])

  catids = categories.find({"$and": [
      {"item": {"$regex": item, "$options": 'i'}},
      {"category": {"$regex": cat, "$options": 'i'}},
      {"subcategory": {"$regex": subcat, "$options": 'i'}}
    ]})
  for i in catids:
    _ids.append(i['_id'])

  contents = [{"$or": [{"itemID": {"$in": _ids}}, {"item": {"$regex": item, "$options": 'i'}}]}]

  if(amt!=""):
    contents.append({"amount": {"$regex": amt, "$options": 'i'}})
  if(acc_name!=""):
    contents.append({"account_name": {"$regex": acc_name, "$options": 'i'}})
  if(person!=""):
    contents.append({"$or":[{"account_name": {"$regex": person, "$options": 'i'}}, {"store_owner": {"$regex": person, "$options": 'i'}}]})
  if(co!=""):
    contents.append({"store_owner": {"$regex": co, "$options": 'i'}})
  if(year!=""):
    contents.append({"ledger.folio_year": {"$regex": year}})
  if(page!=-1):
    contents.append({"ledger.folio_page": page})

  query = {"$and": contents}
  results = entries.find(query)

  ids = ["peopleID", "itemID", "accountHolderID", "entryID", "_id"]

  def bson_objectid_to_str(old_entry: dict):
      entry = {x: old_entry[x] for x in old_entry}
      for id in ids:
          if id in entry:
              entry[id] = str(entry[id])
      
      return entry

  return EntryList.parse_obj({"entries": [bson_objectid_to_str(x) for x in results]})