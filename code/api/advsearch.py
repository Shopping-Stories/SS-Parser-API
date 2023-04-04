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

  # process search terms
  _item = item
  item_query = []
  if(item!=""):
    item = item.strip()
    item = item.split(" ")
    item_terms = [i for i in item if i]
    temp = []
    for i in item_terms:
      temp.append(str(meta(i)))
    item_terms = temp
    for i in item_terms:
      item_query.append({"item_metas": {"$regex": i, "$options": 'i'}})

  _acc_name = acc_name
  if(acc_name!=""):
    acc_name = acc_name.strip()
    acc_name = acc_name.split(" ")
    acc_name_terms = [i for i in acc_name if i]
    temp = []
    for i in acc_name_terms:
      if(i.lower() == "mr"):
        i = "mister"
      temp.append(str(meta(i)))
    acc_name_terms = temp
    acc_query = []
    for i in acc_name_terms:
      acc_query.append({"account_name_metas": {"$regex": i, "$options": 'i'}})

  _person = person
  if(person!=""):
    person = person.strip()
    person = person.split(" ")
    person_terms = [i for i in person if i]
    temp = []
    for i in person_terms:
      if(i.lower() == "mr"):
        i = "mister"
      temp.append(str(meta(i)))
    person_terms = temp
    person_query = []
    for i in person_terms:
      person_query.append({"all_metas": {"$regex": i, "$options": 'i'}})

  _co = co
  if(co!=""):
    co = co.strip()
    co = co.split(" ")
    co_terms = [i for i in co if i]
    temp = []
    for i in co_terms:
      if(i.lower() == "mr"):
        i = "mister"
      temp.append(str(meta(i)))
    co_terms = temp
    co_query = []
    for i in co_terms:
      co_query.append({"store_owner_metas": {"$regex": i, "$options": 'i'}})


  itemids = items.find({"item": {"$regex": _item, "$options": 'i'}})
  for i in itemids:
    _ids.append(i['_id'])

  catids = categories.find({"$and": [
      {"item": {"$regex": _item, "$options": 'i'}},
      {"category": {"$regex": cat, "$options": 'i'}},
      {"subcategory": {"$regex": subcat, "$options": 'i'}}
    ]})
  for i in catids:
    _ids.append(i['_id'])

  if(_item!=""):
    contents = [{"$or": [{"itemID": {"$in": _ids}}, {"$and": item_query}]}]
  else:
    contents = [{"itemID": {"$in": _ids}}]

  if(amt!=""):
    contents.append({"amount": {"$regex": amt, "$options": 'i'}})
  if(_acc_name!=""):
    contents.append({"$and": acc_query})
  if(_person!=""):
    contents.append({"$and": person_query})
  if(_co!=""):
    contents.append({"$and": co_query})
  if(year!=""):
    contents.append({"ledger.folio_year": {"$regex": year}})
  if(page!=-1):
    contents.append({"ledger.folio_page": page})

  query = {"$and": contents}
  res = entries.find(query)

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
    }}
  ])

  ids = ["peopleID", "itemID", "accountHolderID", "entryID", "_id", "related_people", "related_items"]

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

  itemids = items.find({"item": {"$regex": '(^|\\s)'+item, "$options": 'i'}})
  for i in itemids:
    _ids.append(i['_id'])

  catids = categories.find({"$and": [
      {"item": {"$regex": '(^|\\s)'+item, "$options": 'i'}},
      {"category": {"$regex": cat, "$options": 'i'}},
      {"subcategory": {"$regex": subcat, "$options": 'i'}}
    ]})
  for i in catids:
    _ids.append(i['_id'])

  contents = [{"$or": [{"itemID": {"$in": _ids}}, {"item": {"$regex": '(^|\\s)'+item, "$options": 'i'}}]}]

  if(amt!=""):
    contents.append({"amount": {"$regex": amt, "$options": 'i'}})
  if(acc_name!=""):
    contents.append({"account_name": {"$regex": acc_name, "$options": 'i'}})
  if(person!=""):
    contents.append({"$or":[{"people": {"$regex": person, "$options": 'i'}}, {"account_name": {"$regex": person, "$options": 'i'}}, {"store_owner": {"$regex": person, "$options": 'i'}}]})
  if(co!=""):
    contents.append({"store_owner": {"$regex": co, "$options": 'i'}})
  if(year!=""):
    contents.append({"ledger.folio_year": {"$regex": year}})
  if(page!=-1):
    contents.append({"ledger.folio_page": page})

  query = {"$and": contents}
  res = entries.find(query)

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
    }}
  ])

  ids = ["peopleID", "itemID", "accountHolderID", "entryID", "_id", "related_people", "related_items"]

  def bson_objectid_to_str(old_entry: dict):
      entry = {x: old_entry[x] for x in old_entry}
      for id in ids:
          if id in entry:
              entry[id] = str(entry[id])
      
      return entry

  return EntryList.parse_obj({"entries": [bson_objectid_to_str(x) for x in results]})