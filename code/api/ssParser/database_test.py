from parser import db
from bson.objectid import ObjectId
import re
import json

# add collections
entries_collection = db.entries
people_collection = db.people
item_collection = db.items

# sample parser data as dict
test_parser = {
  "item": "Test5",
  "type": "Liber",
  "people": [
    "Thomas Hanam",
    "Jacob Smith"
  ],
  "liber_book": "A",
  "phrases": [
    {
      "modifies": "making",
      "phrase": [
        "for",
        "making",
        "a",
        "Desk"
      ]
    }
  ],
  "currency_type": "Both",
  "currency_totaling_contextless": "false", #false put in quotations
  "commodity_totaling_contextless": "false", #false put in quotations
  "account_name": "Joe Mama3",
  "reel": 58,
  "store_owner": "John Glassford & Company",
  "folio_year": "1760/1761",
  "folio_page": 70,
  "entry_id": "2",
  "Date Year": "1760",
  "Folio Reference": "131",
  "debit_or_credit": "Dr",
  "context": [
    [
      "To",
      "TRANS",
      "TO"
    ],
    [
      "Balance",
      "",
      "VB"
    ],
    [
      "from",
      "",
      "IN"
    ],
    [
      "Liber A",
      "LIBER",
      "NNP"
    ]
  ],
  "pounds": 0,
  "shillings": 7,
  "pennies": 0,
  "farthings": 0,
  "pounds_ster": 2,
  "shillings_ster": 10,
  "pennies_ster": 0,
  "Commodity": "Tobacco",
  "Quantity": 35
}

#functions
## exact search for account_holder in people, add accountHolderID to entries
## checks if already in db, adds to db if not, makes relationship regardless
def account_holder_rel(test_parser):
  if people_collection.count_documents({"name": test_parser["account_name"]}, limit = 1):
    people_id = (people_collection.find_one({"name": test_parser["account_name"]}, {"_id"}))
    test_parser.update({"accountHolderID": people_id["_id"]})
  else :
    people_id = people_collection.insert_one({"name": test_parser["account_name"]}).inserted_id
    test_parser.update({"accountHolderID": people_id})
  del test_parser["account_name"] 
  return

## exact search for item in items, add itemID to dict
## checks if already in db, adds to db if not, makes relationship regardless
def item_rel(test_parser):
  if item_collection.count_documents({"item": test_parser["item"]}, limit = 1) > 0:
    item_id = (item_collection.find_one({"item": test_parser["item"]}, {"_id"}))
    test_parser.update({"itemID": item_id["_id"]})
  else:
    item_id = item_collection.insert_one({"item": test_parser["item"]}).inserted_id
    test_parser.update({"itemID": item_id})
  del test_parser["item"]  
  return

## exact search for people array in people, add peopleID(s) to dict
## loops through array of people, checks if already in db, adds to db if not, makes relationship regardless
def people_rel(test_parser):
  test_parser.update({"peopleID": []})
  for person in test_parser["people"]:
    if people_collection.count_documents({"name": person}, limit = 1):
      people_id = (people_collection.find_one({"name": person}, {"_id"}))
      test_parser["peopleID"].append(people_id["_id"])
    else :
      people_id = people_collection.insert_one({"name": person}).inserted_id
      test_parser["peopleID"].append(people_id)
  del test_parser["people"]
  return

## puts specified values (keys) into an object (type) together for entries
## what can go wrong: if input does not contain all keys
def create_object(test_parser, keys, type):
  for key in keys:
    if key not in test_parser:
      print("does not contain all keys\n")
      return
  object = test_parser.fromkeys(keys)
  for key in keys:
    object.update({key: test_parser[key]})
    del test_parser[key]
  test_parser.update({type: object})
  return

# main
## ensure that keys exist, then create relationships
if "account_name" in test_parser:
  account_holder_rel(test_parser)
if "item" in test_parser:
  item_rel(test_parser)
if "people" in test_parser:
  people_rel(test_parser)

## create objects to group together similar data
## define keys, change key values to change which variables are grouped
## what can go wrong: will not create object if not all specified keys are in the input  
currency_keys = ["pounds", "shillings", "pennies", "farthings"]
sterling_keys = ["pounds_ster", "shillings_ster", "pennies_ster"]
ledger_keys = ["reel", "folio_year", "folio_page", "entry_id"]
create_object(test_parser, currency_keys, "currency")
create_object(test_parser, sterling_keys, "sterling")
create_object(test_parser, ledger_keys, "ledger")

## add dict to database
#print(test_parser) # prints final entry to terminal 
test_id = entries_collection.insert_one(test_parser).inserted_id
#print(test_id) # prints entryID to terminal

#NEED
## format currency
## fuzzy search for items
## accountHolder relationship
## parent/child data placeholders people & items
## items relationship
## tobaccoMarks relationship
## places relarionship
## people relationship
## combine items and categories

# partital item search for parent/child ---- in progress
#if db.items_collection.count_documents({"item": {$regex: item}}) > 0:

#{_id: ObjectId('')}