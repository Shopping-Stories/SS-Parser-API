from parser import db
from bson.objectid import ObjectId

entries_collection = db.entries

test = {
  "folioRefs": [
    ""
  ],
  "ledgerRefs": [
    ""
  ],
  "tobaccoEntry": "",
  "entry": "test entry 1",
  "accountHolder": {
    "prefix": "",
    "accountFirstName": "James",
    "accountLastName": "Edwards",
    "suffix": "",
    "profession": "",
    "location": "",
    "reference": "",
    "debitOrCredit": 1,
  },
  "meta": {
    "ledger": "C_1760",
    "reel": "58",
    "owner": "John Glassford & Company",
    "store": "Colchester",
    "year": "1760",
    "folioPage": "19",
    "entryID": "18",
    "comments": "Jane or Jean?"
  },
  "dateInfo": {
    "day": 7,
    "month": 7,
    "year": 1761,
    "fullDate": {
      "$date": {
        "$numberLong": "-6579187200000"
      }
    }
  },
  "itemEntries": {
      "perOrder": 0,
      "percentage": 0,
        "variants": [
            ""
        ],
        "quantity": 7,
        "qualifier": "yard",
        "item": "Test 2",
        "category": "Textiles",
        "subcategory": "Fabric",
        "unitCost": {
            "pounds": 0,
            "shilling": 0,
            "pence": 0
        },
        "itemCost": {
            "pounds": 0,
            "shilling": 7,
            "pence": 7
        },
    "itemsMentioned": [] #maybe delete
    },
  "people": [
    {
      "name": "Jean Shields"
    }
  ],
  "places": [],
  "money": {
    "quantity": "",
    "commodity": "",
    "colony": "Virginia",
    "sterling": {
      "pounds": 0,
      "shilling": 0,
      "pence": 0
    },
    "currency": {
      "pounds": 1,
      "shilling": 2,
      "pence": 3
    }
  },
  "__v": 0
}

# exact search for people (accountHolder specifically), add accountHolderID to dict
account_holder = test["accountHolder"]
first_name = account_holder["accountFirstName"]
last_name = account_holder["accountLastName"]

people_collection = db.people
## print(people_collection.find_one({"firstName": "James", "lastName": "Edwards"}, {"_id"}))
if db.people_collection.count_documents({"firstName": first_name, "lastName": last_name}, limit = 1):
  people_id = (people_collection.find_one({"firstName": first_name, "lastName": last_name}, {"_id"}))
  test["accountHolder"].update({"accountHolderID": people_id})
else :
  people_id = people_collection.insert_one(test["accountHolder"]).inserted_id
  test["accountHolder"].update({"accountHolderID": people_id})

# exact search for items, add itemID to dict
item_entries = test["itemEntries"]
item = item_entries["item"]
print(item)  

item_collection = db.items
if db.items_collection.count_documents({"item": item}, limit = 1):
    item_id = (item_collection.find_one({"item": item}, {"_id"}))
    test["itemEntries"].update({"itemID": item_id})
else:
    item_id = item_collection.insert_one(test["itemEntries"]).inserted_id
    test["itemEntries"].update({"itemID": item_id})

# add dict to database
test_id = entries_collection.insert_one(test).inserted_id
print(test_id)

#NEED
## fuzzy search for items
## accountHolder relationship
## items relationship
## tobaccoMarks relationship
## places relarionship
## people relationship
## combine items and categories

#"accountHolderID": {
#      "$oid": "61788aa24e984cc095983aa2"
#    }