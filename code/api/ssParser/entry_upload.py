from .database import db
from bson.objectid import ObjectId
from traceback import format_exc
from ..api_types import Message
from typing import List, Dict, Any, Optional, Union
from fastapi import APIRouter
from pydantic import BaseModel, Field

# add collections
entries_collection = db.entries
people_collection = db.people
item_collection = db.items

# sample parser data as dict
if __name__ == "__main__":
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
        "currency_totaling_contextless": "false",  # false put in quotations
        "commodity_totaling_contextless": "false",  # false put in quotations
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

# Definition of what parser returns

class ParserOutput(BaseModel):
    errors: Optional[List[str]]
    error_context: Optional[List[List[Union[str, List[str]]]]]
    amount: Optional[str]
    amount_is_combo: Optional[bool]
    item: Optional[str]
    price: Optional[str]
    price_is_combo: Optional[bool]
    phrases: Optional[List[Dict[str, Union[str, List[str]]]]]
    date: Optional[str]
    pounds: Optional[int]
    pounds_ster: Optional[int]
    shillings: Optional[int]
    shillings_ster: Optional[int]
    pennies_ster: Optional[int]
    pennies: Optional[int]
    farthings_ster: Optional[int]
    Marginalia: Optional[str]
    farthings: Optional[int]
    currency_type: Optional[str]
    currency_totaling_contextless: Optional[bool]
    commodity_totaling_contextless: Optional[bool]
    account_name: Optional[str]
    reel: Optional[int]
    store_owner: Optional[str]
    folio_year: Optional[str]
    folio_page: Optional[int]
    entry_id: Optional[str]
    date_year: Optional[str] = Field(alias="Date Year")
    month: Optional[str] = Field(alias="_Month")
    Day: Optional[str]
    debit_or_credit: Optional[str]
    context: Optional[List[List[str]]]
    Quantity: Optional[str]
    Commodity: Optional[str]
    people: Optional[List[str]]
    type: Optional[str]
    liber_book: Optional[str]
    mentions: Optional[List[str]]


# functions
# exact search for account_holder in people, add accountHolderID to entries
# checks if already in db, adds to db if not, makes relationship regardless

router = APIRouter()


def _create_account_holder_rel(parsed_entry: Dict[str, Any]):
    if people_collection.count_documents({"name": parsed_entry["account_name"]}, limit=1):
        people_id = (people_collection.find_one(
            {"name": parsed_entry["account_name"]}, {"_id"}))

        parsed_entry.update({"accountHolderID": people_id["_id"]})

    else:
        people_id = people_collection.insert_one(
            {"name": parsed_entry["account_name"]}).inserted_id

        parsed_entry.update({"accountHolderID": people_id})


# exact search for item in items, add itemID to dict
# checks if already in db, adds to db if not, makes relationship regardless
def _create_item_rel(parsed_entry: Dict[str, Any]):
    if item_collection.count_documents({"item": parsed_entry["item"]}, limit=1) > 0:
        item_id = (item_collection.find_one(
            {"item": parsed_entry["item"]}, {"_id"}))

        parsed_entry.update({"itemID": item_id["_id"]})

    else:
        item_id = item_collection.insert_one(
            {"item": parsed_entry["item"]}).inserted_id

        parsed_entry.update({"itemID": item_id})


# exact search for people array in people, add peopleID(s) to dict
# loops through array of people, checks if already in db, adds to db if not, makes relationship regardless
def _create_people_rel(parsed_entry: Dict[str, Any]):
    parsed_entry.update({"peopleID": []})
    for person in parsed_entry["people"]:
        if people_collection.count_documents({"name": person}, limit=1):
            people_id = (people_collection.find_one({"name": person}, {"_id"}))

            parsed_entry["peopleID"].append(people_id["_id"])

        else:
            people_id = people_collection.insert_one(
                {"name": person}).inserted_id

            parsed_entry["peopleID"].append(people_id)


# puts specified values (keys) into an object by name new_key together for entries
# what can go wrong: if input does not contain all keys
def _create_object(parsed_entry: Dict[str, Any], keys: List[str], new_key: str):
    for key in keys:
        if key not in parsed_entry:
            print("does not contain all keys\n")
            return

    object: Dict[str, Any] = dict.fromkeys(keys)

    for key in keys:
        if key.endswith("_ster"):
            nk = key.replace("_ster", "")
            object.update({nk: parsed_entry[key]})
            del parsed_entry[key]
        else:
            object.update({key: parsed_entry[key]})
            del parsed_entry[key]

    parsed_entry.update({new_key: object})


@router.post("/create_entry/", tags=["Parser Management"], response_model=Message)
def insert_parsed_entry(parsed_entry: ParserOutput):
    """
    Creates a new database entry from the parser output.
    """
    parsed_entry = parsed_entry.dict()
    try:
        # Ensure no keys evaluate to None.
        todel = []
        for key in parsed_entry:
            if parsed_entry[key] == None:
                todel.append(key)

        for key in todel:
            del parsed_entry[key]

        if "errors" in parsed_entry:
            return Message(message="Errors were present in entry and as such the entry was not inserted.")

        # main
        # ensure that keys exist, then create relationships
        if "account_name" in parsed_entry:
            _create_account_holder_rel(parsed_entry)
        if "item" in parsed_entry:
            _create_item_rel(parsed_entry)
        if "people" in parsed_entry:
            _create_people_rel(parsed_entry)

        # create objects to group together similar data
        # define keys, change key values to change which variables are grouped
        # what can go wrong: will not create object if not all specified keys are in the input
        currency_keys = ["pounds", "shillings", "pennies", "farthings"]
        sterling_keys = ["pounds_ster", "shillings_ster", "pennies_ster"]
        ledger_keys = ["reel", "folio_year", "folio_page", "entry_id"]

        _create_object(parsed_entry, currency_keys, "currency")
        _create_object(parsed_entry, sterling_keys, "sterling")
        _create_object(parsed_entry, ledger_keys, "ledger")

        # add dict to database
        # print(test_parser) # prints final entry to terminal
        test_id = entries_collection.insert_one(parsed_entry).inserted_id

        return Message(message=f"Successfully inserted entry. New entry has id {test_id}")
    
    except Exception as e:
        return Message(message=format_exc())

# print(test_id) # prints entryID to terminal

# NEED
# format currency
# fuzzy search for items
# accountHolder relationship
# parent/child data placeholders people & items
# items relationship
# tobaccoMarks relationship
# places relarionship
# people relationship
# combine items and categories

# partital item search for parent/child ---- in progress
# if db.items_collection.count_documents({"item": {$regex: item}}) > 0:

#{_id: ObjectId('')}
