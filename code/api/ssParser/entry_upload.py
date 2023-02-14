from .database import db
from bson.objectid import ObjectId
from traceback import format_exc
from ..api_types import Message, ParserOutput
from typing import List, Dict, Any, Optional, Union
from fastapi import APIRouter, BackgroundTasks
from pydantic import BaseModel, Field
from json import load
from os import listdir
from os.path import join, dirname
from ..fuzzysearch import createMetasForEntries

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


class POutputList(BaseModel):
    entries: List[ParserOutput]

class ItemInput(BaseModel):
    item: Optional[str]
    related: Optional[List[str]]

class PeopleInput(BaseModel):
    name: Optional[str]
    related: Optional[List[str]]

router = APIRouter()

# exact search for account_holder in people, add accountHolderID to entries
# checks if already in db, adds to db if not, makes relationship regardless
def _create_account_holder_rel(parsed_entry: Dict[str, Any]):
    if people_collection.count_documents({"name": {
        "$regex": parsed_entry["account_name"],
        "$options": 'i'
        }}, limit=1):
        people_id = (people_collection.find_one(
            {"name": {
                "$regex": parsed_entry["account_name"],
                "$options": 'i'
            }}, {"_id"}))

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
        if people_collection.count_documents({"name": {
            "$regex": person,
            "$options": 'i'
        }}, limit=1):
            people_id = (people_collection.find_one({"name": {
                "$regex": person,
                "$options": 'i'
            }}, {"_id"}))

            parsed_entry["peopleID"].append(people_id["_id"])

        else:
            people_id = people_collection.insert_one(
                {"name": person}).inserted_id

            parsed_entry["peopleID"].append(people_id)


# puts specified values (keys) into an object by name new_key together for entries
# what can go wrong: if input does not contain all keys
def _create_object(parsed_entry: Dict[str, Any], keys: List[str], new_key: str):
    # if new_key != "sterling":
    for key in keys:
        if key not in parsed_entry:
            # print("does not contain all keys\n")
            return

    object: Dict[str, Any] = dict.fromkeys(keys)

    for key in keys:
        if key not in parsed_entry:
            if new_key == "sterling" or new_key == "currency":
                parsed_entry[key] = 0
            else:
                parsed_entry[key] = ""
        if key.endswith("_ster"):
            nk = key.replace("_ster", "")
            object.update({nk: parsed_entry[key]})
            del parsed_entry[key]
        else:
            object.update({key: parsed_entry[key]})
            del parsed_entry[key]

    parsed_entry.update({new_key: object})


# creates relationship between people that were mentioned in the same entry, skips duplicates
def _create_people_to_people_rel(parsed_entry: Dict[str, Any]):
    for person1 in parsed_entry["peopleID"]:
        for person2 in parsed_entry["peopleID"]:
            if person1 != person2:
                newRelFlag = True
                person1Data = people_collection.find_one({'_id': person1})
                if "related" in person1Data:
                    for related in person1Data["related"]:
                        if related == person2:
                            newRelFlag = False
                            break
                if newRelFlag == True:
                    people_collection.update_one({'_id': person1}, {'$push': {'related': person2}}) 


# creates relationship between people and account holders that were mentioned in the same entry, skips duplicates
def _create_people_to_account_holder_rel(parsed_entry: Dict[str, Any]):
    accHol = parsed_entry["accountHolderID"]
    for person in parsed_entry["peopleID"]:
        if person != accHol:
            # add person to account holder's realted field
            newRelFlag = True
            accHolData = people_collection.find_one({'_id': accHol})
            if "related" in accHolData:
                for related in accHolData["related"]:
                    if related == person:
                        newRelFlag = False
                        break
            if newRelFlag == True:
                people_collection.update_one({'_id': accHol}, {'$push': {'related': person}}) 

            # add account holder to person's related field
            newRelFlag = True
            personData = people_collection.find_one({'_id': person})
            if "related" in personData:
                for related in personData["related"]:
                    if related == accHol:
                        newRelFlag = False
                        break
            if newRelFlag == True:
                people_collection.update_one({'_id': person}, {'$push': {'related': accHol}}) 


# creates relationship between similar items in the database, skips duplicates
# substring will relate to all items containing it, larger 
def _create_item_to_item_rel(parsed_entry: Dict[str, Any]):
    relatedItems = item_collection.find({"item": {
        "$regex": '.*' + parsed_entry["item"] + '.*', # edit regex?
        "$options": 'i'
    }})
    for item in relatedItems:
        if parsed_entry["itemID"] != item["_id"]:
            item_collection.update_one({'_id': item["_id"]}, {'$push': {'related': parsed_entry["itemID"]}}) 
            item_collection.update_one({'_id': parsed_entry["itemID"]}, {'$push': {'related': item["_id"]}}) 


# Helper function to make database formatted entries
def _make_db_entry(parsed_entry: ParserOutput):
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

        # extra relationships within collections
        if "peopleID" in parsed_entry:
            if len(parsed_entry["peopleID"]) > 1:
                _create_people_to_people_rel(parsed_entry)
        if "accountHolderID" in parsed_entry and "peopleID" in parsed_entry:
            _create_people_to_account_holder_rel(parsed_entry)
        if "itemID" in parsed_entry:
            _create_item_to_item_rel(parsed_entry)

        # create objects to group together similar data
        # define keys, change key values to change which variables are grouped
        # what can go wrong: will not create object if not all specified keys are in the input
        currency_keys = ["pounds", "shillings", "pennies", "farthings"]
        sterling_keys = ["pounds_ster", "shillings_ster", "pennies_ster", "farthings_ster"]
        ledger_keys = ["reel", "folio_year", "folio_page", "entry_id"]

        _create_object(parsed_entry, currency_keys, "currency")
        _create_object(parsed_entry, sterling_keys, "sterling")
        _create_object(parsed_entry, ledger_keys, "ledger")

        return parsed_entry

    except Exception as e:
        return "ERROR: " + format_exc()

# TODO: Ignore duplicates when inserting
@router.post("/create_entry/", tags=["Database Management"], response_model=Message)
def insert_parsed_entry(parsed_entry: ParserOutput, many=False):
    """
    Creates a new database entry from the parser output.
    Sets error flag and has ERROR at the front of the message if any errors occur.
    """

    entry = _make_db_entry(parsed_entry)

    if isinstance(entry, str):
        return Message(message=entry, error=True)
    
    # add dict to database
    # print(test_parser) # prints final entry to terminal
    test_id = entries_collection.insert_one(entry).inserted_id

    return Message(message=f"Successfully inserted entry. New entry has id {test_id}")
    

@router.post("/create_entries/", tags=["Database Management"], response_model=Message)
def insert_parsed_entries(parsed_entry: POutputList, background_tasks: BackgroundTasks):
    """
    Creates multiple new database entries from a list of parser output entries.
    Can return errors. If this happens, the database is guaranteed to not be updated with any of the new data.
    """
    new_entries = [_make_db_entry(x) for x in parsed_entry.entries]
    
    # If any errors present, return error.
    if any([isinstance(x, str) for x in new_entries]):
        return Message(message="ERROR: At least one error occured when uploading so nothing was uploaded.\nERRORS:\n" + "\n  ".join([x for x in new_entries if isinstance(x, str)]), error=True)

    try:
        # Add all to database
        result = entries_collection.insert_many(new_entries)
        # Create search terms for new entries
        background_tasks.add_task(createMetasForEntries, result.inserted_ids)
    except Exception as e:
        # Return any errors that may happen
        return Message(message="ERROR: " + format_exc, error=True)
    
    # If nothing went wrong, return successful message.
    return Message(message="Successfully inserted entries.")


def parse_file_exclude_errors(filename):
    data = None
    with open(filename, 'r') as file:
        data = load(file)
    
    if data is None:
        return
    
    for translist in data:
        for transaction in translist:
            if "errors" in transaction:
                continue
            insert_parsed_entry(ParserOutput.parse_obj(transaction))


@router.get("/upload_example_data", tags=["Parser Management"], response_model=Message)
def parse_folder_exclude_errors():
    folder = join(dirname(__file__), "outs")
    files = listdir(folder)
    for file in files:
        if ".json" in file:
            file = join(folder, file)
            parse_file_exclude_errors(file)

    return Message(message="Successfully uploaded example data.")


@router.post("/delete_entry/", tags=["Database Management"], response_model=Message)
def remove_entry(entry_id: str):
    """
    Removes a specified entry from the database (specified by ID).
    Sets error flag and has ERROR at the front of the message if any errors occur.
    """

    if entries_collection.find_one({'_id': ObjectId(entry_id)}):
        entries_collection.delete_one({'_id': ObjectId(entry_id)})
        return Message(message="Successfully deleted entry.")
    else:
        return Message(message=f"ERROR: Entry {entry_id} not found.", error=True)


@router.post("/edit_entry/", tags=["Database Management"], response_model=Message)
def edit_entry(entry_id: str, new_values: ParserOutput):
    """
    Edits an entry (specified by ID) in the database, intakes edited data in parser output format. 
    Sets error flag and has ERROR at the front of the message if any errors occur. 
    """

    if entries_collection.find_one({'_id': ObjectId(entry_id)}) == None:
        return Message(message=f"ERROR: Entry {entry_id} not found.", error=True)

    new_values = new_values.dict()

    todel = []
    for key in new_values:
        if new_values[key] == None:
            todel.append(key)

    for key in todel:
        del new_values[key]

    # ensure that keys exist, then create relationships
    if "account_name" in new_values:
        entries_collection.update_one({'_id': ObjectId(entry_id)}, {"$unset": {"accountHolderID": ""}})
        _create_account_holder_rel(new_values)
    if "item" in new_values:
        entries_collection.update_one({'_id': ObjectId(entry_id)}, {"$unset": {"itemID": ""}})
        _create_item_rel(new_values)
    if "people" in new_values:
        entries_collection.update_one({'_id': ObjectId(entry_id)}, {"$unset": {"peopleID": ""}})
        _create_people_rel(new_values)

    new_values_set = {"$set": new_values}
    entries_collection.update_one({'_id': ObjectId(entry_id)}, new_values_set)

    # extra relationships within collections
    if "peopleID" in new_values:
        if len(new_values["peopleID"]) > 1:
            _create_people_to_people_rel(new_values)
    if "accountHolderID" in new_values and "peopleID" in new_values:
        _create_people_to_account_holder_rel(new_values)
    if "itemID" in new_values:
        _create_item_to_item_rel(new_values)

    return Message(message="Successfully edited entry.")
    

@router.post("/edit_item/", tags=["Database Management"], response_model=Message)
def edit_item(item_id: str, new_values: ItemInput):
    """
    Edits an item (specified by ID) in the database.
    Sets error flag and has ERROR at the front of the message if any errors occur. No data will be edited in an error occurs.
    """

    if item_collection.find_one({'_id': ObjectId(item_id)}) == None:
        return Message(message=f"ERROR: Item {item_id} not found.", error=True)

    new_values = new_values.dict()

    todel = []
    for key in new_values:
        if new_values[key] == None:
            todel.append(key)

    for key in todel:
        del new_values[key]

    new_values_set = {"$set": new_values}
    item_collection.update_one({'_id': ObjectId(item_id)}, new_values_set)

    return Message(message="Successfully edited item.")


@router.post("/edit_person/", tags=["Database Management"], response_model=Message)
def edit_person(person_id: str, new_values: PeopleInput):
    """
    Edits a person (specified by ID) in the database.
    Sets error flag and has ERROR at the front of the message if any errors occur. No data will be edited in an error occurs.
    """

    if people_collection.find_one({'_id': ObjectId(person_id)}) == None:
        return Message(message=f"ERROR: Person {person_id} not found.", error=True)

    new_values = new_values.dict()

    todel = []
    for key in new_values:
        if new_values[key] == None:
            todel.append(key)

    for key in todel:
        del new_values[key]

    new_values_set = {"$set": new_values}
    people_collection.update_one({'_id': ObjectId(person_id)}, new_values_set)

    return Message(message="Successfully edited person.")


@router.post("/add_people_relationship/", tags=["Database Management"], response_model=Message)
def add_relationship(person1_name: str, person2_name: str):
    """
    Creates a relationship between two people (both specified by name) in the database.
    Sets error flag and has ERROR at the front of the message if any errors occur. No relationships will be updated if any error occurs.
    """

    person1_data = people_collection.find_one({'name': person1_name})
    person2_data = people_collection.find_one({'name': person2_name})

    if person1_data == None:
        return Message(message=f"ERROR: {person1_name} not found.", error=True)
    if person2_data == None:
        return Message(message=f"ERROR: {person2_name} not found.", error=True)
        
    person1_id = person1_data['_id'] 
    person2_id = person2_data['_id'] 

    if person1_id != person2_id:
        if "related" in person1_data:
            for related in person1_data['related']:
                if related == person2_id:
                    return Message(message=f"ERROR: Relationship already exists.", error=True)
        if "related" in person2_data:
            for related in person2_data['related']:
                if related == person1_id:
                    return Message(message=f"ERROR: Relationship already exists.", error=True)
        people_collection.update_one({'_id': person1_id}, {'$push': {'related': person2_id}}) 
        people_collection.update_one({'_id': person2_id}, {'$push': {'related': person1_id}})
    else: 
        return Message(message=f"ERROR: Both people are the same.", error=True)

    return Message(message="Successfully added relationship.")


#@router.post("/hash/", tags=["Database Management"], response_model=Message)
#def hashing(doc: ParserOutput):

    #print(hash(doc))

# print(test_id) # prints entryID to terminal

# NEED
# tobaccoMarks relationship
# places relationship