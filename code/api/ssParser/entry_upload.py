from .database import db
from bson.objectid import ObjectId
from traceback import format_exc
from ..api_types import Message, ParserOutput
from typing import List, Dict, Any, Optional
from fastapi import APIRouter, BackgroundTasks
from pydantic import BaseModel
from json import dumps
from json import JSONEncoder 
import hashlib
from ..fuzzysearch import createMetasForEntries

# add collections
entries_collection = db.entries
people_collection = db.people
item_collection = db.items

class POutputList(BaseModel):
    entries: List[ParserOutput]

class ItemInput(BaseModel):
    item: Optional[str]
    related: Optional[List[str]]
    archMat: Optional[int]
    category: Optional[str]
    subcategory: Optional[str]

class PeopleInput(BaseModel):
    name: Optional[str]
    related: Optional[List[str]]

class HashEncoder(JSONEncoder): 
        def default(self, o):
            return o.__dict__

router = APIRouter()

# exact search for account_holder in people, add accountHolderID to entries
# checks if already in db, adds to db if not, makes relationship regardless
def _create_account_holder_rel(parsed_entry: Dict[str, Any]):
    people_id = create_people(parsed_entry["account_name"])
    parsed_entry.update({"accountHolderID": people_id}) 


# exact search for "item" in items collection (case insensitive), returns itemID
# checks if already in db, adds to db if not, calls _create_item_to_item_rel for new items
def create_item(item):
    if item_collection.find_one({'item': {'$regex': '^' + item + '$', '$options': 'i'}}): # choose regex
        item_id = item_collection.find_one({'item': {'$regex': '^' + item + '$', '$options': 'i'}}, {'_id'}) #update? simplify?
        item_id = item_id['_id']
    else:
        item_id = item_collection.insert_one({'item': item}).inserted_id
        _create_item_to_item_rel(item, item_id)

    return item_id


# exact search for item in items (case insensitive), add itemID to dict
# checks if already in db, adds to db if not, makes relationship regardless
def _create_item_rel(parsed_entry: Dict[str, Any]):
    item_id = create_item(parsed_entry["item"])
    parsed_entry.update({"itemID": item_id})


# exact search for "person" in people collection (case insensitive), returns peopleID
# checks if already in db, adds to db if not
def create_people(person):
    if people_collection.find_one({'name': {'$regex': '^' + person + '$', '$options': 'i'}}):
        people_id = people_collection.find_one({'name': {'$regex': '^' + person + '$', '$options': 'i'}}, {'_id'})
        people_id = people_id['_id']
    else:
        people_id = people_collection.insert_one({'name': person}).inserted_id

    return people_id


# exact search for people array in people, add peopleID(s) to dict
# loops through array of people, checks if already in db, adds to db if not, makes relationship regardless
def _create_people_rel(parsed_entry: Dict[str, Any]):
    parsed_entry.update({"peopleID": []})
    for person in parsed_entry["people"]:
        people_id = create_people(person)
        parsed_entry["peopleID"].append(people_id) 


# puts specified values (keys) into an object by name new_key together for entries
# what can go wrong: if input does not contain all keys
def _create_object(parsed_entry: Dict[str, Any], keys: List[str], new_key: str):
    for key in keys:
        if key not in parsed_entry:
            return

    object: Dict[str, Any] = dict.fromkeys(keys)

    for key in keys:
        if key not in parsed_entry:
            if new_key == "sterling" or new_key == "currency":
                parsed_entry[key] = 0
            else:
                parsed_entry[key] = ""
        
        if key == "entry_id" and parsed_entry["entry_id"].endswith(".0"):
            parsed_entry["entry_id"] = parsed_entry["entry_id"].removesuffix(".0")
        
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


# regex search for related items
# separates every word in a string, removes plurals, and regex searches for strings containing word, returns list or related items
def item_regex(item):
    regex_matches = []
    item = item.split(" ")
    for i in item:
        i = i.rstrip("s")
        related_items = item_collection.find({"item": {
        "$regex": '(^|\s+)' + i + 's*($|\s+)', 
        "$options": 'i'
        }})
        for rel in related_items:
            regex_matches.append(rel)

    return regex_matches


# creates relationship between similar items in the database, skips duplicates
# substring will relate to all items containing it, larger 
def _create_item_to_item_rel(item, item_id):
    relatedItems = item_regex(item) 
    for rel_item in relatedItems:
        if item_id != rel_item["_id"]:
            item_collection.update_one({'_id': rel_item["_id"]}, {'$push': {'related': item_id}}) 
            item_collection.update_one({'_id': item_id}, {'$push': {'related': rel_item["_id"]}}) 


# hashes parsed_entry and adds value to entry, done so that we do not insert duplicate database entries
def hash_entry(parsed_entry: Dict[str, Any]):
    entry_dumps = dumps(parsed_entry, cls=HashEncoder)
    entry_hash = hashlib.sha256(entry_dumps.encode()).hexdigest()
    parsed_entry.update({"hash": entry_hash})


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
            return f"Errors were present in entry: {parsed_entry} and as such the entry was not inserted."

        # main
        # hashes entry and checks db for matching hash to ensure that it is unique
        hash_entry(parsed_entry) 
        if "hash" in parsed_entry: 
            if entries_collection.find_one({'hash': parsed_entry['hash']}):
                return f"ERROR: duplicate entry {parsed_entry}, entry was not inserted."

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
        #if "itemID" in parsed_entry:
            #_create_item_to_item_rel(parsed_entry)

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
    new_entries = ["nothing"]
    try:
        alreadyFound = set()
        def checkDuplicates(entry):
            if "hash" in entry and type(entry) is not str:
                if entry["hash"] in alreadyFound:
                    return f"ERROR: Entry with hash {entry['hash']} already being inserted. Not inserting same entry twice."
                else:
                    alreadyFound.add(entry["hash"])
                    # print(alreadyFound)
                    return entry
            else:
                return f"ERROR: Could not hash entry, or other error occured... {entry}."
        
        new_entries = [_make_db_entry(x) for x in parsed_entry.entries]
        
        # Do not insert duplicate entries
        new_new_entries = []
        for x in new_entries:
            if type(x) is str:
                new_new_entries.append(x)
            else:  
                n = checkDuplicates(x)
                if type(n) is str:
                    pass
                else:
                    new_new_entries.append(n)
        new_entries = new_new_entries

    except Exception as e:
        return Message(message="ERROR: " + format_exc(), error=True)

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
        return Message(message="ERROR: " + format_exc(), error=True)
    
    # If nothing went wrong, return successful message.
    return Message(message="Successfully inserted entries.")

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

    currency_keys = ["pounds", "shillings", "pennies", "farthings"]
    sterling_keys = ["pounds_ster", "shillings_ster", "pennies_ster", "farthings_ster"]
    ledger_keys = ["reel", "folio_year", "folio_page", "entry_id"]

    if all([x in new_values for x in currency_keys]):
        _create_object(new_values, currency_keys, "currency")
    if all([x in new_values for x in sterling_keys]):
        _create_object(new_values, sterling_keys, "sterling")
    if all([x in new_values for x in ledger_keys]):
        _create_object(new_values, ledger_keys, "ledger")

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
    