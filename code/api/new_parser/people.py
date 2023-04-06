from requests import get
from threading import Lock
import pandas as pd
import logging
from traceback import format_exc
from time import sleep as tsleep
import asyncio
from re import sub, search

# In this file: We create a continuously updating Map (updates every 30 minutes) from an online spreadsheet that looks like this:
# {("absalom reid", "widow"): ["sarah reid"], ("person name", "relationship type"): ["relation 1", "relation 2"]}
# We then wrap this data structure with the Person class to make these easy to use, i.e. we can then do:
# Person("Absalom Reid")["widow"] in order to get "sarah reid"
# Also contains a few string sets to make parsing relationships between people easier.

# All the various relationship words we know of in our data
relationships = frozenset(["son", "son-in-law", "daughter", "mother", "father", "wife", "husband", "sister", "brother", "widow", "widower", "mother-in-law", "father-in-law", "cousin", "tenant", "grandson", "granddaughter", "grandaughter", "niece", "nephew", "uncle", "aunt", "grandfather", "grandmother", "slave", "negro", "daughter-in-law", "boy", "girl"])

familial = frozenset(["son", "daughter", "mother", "father", "wife", "husband", "sister", "brother", "widow", "widower", "grandson", "granddaughter", "grandaughter", "grandfather", "grandmother"])

conjunctions = frozenset(["of", "to"])

namelist = set()

_data_lock = Lock()

_people_data: pd.DataFrame = None
_data_updated = True

_bg_tasks = set()

# Function to update the data
def _update_data(i = 0):
    global _people_data
    global _data_updated

    if i == 3:
        logging.error("Failed to update people file")
        return
    _data_lock.acquire()
    try:
        _people_data = pd.read_excel(get("https://shoppingstories.s3.amazonaws.com/PeopleIndex/C_1760_PP_Master+List.xlsx").content)
        _data_updated = True
        # print("Got data!")
        # print(_people_data)
        _data_lock.release()
    except:
        logging.warning(f"Request to people date failed! Error: {format_exc()}")
        _data_lock.release()
        tsleep(1)
        _update_data(i + 1)

# Allows data to update itself continuously
async def _update_periodically():
    while True:
        # print("Creating thread/task _update_data")
        await asyncio.sleep(1800)
        thread = asyncio.to_thread(_update_data)
        task = asyncio.create_task(thread)
        _bg_tasks.add(task)
        task.add_done_callback(_bg_tasks.discard)

# Function to put the data in the format we need it in
def _parse_people_data(data: pd.DataFrame):
    global _last_data
    
    try:
        lookup = {}

        # Function to create the lookup data we need, to be used by pd.DataFrame.apply
        def create_lookup_for_row(entry: pd.Series):
            entry["First Name"] = str(entry["First Name"])
            entry["Last Name"] = str(entry["Last Name"])
            entry["Reference"] = str(entry["Reference"])
            
            if "FNU" in entry["First Name"]:
                return
            
            # Handles the fact that we still need to do a bunch of logic depending on the format of the name
            def create_lookups(toadd: tuple):
                newStr = sub(r"(\[?[Ss]enior\]?)|\[?[Jj]unior\]?", "", toadd[0]).strip().lower()
                toadd = (newStr, toadd[1].lower())
                
                if toadd[1] in familial:
                    if len(toadd[0].split(" ")) == 1:
                        newName = toadd[0]
                        newName += f" {entry['Last Name'].strip().lower()}"
                        if newName in namelist:
                            toadd = (newName, toadd[1])

                if "LNU" not in entry["Last Name"]:
                    if toadd in lookup:
                        lookup[toadd].append(entry["First Name"].strip().lower() + " " + entry["Last Name"].strip().lower())
                    else:
                        lookup[toadd] = [entry["First Name"].strip().lower() + " " + entry["Last Name"].strip().lower()]
                else:
                    if toadd in lookup:
                        lookup[toadd].append(entry["First Name"].strip().lower())
                    else:
                        lookup[toadd] = [entry["First Name"].strip().lower()]

            # Remove annoying characters and spaces to make parsing easier
            refs = sub(r"[^A-z\s\-\']", "", entry["Reference"])
            refs = sub(r"\s+", " ", refs)
            refs = refs.split(" ")

            for i, word in enumerate(refs):
                prev_word = None
                next_word = None
                if i - 1 >= 0:
                    prev_word = refs[i - 1]
                if i + 1 < len(refs):
                    next_word = refs[i + 1]

                if next_word is not None:
                    # If we see possessive ending 's or s'
                    if search(r"\'s|s\'", word):
                        if next_word.lower() in relationships:
                            relationship = next_word
                            if prev_word is None or (prev_word[0].upper() != prev_word[0]):
                                first_name = sub(r"\'s|s\'", "", word)
                                create_lookups((first_name, relationship))
                            else:
                                first_name = prev_word
                                last_name = sub(r"\'s|s\'", "", word)
                                create_lookups((f"{first_name.strip()} {last_name.strip()}", relationship))

                if word in conjunctions and next_word is not None and prev_word is not None:
                    relationship = prev_word
                    if relationship in relationships:
                        first_name = next_word
                        next_next_word = None
                        if i + 2 < len(refs):
                            next_next_word = refs[i + 2]
                        
                        # Case: relationship of|to first_name
                        if next_next_word is None or (next_next_word[0].upper() != next_next_word[0]):
                            create_lookups((first_name, relationship))
                        
                        # Case: relationship of|to first_name last_name
                        else:
                            last_name = next_next_word
                            create_lookups((f"{first_name.strip()} {last_name.strip()}", relationship))

        def add_to_namelist(entry: pd.Series):
            fn = str(entry["First Name"]).strip().lower()
            ln = str(entry["Last Name"]).strip().lower()
            if "lnu" in ln:
                namelist.add(fn)
            elif "fnu" not in fn:
                namelist.add(fn)
                namelist.add(fn + " " + ln)

        data.apply(add_to_namelist, axis=1)
        data.apply(create_lookup_for_row, axis=1)    
        return lookup
    
    except:
        logging.error(f"Issue with format of excel document! {format_exc()}")
        return _last_data

# Function to ensure we don't read _people_data at the same time we are writing to it
_last_data = None
def _get_people_data():
    global _last_data
    global _data_updated
    
    # Get the data if we have none
    if _last_data is None:
        _data_lock.acquire()
        if _people_data is None:
            _data_lock.release()
            _update_data()
            _data_lock.acquire()
        _last_data = _parse_people_data(_people_data.copy())
        _data_updated = False
        _data_lock.release()

    else:
        # Only update the data if the data has been updated recently
        if _data_updated:
            acquired = _data_lock.acquire(False)
            if acquired:
                _last_data = _parse_people_data(_people_data.copy())
                _data_updated = False
                _data_lock.release()

    return _last_data

async def people_index_coro():
    # Schedule continuous updating of _people_data, make sure we quickly get the data the first time
    _updater = asyncio.create_task(_update_periodically())
    _bg_tasks.add(_updater)
    thread = asyncio.to_thread(_update_data)
    await thread
    if __name__ == "__main__":
        print("sleeping")
        tsleep(2)
        print("starting")
        print(_get_people_data())
        print(Person("Sharshall Grasty")["brother"])
        print(Person("Absalom Reid")["all_relations"])
        print(Person("absalom reid"))

# Helper class for dealing with people, allows you to do Person["son"], Person["daughter"], Person["exists"], etc.
class Person:
    def __init__(self, name: str):
        self.full_name = name.lower().strip()
        self.has_first_last = False
        if len(self.full_name.split(" ")) > 1:
            self.has_first_last = True
            splitName = self.full_name.split(" ")
            self.first_name = splitName[0]
            self.last_name = splitName[1]

    def __getitem__(self, key: str):
        people_data = _get_people_data()
        key = key.lower()
        
        if key in relationships:
            if (self.full_name, key) in people_data:
                return people_data[(self.full_name, key)]
            else:
                return []

        elif key == "exists":
            return self.full_name in namelist

        elif "all_relations" in key:
            relations = {}
            for relationship in relationships:
                if (self.full_name, relationship) in people_data:
                    relations[relationship] = people_data[(self.full_name, relationship)]
            
            return relations

    def __str__(self) -> str:
        """
        Returns a string representation of the Person's name, capitalized properly
        """
        if self.has_first_last:
            return self.first_name[0].upper() + self.first_name[1:] + " " + self.last_name[0].upper() + self.last_name[1:]
        else:
            return self.full_name[0].upper() + self.full_name[1:]

if __name__ == "__main__":
    asyncio.run(people_index_coro())