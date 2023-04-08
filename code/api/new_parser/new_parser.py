import pandas as pd
from sys import argv
from os import listdir
from os import path
import traceback
from re import sub
from .british_money import Money
from json import dump
from .parser_utils import add_error, get_col, month_to_number
from .indices import drink_set
from unicodedata import numeric
import logging
from itertools import combinations
from .parse_transactions import print_debug, get_transactions

# Performs a clean up on parser output, destroys the dict you give it
def _clean_pass(entry: dict):
    if "item" in entry:
        # Replace ballance with balance
        if entry["item"].lower() == "ballance":
            entry["item"] = "Balance"
    
    if "item" not in entry:
        if "type" in entry and entry["type"] == "Cash":
            entry["item"] = "Currency"
    
    if "item" in entry:
        # Convert 1/4 in a drink transaction to 1 quart
        if entry["item"].lower() in drink_set:
            if "amount" in entry and type(entry["amount"]) is str and entry["amount"].isnumeric() and len(entry["amount"]) == 1:
                if numeric(entry["amount"]) < 1:
                    quarts = int(numeric(entry["amount"]) * 4)
                    if quarts == 1:
                        entry["amount"] = f"{quarts} quart"
                    elif quarts < 1:
                        entry["amount"] = f"{numeric(entry['amount'] * 4)} quarts"
                    else:
                        entry["amount"] = f"{quarts} quarts"
            elif "amount" in entry and type(entry["amount"]) is str and len(entry["amount"].split("/")) == 2:
                    try:
                        quarts = int(entry["amount"].split("/")[0])
                        if quarts == 1:
                            entry["amount"] = f"{quarts} quart"
                        else:
                            entry["amount"] = f"{quarts} quarts"
                    except:
                        add_error(entry, f"Failed to parse amount: {entry['amount']}", "")

    if "amount" in entry:
        if type(entry["amount"]) is str:
            # Replace a/an/the with 1
            if entry["amount"].lower().split(" ")[0] in {"a", "an", "the"}:
                entry["amount"] = "1 " + " ".join(entry["amount"].lower().split(" ")[1:])
            
            # Convert fractional amounts to decimal
            try:
                entry["amount"] = sub(r"(\d+)?\s*([\u00BC-\u00BE\u2150-\u215E])", lambda x: str(int(x.group(1) if x.group(1) != None else 0) + numeric(x.group(2))), entry["amount"])
            except:
                if "errors" in entry:
                    entry["errors"].append("Failed to convert fraction to decimal.")
                else:
                    entry["errors"] = ["Failed to convert fraction to decimal.", ]

    if "context" in entry:
        entry["text_as_parsed"] = " ".join([x if type(x) is str else x[0] for x in entry["context"]])

    if "item" in entry:
        # Convert 1/4 in a drink transaction to 1 quart
        if entry["item"].lower() in drink_set:
            if ("amount" not in entry or entry["amount"] == "" or entry["amount"] == None) and "text_as_parsed" in entry:
                if entry["text_as_parsed"][0].isnumeric() and not entry["text_as_parsed"][1].isnumeric():
                    entry["amount"] = entry["text_as_parsed"][0]
                    if numeric(entry["amount"]) < 1:
                        quarts = int(numeric(entry["amount"]) * 4)
                    else:
                        quarts = None
                    if quarts is None:
                        pass
                    elif quarts == 1:
                        entry["amount"] = f"{quarts} quart"
                    elif quarts < 1:
                        entry["amount"] = f"{numeric(entry['amount']) * 4} quarts"
                    else:
                        entry["amount"] = f"{quarts} quarts"

    if "Folio Reference" in entry:
        entry["folio_reference"] = entry["Folio Reference"]

    if "currency_colony" in entry:
        if entry["currency_colony"] in ["-", " -", "- "] or (entry["currency_colony"] != entry["currency_colony"]):
            entry["currency_colony"] = "Unknown"

    if "type" in entry:
        if entry["type"] == "Cash":
            entry["item"] = "Currency"

    return entry


# Chains all the parsing functions together to actually parse df.
def parse(df: pd.DataFrame):
    logging.info("Parsing")
    out = get_transactions(df)
    todump = []
    for transaction in out:
        # Do some basic cleanup
        toOut = [_clean_pass({key: val for key, val in x.items() if key != "money_obj" and key != "money_obj_ster"}) for x in transaction]

        # Group all entrys with the same id together in order to attempt to backsolve currency types on entries with both currency and sterling
        # also used to fix dates when we see specific strings that should change the date
        entry_id_to_index = {}
        both_entries = set()
        eids = []
        eid_seen = set()
        for i, entry in enumerate(toOut):
            if "entry_id" in entry:
                if entry["entry_id"] in eid_seen:
                    pass
                else: 
                    eid_seen.add(entry["entry_id"])
                    eids.append(entry["entry_id"])

                if entry["entry_id"] in entry_id_to_index:
                    entry_id_to_index[entry["entry_id"]].append(i)
                else:
                    entry_id_to_index[entry["entry_id"]] = [i,]
                
                if "currency_type" in entry:
                    if entry["currency_type"] == "Both":
                        both_entries.add(entry["entry_id"])

        # Fix dates when we see a string like "november 19th" telling us the date needs to change
        for eid in eids:
            curr_date = {"date_month": None, "date_day": None}
            for i in entry_id_to_index[eid]:
                if "date_month" in toOut[i]:
                    curr_date["date_month"] = toOut[i]["date_month"]
                    curr_date["date_day"] = toOut[i]["date_day"]
                
                if curr_date["date_month"] is not None:
                    toOut[i]["_Month"] = month_to_number[curr_date["date_month"].lower()]
                    toOut[i]["Day"] = curr_date["date_day"]

        # For all entries with same id, do the currency backsolving by inspecting all possible price sum combinations
        for eid in both_entries:
            indices = entry_id_to_index[eid]
            try:
                to_backsolve = set([(x, Money(toOut[x]["price"])) for x in indices if "price" in toOut[x]])
                
                # Don't try to backsolve if there is only 1 entry.
                if len(to_backsolve) < 2:
                    raise AssertionError()
                
                # print(to_backsolve)
                
                if "original_money_obj" in toOut[indices[0]] and "original_money_obj_ster" in toOut[indices[0]]:
                    ster_sum = toOut[indices[0]]["original_money_obj_ster"]
                    curr_sum = toOut[indices[0]]["original_money_obj"]
                    # print(ster_sum)
                    # print(curr_sum)

                    valid_currrency_sums = []
                    valid_sterling_sums = []
                    for i in range(1, (len(to_backsolve) // 2) + 1):
                        for combo in combinations(to_backsolve, i):
                            combo = set(combo)
                            complement = to_backsolve.difference(combo)
                            combo_sum = sum(x[1] for x in combo)
                            complement_sum = sum(x[1] for x in complement)
                            # print(combo, combo_sum, ster_sum, combo_sum == ster_sum, curr_sum, combo_sum == curr_sum)
                            # print(complement, complement_sum, curr_sum, complement_sum == curr_sum, ster_sum, complement_sum == ster_sum)
                            if combo_sum == ster_sum and complement_sum == curr_sum:
                                if combo not in valid_sterling_sums and complement not in valid_currrency_sums:
                                    valid_sterling_sums.append(combo)
                                    valid_currrency_sums.append(complement)
                            elif combo_sum == curr_sum and complement_sum == ster_sum:
                                if combo not in valid_currrency_sums and complement not in valid_sterling_sums:
                                    valid_sterling_sums.append(complement)
                                    valid_currrency_sums.append(combo)
                    
                    if len(valid_currrency_sums) == 1 and len(valid_sterling_sums) == 1:
                        for i, price in valid_currrency_sums[0]:
                            toOut[i]["currency_type"] = "Currency"
                        for i, price in valid_sterling_sums[0]:
                            toOut[i]["currency_type"] = "Sterling"
                    else:
                        # print("Curr sums: ")
                        # print(valid_currrency_sums)
                        # print("Ster sums: ")
                        # print(valid_sterling_sums)
                        # print()
                        raise OSError("Intentional error that shouldn't be raised by anything else in this code block")
                
                # OSError is our shorthand for when we cannot figure out which items are sterling and which are currency
            except OSError:
                for index in indices:
                    if "tobacco_entries" in toOut[index] and toOut[index]["tobacco_entries"]:
                        pass
                    elif "errors" in toOut[index]:
                        toOut[index]["errors"].append("Failed to separate sterling from currency.")
                    else:
                        toOut[index]["errors"] = ["Failed to separate sterling from currency.", ]
            
            # Don't backsolve when only 1 entry
            except AssertionError:
                pass

            except:
                for index in indices:
                    if "tobacco_entries" in toOut[index] and toOut[index]["tobacco_entries"]:
                        pass
                    elif "errors" in toOut[index]:
                        toOut[index]["errors"].append("Could not separate sterling from currency due to internal price parsing error. " + traceback.format_exc())
                    else:
                        toOut[index]["errors"] = ["Could not separate sterling from currency due to internal price parsing error. " + traceback.format_exc(), ]
        
        todump.append([{key: val for key, val in x.items() if key != "original_money_obj" and key != "original_money_obj_ster"} for x in toOut])

    return todump
    

# Runs parse_folder but on a single file
def parse_file_and_dump(folder, filename):
    logging.info(f"Parsing file: {filename} in folder {folder}.")
    try:
        out = parse_file(path.join(folder, filename))
        file = open(path.join(folder, filename) + ".json", 'w')
        dump(out, file)
        file.close()
        print_debug(f"Finished file {filename}")
        print_debug()
    except Exception as e:
        print_debug(f"Parsing file {filename} failed. Exception dumped.")
        print_debug()
        file = open(path.join(folder, filename) + ".exception", 'w')
        file.write(str(e) + "\n" + traceback.format_exc())
        file.close()

# Reads in an excel file and parses it
def parse_file(filePath):
    logging.info(f"Parsing file: {filePath}")
    df = pd.read_excel(filePath)
    
    n = 0
    while "EntryID" not in df and "[EntryID]" not in df:
        n += 1
        df = pd.read_excel(filePath, skiprows=n)
    for idx in range(0, df.shape[0]-1):
        if "EntryID" in df:
            if str(df['EntryID'][idx+1])[:-1] == str(df['EntryID'][idx]):
                df = df.reset_index(drop=True)
        else:
            if str(df['[EntryID]'][idx+1])[:-1] == str(df['[EntryID]'][idx]):
                df = df.reset_index(drop=True)
    
    df = df[get_col(df, "EntryID") != ""]
    
    out = parse(df)

    return out

# set_progress is a function that takes a float reprsenting the current parsing progress
def parse_folder(folder, set_progress = None):
    logging.info(f"Parsing folder: {folder}")
    filenames = listdir(folder)
    filenames = [x for x in filenames if x.split(".")[-1] in ["xls", "xlsx"]]
    n = 0
    total = len(filenames)
    for filename in filenames:
        try:
            out = parse_file(path.join(folder, filename))
            file = open(path.join(folder, filename) + ".json", 'w')
            dump(out, file)
            file.close()
            if set_progress is not None:
                n += 1
                set_progress(n / total)
            print_debug(f"Finished file {filename}")
            print_debug()
        except Exception as e:
            text = traceback.format_exc()
            print_debug(f"Parsing file {filename} failed. Exception dumped. {text}")
            print_debug()
            file = open(path.join(folder, filename) + ".exception", 'w')
            file.write(str(e) + "\n" + text)
            file.close()
            if set_progress is not None:
                n += 1
                set_progress(n / total)
            
    
# If we are executed directly from command line, parse the file given in the first argument to the program
if __name__ == "__main__":
    if argv[1] in ("..\..\..\data\Amelia", "..\..\..\data\Mahlon"):
        parse_folder(argv[1])
    else:
        out = parse_file(argv[1])
        file = open("out.json", 'w')
        dump(out, file)
        file.close()
