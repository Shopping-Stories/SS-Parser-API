from typing import List
import nltk
from re import split, search
from unicodedata import numeric

# Adds to or by to a list of entries, using the first entry in the list 
# to determine whether to add to or by.
def add_to_by(entries: List[str]):
    smaller_entries = entries
    new_entries = []
    for j, smaller_entry in enumerate(smaller_entries):
        lower_tok = nltk.word_tokenize(smaller_entry.lower())
        if "to" not in lower_tok and "by" not in lower_tok and j-1 >= 0:
            if "to" in nltk.word_tokenize(new_entries[j-1].lower()):
                smaller_entry = "To " + smaller_entry
            elif "by" in nltk.word_tokenize(new_entries[j-1].lower()):
                smaller_entry = "By " + smaller_entry
        new_entries.append(smaller_entry)
    return new_entries

# Parse complicated number structures
# Can error out
def parse_numbers(num_str: str):
    if type(num_str) is not str:
        return num_str
    
    num_str = split(r"\s+", num_str)
    if num_str:
        if num_str[-1].isalpha():
            num_str = num_str[:-1]
        if len(num_str) == 1:
            if len(num_str[0]) == 1:
                return numeric(num_str[0])
            else:
                nums = search(r"\d+", num_str[0])
                if nums:
                    return int(nums[0])
                else:
                    raise ValueError(f"Error, bad numbers {num_str}")
        elif len(num_str) == 2:
            number = 0
            if len(num_str[1]) == 1:
                number += numeric(num_str[1])
            else:
                raise ValueError(f"Error: Strange number format {num_str}.")
            number += int(num_str[0])
            return number
        else:
            raise ValueError(f"Error, no recognizeable number patterns in {num_str=}.")
    else:
        raise ValueError(f"Error: invalid number string {num_str=}")


# Tells us if token is noun, accepts tuple(text, entity tag, grammar tag) or spacy token
def isNoun(token) -> bool:
    if type(token) is tuple:
        return "VBG" in token[2] or "VBN" in token[2] or "NN" in token[2] or "UH" in token[2]
    else:
        return "VBG" in token.tag_ or "VBN" in token.tag_ or "NN" in token.tag_ or "UH" in token.tag_

# Deal with there sometimes being multiple entries in one entry.
def handle_multiple_prices(entry) -> list:
    # Search for the number of noun, price pairs, if more than one split around noun followed by price
    # Ignore people and dates because they are definitely not the item being purchased
    found_trans = []
    cur_entry = []
    found_noun_last = False
    for word, info, pos in entry:
        cur_entry.append((word, info, pos))
        if info == "PRICE" and found_noun_last:
            found_noun_last = False
            found_trans.append(cur_entry)
            cur_entry = []
        if "NN" in pos and info not in ["PERSON", "DATE"]:
            found_noun_last = True
    if found_trans == []:
        found_trans.append(cur_entry)

    return found_trans
    
def add_error(map, error, error_context):
    if type(error) is str:
        pass
    else:
        error = str(error)
    if "errors" in map:
        map["errors"].append(error)
    else:
        map["errors"] = [error, ]
    
    if "error_context" in map:
        map["error_context"].append(error_context)
    else:    
        map["error_context"] = [error_context, ]

# Returns df[colname], allowing for some variations in the exact column names
# Column names include: "L Currency", "L Sterling", "Colony Currency", "Folio Year", "EntryID", etc.
def get_col(df, colname: str):
        colname2 = colname[:]
        if colname2[0] != "[":
            colname2 = "[" + colname2 + "]"
        else:
            colname2 = colname.strip("[]")
        if colname2 in df:
            return df[colname2]
        elif colname in df:
            return df[colname]
        else:
            if colname == "Folio Year":
                if "Year" in df:
                    return get_col(df, "Year")
                else:
                    return df[colname]
            elif colname == "Date Year":
                if "Year.1" in df:
                    return get_col(df, "Year.1")
                else:
                    return get_col(df, "Year")
            elif colname == "Colony Currency":
                if "Colony" in df:
                    return get_col(df, "Colony")
                else:
                    return df[colname]
            elif len(col := colname.split(" ")) > 1:
                if col[1] == "Sterling":
                    return get_col(df, col[0])
                elif col[1] == "Currency":
                    return get_col(df, col[0] + ".1")
                raise KeyError(f"Column with name {colname} not in df")
            elif colname == "Marginalia":
                return get_col(df, "Marginialia")
            raise KeyError(f"Column with name {colname} not in df")
