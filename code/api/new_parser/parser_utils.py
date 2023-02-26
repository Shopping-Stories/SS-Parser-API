from typing import List, Tuple
from re import split, search
from unicodedata import numeric

# Adds to or by to a list of entries, using the first entry in the list 
# to determine whether to add to or by.
def add_to_by(entries: List[str]):
    smaller_entries = entries
    new_entries = []
    for j, smaller_entry in enumerate(smaller_entries):
        lower_tok = split(r"\s+", smaller_entry.lower())
        if "to" not in lower_tok and "by" not in lower_tok and j-1 >= 0:
            if "to" in split(r"\s+", new_entries[j-1].lower()):
                smaller_entry = "To " + smaller_entry
            elif "by" in split(r"\s+", new_entries[j-1].lower()):
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
def handle_multiple_prices(entry: List[Tuple[str, str, str]]) -> List[Tuple[str, str, str]]:
    # Search for the number of noun, price pairs, if more than one split around noun followed by price
    # Ignore people and dates because they are definitely not the item being purchased
    found_trans = []
    mltbe = False
    cur_entry = []
    found_noun_last = False
    found_price_last = False
    app_until_to_by = False
    # print("Handling multiple prices: ")
    # print(entry)
    for word, info, pos in entry:
        cur_entry.append((word, info, pos))
        # If we have a multiline tobacco entry, pass it all through as one entry.
        if pos == "MLTBE":
            mltbe = True
        
        if mltbe:
            continue

        # Allow prices to be followed by per [person] or price per [person]
        if app_until_to_by:
            if word in {"To", "By"} or info == "DATE":
                app_until_to_by = False
            elif info == "PERSON":
                if cur_entry:
                    cur_entry.pop()
                found_trans[-1].append((word, info, pos))
                app_until_to_by = False
            else:
                if cur_entry:
                    cur_entry.pop()
                found_trans[-1].append((word, info, pos))

        if found_price_last and (word.lower() in {"per", "[per]"} or info == "PRICE"):
            if info == "PRICE":
                found_trans[-1].append((word, info, pos))
            app_until_to_by = True
            if cur_entry:
                cur_entry.pop()
            found_trans[-1].append((word, info, pos))
        
        elif found_price_last:
            found_price_last = False

        if info == "PRICE" and found_noun_last:
            found_noun_last = False
            found_price_last = True
            found_trans.append(cur_entry)
            cur_entry = []

        if info == "TRANS" and len(cur_entry) > 1:
            found_price_last = False
            found_noun_last = False
            cur_entry.pop()
            found_trans.append(cur_entry)
            cur_entry = [(word, info, pos)]
        
        if "NN" in pos and info not in ["PERSON", "DATE"]:
            found_noun_last = True
        
    if found_trans == []:
        found_trans.append(cur_entry)
    elif found_trans[-1] != cur_entry and cur_entry != []:
        found_trans.append(cur_entry)

    # print(found_trans)
    # print()
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
        colname3 = colname + " "
        if colname2[0] != "[":
            colname2 = "[" + colname2 + "]"
        else:
            colname2 = colname.strip("[]")
        colname4 = colname2 + " "
        if colname2 in df:
            return df[colname2]
        elif colname in df:
            return df[colname]
        elif colname3 in df:
            return df[colname3]
        elif colname4 in df:
            return df[colname4]
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
                elif colname == "Store Location":
                    return get_col(df, "Store_Location")
                elif " " in colname:
                    return get_col(df, colname.replace(" ", "_"))
                raise KeyError(f"Column with name {colname} not in df")
            elif colname == "Marginalia":
                return get_col(df, "Marginialia")
            elif colname == "Store":
                return get_col(df, "Store Location")
            raise KeyError(f"Column with name {colname} not in df")

# Returns the name in the df corresponding to the name we give it, allows for column names to vary
# Column names include: "L Currency", "L Sterling", "Colony Currency", "Folio Year", "EntryID", etc.
def get_col_name(df, colname: str):
        colname2 = colname[:]
        colname3 = colname + " "
        if colname2[0] != "[":
            colname2 = "[" + colname2 + "]"
        else:
            colname2 = colname.strip("[]")
        colname4 = colname2 + " "
        if colname2 in df:
            return colname2
        elif colname in df:
            return colname
        elif colname3 in df:
            return colname3
        elif colname4 in df:
            return colname4
        else:
            if colname == "Folio Year":
                if "Year" in df:
                    return get_col_name(df, "Year")
                else:
                    return df[colname]
            elif colname == "Date Year":
                if "Year.1" in df:
                    return get_col_name(df, "Year.1")
                else:
                    return get_col_name(df, "Year")
            elif colname == "Colony Currency":
                if "Colony" in df:
                    return get_col_name(df, "Colony")
                else:
                    return df[colname]
            elif len(col := colname.split(" ")) > 1:
                if col[1] == "Sterling":
                    return get_col_name(df, col[0])
                elif col[1] == "Currency":
                    return get_col_name(df, col[0] + ".1")
                elif colname == "Store Location":
                    return get_col_name(df, "Store_Location")
                raise KeyError(f"Column with name {colname} not in df")
            elif " " in colname:
                return get_col_name(df, colname.replace(" ", "_"))
            elif colname == "Marginalia":
                return get_col_name(df, "Marginialia")
            elif colname == "Store":
                return get_col_name(df, "Store Location")
            raise KeyError(f"Column with name {colname} not in df")