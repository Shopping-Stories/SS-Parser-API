from typing import List, Tuple
from re import split, search
from unicodedata import numeric
import pandas as pd
from itertools import chain
from british_money import Money
import traceback

def print_debug(string):
    pass

# Various Utility Functions for the parser

month_to_number = {"january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6, "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12}

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
    ignoring_prices = False
    cur_entry = []
    found_noun_last = False
    found_price_last = False
    app_until_to_by = False
    # print("Handling multiple prices: ")
    # print(entry)
    for word, info, pos in entry:
        cur_entry.append((word, info, pos))
        # If we have a multiline tobacco entry, or otherwise don't want to split up entry by price locations, pass it all through as one entry.
        if pos == "MLTBE":
            mltbe = True
        
        if mltbe:
            continue
        
        if pos == "IGNORE_PRICES":
            ignoring_prices = True
            if cur_entry:
                cur_entry.pop()

        appd = False

        # Allow prices to be followed by per [person] or price per [person]
        if app_until_to_by:
            if word in {"To", "By"} or info == "DATE":
                app_until_to_by = False
            elif info == "PERSON":
                if cur_entry:
                    cur_entry.pop()
                found_trans[-1].append((word, info, pos))
                appd = True
                app_until_to_by = False
            else:
                if cur_entry:
                    cur_entry.pop()
                if pos != "IGNORE_PRICES":
                    found_trans[-1].append((word, info, pos))
                appd = True

        if found_price_last and (word.lower() in {"per", "[per]"} or info == "PRICE" or "fancy_" in pos or pos == "IGNORE_PRICES"):
            app_until_to_by = True
            if cur_entry and cur_entry[-1] == (word, info, pos):
                cur_entry.pop()
            if pos != "IGNORE_PRICES":
                if not appd:
                    found_trans[-1].append((word, info, pos))
        
        elif found_price_last:
            found_price_last = False

        if info == "PRICE" and found_noun_last and not ignoring_prices:
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

# Modifies the df to copy marginalia values down into rows for which they are null
# Also does the same for date year, month, and day.
def fix_marginalia_dates(df: pd.DataFrame):
    nrows = df.shape[0]
    marg_name = get_col_name(df, "Marginalia")
    year_name = get_col_name(df, "Date Year")
    month_name = get_col_name(df, "_Month")
    day_name = get_col_name(df, "Day")
    dr_cr_name = get_col_name(df, "Dr/Cr")
    last_date = {"year": None, "month": None, "day": None}
    last_marg = ""

    in_cr = False

    def isNull(val: str):
        return val is None or val == "-" or val == "" or str(val) == "nan"

    for i in range(nrows):
        # Fix marginalia
        if not isNull(val := df.at[i, marg_name]):
            last_marg = val
        else:
            if last_marg != "":
                df.at[i, marg_name] = last_marg

        # If in cr, set in_cr to true and reset last date
        if not in_cr and df.at[i, dr_cr_name] == "Cr":
            in_cr = True
            last_date["year"] = None
            last_date["month"] = None
            last_date["day"] = None

        # Fix dates
        # If date is not null, remember it
        if not isNull(year := df.at[i, year_name]):
            last_date["year"] = year
            last_date["month"] = None
            last_date["day"] = None
            # Sometimes year is defined but no month or day is defined
            if not isNull(month := df.at[i, month_name]):
                last_date["month"] = month
            if not isNull(day := df.at[i, day_name]):
                last_date["day"] = day

        # When date is undefined, use the last date we saw
        else:
            if last_date["year"] != None:
                df.at[i, year_name] = last_date["year"]
                if  last_date["month"] != None:
                    df.at[i, month_name] = last_date["month"]
                if last_date["day"] != None:
                    df.at[i, day_name] = last_date["day"]

# Checks if there is values in all nullable columns listed in nullable_cols
# If there is, save it, otherwise, don't.
def remember_nullable_cols(row_context: dict, nullable_cols: List[str], row):
    for entry_name in nullable_cols:
        val = get_col(row, entry_name)
        if val in ["-", " -", "- "] or val == "" or str(val) == "nan":
            pass
        else:
            if entry_name == "Quantity":
                try:
                    row_context[entry_name] = parse_numbers(get_col(row, entry_name))
                except Exception as e:
                    add_error(row_context, "Error: Quantity parsing failed in: " + str(e), get_col(row, "Entry"))
            elif entry_name == "Marginalia":
                # Marginalia sometimes has funky spacing so remove that
                row_context[entry_name] = str(get_col(row, entry_name)).strip()
            else:
                row_context[entry_name] = str(get_col(row, entry_name)).strip("[]")


# Mark any rows with no currency as totaling contextless and
# save all values in the currency columns in the row context
def setup_row_currency(row_context: dict, row, entries, transactions_context: dict):
    # If there is no currency money, mark as contextless transaction
    if all([get_col(row, x) in ["-", " -", "- "] for x in ["L Currency", "s Currency", "d Currency", "L Sterling", "s Sterling", "d Sterling"]]):
        row_context["currency_totaling_contextless"] = True
    
    # If there is Colony Currency Money, remember it
    elif not all([get_col(row, x) in ["-", " -", "- "] for x in ["L Currency", "s Currency", "d Currency"]]):
        row_context["currency_type"] = "Currency"
        row_context["pounds"] = get_col(row, "L Currency")
        row_context["shillings"] = get_col(row, "s Currency")
        row_context["pennies"] = get_col(row, "d Currency")
        row_context["farthings"] = 0
        try:
            row_context["money_obj"] = Money(l=row_context["pounds"], s=row_context["shillings"], d=row_context["pennies"])
            row_context["farthings"] = row_context["money_obj"]["f"]
            row_context["pennies"] = row_context["money_obj"]["d"]
            row_context["shillings"] = row_context["money_obj"]["s"]
            row_context["pounds"] = row_context["money_obj"]["l"]
        except Exception as e:
            row_context["money_obj"] = Money(l=0, s=0, d=0)
            row_context["farthings"] = row_context["money_obj"]["f"]
            add_error(row_context, "Error in colony currency parsing: " +  traceback.format_exc(), get_col(row, "Entry"))
            add_error(transactions_context, "Error in colony currency parsing: " + traceback.format_exc(), get_col(row, "Entry"))
        row_context["currency_totaling_contextless"] = False
    
    # If there is British Sterling Currency Money, remember it, setting currency type to both if there is both
    # Colony currency and sterling.
    if not all([get_col(row, x) in ["-", " -", "- "] for x in ["L Sterling", "s Sterling", "d Sterling"]]):
        if "currency_type" in row_context:
            row_context["currency_type"] = "Both"
        else:
            row_context["currency_type"] = "Sterling"
        row_context["pounds_ster"] = get_col(row, "L Sterling")
        row_context["shillings_ster"] = get_col(row, "s Sterling")
        row_context["pennies_ster"] = get_col(row, "d Sterling")
        row_context["farthings_ster"] = 0
        try:
            row_context["money_obj_ster"] = Money(l=row_context["pounds_ster"], s=row_context["shillings_ster"], d=row_context["pennies_ster"], context=entries)
            row_context["farthings_ster"] = row_context["money_obj_ster"]["f"]
            row_context["pennies_ster"] = row_context["money_obj_ster"]["d"]
            row_context["shillings_ster"] = row_context["money_obj_ster"]["s"]
            row_context["pounds_ster"] = row_context["money_obj_ster"]["l"]
        except Exception as e:
            add_error(row_context, "Error in sterling currency parsing " + traceback.format_exc(), get_col(row, "Entry"))
            add_error(transactions_context, "Error in sterling currency parsing " + traceback.format_exc(), get_col(row, "Entry"))
            row_context["money_obj_ster"] = Money(l=0, s=0, d=0)
            row_context["farthings_ster"] = row_context["money_obj_ster"]["f"]
        row_context["currency_totaling_contextless"] = False

# Checks Commodity and Currency totaling on lists of transactions ended by [Total]
# Writes down an error in the [Total] transaction if totals don't add up.
def verify_ender_totaling(row_context: dict, transactions: list, row):
    # Verify transactions add up if there are no errors
    if any(["errors" in x for x in transactions]):
        print_debug("Skipping totaling due to errors.\n")
    
    # If there are no errors in the transactions
    else:
        # Check for adding up
        total_money_curr = sum([x["money_obj"] for x in transactions[:-1] if not x["currency_totaling_contextless"] and "errors" not in x and "money_obj" in x])
        total_money_ster = sum([x["money_obj_ster"] for x in transactions[:-1] if not x["currency_totaling_contextless"] and "errors" not in x and "money_obj_ster" in x])
        
        # If there is a quantity in the total, total all commodities
        if not row_context["commodity_totaling_contextless"]:
            total_commodity = sum([x["Quantity"] for x in transactions[:-1] if not x["commodity_totaling_contextless"] and "errors" not in x])

            if total_commodity != row_context["Quantity"]:
                # Add error if commodity totaling fails
                endl = "\n"
                print_debug(f"Error: Commodity totaling failed on entries: {''.join(chain(*[str(x) + endl for x in transactions]))}\nTotal was {total_commodity}, and expected total was {row_context['Quantity']}")
                add_error(transactions[-1], f"Commodity totaling failed, total was {total_commodity}, expected was {row_context['Quantity']}", get_col(row, "Entry"))
        
        # If currency totaling successful do nothing
        if "currency_type" not in row_context:
            pass

        elif row_context["currency_type"] == "Both":
            if total_money_curr == row_context["money_obj"] and total_money_ster == row_context["money_obj_ster"]:
                pass
            # Otherwise add error
            else:
                endl = "\n"
                print_debug(f"Error: Totaling failed on entries: {''.join(chain(*[str(x) + endl for x in transactions]))}\nTotals were {total_money_curr} and {total_money_ster}, and expected totals were {row_context['money_obj']} and {row_context['money_obj_ster']}")
                add_error(transactions[-1], f"Currency totaling failed, totals were {total_money_curr} and {total_money_ster}, expected were {row_context['money_obj']} and {row_context['money_obj_ster']}", get_col(row, "Entry"))
        
        elif row_context["currency_type"] == "Sterling":
            if total_money_ster == row_context["money_obj_ster"]:
                pass
            # Otherwise add error
            else:
                endl = "\n"
                print_debug(f"Error: Totaling failed on entries: {''.join(chain(*[str(x) + endl for x in transactions]))}\nTotal was {total_money_ster}, and expected total was {row_context['money_obj_ster']}")
                add_error(transactions[-1], f"Currency totaling failed, total was {total_money_ster}, expected was {row_context['money_obj_ster']}", get_col(row, "Entry"))
        
        elif row_context["currency_type"] == "Currency":
            if total_money_curr == row_context["money_obj"]:
                pass
            # Otherwise add error
            else:
                endl = "\n"
                print_debug(f"Error: Totaling failed on entries: {''.join(chain(*[str(x) + endl for x in transactions]))}\nTotal was {total_money_curr}, and expected total was {row_context['money_obj']}")
                add_error(transactions[-1], f"Currency totaling failed, total was {total_money_curr}, expected was {row_context['money_obj']}", get_col(row, "Entry"))

