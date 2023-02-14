import pandas as pd
from .parser_utils import get_col, get_col_name, add_to_by, isNoun
from re import split, match, search, sub, finditer, Match
import spacy
from itertools import chain
from .indices import amount_set, item_set
import logging

            
# Regex for the price
price_regex = r"((\d+[Lsdp])|((\:|(\d+))\/)?(\:|(\d+))\/(\:|(\d+)))"

# Regex for tobacco marks
mark_regex = r"\[TM:\s+(\d+)\s*(\w+)\]"

# Modifies the df to copy marginalia values down into rows for which they are null
# Also does the same for date year, month, and day.
def _fix_marginalia_dates(df: pd.DataFrame):
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

# Simple function to remove the XX tag as it usually indicates a parser error
def _remove_xx(tag, replacement):
    if tag == "XX":
        return replacement
    else:
        return tag

# Exceptions to the normal rule of not deleting words before [something] unless it starts with the same letter as something
def is_exception(word, i, smaller_entry):
    if (word.strip("[]<>^") in {"pound", "pounds"}) and i - 1 > 0 and (smaller_entry[i - 1] == "w" or smaller_entry[i - 1] == "wt"):
        return True
    elif word == "[thousand]" or word == "[thousands]" and i - 1 > 0 and smaller_entry[i - 1] in ["M", "m"]:
        return True
    else:
        return False

def handle_bracket_replacements(match: Match) -> str:
    if match.group(1)[0].lower() == match.group(2)[0].lower():
        return match.group(2)
    elif is_exception(f"[{match.group(2)}]", 1, [match.group(1), match.group(2)]):
        return match.group(2)
    else:
        return match.group(1) + " " + match.group(2)

# Handles entries of the form:
# By 2 hogshead tobacco on occoquan
# [TM: 0780 BH] N 1  1116. .130. .986
#               N 2  1025. .100. .925
#                                1911
#                                 200
#                                1711 at 11/: & 4/: for 2 Casks
# Except that the [TM: 0780 BH] is already replaced with its replacement when we get here so we don't have to worry about it
def _handle_multiline_tobacco(tob_match: list[Match], entry: str):
    # Clean up brackets in entry
    replace_regex = r"(\w+)[^\S\r\n]*\[\s*(\w+)\s*\]"
    entry = sub(replace_regex, handle_bracket_replacements, entry)
    remove_brackets_regex = r"\[\s*(\w+)\s*\]"
    entry = sub(remove_brackets_regex, lambda x: x.group(1), entry)

    # If we see a location for the tobacco, flag it as such
    location_regex = r"on\s+(\w+)"
    entry = sub(location_regex, lambda x: " tobacco_location " + x.group(1) + " ", entry)

    # Regex to match the final line of trasactions similar to the above
    final_line_regex = r"\n\s+(\d+)\s+at\s+((\d+[Lsdp])|((\:|(\d+))\/)?(\:|(\d+))\/(\:|(\d+)))\s+([^\n]+)"
    final = [x for x in finditer(final_line_regex, entry)]
    if final:
        # Only do this for the actual final line, not if another line looks similar to the final line
        final = final[-1]
        
        new_str = " "
        new_str += f"final_weight {final.group(1)} "
        new_str += f"unit_price {final.group(2)} "
        new_str += final.group(11)
        entry = entry.replace(final.group(), new_str)
    else:
        entry += " no_final_tobacco"
    
    for m in tob_match:
        # print(m)
        new_str = " "
        new_str += f"tobacco_note {m.group(3)} "
        new_str += f"total_weight {m.group(4)} "
        new_str += f"tare_weight {m.group(5)} "
        new_str += f"tobacco_weight {m.group(6)} "
        entry = entry.replace(m.group().strip(), new_str)
        # print(new_str, ",", entry, ",", repr(m.group()))

    entry = sub(r"\s+|\n", " ", entry)
    entry = sub(r"(&|[aA]nd)\s+", "", entry)
    # print(entry)
    return entry

# Handles tobacco marks i.e. [TM: 0780 BH],
def _get_tobacco_mark_replacement(mark: Match) -> str:
    return f"tobacco_mark_number {mark.group(1)} tobacco_mark_text {mark.group(2)}"

# Initial processing and labelling of transaction parts e.g. nouns, keywords, etc.
# Note that this is a generator due to it being slow
# Goal of this function is to create a list for every row that contains a list of important data from the entry
# This is done via first tokenizing and then tagging every word in the entry, then combining all tokens with similar enough tags or according to
# other rules. Conceptually, we want to tag an entry like the following:
# By 6 yd bed sheets for Jeff 6:/
# Should become something like this:
# [("By", "TRANS", ""), ("6 yd", "AMT", "CARDINAL"), ("bed sheets", "", "NN"), ("for", "", "IN"), ("Jeff", "PERSON", "NNP"), ("6:/", "PRICE", "CD")]
# This allows us to do a much higher level parse in the next step.
def preprocess(df: pd.DataFrame):
    logging.info("Preprocessing.")
    parsed_entries = []
    
    # Fix the marginalia issues present in the underlying spreadsheets
    # Fix missing dates by imputing with previous data
    _fix_marginalia_dates(df)

    
    # For row in df
    for key, row in df.iterrows():
        
        # Ignore rows with no entry text
        big_entry = get_col(row, "Entry")
        if big_entry == "-" or big_entry == "" or big_entry is None or str(big_entry) == "nan":
            continue

        if match(r"\s*\d+[a-zA-Z]+\s*", str(get_col(row, "EntryID"))):
            continue

        # Remove } from the text as it messes everything up
        big_entry = big_entry.replace("}", "")

        # Replace all tobacco marks with easily parseable tokens
        big_entry = sub(mark_regex, _get_tobacco_mark_replacement, big_entry)

        # print(big_entry)
        # Check for multiline tobacco entries and use special parsing rules if we find one
        tob_match = [x for x in finditer(r"((N|N[oO]|N[oO]\.|Note)\s+)?(\d+)\s+(\d+)\.\s+\.(\d+)\.\s+\.(\d+)(\s+)?\n?", big_entry)]
        if tob_match:
            if (all([get_col(row, x).strip() in {"-", "", None} for x in ("L Sterling", "s Sterling", "d Sterling", "L Currency", "s Currency", "d Currency")])):
                pass
            big_entry = _handle_multiline_tobacco(tob_match, big_entry)
            
            # Make sure there is not enough leftover whitespace to cause us to automatically split this transaction into multiple later on
            # print(big_entry)
            big_entry = sub(r"(\s\s+)|\n", " ", big_entry)
            # print(big_entry)
            # print()

        # Remove "Ditto"
        ditto = search(r"(DO|Do|DITTO|Ditto)\.*\s*\[\w+\]", big_entry)
        if ditto:
            newRe = search("\[\w+\]", ditto.group())
            newStr = (newRe.group())[1:-1]
            big_entry = big_entry.replace(ditto.group(), newStr)

        # Replace 1w with 1 w and 1M with 1 M and so on
        big_entry = sub(r"(?<=\s)\d+([Mm]|wt|w)(?=\s\[)", lambda match: match.group(0)[:-2] + " " + match.group(0)[-2:] if "wt" in match.group(0) else match.group(0)[:-1] + " " + match.group(0)[-1], big_entry)
        
        # If we see tobacco notes, remove spaces so we don't split entry.
        if search(r"N\s+\d+\s+\d+", big_entry):
            big_entry = sub(r"\s+", " ", big_entry)

        # Remove mini subtotals
        big_entry = sub(r"[\u00a3]?\s?(\d+)?\s?\.\.\s?\d+\s?\.\.\s?\d+\s?[\u00BC-\u00BE\u2150-\u215E]?", " ", big_entry)
        
        # Split the entry by "    " or \n or \t
        smaller_entries = split(r"(?<!\s)([\n\t]|    )(?!\s)", big_entry)
        smaller_entries = [x for x in smaller_entries if match(r"[\n\t]|    ", x) is None]
        smaller_entries = add_to_by(smaller_entries)
        new_smaller_entries = []

        # Remove words before words with [] if they follow our rules
        # Remove <>[] from words
        for j, smaller_entry in enumerate(smaller_entries):
            new_sent = []
            smaller_entry = smaller_entry.split(" ")
            for i, word in enumerate(smaller_entry):
                if word.startswith("<") and word.endswith(">") and not word[1].isnumeric():
                    continue
                word = word.replace(">", "").replace("<", "").replace("^", "")
                if "wt." == word[-3:]:
                    word = word.replace("wt.", "wt")
                    smaller_entry[i] = word
                if word.startswith("[") and i-1 >= 0 and smaller_entry[i-1].lower().startswith(word[1].lower()):
                    new_sent.pop()
                elif is_exception(word, i, smaller_entry):
                    new_sent.pop()
                elif word.strip("[]<>^") == ".":
                    continue
                new_sent.append(word.strip("[]<>^").replace(">", "").replace("<", "").replace("^", "").replace("[", "").replace("]", ""))
            new_smaller_entries.append(" ".join(new_sent))
        

        parsed_entries_in_row = []
        # For entry in row
        for entry in new_smaller_entries:
            nlp = spacy.load("en_core_web_trf")
            entry = nlp(entry)

            entry = [x for x in entry if x.tag_ != "_SP"]

            # Sometimes spacy thinks folio is an incomplete word
            for x in entry:
                if x.text == "folio":
                    x.tag_ = "NN"
                    
            # Function to combine tokens based on the context
            # If they are probably the same thing we want to combine them to be the same thing
            def combine_tok_with_prev(entries: list, token, space: bool = True, new_ent: str = None, new_pos: str = None, toret: bool=False):
                old_text, old_ent, old_pos = entries.pop()
                if not toret:
                    token_text = token.text
                else:
                    token_text = token[0]

                if new_ent is None:
                    new_ent = old_ent
                if new_pos is None:
                    new_pos = old_pos
                
                if new_ent == "PRICE" and new_pos == "XX":
                    new_pos = "CD"

                if space:
                    new_word = (old_text + " " + token_text, new_ent, new_pos)
                else:
                    new_word = (old_text + token_text, new_ent, new_pos)
                
                if not toret:
                    entries.append(new_word)
                else:
                    return new_word

            # If the token is at the end of the entry and it looks like 8d or 10s or 5/8 it is probably the total price. 
            def isProbablyPrice(token):
                if not entry:
                    return False
                if type(token) is tuple:
                    return entry[-1] == token and (match(r"\d+[Lsdp]", token[0]) or match(r"((\:|(\d+))\/)?(\:|(\d+))\/(\:|(\d+))", token[0]))
                else:
                    return entry[-1] == token and (match(r"\d+[Lsdp]", token.text) or match(r"((\:|(\d+))\/)?(\:|(\d+))\/(\:|(\d+))", token.text))


            # Token stack
            new_entry = []

            # Labels specific tokens, and attmepts to combine as many adjacent tokens as possible into larger tokens 
            for i, token in enumerate(entry):
                # print(token.text, token.ent_type_, token.tag_)
                
                # Initialize prev_token to the last token in the stack and next_token to the next token in the token list
                prev_token = None
                next_token = None
                if new_entry:
                    prev_token = new_entry[-1]
                if i + 1 < len(entry):
                    next_token = entry[i + 1]

                # Remove strange things like cardinal numbers that are IDd as people
                if token.ent_type_ == "PERSON":
                    if "NN" not in token.tag_:
                        token.ent_type_ = ""

                # If we see the sheriff or the parish collector mark them as people
                if token.text.lower() == "sherriff" or token.text.lower() == "sheriff" or token.text.lower() == "parish" or token.text.lower() == "collector" or token.text.lower() == "parrish":
                    token.ent_type_ = "PERSON"
                    token.tag_ = "NNP"

                # Allows us to start elif chain
                if False:
                    pass

                # Handle special markers from tobacco marks, applying these ent_type and tag to the token following them
                elif token.text == "tobacco_mark_number":
                    new_entry.append(("", "TM#", "TMs"))
                
                elif token.text == "tobacco_mark_text":
                    new_entry.append(("", "TM.TEXT", "TMs"))

                # Handle special markers from multiline tobacco entries, applying these ent_type and tag to the token following them
                elif token.text == "tobacco_note":
                    new_entry.append(("", "TB_N", "MLTBE"))
                
                elif token.text == "total_weight":
                    new_entry.append(("", "TB_GW", "MLTBE"))

                elif token.text == "tare_weight":
                    new_entry.append(("", "TB_TW", "MLTBE"))

                elif token.text == "tobacco_weight":
                    new_entry.append(("", "TB_W", "MLTBE"))

                # In case we can't find the tobacco totaling line, mark this entry to be combined with the next entry later.
                elif token.text == "no_final_tobacco":
                    new_entry.append(("", "TB_NF", "MLTBE"))

                elif token.text == "final_weight":
                    new_entry.append(("", "TB_FW", "MLTBE"))
                
                elif token.text == "unit_price":
                    new_entry.append(("", "TB_UP", "MLTBE"))
                
                elif token.text == "tobacco_location":
                    new_entry.append(("", "TB_LOC", "MLTBE"))

                # Handle random tobacco notes like N 15  39
                elif token.text == "N" and next_token is not None and next_token.text.isnumeric():
                    new_entry.append(("", "TB_NOTE", "SLTBE"))

                elif prev_token is not None and prev_token[2] == "SLTBE":
                    if prev_token[0] == "":
                        combine_tok_with_prev(new_entry, token, space=False)
                    else:
                        if next_token is not None and next_token.text in ["pound", "pounds"]:
                            combine_tok_with_prev(new_entry, token)
                        else:
                            # print(next_token.text, new_entry[-1][0])
                            combine_tok_with_prev(new_entry, token, new_pos="SLTBE_F")
                            # print(next_token.text, new_entry[-1][0])

                # If we find a cardinal in the item set, it is probably not a cardinal.
                elif token.tag_ == "CD" and token.text.lower() in item_set:
                    new_entry.append((token.text, "", "NN"))

                # Label tokens indicating record type as TRANS
                elif token.text == "By" or token.text == "To":
                    new_entry.append((token.text, "TRANS", token.tag_))
 
                # Label end of list tokens as ENDER
                elif token.text == "Total" or token.text == "Subtotal":
                    new_entry.append((token.text, "ENDER", token.tag_))
                
                # Label prices as PRICE
                elif isProbablyPrice(token):
                    new_entry.append((token.text, "PRICE", _remove_xx(token.tag_, "CD")))
                
                # If we find something in the amount word index, combine it with the previous token and mark as amt unless there are no numbers to combine it with
                elif prev_token is not None and token.text.lower() in amount_set and (prev_token[2] in {"DT", "CD"} or prev_token[1] == "CARDINAL" or prev_token[1] == "QUANTITY" or prev_token[1] == "COMB.QUANTITY" or prev_token[1] == "AMT" or prev_token[0] in amount_set or prev_token[0].isnumeric()):
                    combine_tok_with_prev(new_entry, token, new_ent="AMT")

                elif token.text.lower() in amount_set and "VB" in token.tag_:
                    new_entry.append(("1 " + token.text, "AMT", "CARDINAL"))

                # Check for 1 ⅔ style mixed numbers, combine them if found
                # The regex makes extra super sure we don't have a price when we do this
                elif prev_token is not None and token.text.isnumeric() and prev_token[0].isnumeric() and search(r"(?<!/|\d)\d+\s[\u00BC-\u00BE\u2150-\u215E]", " ".join((prev_token[0], token.text))):
                    combine_tok_with_prev(new_entry, token)
                
                # Attempt to combine similar tokens into 1 token for easier parsing
                elif prev_token is not None and token.ent_type_ != "" and new_entry and token.ent_type_ == prev_token[1] and token.tag_ == prev_token[2]:
                    # Only combine cardinals if they are prices
                    if token.ent_type_ == "CARDINAL":
                        if entry[-1] == token:
                            # Is probably a price
                            combine_tok_with_prev(new_entry, token, new_ent="COMB.PRICE")
                        elif token.ent_type_ == "CARDINAL" and match(price_regex, token.text):
                            # Is probably a price
                            new_entry.append((token.text, "PRICE", _remove_xx(token.tag_, "CD")))
                        else:
                            new_entry.append((token.text, token.ent_type_, token.tag_))
                    # Label Liber things as LIBER when combining
                    elif prev_token[0] == "Liber" and token.text in "ABCDEFGabcdefg":
                        combine_tok_with_prev(new_entry, token, new_ent="LIBER")
                    # Combine normally
                    else:
                        combine_tok_with_prev(new_entry, token)
                
                # Apply the special markings to tobacco marks and multiline tobacco entires, markings were setup in the previous token.
                elif prev_token is not None and prev_token[2] in {"TMs", "MLTBE"} and prev_token[0] == "":
                    combine_tok_with_prev(new_entry, token, space=False)

                # Combine Quantities into 1 larger Quantity Token
                elif prev_token is not None and token.ent_type_ == "QUANTITY" and prev_token[1] == "QUANTITY":
                    token = (token.text, token.ent_type_, token.tag_)
                    while (prev_token is not None) and ("QUANTITY" in prev_token[1] or prev_token[2] == "CD" or prev_token[2] == "DT") and "PRICE" not in prev_token[1]:
                        token = combine_tok_with_prev(new_entry, token, new_ent="COMB.QUANTITY", new_pos=token[2], toret=True)
                        if new_entry:
                            prev_token = new_entry[-1]
                        else:
                            prev_token = None
                    new_entry.append(token)
                
                # Combine Dates into 1 larger date unless the date is probably a price misclassified as a date
                elif prev_token is not None and token.ent_type_ == "DATE" and prev_token[1] == "DATE" and "NN" in prev_token[2] and not isProbablyPrice(token):
                    combine_tok_with_prev(new_entry, token)
                
                # Combine Liber followed by A/B/C/D/etc.
                elif prev_token is not None and prev_token[0] == "Liber" and token.text in "ABCDEFGabcdefg":
                    combine_tok_with_prev(new_entry, token, new_ent="LIBER")

                # If we see money, combine it with any previous prices
                elif prev_token is not None and prev_token[1] == "PRICE" and token.ent_type_ == "MONEY":
                    combine_tok_with_prev(new_entry, token, new_ent="COMB.PRICE")

                # Combine nouns into larger nouns
                elif prev_token is not None and isNoun(token) and isNoun(prev_token):
                    # Don't do it if it is a verb gerund as that probably will be the start of a phrase describing the item, and not the item itself
                    if token.tag_ == "VBG" and "NN" in prev_token[2]:
                        new_entry.append((token.text, "NOUN.PHRASE", "IN"))
                    else:
                        combine_tok_with_prev(new_entry, token, new_ent="COMB.NOUN", new_pos=token.tag_)
                
                # If we see 10/ in the last token and : in this token, combine into a single price token of 10/:
                elif token.text == ":" and prev_token[0].endswith("/"):
                    combine_tok_with_prev(new_entry, token, space=False, new_ent="PRICE")
                
                # If there are a bunch of cardinal numbers at the end, combine them into 1 price
                elif prev_token is not None and entry[-1] == token and prev_token[2] == "CD" and token.tag_ == "CD" and not prev_token[0].isalpha():
                    combine_tok_with_prev(new_entry, token, new_ent="COMB.PRICE")
                
                # Combine [adj]+[noun] (+ is regex greedy +) into 1 big noun token
                elif prev_token is not None and isNoun(token) and "JJ" in prev_token[2]:
                    token = (token.text, token.ent_type_, token.tag_)
                    while prev_token is not None and isNoun(token) and prev_token[2] == "JJ":
                        token = combine_tok_with_prev(new_entry, token, new_ent="COMB.NOUN", new_pos=token[2], toret=True)
                        if new_entry:
                            prev_token = new_entry[-1]
                        else:
                            prev_token = None
                    new_entry.append(token)

                # Combine adverbs and verb participles
                elif prev_token is not None and prev_token[2] in ["VBN", "VBG"] and "RB" in token.tag_:
                    combine_tok_with_prev(new_entry, token)

                # Label Cash transactions as CASH
                elif token.text == "Cash":
                    new_entry.append((token.text, "CASH", token.tag_))
                
                # If we see coordinating conjunctions, attempt to combine the things they conjoin into 1 token, only do it if we find 2 nouns on either side of the CC
                elif prev_token is not None and prev_token[2] == "CC" and isNoun(token):
                    combine_tok_with_prev(new_entry, token, new_pos=token.tag_)
                elif prev_token is not None and isNoun(prev_token) and token.tag_ == "CC" and next_token is not None and isNoun(next_token):
                    # Only mark as COMB.NOUN if it is not an important entity (e.g. person)
                    if prev_token[1] == "":
                        combine_tok_with_prev(new_entry, token, new_ent="COMB.NOUN", new_pos=token.tag_)
                    else:
                        combine_tok_with_prev(new_entry, token, new_pos=token.tag_)
                
                # If the coordinating conjunction cannot find nouns on either side of it, mark as CC.DENIED so we don't try and combine it later
                elif token.tag_ == "CC":
                    # If there is what appears to be a price following the CC, allow certain entries to use that as extra money added on later
                    if next_token is not None and match(r"((\d+[Lsdp])|((\:|(\d+))\/)?(\:|(\d+))\/(\:|(\d+))?)", next_token.text):
                        new_entry.append((token.text, "CC.TOB", "CC.DENIED"))
                    else:
                        new_entry.append((token.text, "CC", "CC.DENIED"))
                
                # If a cardinal number is probably a price but is not at the end, mark as price
                elif token.ent_type_ == "CARDINAL" and match(price_regex, token.text):
                    new_entry.append((token.text, "PRICE", _remove_xx(token.tag_, "CD")))
                
                # Label Money ent type as price
                elif token.ent_type_ == "MONEY":
                    new_entry.append((token.text, "PRICE", _remove_xx(token.tag_, "CD")))

                # If we have a cardinal number that appears to be a price, mark it as such.
                elif prev_token is not None and prev_token[0] != "at" and token.tag_ == "CD" and match(price_regex, token.text):
                    new_entry.append((token.text, "PRICE", _remove_xx(token.tag_, "CD")))

                # Otherwise just add token to stack
                else:
                    new_entry.append((token.text, token.ent_type_, token.tag_))
            
            def stack_append(stack: list, token, info=None, tag=None):
                if info is None:
                    info = token[1]
                if tag is None:
                    tag = token[2]
                stack.append((token[0], info, tag))

            # Makes a second pass, checking for issues resulting from token combination
            token_stack = []
            for i, token in enumerate(new_entry):
                prev_token = None
                next_token = None
                if token_stack:
                    prev_token = token_stack[-1]
                if i + 1 < len(new_entry):
                    next_token = new_entry[i + 1]
                # TODO: Allow for bulk prices and overall prices at once.
                if token[0].lower() == "at" and next_token[1] in ["PRICE", "COMB.PRICE"]:
                    stack_append(token_stack, token, "IS.BULK")
                else:
                    stack_append(token_stack, token)

            # If we detect weird characters (e.g. *), stop processing the row
            parsed_entries_in_row.append(token_stack)
            if any((x[2] == "XX" for x in token_stack)):
                break
        
        # If there is weird stuff, we know we probably have a bad entry and we will pass it through as such.
        if any([x[2] == "XX" for x in chain(*parsed_entries_in_row)]):
            # print(f"Error, Bad entry: {big_entry}")
            # print(parsed_entries_in_row)
            if parsed_entries_in_row:
                parsed_entries_in_row[0] = "BAD_ENTRY"
                parsed_entries_in_row.append(big_entry)
            else:
                parsed_entries_in_row.append("BAD_ENTRY")
                parsed_entries_in_row.append(big_entry)
            yield (parsed_entries_in_row, row)

        else:
            yield (parsed_entries_in_row, row)    