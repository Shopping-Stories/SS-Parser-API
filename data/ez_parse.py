from unicodedata import numeric
import pandas as pd
from sys import argv
import nltk
import spacy
from re import split, match, search
from typing import List
from itertools import chain
from pickle import dump
from british_money import Money

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
                return int(num_str[0])
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



# Initial processing and labelling of transaction parts e.g. nouns, keywords, etc.
# Note that this is a generator due to it being slow
def preprocess(df: pd.DataFrame):
    parsed_entries = []
    # For row in df
    for key, row in df.iterrows():
        
        # Ignore rows with no entry text
        big_entry = get_col(row, "Entry")
        if big_entry == "-" or big_entry == "" or big_entry is None or str(big_entry) == "nan":
            continue
        
        # Split the entry by "    " or \n or \t
        smaller_entries = split(r"(?<!\s)([\n\t]|    )(?!\s)", big_entry)
        smaller_entries = [x for x in smaller_entries if match(r"[\n\t]|    ", x) is None]
        smaller_entries = add_to_by(smaller_entries)
        new_smaller_entries = []

        # Exceptions to the normal rule of not deleting words before [something] unless it starts with the same letter as something
        def is_exception(word, i, smaller_entry):
            if word == "[pound]" or word == "[pounds]" and i - 1 > 0 and smaller_entry[i - 1] == "w":
                return True
            elif word == "[thousand]" or word == "[thousands]" and i - 1 > 0 and smaller_entry[i - 1] in ["M", "m"]:
                return True
            else:
                return False

        # Remove words before words with [] if they follow our rules
        # Remove <>[] from words
        for j, smaller_entry in enumerate(smaller_entries):
            new_sent = []
            smaller_entry = smaller_entry.split(" ")
            for i, word in enumerate(smaller_entry):
                word = word.replace(">", "").replace("<", "")
                if word.startswith("[") and i-1 >= 0 and smaller_entry[i-1].startswith(word[1]):
                    new_sent.pop()
                elif is_exception(word, i, smaller_entry):
                    new_sent.pop()
                new_sent.append(word.strip("[]<>").replace(">", "").replace("<", ""))
            new_smaller_entries.append(" ".join(new_sent))
        
        parsed_entries_in_row = []
        # For entry in row
        for entry in new_smaller_entries:
            # print(entry)
            nlp = spacy.load("en_core_web_trf")
            entry = nlp(entry)
            entry = [x for x in entry if x.tag_ != "_SP"]

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
                return entry[-1] == token and (match(r"\d+[Lsdp]", token.text) or match(r"((\:|(\d+))\/)?(\:|(\d+))\/(\:|(\d+))", token.text))
            
            # Tells us if token is noun, accepts tuple(text, entity tag, grammar tag) or spacy token
            def isNoun(token):
                if type(token) is tuple:
                    return "VBG" in token[2] or "NN" in token[2] or "UH" in token[2]
                else:
                    return "VBG" in token.tag_ or "NN" in token.tag_ or "UH" in token.tag_

            # Replaces interjection with noun as there should be no interjections in the dataset
            def get_tag(token):
                if type(token) is tuple:
                    if token[2] == "UH":
                        return "NN"
                    return token[2]
                else:
                    if token.tag_ == "UH":
                        return "NN"
                    return token.tag_

            # Regex for the price
            price_regex = r"((\d+[Lsdp])|((\:|(\d+))\/)?(\:|(\d+))\/(\:|(\d+)))"
           
            # Token stack
            new_entry = []
            # print()
            # print(entry)
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

                # Allows us to start elif chain
                if False:
                    pass
                
                # Label tokens indicating record type as TRANS
                elif token.text == "By" or token.text == "To":
                    new_entry.append((token.text, "TRANS", token.tag_))
                
                # Label end of list tokens as ENDER
                elif token.text == "Total" or token.text == "Subtotal":
                    new_entry.append((token.text, "ENDER", token.tag_))
                
                # Label prices as PRICE
                elif isProbablyPrice(token):
                    new_entry.append((token.text, "PRICE", token.tag_))
                
                # Attempt to combine similar tokens into 1 token for easier parsing
                elif prev_token is not None and token.ent_type_ != "" and new_entry and token.ent_type_ == prev_token[1] and token.tag_ == prev_token[2]:
                    # Only combine cardinals if they are prices
                    if token.ent_type_ == "CARDINAL":
                        if entry[-1] == token:
                            # Is probably a price
                            combine_tok_with_prev(new_entry, token, new_ent="COMB.PRICE")
                        elif token.ent_type_ == "CARDINAL" and match(price_regex, token.text):
                            # Is probably a price
                            new_entry.append((token.text, "PRICE", token.tag_))
                        else:
                            new_entry.append((token.text, token.ent_type_, token.tag_))
                    # Label Liber things as LIBER when combining
                    elif prev_token[0] == "Liber" and token.text in "ABCDEFGabcdefg":
                        combine_tok_with_prev(new_entry, token, new_ent="LIBER")
                    # Combine normally
                    else:
                        combine_tok_with_prev(new_entry, token)
                
                # Combine Quantities into 1 larger Quantity Token
                elif prev_token is not None and token.ent_type_ == "QUANTITY" and prev_token[1] == "QUANTITY":
                    combine_tok_with_prev(new_entry, token, new_ent="COMB.QUANTITY")
                
                # Combine Dates into 1 larger date unless the date is probably a price misclassified as a date
                elif prev_token is not None and token.ent_type_ == "DATE" and prev_token[1] == "DATE" and "NN" in prev_token[2] and not isProbablyPrice(token):
                    combine_tok_with_prev(new_entry, token)
                
                # Combine Liber followed by A/B/C/D/etc.
                elif prev_token is not None and prev_token[0] == "Liber" and token.text in "ABCDEFGabcdefg":
                    combine_tok_with_prev(new_entry, token, new_ent="LIBER")
                
                # Combine nouns into larger nouns
                elif prev_token is not None and isNoun(token) and isNoun(prev_token):
                    combine_tok_with_prev(new_entry, token, new_ent="COMB.NOUN", new_pos=token.tag_)
                
                # If we see 10/ in the last token and : in this token, combine into a single price token of 10/:
                elif token.text == ":" and prev_token[0].endswith("/"):
                    combine_tok_with_prev(new_entry, token, space=False, new_ent="PRICE")
                
                # If there are a bunch of cardinal numbers at the end, combine them into 1 price
                elif prev_token is not None and entry[-1] == token and prev_token[2] == "CD" and token.tag_ == "CD":
                    combine_tok_with_prev(new_entry, token, new_ent="COMB.PRICE")
                
                # Combine [adj]+[noun] (+ is regex greedy +) into 1 big noun token
                elif prev_token is not None and isNoun(token) and prev_token[2] == "JJ":
                    token = (token.text, token.ent_type_, token.tag_)
                    while prev_token is not None and isNoun(token) and prev_token[2] == "JJ":
                        token = combine_tok_with_prev(new_entry, token, new_ent="COMB.NOUN", new_pos=token[2], toret=True)
                        if new_entry:
                            prev_token = new_entry[-1]
                        else:
                            prev_token = None
                    new_entry.append(token)

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
                    new_entry.append((token.text, "CC", "CC.DENIED"))
                
                # If a cardinal number is probably a price but is not at the end, mark as price
                elif token.ent_type_ == "CARDINAL" and match(price_regex, token.text):
                    new_entry.append((token.text, "PRICE", token.tag_))
                
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
                
                if token[0].lower() == "at" and next_token[1] in ["PRICE", "COMB.PRICE"]:
                    stack_append(token_stack, token, "IS.BULK")
                else:
                    stack_append(token_stack, token)


            # print(new_entry)
            # If we detect weird characters (e.g. *), stop processing the row
            parsed_entries_in_row.append(token_stack)
            if any((x[2] == "XX" for x in token_stack)):
                break
        
        # If there is weird stuff, we know we have a bad entry and we will drop it.
        if any([x[2] == "XX" for x in chain(*parsed_entries_in_row)]):
            print(f"Error, Bad entry: {big_entry}")
        else:
            yield (parsed_entries_in_row, row)    

# Deal with there sometimes being multiple entries in one entry.
def handle_multiple_prices(entry):
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
    
# Again, generator because slow.
# Parse the results of preprocess into json transactions
# Get the data into machine processable format ASAP
def get_transactions(df: pd.DataFrame):
    rows = preprocess(df)
    transactions = []

    # For all rows in the preprocessed df
    for entries, row in rows:
        # TODO: Ignore tobacco mark rows and column total rows for now
        if "TM" in get_col(row, "Entry") or search(r"\sN\s\d", get_col(row, "Entry")):
            continue
        # Remember specific things about the row
        row_context = {}
        # If there is no currency money, mark as contextless transaction
        if all([get_col(row, x) == "-" for x in ["L Currency", "s Currency", "d Currency", "L Sterling", "s Sterling", "d Sterling"]]):
            row_context["totaling_contextless"] = True
        # If there is not Colony Currency Money
        elif all([get_col(row, x) == "-" for x in ["L Currency", "s Currency", "d Currency"]]):
            row_context["currency_type"] = "Sterling"
            row_context["pounds"] = get_col(row, "L Sterling")
            row_context["shillings"] = get_col(row, "s Sterling")
            row_context["pennies"] = get_col(row, "d Sterling")
            row_context["money_obj"] = Money(l=row_context["pounds"], s=row_context["shillings"], d=row_context["pennies"], context=entries)
            row_context["totaling_contextless"] = False
        # If there is not British Sterling Currency Money, TODO: Switch these to any (!=) instead of all(==) due to commodities
        elif all([get_col(row, x) == "-" for x in ["L Sterling", "s Sterling", "d Sterling"]]):
            row_context["currency_type"] = "Currency"
            row_context["pounds"] = get_col(row, "L Currency")
            row_context["shillings"] = get_col(row, "s Currency")
            row_context["pennies"] = get_col(row, "d Currency")
            row_context["money_obj"] = Money(l=row_context["pounds"], s=row_context["shillings"], d=row_context["pennies"])
            row_context["totaling_contextless"] = False
        # TODO: Later this case will be for commodity totaling, right now this cannot run.
        else:
            row_context["totaling_contextless"] = False
       
        # Keep track how how many transactions are in the row
        trans_in_row_counter = 0
        # For all entries in the row
        for b_entry in entries:
            for entry in handle_multiple_prices(b_entry):
                # print(entry)
                # Keep track of all these things on an entry level
                transaction = {}
                nouns = []
                phrase_depth = 0
                phrases = []
                cur_phrase = []
                poss_amounts = []
                errors = []
                trans_in_row_counter += 1
                
                # For token in entry
                for word, info, pos in entry:
                    # Leftover code to uncombine coordinating conjunctions that failed to combine
                    if pos == "CC" and len(word.split(" ")) > 1:
                        word = " ".join(word.split(" ")[:-1])
                        pos = "NN"
                    
                    # If we don't know yet whether the row is a debit record or credit record and we
                    # see a word telling us that info, write it down
                    if info == "TRANS" and "debit_or_credit" not in row_context:
                        if word == "To":
                            row_context["debit_or_credit"] = "Dr"
                        elif word == "By":
                            row_context["debit_or_credit"] = "Cr"
                        else:
                            print(f"Error, unrecognized transaction type: {word} in {entry}")
                    
                    # Remember if the entry is a cash transaction
                    elif info == "CASH":
                        transaction["type"] = "Cash"

                    # Remember if the price is the unit price or the total price
                    elif info == "IS.BULK":
                        transaction["price_is_bulk"] = True

                    # Remember if the entry is a Liber transaction
                    elif info == "LIBER":
                        transaction["type"] = "Liber"
                        transaction["liber_book"] = word.split(" ")[1]

                    # Remember the price of the entry, marking if it is a complex price
                    elif info in ("PRICE", "COMB.PRICE"):
                        transaction["price"] = word
                        transaction["price_is_combo"] = False
                        if info == "COMB.PRICE":
                            transaction["price_is_combo"] = True
                    
                    # Remember all the nouns in the entry, setting item to the last noun not inside a phrase
                    elif "NN" in pos and phrase_depth == 0:
                        nouns.append((word, info, pos))
                        transaction["item"] = word

                    # If we see a verb gerund (noun) and there is no item in our transaction, it is probably a misclassification
                    elif "VBG" in pos and "item" not in transaction:
                        transaction["item"] = word
                    
                    # Remember all nouns, including verb gerund
                    elif "VBG" in pos:
                        nouns.append((word, info, pos))
                    
                    # If we see a verb and there is no item in our transaction and the verb is capitalized for some strange reason (i.e. its not a verb), mark it as our item
                    elif "VB" in pos and "item" not in transaction and word[0].isupper():
                        transaction["item"] = word
                    
                    # When we see nouns that are the object of phrases, only mark them as the item if the phrase starts with "for" or "of"
                    # as those are likely to be telling us what the transaction is FOR (of is often inside for e.g. for <verb gerund> of <item>) where verb gerund is like making or storing, etc.
                    elif "NN" in pos and phrase_depth > 0:
                        phrase_depth -= 1
                        nouns.append((word, info, pos))
                        if phrase_depth == 0:
                            cur_phrase.append(word)
                            phrases.append(cur_phrase)
                            if cur_phrase[0] == "for" or cur_phrase[0] == "of":
                                transaction["item"] = word
                            cur_phrase = []
                    
                    # If we see a definite cardinal number or quantity, write it down as the amount
                    elif info == "CARDINAL" or info == "QUANTITY" or info == "COMB.QUANTITY":
                        transaction["amount"] = word
                        if info == "COMB.QUANTITY":
                            transaction["amount_is_combo"] = True
                        else:
                            transaction["amount_is_combo"] = False
                    
                    # If we see a preposition that is not telling us the transaction type, mark the start of a phrase
                    elif pos == "IN" and info != "TRANS":
                        phrase_depth += 1
                    
                    # If there is a random cardinal that was not classified as definitely a cardinal, or there is a random determiner
                    # (determiners are words like a, an, the), mark it down as possibly an amount
                    elif pos == "CD" or pos == "DT":
                        poss_amounts.append(word)
                    
                    # If we see a list ender word, mark down the row as being an ender row.
                    elif info == "ENDER":
                        row_context["is_ender"] = True
                        transaction["type"] = "Ender"
                    
                    # Save all phrases contained in the entry for later
                    if phrase_depth > 0:
                        cur_phrase.append(word)
                    
                # Now we are done writing things down

                # Loop through the nouns in the entry, marking down people and dates as such, and remembering any other random nouns
                for noun in nouns:
                    if "item" not in transaction and "type" not in transaction:
                        errors.append(f"Error, failed to find item in {entry} even though entry has nouns.")
                        print(f"Error, failed to find item in {entry} even though entry has nouns.")
                    elif "item" in transaction and noun[0] == transaction["item"]:
                        pass
                    elif noun[1] == "PERSON":
                        if "people" in transaction:
                            transaction["people"].append(noun[0])
                        else:
                            transaction["people"] = [noun[0],]
                    elif noun[1] == "DATE":
                        if "date" in transaction:
                            transaction["date"] += " " + noun[0]
                        else:
                            transaction["date"] = noun[0]
                    else:
                        if "mentions" in transaction:
                            transaction["mentions"].append(noun[0])
                        else:
                            transaction["mentions"] = [noun[0],]
                
                # Save the phrases in the transaction
                transaction["phrases"] = phrases

                # If there is no amount and the transaction is an item transaction, reveal possible amounts so we can pick between them later
                # unless there is only 1 possible amount then that is probably the amount
                if "amount" not in transaction and "type" not in transaction:
                    if len(poss_amounts) == 1:
                        transaction["amount"] = poss_amounts[0]
                    else:
                        transaction["poss_amounts"] = poss_amounts

                # If there is not an item in the transaction and it is not a special type (e.g. Liber or Cash), error out.
                if "item" not in transaction and "type" not in transaction:
                    print(f"Error, failed to find item in {entry}")
                    errors.append(f"Error, failed to find item in {entry}")
                
                # If there is no price in the row and there is no price in the entries, error out
                if "price" not in transaction and row_context["totaling_contextless"] == True:
                    print(f"Error, failed to find price in transaction {entry}.")
                    errors.append(f"Error, failed to find price in transaction {entry}.")
                
                # If there is just a price in the transaction, save the amount of the transaction
                # Calculates total price for bulk prices
                elif "price" in transaction:
                    currency = Money(transaction["price"], context=transaction)
                    # Calculate total price is price is a unit price
                    if "price_is_bulk" in transaction and transaction["price_is_bulk"]:
                        if "amount" in transaction:
                            amount = parse_numbers(transaction["amount"])
                            currency *= amount
                        else:
                            print(f"Error, failed to find amount in transaction with bulk price, transaction is: {entry}.")             
                    
                    # Save the amount of the transaction
                    transaction["pounds"] = currency["pounds"]
                    transaction["shillings"] = currency["shillings"]
                    transaction["pennies"] = currency["pennies"]
                    transaction["money_obj"] = currency
                
                # If there is just a row total and no price, mark for this to be fixed later if this is the only transaction in the row.
                else:
                    transaction["fix_price"] = True

                # If there are errors with our parsing, show them in the transaction
                if errors:
                    transaction["errors"] = errors


                # Remember the totaling contextfulness of the transaction
                transaction["totaling_contextless"] = row_context["totaling_contextless"]

                # Append the transaction to the list
                transactions.append(transaction)
        
        # Fix prices on singular entry rows
        if transactions and trans_in_row_counter == 1 and not row_context["totaling_contextless"]:
            currency = row_context["money_obj"]
            transactions[-1]["pounds"] = currency["pounds"]
            transactions[-1]["shillings"] = currency["shillings"]
            transactions[-1]["pennies"] = currency["pennies"]
            transactions[-1]["money_obj"] = currency
            if "fix_price" in transactions[-1]:
                del transactions[-1]["fix_price"]

        # Yield transactions grouped by ends of lists of transactions
        if "is_ender" in row_context and row_context["is_ender"]:
            # Verify transactions add up if there are no errors
            if any(["errors" in x for x in transactions]):
                pass
            else:
                # TODO: Fix this
                total_money = sum([x["money_obj"] for x in transactions[:-1] if not x["totaling_contextless"] and "errors" not in x])
                if total_money == row_context["money_obj"]:
                    pass
                else:
                    print(f"Error: Totaling failed on entries: {[x for x in transactions]},\ntotal was {total_money}, and expected total was {row_context['money_obj']}")
                    print(total_money)
            yield transactions
            transactions = []
    


# Chains all the parsing functions together to actually parse df.
def parse(df: pd.DataFrame):
    out = get_transactions(df)
    for transaction in out:
        for row in transaction:
            print(row)
            print()
    
        
# Returns df[colname], allowing for some variations in the exact column names
# Column names include: "L Currency", "L Sterling", "Colony Currency", "Folio Year", "EntryID", etc.
def get_col(df, colname):
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
                return get_col(df, "Year")
            elif colname == "Colony Currency":
                return get_col(df, "Colony")
            elif len(col := colname.split(" ")) > 1:
                if col[1] == "Sterling":
                    return get_col(df, col[0])
                elif col[1] == "Currency":
                    return get_col(df, col[0] + ".1")
                raise KeyError(f"Column with name {colname} not in df")
            raise KeyError(f"Column with name {colname} not in df")

# Reads in an excel file and parses it, saving as csv for now
def parse_file(filePath):
    # print(f'dir is : {above}')

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
    
    parse(df)

    # entry_objs = parse()
    # # result_ids = db.parsedEntries.insert_many(entry_objs)
    # file = open('out.json', 'w')
    # dump(entry_objs, file)
    # file.close()
    # # print(type(entry_objs[0]["createdAt"]))
    # notAllNullableAccHolder = ["prefix", "accountFirstName", "accountLastName", "suffix"]
    # notAllNullableMeta = ["reel", "owner", "store"]

    # def is_good_entry(x):
    #     return not (x["dateInfo"]["year"] == '-')

    # entry_objs = [x for x in entry_objs if is_good_entry(x)]
    
    # for x in entry_objs:
    #     print(x)
    #     input()

    df.to_csv("out.csv")
    
# If we are executed directly from command line, parse the file given in the first argument to the program
if __name__ == "__main__":
    parse_file(argv[1])