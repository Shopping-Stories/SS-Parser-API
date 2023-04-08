import logging
import pandas as pd
from .preprocessor import preprocess
from .parser_utils import parse_numbers, isNoun, handle_multiple_prices, add_error, get_col, remember_nullable_cols, verify_ender_totaling, setup_row_currency
from .british_money import Money
import traceback
from .indices import item_set
from re import split, search
from .people import Person, relationships, namelist

# Replace this with prints if you want to debug
def print_debug(message=""):
    pass

# Parse the results of preprocess into json transactions
# Get the data into machine processable format ASAP
def get_transactions(df: pd.DataFrame):
    logging.info("Getting transactions")
    rows = preprocess(df)
    transactions = []
    break_transactions = False
    break_counter = 0
    transactions_context = {}

    def add_errors_to_transactions():
            # Do a pass on all transactions, making sure they all have money or commodity listed on them.
            # Add errors to transactions if they are missing these.
            # We have to indirect it through this list because editing a list you are looping through produces strange behaviour.
            to_add_errors = []
            for i, transaction in enumerate(transactions):
                if any([x not in transaction for x in ("pennies_ster", "pounds_ster", "shillings_ster")]) and any([x not in transaction for x in ("pounds", "shillings", "pennies")]) and any(["Commodity" not in transaction, "Quantity" not in transaction]):
                    to_add_errors.append(i)
            for i in to_add_errors:
                # Exceptions to errors for specific transaction types
                if "item" in transactions[i] and "tobacco" in transactions[i]["item"].lower():
                    continue
                if "type" in transactions[i] and "Cash" == transactions[i]["type"]:
                    continue
                if "context" in transactions[i]:
                    add_error(transactions[i], f"No prices or commodities found in transaction.", transactions[i]["context"])
                else:
                    add_error(transactions[i], f"No prices or commodities found in transaction.", "")

    # For all rows in the preprocessed df
    for entries, row in rows:
        # Remember specific things about the row
        row_context = {}

        # Setup the currency values in the row
        setup_row_currency(row_context, row, entries, transactions_context)
        
        # Should not be possible to run this, just here to alert us of errors if they happen
        if "currency_type" not in row_context and "currency_totaling_contextless" not in row_context:
            print_debug("Error: unreachable code being run")
            row_context["currency_totaling_contextless"] = False

        # Detect if we need to do commodity totaling
        if all([get_col(row, x) in ["-", " -", "- "] for x in ["Quantity", "Commodity"]]):
            row_context["commodity_totaling_contextless"] = True
        
        # If we find both a quantity and a commodity, mark the transaction for commodity totaling
        elif all([get_col(row, x) != "-" for x in ["Quantity", "Commodity"]]):
            row_context["commodity_totaling_contextless"] = False
        
        # Do not do commodity totaling if there is only partial information 
        else:
            row_context["commodity_totaling_contextless"] = True

        # Remember account holder, data, folio reference, etc.
        acct_name = " ".join([get_col(row, x).strip() for x in ("Prefix", "Account First Name", "Account Last Name", "Suffix") if get_col(row, x) != "-"])
        if "[" in acct_name:
            acct_name = split(r"\s+", acct_name)
            n_acct_name = []
            for word in acct_name:
                if "[" in word:
                    n_acct_name.pop()
                n_acct_name.append(word.strip("[]"))
            acct_name = " ".join(n_acct_name)
        
        row_context["account_name"] = acct_name

        row_context["reel"] = get_col(row, "Reel")
        row_context["store_owner"] = get_col(row, "Owner")
        row_context["folio_year"] = get_col(row, "Folio Year")
        row_context["folio_page"] = get_col(row, "Folio Page")
        row_context["entry_id"] = str(get_col(row, "EntryID"))
        row_context["store"] = get_col(row, "Store")
        row_context["genmat"] = (int(float(str(get_col(row, "GenMat")))), get_col(row, "EntryID"))
        row_context["currency_colony"] = get_col(row, "Colony Currency")
        row_context["original_entry"] = get_col(row, "Entry")

        # For all nullable entries, do not remember them if they are null.
        nullable_entries = ["Marginalia", "Date Year", "_Month", "Day", "Folio Reference", "Quantity", "Commodity", "Final"]
        remember_nullable_cols(row_context, nullable_entries, row)

        # Keep track how how many transactions are in the row
        trans_in_row_counter = 0

        # First check if it is a bad row, if so, make a mostly empty transaction with an error.
        if entries and entries[0] == "BAD_ENTRY":
            add_error(row_context, f"Bad entry: {entries[-1]}.", entries)
            transaction = {}
            # Still add in everything from row context to assist in manual editing process later.
            for key, value in row_context.items():
                if "pounds" in key or "shillings" in key or "pennies" in key or "farthings" in key:
                    pass
                else:
                    if key == "money_obj":
                        currency = row_context["money_obj"]
                        transaction["original_money_obj"] = value
                        transaction["pounds"] = currency["pounds"]
                        transaction["shillings"] = currency["shillings"]
                        transaction["pennies"] = currency["pennies"]
                        transaction["farthings"] = currency["farthings"]
                    elif key == "money_obj_ster":
                        currency = row_context["money_obj_ster"]
                        transaction["pounds_ster"] = currency["pounds"]
                        transaction["shillings_ster"] = currency["shillings"]
                        transaction["pennies_ster"] = currency["pennies"]
                        transaction["farthings_ster"] = currency["farthings"]
                        transaction["original_money_obj_ster"] = value
                    elif key not in transaction:
                        if "farthings" not in key:
                            transaction[key] = value
            
            add_error(transaction, f"Bad entry: {entries[-1]}.", entries)
            trans_in_row_counter += 1
            # Break the transaction list when the account holder changes if a total has not occurred.
            if transactions and "account_name" in transaction and "account_name" in transactions[-1] and transaction["account_name"] != transactions[-1]["account_name"]:
                break_transactions = True
            if break_transactions:
                break_counter += 1
            transactions.append(transaction)
        
        # If row has a good entry
        else:
            # For all entries in the row
            addPriceToItem = False
            for b_entry in entries:
                # print(b_entry)
                for entry in handle_multiple_prices(b_entry):
                    # Skip entries with nothing Fin them
                    if len(entry) == 1:
                        continue

                    # Keep track of all these things on an entry level
                    transaction = {}
                    nouns = []
                    phrase_depth = 0
                    phrases = []
                    cur_phrase = {"modifies": "", "phrase": []}
                    poss_amounts = []
                    errors = []
                    tobacco_marks = []
                    cur_tobacco_entry = {}
                    tobacco_entries = []
                    trans_in_row_counter += 1

                    # For token in entry
                    for i, ex in enumerate(entry):
                        word, info, pos = ex
                        if i - 1 >= 0:
                            prev_word, prev_info, prev_pos = entry[i-1]
                        else:
                            prev_word = None
                            prev_info = None
                            prev_pos = None

                        if i + 1 < len(entry):
                            next_word, next_info, next_pos = entry[i + 1]
                        else:
                            next_word = None
                            next_info = None
                            next_pos = None
                        
                        # print_debug(word, info, pos)
                        # Leftover code to uncombine coordinating conjunctions that failed to combine
                        if pos == "CC" and len(word.split(" ")) > 1:
                            word = " ".join(word.split(" ")[:-1])
                            pos = "NN"
                        
                        # If the previous word was coordinating conjunection in a tobacco entry and a price follows it,
                        # that price is part of what was transacted.
                        if addPriceToItem and "PRICE" in info:
                            transaction["item"] += " & " + word
                            continue
                        
                        # Ignore IGNORE_PRICES tokens
                        if pos == "IGNORE_PRICES":
                            continue

                        if addPriceToItem:
                            addPriceToItem = False

                        # If we don't know yet whether the row is a debit record or credit record and we
                        # see a word telling us that info, write it down
                        if info == "TRANS" and "debit_or_credit" not in row_context:
                            if word == "To":
                                row_context["debit_or_credit"] = "Dr"
                            elif word == "By":
                                row_context["debit_or_credit"] = "Cr"
                            else:
                                print_debug(f"Error, unrecognized transaction type: {word} in {entry}")

                        elif "fancy_" in pos:
                            unit = pos.split("_")[1]

                            if unit == "pounds":
                                transaction["real_price"] = word
                            elif unit != "frac":
                                transaction["real_price"] += "/" + word
                            elif unit == "frac" and word != "0":
                                transaction["real_price"] += " " + word

                        # Handle our date regex to fix dates later
                        elif pos == "DATE_REGEX":
                            if info == "DATE.MONTH":
                                transaction["date_month"] = word
                            
                            elif info == "DATE.DAY":
                                transaction["date_day"] = word

                        # Handle multiline tobacco entries
                        elif pos == "MLTBE":
                            if info == "TB_LOC":
                                transaction["tobacco_location"] = word

                            elif info == "TB_N":
                                cur_tobacco_entry["number"] = word
                                transaction["item"] = "Tobacco"

                            elif info == "TB_GW":
                                cur_tobacco_entry["gross_weight"] = word
                            
                            elif info == "PRICE":
                                transaction["price"] = word

                            elif info == "TB_TW":
                                cur_tobacco_entry["tare_weight"] = word

                            # When we get to the tobacco weight, append the tobacco entry to the row context.
                            elif info == "TB_W":
                                cur_tobacco_entry["weight"] = word

                                tobacco_entries.append(cur_tobacco_entry)
                                transaction["tobacco_entries"] = tobacco_entries
                                                                
                                # If the math doesn't work out
                                if int(gross := cur_tobacco_entry["gross_weight"]) - int(tare := cur_tobacco_entry["tare_weight"]) != int(tobacco := cur_tobacco_entry["weight"]):
                                    add_error(transaction, f"Error: Tobacco entry weights don't add up. Gross {gross} - Tare {tare} != tobacco {tobacco}", entry)
                                
                                cur_tobacco_entry = {}

                            # If we see that we can't find the final tobacco weight
                            elif info == "TB_NF" and "Quantity" not in row_context:
                                add_error(transaction, f"Error: Cannot find final tobacco weight in this tobacco transaction, likely indicates multiple tobacco transactions rolled into 1 in a later transaction", entry)
                            
                            # Remember tobacco weight as amount, and unit price as price
                            elif info == "TB_FW":
                                transaction["amount"] = word

                            elif info == "TB_UP":
                                transaction["price"] = word

                        # Handle random tobacco notes
                        elif pos == "SLTBE_F":
                            m = search(r"(\d+)\s+(\d+)", word)
                            if m is None:
                                add_error(transaction, "Complex note confused parser.", word)
                            else:
                                cur_tobacco_entry["number"] = m.group(1)
                                cur_tobacco_entry["weight"] = m.group(2)
                                tobacco_entries.append(cur_tobacco_entry)
                                transaction["tobacco_entries"] = tobacco_entries
                                cur_tobacco_entry = {}
                                transaction["item"] = "Tobacco"
                                transaction["amount_unreliable"] = True
                        
                        # Handle tobacco marks
                        elif info == "TM.TEXT":
                            # The previous token is guaranteed to be the mark number if this one is the mark text
                            tobacco_marks.append({"mark_number": prev_word, "mark_text": word})

                        # Remember if the entry is a cash transaction
                        elif info == "CASH":
                            transaction["type"] = "Cash"

                        # Remember if the price is the unit price or the total price
                        elif info == "IS.BULK":
                            transaction["price_is_bulk"] = True

                        # Specific carve out for tobacco, auto add it to commodity if found
                        elif word.lower() == "tobacco":
                            if "Quantity" in row_context and row_context["Quantity"] not in {"", "-"}:
                                row_context["Commodity"] = "Tobacco"
                            
                            transaction["item"] = "Tobacco"

                        # In case of not being able to find nouns for the coordinating conjunction
                        elif pos == "CC.DENIED":
                            # If this is a tobacco entry and the next 
                            if "item" in transaction and "tobacco" in transaction["item"].lower() and info == "CC.TOB":
                                addPriceToItem = True
                            # Mark as error if we can't figure out how to use the Coordinating Conjunction
                            else:
                                add_error(transaction, f"Error: Likely parsing failure due to complex use of coordinating conjunction.", entry)

                        # Remember if the entry is a Liber transaction
                        elif info == "LIBER":
                            transaction["type"] = "Liber"
                            transaction["liber_book"] = word.split(" ")[1]

                        # If we see something from the amount index, search for a noun following it and if it exists, mark this as the real amount
                        elif info == "AMT":
                            if prev_word is not None and prev_word.lower() == "off":
                                transaction["tobacco_off"] = word
                            # If it is in a phrase, use poss_amounts instead as if this is the only amount it will still get set the amount at the end
                            if phrase_depth == 0:
                                if "item" in transaction and "tobacco" in transaction["item"].lower():
                                    pass
                                else:
                                    # Make sure that there is a noun after the amount
                                    if i < len(entry) - 1:
                                        flag = False
                                        for w, e_m, tag in entry[i + 1:]:
                                            # Allow verbs to be nouns for this purpose
                                            if (isNoun((w, e_m, tag))  and e_m != "DATE") or "VB" in tag or w.lower() in item_set:
                                                flag = True
                                                break
                                        # If we find a noun after the amount
                                        if flag:
                                            transaction["amount"] = word
                                            # Remove the currently found item as it is definitely not right
                                            if "item" in transaction:
                                                del transaction["item"]
                                            if info == "COMB.QUANTITY":
                                                transaction["amount_is_combo"] = True
                                            else:
                                                transaction["amount_is_combo"] = False
                                        else:
                                            poss_amounts.append(word)
                                    else:
                                        poss_amounts.append(word)
                            else:
                                poss_amounts.append(word)

                        # Remember the price of the entry, marking if it is a complex price
                        elif info in ("PRICE", "COMB.PRICE"):
                            # If there is an existing price with a / in it it might be an amount, so write that down as a poss amount.
                            if "price" in transaction and "/" in transaction["price"]: 
                                poss_amounts.append(transaction["price"])
                            
                            # If this is a pennyweight for nails, add it to the following token as a modifier
                            if next_word is not None and next_word.lower() == "nails":
                                entry[i + 1] = (f"{word} {next_word}", "", "NN")
                                continue

                            # Write this down as the price
                            transaction["price"] = word
                            transaction["price_is_combo"] = False
                            
                            # Add combination metadata
                            if info == "COMB.PRICE":
                                transaction["price_is_combo"] = True
                        
                        # Remember all the nouns in the entry, setting item to the last noun not inside a phrase
                        elif "NN" in pos and phrase_depth == 0:
                            nouns.append((word, info, pos))
                            # If something is an organization, don't set it as the item
                            if info != "ORG" or word.lower() in item_set:
                                if ((info == "PERSON" or info == "DATE") and word.lower() not in item_set) or ("item" in transaction and "tobacco" in transaction["item"].lower()):
                                    nouns.append((word, info, pos))
                                else:
                                    if prev_pos is not None and "JJ" in prev_pos:
                                        transaction["item"] = f"{prev_word} {word}"
                                    else:
                                        transaction["item"] = word
                                    # If there is a per phrase in the noun, split it out.
                                    if search(r"\s+Per\s+", transaction["item"]):
                                        transaction["item"] = split(r"\s+Per\s+", transaction["item"])[0]

                        # If the preprocesser thinks we have an interjection but it is in the item set, it is probably the item
                        elif "UH" in pos and word.lower() in item_set:
                            if prev_pos is not None and "JJ" in prev_pos:
                                transaction["item"] = f"{prev_word} {word}"
                            else:
                                transaction["item"] = word

                        # If we see a verb gerund (noun) and there is no item in our transaction, it is probably a misclassification
                        elif "VBG" in pos and "item" not in transaction:
                            if prev_pos is not None and "JJ" in prev_pos:
                                transaction["item"] = f"{prev_word} {word}"
                            else:
                                transaction["item"] = word
                        
                        # Remember all nouns, including verb gerund
                        elif "VBG" in pos:
                            nouns.append((word, info, pos))
                        
                        # If we see a verb and there is no item in our transaction and the verb is capitalized for some strange reason (i.e. its not a verb), mark it as our item
                        elif "VB" in pos and "item" not in transaction and word[0].isupper():
                            if prev_pos is not None and "JJ" in prev_pos:
                                transaction["item"] = f"{prev_word} {word}"
                            else:
                                transaction["item"] = word

                        # If we see a verb in the transaction and it is in the object index, it is actually the item.
                        elif "VB" in pos and word.lower() in item_set:
                            if prev_pos is not None and "JJ" in prev_pos:
                                transaction["item"] = f"{prev_word} {word}"
                            else:
                                transaction["item"] = word

                        # Same thing as above but for adjective/adverb
                        elif "JJ" in pos and "item" not in transaction and word.lower() in item_set:
                            if prev_pos is not None and "JJ" in prev_pos:
                                transaction["item"] = f"{prev_word} {word}"
                            else:
                                transaction["item"] = word

                        # If we see something appearing to be a verb in a short entry it is probably the item 
                        elif len(entry) <= 4 and "VB" in pos and "item" not in transaction:
                            transaction["item"] = word
                        
                        # When we see nouns that are the object of phrases, only mark them as the item if the phrase starts with "for" or "of"
                        # as those are likely to be telling us what the transaction is FOR (of is often inside for e.g. for <verb gerund> of <item>) where verb gerund is like making or storing, etc.
                        # Only do this if the item is in the object index, though, as it might be a person. 
                        elif "NN" in pos and phrase_depth > 0:
                            phrase_depth -= 1
                            nouns.append((word, info, pos))
                            if phrase_depth == 0:
                                cur_phrase["phrase"].append(word)
                                phrases.append(cur_phrase)
                                if (cur_phrase["phrase"][0] == "for" or cur_phrase["phrase"][0] == "of") and word.lower() in item_set:
                                    if "item" in transaction and "tobacco" in transaction["item"].lower():
                                        nouns.append((word, info, pos))
                                    else:
                                        transaction["item"] = word
                                cur_phrase = {"modifies": "", "phrase": []}
                        
                        # Same as above but for COMB.NOUNs with weird POS identification.
                        elif "COMB.NOUN" == info and phrase_depth > 0:
                            phrase_depth -= 1
                            nouns.append((word, info, pos))
                            if phrase_depth == 0:
                                cur_phrase["phrase"].append(word)
                                phrases.append(cur_phrase)
                                if (cur_phrase["phrase"][0] == "for" or cur_phrase["phrase"][0] == "of") and word.lower() in item_set:
                                    if "item" in transaction and "tobacco" in transaction["item"].lower():
                                        nouns.append((word, info, pos))
                                    else:
                                        transaction["item"] = word
                                cur_phrase = {"modifies": "", "phrase": []}

                        # If we see a definite cardinal number or quantity, write it down as the amount
                        elif info == "CARDINAL":
                            # Make sure that there is a noun after the amount
                            if i < len(entry) - 1:
                                flag = False
                                for w, e_m, tag in entry[i + 1:]:
                                    # Allow verbs to be nouns for this purpose
                                    if (isNoun((w, e_m, tag)) and e_m != "PERSON" and e_m != "DATE") or "VB" in tag or w.lower() in item_set:
                                        flag = True
                                        break
                                # If we find a noun after the amount
                                if flag and not ("item" in transaction and "tobacco" in transaction["item"].lower()):
                                    transaction["amount"] = word
                                    
                                    # Remove the currently found item as it is definitely not right
                                    if "item" in transaction:
                                        del transaction["item"]

                                    if info == "COMB.QUANTITY":
                                        transaction["amount_is_combo"] = True
                                    else:
                                        transaction["amount_is_combo"] = False
                            else:
                                # Not an amount since it is at the end
                                pass
                        
                        # If we have something labelled specifically as quantity, write it down as amount
                        elif info == "QUANTITY" or info == "COMB.QUANTITY":
                            # Make sure that there is a noun after the amount
                            if i < len(entry) - 1:
                                flag = False
                                for w, e_m, tag in entry[i + 1:]:
                                    # Allow verbs to be nouns for this purpose
                                    if (isNoun((w, e_m, tag)) and e_m != "PERSON" and e_m != "DATE") or "VB" in tag or w.lower() in item_set:
                                        flag = True
                                        break
                                # If we find a noun after the amount
                                if flag and not ("item" in transaction and "tobacco" in transaction["item"].lower()):
                                    transaction["amount"] = word

                                    # Remove the currently found item as it is definitely not right
                                    if "item" in transaction:
                                        del transaction["item"]
                                    
                                    if info == "COMB.QUANTITY":
                                        transaction["amount_is_combo"] = True
                                    else:
                                        transaction["amount_is_combo"] = False
                                else:
                                    # If we don't find an amount, still write this down as a possible amount since it was marked as a quantity
                                    poss_amounts.append(word)
                            else:
                                # Still append to poss_amounts even if this is the last thing in the transaction
                                poss_amounts.append(word)
                        
                        # If we see \d+ off in a tobacco transaction mark it as an amount off
                        elif word.lower() == "off" and "item" in transaction and ("Tobacco" in transaction["item"].lower() or ("Commodity" in row_context and "tobacco" in row_context["Commodity"].lower())) and prev_word is not None and prev_word.isnumeric():
                            transaction["tobacco_amount_off"] = prev_word

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
                        
                        # Save all phrases contained in the entry for later, along with which words they modify.
                        # If the thing the phrase modifies is in the item set and we have not id'd an item yet,
                        # that is probably the item.
                        if phrase_depth > 0:
                            if prev_word is not None and phrase_depth == 1:
                                if "modifies" not in cur_phrase or cur_phrase["modifies"] == "":
                                    cur_phrase["modifies"] = prev_word
                                    if prev_word.lower() in item_set and "item" not in transaction:
                                        transaction["item"] = prev_word
                            cur_phrase["phrase"].append(word)
                        
                    # Now we are done writing things down
                    
                    # If we saw a fancy price, override the price with that and unset price_is_bulk
                    if "real_price" in transaction:
                        transaction["price"] = transaction["real_price"]
                        if "price_is_bulk" in transaction:
                            del transaction["price_is_bulk"]

                    # Make sure we have an item in our transaction
                    if "item" not in transaction and "type" not in transaction:
                        # Check if any of the nouns are probably the item
                        for noun in nouns:
                            if noun[0].lower() in item_set:
                                transaction["item"] = noun[0]
                        
                        if "item" not in transaction:
                            # Failed to find item in entry even though we have nouns.
                            if "Quantity" in row_context and "Commodity" in row_context:
                                # Item is probably whatever the commodity is
                                transaction["item"] = row_context["Commodity"]
                            else:
                                # The item is probably nothing, just money
                                print_debug(f"Could not find item in entry {entry}.")
                                transaction["item"] = "Currency"

                    # Loop through the nouns in the entry, marking down people and dates as such, and remembering any other random nouns
                    # transaction["nouns"] = nouns
                    for noun in nouns:
                        if "item" in transaction and noun[0] == transaction["item"]:
                            pass
                        elif noun[1] == "PERSON":
                            # Don't put the person in the people list if they are already in there
                            if "people" in transaction:
                                if noun[0] not in transaction["people"]:
                                    transaction["people"].append(noun[0])
                            else:
                                transaction["people"] = [noun[0],]
                        elif noun[1] == "DATE":
                            if "date" in transaction and transaction["date"] != noun[0]:
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

                    # Save the tobacco marks in the transaction
                    transaction["tobacco_marks"] = tobacco_marks

                    # If there is no amount and the transaction is an item transaction, reveal possible amounts so we can pick between them later
                    # unless there is only 1 possible amount then that is probably the amount
                    if "amount" not in transaction and "type" not in transaction:
                        if len(poss_amounts) == 1:
                            transaction["amount"] = poss_amounts[0]
                        else:
                            transaction["poss_amounts"] = poss_amounts
                    
                    if "amount" in transaction:
                        transaction["amount"] = transaction["amount"].strip()

                    # If there is not an item in the transaction and it is not a special type (e.g. Liber or Cash), error out.
                    if "item" not in transaction and "type" not in transaction:
                        print_debug(f"Error, failed to find item in {entry}")
                        errors.append(f"Error, failed to find item in {entry}")
                    
                    # If there is no price in the row and there is no price in the entries, error out if there is also no commodity
                    if "price" not in transaction and row_context["currency_totaling_contextless"] == True and row_context["commodity_totaling_contextless"] == True:
                        if "item" in transaction and transaction["item"] != "Tobacco":
                            print_debug(f"Error, failed to find price in transaction {entry}.")
                            errors.append(f"Error, failed to find price in transaction {entry}.")
                    
                    # If there is just a price in the transaction, save the amount of the transaction
                    # Calculates total price for bulk prices
                    elif "price" in transaction:
                        try:
                            currency = Money(transaction["price"], context=transaction)
                            # Calculate total price is price is a unit price
                            if "price_is_bulk" in transaction and transaction["price_is_bulk"]:
                                if "amount" in transaction:
                                        amount = parse_numbers(transaction["amount"])
                                        currency *= amount
                                else:
                                    print_debug(f"Error, failed to find amount in transaction with bulk price, transaction is: {entry}.")             
                            
                            # If we are in a totaling contextless transaction, make sure we still put values for pounds, shillings, and pennies. We assume that it uses colony currency  
                            if "currency_type" not in row_context:
                                transaction["pounds"] = currency["pounds"]
                                transaction["shillings"] = currency["shillings"]
                                transaction["pennies"] = currency["pennies"]
                                transaction["farthings"] = currency["farthings"]
                                transaction["money_obj"] = currency
                            else:
                                # Save the amount of the transaction
                                if row_context["currency_type"] == "Sterling":
                                    transaction["pounds_ster"] = currency["pounds"]
                                    transaction["shillings_ster"] = currency["shillings"]
                                    transaction["pennies_ster"] = currency["pennies"]
                                    transaction["farthings_ster"] = currency["farthings"]
                                    transaction["money_obj_ster"] = currency
                                else:
                                    transaction["pounds"] = currency["pounds"]
                                    transaction["shillings"] = currency["shillings"]
                                    transaction["pennies"] = currency["pennies"]
                                    transaction["farthings"] = currency["farthings"]
                                    transaction["money_obj"] = currency
                                
                        except Exception as e:
                            add_error(transaction, traceback.format_exc(), entry)
                            add_error(transactions_context, traceback.format_exc(), entry)

                    # If there is just a row total and no price, mark for this to be fixed later if this is the only transaction in the row.
                    else:
                        transaction["fix_price"] = True

                    # If there are errors with our parsing, show them in the transaction
                    if errors:
                        transaction["errors"] = errors
                    
                    # Add any errors from the row into the errors in the individual transactions
                    if "errors" in row_context:
                        add_error(transaction, "Error from row context: " + str(row_context["errors"]), entry)
                    
                    # Remember everything from the row that we haven't remembered already, except for money and commodities
                    for key, value in row_context.items():
                        if "pounds" in key or "shillings" in key or "pennies" in key or "farthings" in key or key == "Quantity" or key == "Commodity":
                            pass
                        else:
                            if key == "money_obj":
                                transaction["original_money_obj"] = value
                            elif key == "money_obj_ster":
                                transaction["original_money_obj_ster"] = value
                            elif key not in transaction:
                                if "farthings" not in key:
                                    transaction[key] = value


                    # Break the transaction list when the account holder changes if a total has not occurred.
                    if transactions and "account_name" in transaction and "account_name" in transactions[-1] and transaction["account_name"] != transactions[-1]["account_name"]:
                        break_transactions = True
                    if break_transactions:
                        break_counter += 1
                    
                    # Save the entry for debug
                    transaction["context"] = entry

                    if "amount_unreliable" in transaction:
                        if "amount" in transaction:
                            del transaction["amount"]

                    # Append the transaction to the list
                    transactions.append(transaction)
        
        # Print out any errors in row context for debugging
        if "errors" in row_context:
            print_debug(f"Error in row: {row_context['errors']}\nFull row context was {row_context}")

        # Fix prices on singular entry rows
        # Should not be able to raise errors
        if transactions and trans_in_row_counter == 1 and not row_context["currency_totaling_contextless"]:
            # If both sterling and currency
            if "currency_type" not in row_context:
                pass

            elif row_context["currency_type"] == "Both":
                currency = row_context["money_obj"]
                transactions[-1]["pounds"] = currency["pounds"]
                transactions[-1]["shillings"] = currency["shillings"]
                transactions[-1]["pennies"] = currency["pennies"]
                transactions[-1]["farthings"] = currency["farthings"]
                transactions[-1]["money_obj"] = currency
                currency = row_context["money_obj_ster"]
                transactions[-1]["pounds_ster"] = currency["pounds"]
                transactions[-1]["shillings_ster"] = currency["shillings"]
                transactions[-1]["pennies_ster"] = currency["pennies"]
                transactions[-1]["farthings_ster"] = currency["farthings"]
                transactions[-1]["money_obj_ster"] = currency
            
            # If just sterling
            elif row_context["currency_type"] == "Sterling":
                currency = row_context["money_obj_ster"]
                transactions[-1]["pounds_ster"] = currency["pounds"]
                transactions[-1]["shillings_ster"] = currency["shillings"]
                transactions[-1]["pennies_ster"] = currency["pennies"]
                transactions[-1]["farthings_ster"] = currency["farthings"]
                transactions[-1]["money_obj_ster"] = currency
            
            # If just currency
            elif row_context["currency_type"] == "Currency":
                currency = row_context["money_obj"]
                transactions[-1]["pounds"] = currency["pounds"]
                transactions[-1]["shillings"] = currency["shillings"]
                transactions[-1]["pennies"] = currency["pennies"]
                transactions[-1]["farthings"] = currency["farthings"]
                transactions[-1]["money_obj"] = currency

            # Remove fix_price markings    
            if "fix_price" in transactions[-1]:
                del transactions[-1]["fix_price"]

        # Fix commodities on singular entry rows
        if transactions and trans_in_row_counter == 1:
            if "Commodity" in row_context:
                transactions[-1]["Commodity"] = row_context["Commodity"]
            if "Quantity" in row_context:
                transactions[-1]["Quantity"] = row_context["Quantity"]

        for transaction in transactions:
            # Fix up people and mentions fields before we check genmat
            entry = transaction

            # If we see Per [person] at end of transaction, it automatically should apply to all items in the transaction
            if "original_entry" in transaction:
                mtch = search(r"\[?(([pP]er)|([fF]or))\]?(( \[?[A-Za-z\.]+\]?){1,4})\s*$", transaction["original_entry"])
                if mtch:
                    if "people" in entry:
                        entry["people"].append(mtch.group(4).replace("[", "").replace("]", "").strip())
                    else:
                        entry["people"] = [mtch.group(4).replace("[", "").replace("]", "").strip(), ]

            if "people" in entry:
                # If people identified are "wife", lookup who that refers to, if there is only a first name, write down the acct holder as well to help later on
                newPeople = []
                lowerPeople = set()
                # print(frozenset([x.lower() for x in entry["people"]]))
                for word in frozenset([x.lower() for x in entry["people"]]):
                    if " of " in word or " from acct " in word or " or " in word:
                        newPeople.append(word)
                        continue
                    
                    # Ignore common non-people words that often get recognized as people
                    if word.lower().replace(".", "").strip().removeprefix("your ").strip() in ["folio", "account", "order", "nett", "contra", "sundry", "sundries", "sterling", "currency", "insurance", "london", "occoquan", "pohick", "virginia", "quantico", "vizt"]:
                        if "mentions" in entry:
                            entry["mentions"].append(word)
                        else:
                            entry["mentions"] = [word, ]
                        continue
                    
                    elif word.lower().strip().split(" ")[0] in relationships:
                        rship = word.lower().split(" ")[0]
                        if "account_name" in entry:
                            name = Person(entry["account_name"])[rship]
                            if len(name) == 1:
                                if name[0].lower() not in lowerPeople:
                                    newPeople.append(Person(name[0]).__str__())
                                    lowerPeople.add(Person(name[0]).__str__().lower())
                            elif len(name) > 1:
                                if f"{' or '.join([Person(x).__str__() for x in name])}".lower() not in lowerPeople:
                                    lowerPeople.add(f"{' or '.join([Person(x).__str__() for x in name])}".lower())
                                    newPeople.append(f"{' or '.join([Person(x).__str__() for x in name])}")
                            else:
                                if f"{Person(word)} of {Person(entry['account_name'].lower().strip())}".lower() not in lowerPeople:
                                    lowerPeople.add(f"{Person(word)} of {Person(entry['account_name'].lower().strip())}".lower())
                                    newPeople.append(f"{Person(word)} of {Person(entry['account_name'].lower().strip())}")
                    
                    elif len(word.lower().replace(".", "").removeprefix("mr ").removeprefix("ms ").removeprefix("mrs ").removeprefix("your ").split(" ")) == 1:
                        if "account_name" in entry:
                            if word.lower() not in ["folio", "account", "order", "nett", "contra", "sundry", "sundries", "sterling", "currency", "insurance", "london", "occoquan", "pohick", "virginia", "quantico", "vizt"]:
                                if f"{word} from acct {Person(entry['account_name'].lower().strip())}".lower() not in lowerPeople:
                                    lowerPeople.add(f"{word} from acct {Person(entry['account_name'].lower().strip())}".lower())
                                    newPeople.append(f"{word} from acct {Person(entry['account_name'].lower().strip())}")

                    else:
                        if word.lower() not in lowerPeople:
                            lowerPeople.add(word.lower())
                            newPeople.append(word)

                del entry["people"]
                entry["people"] = newPeople

            if "mentions" in entry:
                # Deduplicate entry mentions
                entry["mentions"] = [x for x in frozenset(entry["mentions"])]
                
                # Cleanup any extra people in mentions
                newMentions = []
                newPeople = []
                for oword in entry["mentions"]:
                    # If the word is a relationship word like Wife, attempt to figure out who the wife is, otherwise put Wife of <Acct holder>
                    word = oword.lower().strip()
                    if word in relationships:
                        if "account_name" in entry:
                            name = Person(entry["account_name"])[word]
                            if len(name) == 1:
                                newPeople.append(Person(name[0]).__str__())
                            elif len(name) > 1:
                                newPeople.append(f"{' or '.join([Person(x).__str__() for x in name])}")
                            else:
                                newPeople.append(f"{Person(word)} of {Person(entry['account_name'].lower().strip())}")
                        else:
                            newMentions.append(oword)
                    else:
                        if len(word.split(" ")) == 2:
                            splitWord = word.split(" ")
                            firstName = splitWord[0]
                            lastName = splitWord[1]
                            if firstName in namelist or (firstName + " " + lastName) in namelist:
                                newPeople.append(firstName + " " + lastName)
                            else:
                                newMentions.append(oword)
                        else:
                            if word in namelist:
                                if len(word.split(" ")) == 1:
                                    if "account_name" in entry:
                                        if word.lower() not in ["folio", "account", "order", "nett", "contra", "sundry", "sundries", "sterling", "currency", "insurance", "london", "occoquan", "pohick", "virginia", "quantico", "vizt"]:
                                            newPeople.append(f"{Person(word)} from acct {Person(entry['account_name'].lower().strip())}")
                                        else:
                                            newMentions.append(oword)
                                    else:
                                        newMentions.append(oword)
                                else:
                                    newMentions.append(oword)
                            else:
                                newMentions.append(oword)
                
                del entry["mentions"]
                entry["mentions"] = newMentions
            
                if "people" in entry:
                    entry["people"] += newPeople
                else:
                    entry["people"] = newPeople
                
            if "tobacco_marks" not in transaction:
                transaction["tobacco_marks"] = []

            if "people" in entry:
                entry["people"] = [x for x in frozenset([x.lower() for x in entry["people"]])]


        
        # If there is definitely a person mentioned in this row
        if row_context["genmat"][0] == 1:
            if any([("people" in transaction) or (len(transaction["tobacco_marks"]) > 0) for transaction in transactions if "entry_id" in transaction and transaction["entry_id"] == row_context["genmat"][1]]):
                pass
            else:
                if sum([len(transaction["mentions"]) for transaction in transactions if "mentions" in transaction and "errors" not in transaction and transaction["entry_id"] == row_context["genmat"][1]]) == 1:
                    for transaction in transactions:
                        if "entry_id" in transaction and transaction["entry_id"] == row_context["genmat"][1]:
                            if "mentions" in transaction:
                                transaction["people"] = transaction["mentions"]
                                del transaction["mentions"]
                else:
                    for transaction in transactions:
                        if "entry_id" in transaction and transaction["entry_id"] == row_context["genmat"][1]:
                            add_error(transaction, "Error: Genmat is 1 but unable to find any people to relate to account holder", "EID: " +  transaction["entry_id"])

        # Yield transactions grouped by ends of lists of transactions
        # TODO: Get [Subtotal tobacco] to work and probably subtotals in general to work.
        if "is_ender" in row_context and row_context["is_ender"]:
            # Make sure there are errors in transactions with no money or commodity.
            add_errors_to_transactions()
            # Verify totaling on ender transasction
            verify_ender_totaling(row_context, transactions, row)

            # Yield our list of transactions
            yield transactions
            transactions = []
            break_transactions = False
            break_counter = 0
            transactions_context = {}
        
        # Yield any leftover transactions without totals
        # TODO: Technically this can cause weird behavior in transactions_context, but we don't use it that much so idc, marked as todo in case it causes problems later if we use transactions_context
        elif break_transactions == True:
            if break_counter > len(transactions):
                # Make sure there are errors in transactions with no money or commodity.
                add_errors_to_transactions()
                yield transactions
                transactions = []
                break_transactions = False
                break_counter = 0
                transactions_context = {}
            elif break_counter == 0:
                print_debug("Error, weird break counter, this line of code should not be able to happen")
            else:
                new_transactions =  transactions[-break_counter:]
                transactions = transactions[:-break_counter]
                # Make sure there are errors in transactions with no money or commodity.
                add_errors_to_transactions()
                yield transactions
                transactions = new_transactions
                transactions_context = {}
                break_transactions = False
                break_counter = 0
    

    # Make sure there are errors in transactions with no money or commodity.
    add_errors_to_transactions()

    # Yield any leftover transactions
    yield transactions