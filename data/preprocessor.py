import pandas as pd
from parser_utils import get_col, add_to_by, isNoun
from re import split, match, search
import spacy
from itertools import chain

# Initial processing and labelling of transaction parts e.g. nouns, keywords, etc.
# Note that this is a generator due to it being slow
# TODO: Fix <ink>
def preprocess(df: pd.DataFrame):
    parsed_entries = []
    # For row in df
    for key, row in df.iterrows():
        
        # Ignore rows with no entry text
        big_entry = get_col(row, "Entry")
        if big_entry == "-" or big_entry == "" or big_entry is None or str(big_entry) == "nan":
            continue

        # Remove "Ditto"
        ditto = search("Ditto \[\w+\]", big_entry)
        if ditto:
            newRe = search("\[\w+\]", ditto.group())
            newStr = (newRe.group())[1:-1]
            big_entry = big_entry.replace(ditto.group(), newStr)
        
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

                # Remove strange things like cardinal numbers that are IDd as people
                if token.ent_type_ == "PERSON":
                    if "NN" not in token.tag_:
                        token.ent_type_ = ""

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

                # If we see money, combine it with any previous prices
                elif prev_token is not None and prev_token[1] == "PRICE" and token.ent_type_ == "MONEY":
                    combine_tok_with_prev(new_entry, token, new_ent="COMB.PRICE")

                # Combine nouns into larger nouns
                elif prev_token is not None and isNoun(token) and isNoun(prev_token):
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
                    new_entry.append((token.text, "CC", "CC.DENIED"))
                
                # If a cardinal number is probably a price but is not at the end, mark as price
                elif token.ent_type_ == "CARDINAL" and match(price_regex, token.text):
                    new_entry.append((token.text, "PRICE", token.tag_))
                
                # Label Money ent type as price
                elif token.ent_type_ == "MONEY":
                    new_entry.append((token.text, "PRICE", token.tag_))

                # If we have a cardinal number that appears to be a price, mark it as such.
                elif prev_token is not None and prev_token[0] != "at" and token.tag_ == "CD" and match(price_regex, token.text):
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
                # TODO: Allow for bulk prices and overall prices at once.
                if token[0].lower() == "at" and next_token[1] in ["PRICE", "COMB.PRICE"]:
                    stack_append(token_stack, token, "IS.BULK")
                else:
                    stack_append(token_stack, token)

            # print(token_stack)
            # print(new_entry)
            # If we detect weird characters (e.g. *), stop processing the row
            parsed_entries_in_row.append(token_stack)
            if any((x[2] == "XX" for x in token_stack)):
                break
        
        # If there is weird stuff, we know we probably have a bad entry and we will pass it through as such.
        if any([x[2] == "XX" for x in chain(*parsed_entries_in_row)]):
            print(f"Error, Bad entry: {big_entry}")
            print(parsed_entries_in_row)
            if parsed_entries_in_row:
                parsed_entries_in_row[0] = "BAD_ENTRY"
                parsed_entries_in_row.append(big_entry)
            else:
                parsed_entries_in_row.append("BAD_ENTRY")
                parsed_entries_in_row.append(big_entry)
            yield (parsed_entries_in_row, row)

        else:
            yield (parsed_entries_in_row, row)    