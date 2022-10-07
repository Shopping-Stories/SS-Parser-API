from itertools import count
import pandas as pd
import xlwt
from pathlib import Path
import numpy as np
from PreprocessingFunctions_Updated import *


# code for the function that parses everything
def parse(df):
    
    transcriber_time = df.iloc[0][0]
    filename = df.iloc[0][1]
    # print(f'transcriber: {transcriber_time}\nfilename: {filename}')
    df.at[1,'[Transcriber/Time]'] = np.nan
    df.at[1,'[File Name]'] = np.nan
    print(f'transcriber: {transcriber_time}\nfilename: {filename}')
    df.at[2,'[Transcriber/Time]'] = transcriber_time
    df.at[2,'[File Name]'] = filename
    # df.drop(labels = 1, axis = 0)
    df = df.iloc[1:]
    # df.head()
    


    # run tm first

    # run preprocessing
    entry_mat = preprocess(df)

    # make repeated idxs dict
    repeated_idxs = {}
    for x in range(0,len(entry_mat)):
        l = len(entry_mat[x])
        if l > 1:
            repeated_idxs[x] = l
    
    parsed_transactions = parse_transaction(entry_mat)

    # returned entry obj
    entry_obj_list = []

    # make lists for each column
    transcriber_time = df['Transcriber/Time']
    file_name = df['File Name']
    reel = df['Reel']
    owner = df['Owner']
    store = df['Store']
    folio_year = df['Folio Year']
    folio_page = df['Folio Page']
    entry_id = df['EntryID']
    # marginalia = df['Marginalia']
    prefix = df['Prefix']
    account_firstname = df['Account First Name']
    account_lastname = df['Account Last Name']
    suffix = df['Suffix']
    profession = df['Profession']
    location = df['Location']
    reference = df['Reference']
    drcr = df['Dr/Cr']
    year = df['Year']
    month = df['_Month']
    day = df['Day']

    entry = df['Entry']

    # CHANGE THIS TO EXTRACT INFO
    # people = df['PEOPLE']
    # places = df['PLACES']

    folio_reference = df['Folio Reference']

    # CHANGE THIS TO EXTRACT INFO
    # entry_type = df['ENTRY TYPE']
    # ledger = df['LEDGER']

    quantity = df['Quantity']
    commodity = df['Commodity']
    SE = ['£ Sterling']
    SL = df['L Sterling']
    SS = df['s Sterling']
    SD = df['d Sterling']
    colony = df['Colony Currency']
    CE = df['£ Currency']
    CL = df['L Currency']
    CS = df['s Currency']
    CD = df['d Currency']
    archmat = df['ArchMat']
    genmat = df['GenMat']
    final = df['Final']

    counter = 0

    for transaction_counter in range(0,len(parsed_transactions)):
    # for transaction in parsed_transactions:
        # assignments
        # transaction = parsed_transactions[transaction_counter]

        # meta obj
        meta_obj = {}
        meta_obj['ledger'] = None 
        meta_obj['reel'] = reel[counter]
        meta_obj['owner'] = owner[counter]
        meta_obj['store'] = store[counter]
        meta_obj['year'] = folio_year[counter]
        meta_obj['folioPage'] = folio_page[counter]
        meta_obj['entryID'] = entry_id[counter]
        meta_obj['comments'] = final[counter]

        # meta_obj_list.append(meta_obj)

        # date obj
        date_obj = {}
        date_obj['day'] = day[counter]
        date_obj['month'] = month[counter]
        date_obj['year'] = year[counter]
        # change to an appended datetime obj
        date_obj['fullDate'] = None

        # date_obj_list.append(date_obj)

        # account holder obj
        acc_holder_obj = {}
        acc_holder_obj['prefix'] = prefix[counter]
        acc_holder_obj['accountFirstName'] = account_firstname[counter]
        acc_holder_obj['accountLastName'] = account_lastname[counter]
        acc_holder_obj['suffix'] = suffix[counter]
        acc_holder_obj['profession'] = profession[counter]
        acc_holder_obj['location'] = location[counter]
        acc_holder_obj['reference'] = reference[counter]
        # change this to an int
        temp_drcr = -1
        if drcr[counter].upper() == 'DR':
            temp_drcr = 0
        if drcr[counter].upper() == 'CR':
            temp_drcr = 1
        acc_holder_obj['debitOrCredit'] = temp_drcr

        # make two poundshillingpence objects, one for currency one for sterling
        s_psp = {}
        s_psp['pounds'] = SL[counter]
        s_psp['shillings'] = SS[counter]
        s_psp['pence'] =  SD[counter]

        c_psp = {}
        c_psp['pounds'] = CL[counter]
        c_psp['shilling'] = CS[counter]
        c_psp['pence'] = CD[counter]

        # make money obj for both psp's
        money_obj = {}
        money_obj['quantity'] = quantity[counter]
        money_obj['commodity'] = commodity[counter]
        money_obj['colony'] = colony[counter]
        money_obj['sterling'] = s_psp
        money_obj['currency'] = c_psp
        
        if counter in repeated_idxs.keys:
            for x in range(0,repeated_idxs[counter]):
                # people, places, ledger, entry type, flag?
                entry_obj = {}
                item_entry_obj = {}
                item_or_service_obj = {}

                transaction = parsed_transactions[transaction_counter]

                item_or_service_obj['quantity'] = transaction['quantity']
                item_or_service_obj['qualifier'] = transaction['qualifier']
                item_or_service_obj['variants'] = transaction['variants']
                item_or_service_obj['item'] = transaction['item']
                item_or_service_obj['category'] = transaction['category']
                item_or_service_obj['subcategory'] = None
                item_or_service_obj['unitCost'] = transaction['unitCost']
                item_or_service_obj['itemCost'] = transaction['totalCost']

                item_entry_obj['perOrder'] = None
                item_entry_obj['percentage'] = None
                item_entry_obj['itemOrServices'] = []
                item_entry_obj['itemOrServices'].append(item_or_service_obj)
                item_entry_obj['itemsMentioned'] = None

                people_obj_list = []
                places_obj_list = []

                for person in transaction['mentionedPpl']:
                    person_obj = {}
                    person_obj['name'] = person
                    people_obj_list.append(person_obj)

                for place in transaction['mentionedPlaces']:
                    place_obj = {}
                    place_obj['name'] = place
                    places_obj_list.append(place_obj)
                
                entry_obj['accountHolder'] = acc_holder_obj
                entry_obj['meta'] = meta_obj
                entry_obj['dateInfo'] = date_obj
                entry_obj['folioRefs'] = folio_reference[counter]
                entry_obj['ledgerRefs'] = None
                entry_obj['itemEntries?'] = []
                entry_obj['itemEntries?'].append(item_entry_obj)
                entry_obj['tobaccoEntry?'] = None # CHANGE THIS 
                entry_obj['regularEntry?'] = None
                entry_obj['people'] = people_obj_list
                entry_obj['places'] = places_obj_list
                entry_obj['entry'] = entry[counter]
                entry_obj['money'] = money_obj
                entry_obj['errorReview'] = transaction['errorReview']

                # append
                # ret_list.append(temp_row)
                entry_obj_list.append(entry_obj)
                transaction_counter+=1

        else:
            entry_obj = {}
            item_entry_obj = {}
            item_or_service_obj = {}

            transaction = parsed_transactions[transaction_counter]

            item_or_service_obj['quantity'] = transaction['quantity']
            item_or_service_obj['qualifier'] = transaction['qualifier']
            item_or_service_obj['variants'] = transaction['variants']
            item_or_service_obj['item'] = transaction['item']
            item_or_service_obj['category'] = transaction['category']
            item_or_service_obj['subcategory'] = None
            item_or_service_obj['unitCost'] = transaction['unitCost']
            item_or_service_obj['itemCost'] = transaction['totalCost']

            item_entry_obj['perOrder'] = None
            item_entry_obj['percentage'] = None
            item_entry_obj['itemOrServices'] = []
            item_entry_obj['itemOrServices'].append(item_or_service_obj)
            item_entry_obj['itemsMentioned'] = None

            people_obj_list = []
            places_obj_list = []

            for person in transaction['mentionedPpl']:
                person_obj = {}
                person_obj['name'] = person
                people_obj_list.append(person_obj)

            for place in transaction['mentionedPlaces']:
                place_obj = {}
                place_obj['name'] = place
                places_obj_list.append(place_obj)
                
                

            entry_obj['accountHolder'] = acc_holder_obj
            entry_obj['meta'] = meta_obj
            entry_obj['dateInfo'] = date_obj
            entry_obj['folioRefs'] = folio_reference[counter]
            entry_obj['ledgerRefs'] = None
            entry_obj['itemEntries?'] = []
            entry_obj['itemEntries?'].append(item_entry_obj)
            entry_obj['tobaccoEntry?'] = None # CHANGE THIS 
            entry_obj['regularEntry?'] = None
            entry_obj['people'] = people_obj_list
            entry_obj['places'] = places_obj_list
            entry_obj['entry'] = entry[counter]
            entry_obj['money'] = money_obj
            entry_obj['errorReview'] = transaction['errorReview']

                # append
                # ret_list.append(temp_row)
            entry_obj_list.append(entry_obj)
            transaction_counter+=1

        # increment
        counter+=1
        
    return entry_obj_list


    # --------------------------------------------------------------------

def main():
    cwd = Path.cwd()
    fp = cwd / 'C_1760_002_FINAL_.xlsx'
    # print(f'dir is : {above}')

    df = pd.read_excel(fp)
    entry_objs = parse(df)
    print(entry_objs)

if __name__ == '__main__':
    main()
