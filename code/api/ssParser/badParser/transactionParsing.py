import re
from os import unlink
from re import I
from tkinter import Y

import nltk
import numpy as np
import pandas as pd
import pymongo
from fuzzywuzzy import fuzz, process
from regex import P, R
from sympy import O


#create function that separates 1 entry column into multiple entries to pass to next function
def seperate(text):
  #will always return an array
  textArray = re.split("   ( |\})|\n", text)
  while None in textArray:
    textArray.remove(None)
  return textArray

#creat function to preprocess ALREADY SEPEARTED ENTRIES with nlkt
def editTokens(text):
  #create variable tokens which is tokenized string
  tokens = nltk.word_tokenize(text)
  #begin process of removing words that are replacements
  replaceWordIndex = []
  removenumbersIndex = []
  for idx, word in enumerate(tokens): #this basically means that 'current' index is accesed by variable idx inside loop
    #when for loop reaches square brackets
    if (word == '[' and tokens[idx+2] == ']' and (tokens[idx-1][0] == tokens[idx+1][0] or tokens[idx-1]=="ditto" or tokens[idx-1]=="Ditto")):
      replaceWordIndex.append(idx) #replace words based on matching first letter
    elif (word == '['):#remove numbers in brackets
      if (tokens[idx-1] != '..' and tokens[idx+3] != '..'):
        removenumbersIndex.append(idx)
  for num in removenumbersIndex:#pop the numbers
    tokens.pop(num+1)
    for count in range(0,len(replaceWordIndex)): #adjust index accordingly
      if(replaceWordIndex[count] > num):
        replaceWordIndex[count] = replaceWordIndex[count]-1
    for count in range(0,len(removenumbersIndex)): #adjust index accordingly
      if(removenumbersIndex[count] > num):
        removenumbersIndex[count] = removenumbersIndex[count]-1
  for num in replaceWordIndex: #pop replaced word and brakets
    tokens.pop(num+2)
    tokens.pop(num)
    tokens.pop(num-1)
    for count in range(0,len(replaceWordIndex)): #adjust index accordingly
      if(replaceWordIndex[count] > num):
        replaceWordIndex[count] = replaceWordIndex[count]-3
  #replace where more than 1 word between []
  replaceWordIndex = []
  removenumbersIndex = []
  for idx, word in enumerate(tokens):
    if (word == "["):
      for count in range(idx+1, len(tokens)):
        if (tokens[count] == "["):
          break
        elif (tokens[count] == "]"):
          if (tokens[idx-1][0] == tokens[idx+1][0] or tokens[idx-1]=="ditto" or tokens[idx-1]=="Ditto"):
            replaceWordIndex.append(idx)
          break
      break
  for num in replaceWordIndex:
    tokens.pop(num-1)
    for count in range(0,len(replaceWordIndex)): #adjust index accordingly
      if(replaceWordIndex[count] > num):
        replaceWordIndex[count] = replaceWordIndex[count]-3
  #now remove all contents between parentheses
  for idx, word in enumerate(tokens):
    if (word == '(' and tokens[idx+2] == ')'):
      tokens.pop(idx+1)
  #remove all non-alphanumerica characters between arrows '<' and '>'
  for idx, word in enumerate(tokens):
    if (word == '<' and tokens[idx+2] == '>' and not(tokens[idx+1].isalnum())):
      tokens.pop(idx+1)
  #remove all character removals '^','[',']','<','>', '{', '}', '(', ')',  '&ct', 'etcetera', 'Etcetera', '£'
  removals = ['^','[',']','<','>', '{', '}', '(', ')', '&ct', 'etcetera', 'Etcetera', '£']#add all characters to be removed to this list
  for count in range(0,len(tokens)):
    for character in removals:
      while character in tokens[count]:
        tokens[count] = tokens[count].replace(character,"")

  #remove "" tokens
  while ("" in tokens):
    tokens.remove('')

  #fraction handling
  fractions = ["¼", "½", "¾", "⅓", "⅔", "⅕", "⅖", "⅗", "⅘", "⅙", "⅚", "⅛", "⅜", "⅝", "⅞"]
  fracReplace = [".25", ".5", ".75", ".333", ".667", ".2", ".4", ".6", ".8", ".167", ".833", ".125", ".375", ".625", ".875"]
  for count in range(0,len(tokens)):
    for fraction in fractions:
      tokens[count] = tokens[count].replace(fraction, fracReplace[fractions.index(fraction)])

  #gluing preceding tokens and fractions together
  numberRE = re.compile("[0-9]+")
  decimalRE = re.compile("\.[0-9]+")
  for idx, word in enumerate(tokens):
    if (numberRE.match(tokens[idx]) and idx+1 < len(tokens)):
      if (decimalRE.match(tokens[idx+1])):
        tokens[idx] = tokens[idx] + tokens[idx+1]
        tokens.pop(idx+1)
  
  #gluing 'per' and 'cent' together
  for idx, word in enumerate(tokens):
    if ((word == 'per' or word == 'Per') and idx+1 < len(tokens)):
      if (tokens[idx+1] == "cent" or tokens[idx+1] == "Cent"):
        tokens[idx] = 'percent'
        tokens.pop(idx+1)

  #changing 'w' and 'wt' to pounds
  for idx, word in enumerate(tokens):
    if (numberRE.match(tokens[idx]) and idx+1 < len(tokens)):
      if (tokens[idx+1] == 'w' or tokens[idx+1] == 'wt'):
        tokens[idx+1] = 'pounds'
  numbPounds = re.compile("[0-9]+(\.[0-9]+)?(w|(wt))")
  for idx, word in enumerate(tokens):
    if (numbPounds.match(word)):
      tokens.insert(idx+1, 'pounds')
      while "wt" in tokens[idx]:
        tokens[idx] = tokens[idx].replace('wt', "")
      while "w" in tokens[idx]:
        tokens[idx] = tokens[idx].replace('w', "")

  return tokens

def preprocess(string):#function that is acctually called
  breakup = seperate(string)
  preprocessed = []
  for count in range(0, len(breakup)):
    preprocessed.append(editTokens(breakup[count]))
  while [] in preprocessed:
    preprocessed.remove([])
  return preprocessed

# Function to connect to database
def get_database():  
    # Creates a connection to MongoDB
    client = pymongo.MongoClient("mongodb://root:Lsfr5n3J0Jib@ec2-52-203-105-125.compute-1.amazonaws.com:27017/shoppingStories?authSource=admin&readPreference=primary&ssl=false")

    # Returns the database collection we'll be using
    return client["shoppingStories"]
 
# Allows many files can reuse the function get_database()
if __name__ == "__main__":    
    # Get the database
    db = get_database()

db = get_database()   # Creates a varibale for the database
lem = nltk.WordNetLemmatizer()  # Initializes lemmatizer
tsr = fuzz.token_set_ratio  # Scorer for string comparison
qRatio = fuzz.QRatio    # Scorer for string comparison

# =========================================== #
#    Generating  Lists From Database Data     #
# =========================================== #

# Generates dataframes for the database collections
places_df = pd.DataFrame(db["places"].find())
people_df = pd.DataFrame(db["people"].find())
item_df = pd.DataFrame(db["items"].find())

# Generates List of Professions
pf = np.array(people_df["profession"])
pf = pf[pf!=""]
professionList = np.unique(pf)
professions_df2 = people_df[(people_df['firstName'] == "") & (people_df['profession'] != '' )]
pro = np.array(professions_df2["profession"])
pro = pro[pro!=""]
pro = np.unique(pro)
professionList = np.concatenate((professionList, pro), axis=None)
professionList = np.unique(professionList)

# Generates List Of Organizations
org_df = people_df[people_df['reference'] == 'Organization']
org = np.array(org_df["lastName"])
org = org[org!=""]
organizationList = np.unique(org)

# Generates List Of Accounts
accounts_df = people_df[people_df['reference'] == 'Account']
acc = np.array(accounts_df["lastName"])
acc = acc[acc!=""]
accountsList = np.unique(acc)

# Generates List Of Places
placesList = np.array(places_df["location"])
place_df2 = people_df[people_df['reference'].str.contains('Place')]
pl2 = np.array(place_df2["lastName"])
pl2 = pl2[pl2!=""]
placesList = np.concatenate((placesList, pl2), axis=None)
remove_ = [word for word in placesList if "(" in word]
for i, w in enumerate(remove_): 
  x = np.where(placesList == w)
  placesList = np.delete(placesList, x)
  placesList = np.append(placesList,[i.split(' ', 1) for i in remove_][i][0])
remove2 = [word for word in placesList if len(word.split("-"))>1]
for i, w in enumerate(remove2): 
  x = np.where(placesList == w)
  placesList = np.delete(placesList, x)
  placesList = np.append(placesList,[i.split(' ', 1) for i in remove2][i][0])
placesList = np.array([item.strip() for item in placesList])
placesList = np.unique(placesList)

# Generates List of Place Aliases
placeAliasList = places_df[places_df.alias != '']
placeAliasList = placeAliasList.drop(['_id', "descriptor"], axis=1)

# PLACES with more than one word in it's name
multiPlacesList = [word for word in placesList if len(word.split())>1]
multiPlacesList.sort()

# Cleans up PEOPLE names
ppl_df = people_df.copy()
people_df2 = ppl_df[(ppl_df['reference'] != "Account")&(ppl_df['reference'] != 'Organization')&(ppl_df['reference'].str.contains('Place')==False)&(ppl_df['reference'] != 'Organization')]
ppl_df = people_df2.drop(people_df2[(people_df2.firstName == "") & (people_df2.profession != '')].index)
if ppl_df['firstName'].str.contains('&').any():
  temp = ppl_df[ppl_df['firstName'].str.contains('&')].copy()
  for value in temp['firstName']:
        new = value.split('&')
        x = temp[temp['firstName'] == value].copy()
        ppl_df['firstName'].mask(ppl_df['firstName'] == value, new[0], inplace=True)
        x['firstName'].mask(x['firstName'] == value, new[1], inplace=True)
        ppl_df = pd.concat([ppl_df, x], ignore_index=True, sort=False)
people_df = ppl_df

# Generates List Of Prefixes
pf = np.array(people_df["prefix"])
pf = pf[pf!=""]
prefixList = np.unique(pf)
# Generates List Of First Names
fn = np.array(people_df["firstName"])
fn = fn[fn!=""]
firstNameList = np.unique(fn)
# Generates List Of Last Names
ln = np.array(people_df["lastName"])
ln = ln[ln!=""]
lastNameList = np.unique(ln)
# Generates List of Suffixes
sx = np.array(people_df["suffix"])
sx = sx[sx!=""]
suffixList = np.unique(sx)

# Generates List of Items
items = item_df["item"].values.tolist()
item_list = []
adjs = []
for word in items:
    split = word.split(',')
    item_list.append(split[0])
    for i in range(1,len(split)):
        w = split[i].strip()
        adjs.append(w)
item_list.extend(["Goods","Furniture","Interest","Land Tax","Turlingtons Balsam of Life","Clerks Note","Cattle & Provisions"])
item_list.pop(item_list.index("Blue"))
itemList = sorted(dict.fromkeys(item_list))
# Generates list of ITEMS consisting of more than one word
multiItemList = [word for word in itemList if len(word.split())>1]
multiItemList.sort()
# Generates List of ITEMS end in -ing / -ings
ingItemList = [itemList[i] for i in range(len(itemList)) if itemList[i][-3:]== "ing"]
ingItemList2 = [itemList[i] for i in range(len(itemList)) if itemList[i][-4:]== "ings"]
# Removes multi-ITEMS from ITEM list
itemList = set(itemList).difference(multiItemList)
# Generates VARIANTS List
variants = item_df["variants"].values.tolist()
for word in variants:
    word = word.replace('(',',')
    word = word.replace(')','')
    split = word.split(',')
    for i in range(len(split)):
        k = split[i].strip()
        k.replace(')','')
        adjs.append(k)
while ("" in adjs):
    adjs.remove('')
variantList = sorted(dict.fromkeys(adjs))
variantList.pop(variantList.index("Turlingtons"))
# Generates List of ITEMS and VARIANTS that end in -ing / -ings
ingVariantList = [variantList[i] for i in range(len(variantList)) if variantList[i][-3:]== "ing"]
ingVariantList2 = [variantList[i] for i in range(len(variantList)) if variantList[i][-4:]== "ings"]
ingList = ingItemList + ingItemList2 + ingVariantList + ingVariantList2
# Generates List of Numerical VARIANTS
numVariantList = [variantList[i] for i in range(len(variantList)) if variantList[i][0].isdigit()]
numVariantList.sort()
# Removes numerical VARIANTS from variant list
variantList = set(variantList).difference(numVariantList)
# Generates List of ITEMS that are also VARIANTS
variantItemsList = sorted(set(itemList).intersection(variantList))

# =========================================== #
#    Lists Of Frequently Used Words/Values    #
# =========================================== #

# List of QUALIFIERS
qualifierList = ["acre","bale","barrel","bushel","cord","day","dozen","ell","fathom","foot","feet","firkin","gallon","half","hank","hogshead",
                "loaf","loaves","month","ounce","pair","piece","pint","pound","quart","quire","remnant","row","set","side","stick","yard","year","bottle","cask","pot","suit"]   
# List of services
servicesList = ["making", "mending", "postage", "waggonage", "freight","inspection","pasturage","ferriage","craft hire","hire","rent","wintering","carting","timber"]
# List of relation types
relationsList = ["mother","father","son","daughter","brother","sister","slave","boy","girl", "wench","lady","negro","negroes","uncle", "aunt","wife","husband","child","children"]
personsList = ["man","men","woman","women","boy","girl"]  # List of people nouns
keywordList = ["of", "by", "for", "the", "from", "to", "by","at"]  # List of KeyWords
possessiveList = ["your", "his", "her", "my", "their", "our"]  # List of possesive pronouns
itemQualList = ["bottle","cask","pot","suit"]  # List of ITEMS that are also QUALIFIERS
numItems = ["Check","Dowlass","Linen","Nails","Rug","Stays","Breeches"]  # List of ITEMS with numnerical VARIANTS
liquidItems = ["Rum","Ale", "Brandy"]  # List of liquid ITEMS
conjunctions = ["&","and","with",",", "w/"]
nonItems = ["Horses","Goods","Furniture","Interest","Land Tax","Clerks Note","Cattle & Provisions"]
ignore = ["&", ":","/:"]


# Initializing dictionaries
peopleObject ={"profession": "", "prefix": "", "firstName": "", "lastName": "", "suffix": "", "account":"", "location": "", 
                    "reference": "", "relations": ""}
transactionObject ={"quantity": "", "qualifier": "", "variants": [], "item": "", "unitCost": "", "totalCost": "",
                    "service": "", "includedItems": [],"mentionedPpl": [],"mentionedPlaces": [], "errorReview":[]}
moneyObject = {"pounds": "", "shillings": "", "pence": ""}

# =========================================== #
#    Generating  Lists From Database Data     #
# =========================================== #

# Generates dataframes for the database collections
places_df = pd.DataFrame(db["places"].find())
people_df = pd.DataFrame(db["people"].find())
item_df = pd.DataFrame(db["items"].find())

# Generates List of Professions
pf = np.array(people_df["profession"])
pf = pf[pf!=""]
professionList = np.unique(pf)
professions_df2 = people_df[(people_df['firstName'] == "") & (people_df['profession'] != '' )]
pro = np.array(professions_df2["profession"])
pro = pro[pro!=""]
pro = np.unique(pro)
professionList = np.concatenate((professionList, pro), axis=None)
professionList = np.unique(professionList)

# Generates List Of Organizations
org_df = people_df[people_df['reference'] == 'Organization']
org = np.array(org_df["lastName"])
org = org[org!=""]
organizationList = np.unique(org)

# Generates List Of Accounts
accounts_df = people_df[people_df['reference'] == 'Account']
acc = np.array(accounts_df["lastName"])
acc = acc[acc!=""]
accountsList = np.unique(acc)

# Generates List Of Places
placesList = np.array(places_df["location"])
place_df2 = people_df[people_df['reference'].str.contains('Place')]
pl2 = np.array(place_df2["lastName"])
pl2 = pl2[pl2!=""]
placesList = np.concatenate((placesList, pl2), axis=None)
remove_ = [word for word in placesList if "(" in word]
for i, w in enumerate(remove_): 
  x = np.where(placesList == w)
  placesList = np.delete(placesList, x)
  placesList = np.append(placesList,[i.split(' ', 1) for i in remove_][i][0])
remove2 = [word for word in placesList if len(word.split("-"))>1]
for i, w in enumerate(remove2): 
  x = np.where(placesList == w)
  placesList = np.delete(placesList, x)
  placesList = np.append(placesList,[i.split(' ', 1) for i in remove2][i][0])
placesList = np.array([item.strip() for item in placesList])
placesList = np.unique(placesList)

# Generates List of Place Aliases
placeAliasList = places_df[places_df.alias != '']
placeAliasList = placeAliasList.drop(['_id', "descriptor"], axis=1)

# PLACES with more than one word in it's name
multiPlacesList = [word for word in placesList if len(word.split())>1]
multiPlacesList.sort()

# Cleans up PEOPLE names
ppl_df = people_df.copy()
people_df2 = ppl_df[(ppl_df['reference'] != "Account")&(ppl_df['reference'] != 'Organization')&(ppl_df['reference'].str.contains('Place')==False)&(ppl_df['reference'] != 'Organization')]
ppl_df = people_df2.drop(people_df2[(people_df2.firstName == "") & (people_df2.profession != '')].index)
if ppl_df['firstName'].str.contains('&').any():
  temp = ppl_df[ppl_df['firstName'].str.contains('&')].copy()
  for value in temp['firstName']:
        new = value.split('&')
        x = temp[temp['firstName'] == value].copy()
        ppl_df['firstName'].mask(ppl_df['firstName'] == value, new[0], inplace=True)
        x['firstName'].mask(x['firstName'] == value, new[1], inplace=True)
        ppl_df = pd.concat([ppl_df, x], ignore_index=True, sort=False)
people_df = ppl_df

# Generates List Of Prefixes
pf = np.array(people_df["prefix"])
pf = pf[pf!=""]
prefixList = np.unique(pf)
# Generates List Of First Names
fn = np.array(people_df["firstName"])
fn = fn[fn!=""]
firstNameList = np.unique(fn)
# Generates List Of Last Names
ln = np.array(people_df["lastName"])
ln = ln[ln!=""]
lastNameList = np.unique(ln)
# Generates List of Suffixes
sx = np.array(people_df["suffix"])
sx = sx[sx!=""]
suffixList = np.unique(sx)

# Generates List of Items
items = item_df["item"].values.tolist()
item_list = []
adjs = []
for word in items:
    split = word.split(',')
    item_list.append(split[0])
    for i in range(1,len(split)):
        w = split[i].strip()
        adjs.append(w)
item_list.extend(["Goods","Furniture","Interest","Land Tax","Turlingtons Balsam of Life","Clerks Note","Cattle & Provisions"])
item_list.pop(item_list.index("Blue"))
itemList = sorted(dict.fromkeys(item_list))
# Generates list of ITEMS consisting of more than one word
multiItemList = [word for word in itemList if len(word.split())>1]
multiItemList.sort()
# Generates List of ITEMS end in -ing / -ings
ingItemList = [itemList[i] for i in range(len(itemList)) if itemList[i][-3:]== "ing"]
ingItemList2 = [itemList[i] for i in range(len(itemList)) if itemList[i][-4:]== "ings"]
# Removes multi-ITEMS from ITEM list
itemList = set(itemList).difference(multiItemList)
# Generates VARIANTS List
variants = item_df["variants"].values.tolist()
for word in variants:
    word = word.replace('(',',')
    word = word.replace(')','')
    split = word.split(',')
    for i in range(len(split)):
        k = split[i].strip()
        k.replace(')','')
        adjs.append(k)
while ("" in adjs):
    adjs.remove('')
variantList = sorted(dict.fromkeys(adjs))
variantList.pop(variantList.index("Turlingtons"))
# Generates List of ITEMS and VARIANTS that end in -ing / -ings
ingVariantList = [variantList[i] for i in range(len(variantList)) if variantList[i][-3:]== "ing"]
ingVariantList2 = [variantList[i] for i in range(len(variantList)) if variantList[i][-4:]== "ings"]
ingList = ingItemList + ingItemList2 + ingVariantList + ingVariantList2
# Generates List of Numerical VARIANTS
numVariantList = [variantList[i] for i in range(len(variantList)) if variantList[i][0].isdigit()]
numVariantList.sort()
# Removes numerical VARIANTS from variant list
variantList = set(variantList).difference(numVariantList)
# Generates List of ITEMS that are also VARIANTS
variantItemsList = sorted(set(itemList).intersection(variantList))

# =========================================== #
#    Lists Of Frequently Used Words/Values    #
# =========================================== #

# List of QUALIFIERS
qualifierList = ["acre","bale","barrel","bushel","cord","day","dozen","ell","fathom","foot","feet","firkin","gallon","half","hank","hogshead",
                "loaf","loaves","month","ounce","pair","piece","pint","pound","quart","quire","remnant","row","set","side","stick","yard","year","bottle","cask","pot","suit"]   
# List of services
servicesList = ["making", "mending", "postage", "waggonage", "freight","inspection","pasturage","ferriage","craft hire","hire","rent","wintering","carting","timber"]
# List of relation types
relationsList = ["mother","father","son","daughter","brother","sister","slave","boy","girl", "wench","lady","negro","negroes","uncle", "aunt","wife","husband","child","children"]
personsList = ["man","men","woman","women","boy","girl"]  # List of people nouns
keywordList = ["of", "by", "for", "the", "from", "to", "by","at"]  # List of KeyWords
possessiveList = ["your", "his", "her", "my", "their", "our"]  # List of possesive pronouns
itemQualList = ["bottle","cask","pot","suit"]  # List of ITEMS that are also QUALIFIERS
numItems = ["Check","Dowlass","Linen","Nails","Rug","Stays","Breeches"]  # List of ITEMS with numnerical VARIANTS
liquidItems = ["Rum","Ale", "Brandy"]  # List of liquid ITEMS
conjunctions = ["&","and","with",",", "w/"]
nonItems = ["Horses","Goods","Furniture","Interest","Land Tax","Clerks Note","Cattle & Provisions"]
ignore = ["&", ":","/:"]

# Initializing dictionaries
peopleObject ={"profession": "", "prefix": "", "firstName": "", "lastName": "", "suffix": "", "account":"", "location": "", 
                    "reference": "", "relations": ""}
transactionObject ={"quantity": "", "qualifier": "", "variants": [], "item": "", "unitCost": "", "totalCost": "",
                    "service": "", "includedItems": [],"mentionedPpl": [],"mentionedPlaces": [], "errorReview":[]}
moneyObject = {"pounds": "", "shillings": "", "pence": ""}

# =========================================== #
#             General Functions               #
# =========================================== #
# Parses entries begining withan integer
def intFirstParse(array,transDict,transReview,peopleArray,placesArray,otherItems):
    idx = 0
    QQI_Pattern(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)    
        
# Parses entries begining with a letter
def alphaFirstParse(array,transDict,transReview,peopleArray,placesArray,otherItems): # sourcery skip: merge-duplicate-blocks, merge-nested-ifs, remove-redundant-if
    
    def checkForCost(array,index,transDict,transReview,placesArray,peopleArray,otherItems):
        if transDict["totalCost"]=="" and transDict["unitCost"]=="" and (array[-1][0].isdigit()==True or ".." in array[-1]):
            index = array.index(array[-1])
            x = getCost(array,index,transDict,transReview,placesArray,peopleArray,otherItems) 
            index = compareIdx(idx,x)
            
    x = False
    idx = 0
    if array[0] in ["the", "a"]:  # Removes preceding "the"/"a"
        array.pop(0)

    # Checks what word/phrase/symbol the transaction begins with
    if fuzz.ratio(lem.lemmatize(array[idx]), "charge") >= 90:  
        beginsCharge(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
    elif fuzz.QRatio(array[idx], "allowance ") >= 87 or (array[idx] in ["a","an"] and fuzz.QRatio(array[idx+1], "allowance ") >= 87):
        beginsAllowance(array,transDict, transReview) 
    elif array[idx] == "cash":
        beginsCash(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
    elif array[idx] == "total":
        beginsTotal(array,transDict,transReview,peopleArray,placesArray,otherItems) 
    elif array[idx] == "account":
        beginsAccount(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
    elif array[idx] in ["subtotal" "sum"]:
        beginsSubtotalSum(array) 
    elif array[idx] in ["at","&","and"]:
        idx+=1
        if len(array)>idx and getPlace(array,idx,placesArray) != 0:
            temp = getPlace(array,idx,placesArray)
            placesArray.append(temp[0])
            idx = temp[1]
            if len(array)>idx and array[idx][0].isdigit() == True:
                x = getCost(array,idx,transDict,transReview, placesArray, peopleArray)
                idx = compareIdx(idx,x)
        else:
            temp = [x for x in array if x[0].isdigit()]
            if temp != [] and array.index(temp[0])<4 :
                n = array.index(temp[0])
                pName = ' '.join(array[idx:n+1])
                placesArray.append(pName)
                transReview.append("Review: Confirm PLACE.")
            else:
                placesArray.append(array[idx+1])
                transReview.append("Review: Confirm PLACE.")
    elif len(array)>idx+1 and fuzz.ratio(lem.lemmatize(array[idx+1]), "furniture") >= 90:
        transDict["item"] = f"{array[idx]} Furniture"
        idx+=2
        beginsItem(array,idx,transDict,transReview, peopleArray, placesArray, otherItems, transDict["item"])
    elif array[idx] in ["sundries", "sundry", "sundrys"]:
        pass
    elif fuzz.ratio(lem.lemmatize(array[idx]), "balance") >= 90 or fuzz.ratio(array[idx], "balance") >= 90:
        beginsBalance(array,transDict,transReview)
    elif array[idx] == "contra" and fuzz.ratio(array[idx+1], "balance") :
        beginsContraBalance(array,transDict,transReview, peopleArray, placesArray)
    elif fuzz.ratio(lem.lemmatize(array[idx]), "sterling") >= 90 or fuzz.ratio(lem.lemmatize(array[idx]), "currency") >= 90:
        beginsSterlingCurrency(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
    elif fuzz.ratio(lem.lemmatize(array[idx]), "expense") >= 90 or fuzz.ratio(array[idx], "expence") >= 90:
        array[idx] = ""
        idx+=1
        beginsExpense(array,idx,transDict,transReview, peopleArray, placesArray)
    elif len(array)>idx+1 and (fuzz.ratio(lem.lemmatize(array[idx+1]), "expense") >= 90 or fuzz.ratio(array[idx+1], "expence") >= 90):
        array[idx+1] = ""
        new = peopleObject.copy()
        new["account"] = f"{array[idx]} Expenses"
        peopleArray.append(new)
        idx+=2
        beginsPerson(array,idx,transDict,transReview, peopleArray, placesArray, otherItems)
    elif len(array)>idx+1 and array[idx+1] == "store":
        placesArray.append(f"{array[idx]} Store")
        idx+=2
        beginsPlace(array, idx, transDict,transReview, peopleArray, placesArray)
    elif len(array)>idx+2 and array[idx]!= "account" and array[idx+1] == "of":
        transDict["service"] = array[idx]  # Stores service
        transReview.append("Review: Confirm SERVICE.")
        idx+=1
        if array[idx] in ["a", "an"]:
            idx+=1
            serviceA_Pattern(array,idx,transDict,peopleArray,transReview,placesArray,otherItems)
        elif array[idx] == "of":
            idx+=1
            serviceOf_Pattern(array, idx, transDict, peopleArray, transReview, placesArray,otherItems)
    elif len(array)>idx+2 and array[idx] not in ignore and process.extractBests(lem.lemmatize(array[idx]),servicesList, scorer=tsr, score_cutoff=95):
        transDict["Service"] = array[idx]   # Stores SERVICE
        idx+=1
        if array[idx] in ["a", "an"]:
            idx+=1
            serviceA_Pattern(array, idx, transDict, peopleArray, transReview, placesArray)
        elif array[idx] == "of":
            idx+=1
            serviceOf_Pattern(array, idx, transDict, peopleArray, transReview, placesArray)
    elif len(array[idx])>3 and array[idx] not in ignore and (array[idx][-3:]== "ing" or array[idx][-4:]== "ings") and process.extractBests(array[idx],ingList,scorer=fuzz.QRatio,score_cutoff=90)==[]:
        transDict["Service"] = array[idx]   # Stores SERVICE
        idx+=1
        if array[idx] in ["a", "an"]:
            idx+=1
            serviceA_Pattern(array, idx, transDict, peopleArray, transReview, placesArray)
        elif array[idx] == "of":
            idx+=1
            serviceOf_Pattern(array, idx, transDict, peopleArray, transReview, placesArray)
    elif findName(array,idx,peopleArray,transReview) != 0:
        temp = findName(array,idx,peopleArray,transReview)
        peopleArray.append(temp[0])
        idx = temp[1]
        beginsPerson(array,idx,transDict,transReview, peopleArray, placesArray, otherItems)
    elif array[idx] not in ignore and process.extractBests(array[idx], itemList, scorer=tsr, score_cutoff=85):
        item = process.extractBests(array[idx], itemList, scorer=tsr, score_cutoff=85)[0][0]
        transDict["item"] = item
        i = len(item.split())
        idx = idx+i
        beginsItem(array,idx,transDict,transReview, peopleArray, placesArray, otherItems, item)
    elif getPlace(array,idx,placesArray) != 0:
        temp = getPlace(array,idx,placesArray)
        placesArray.append(temp[0])
        idx = temp[1]
        beginsPlace(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
    elif any(word in '& and with' for word in array) == False and (array[-1][0].isdigit()==True or array[-1] in ["order"]):
        reverseParse(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
    else:
        x = True
        searchAllKeywords(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)

    if x == False:
        searchAllKeywords(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
        
    if transDict["totalCost"] == "" and transDict["unitCost"] == "":
        checkForCost(array,idx,transDict,transReview,placesArray,peopleArray,otherItems)

    if transDict == transactionObject:
        transReview.append("Error: Transaction in unrecognizeable pattern.")


# Determines if  transaction is a trade/bartern transaction
def tradefor(array,idx,transDict,transReview,peopleArray,placesArray,otherItems):
    if array[idx] == "sundry":
        idx+=1

    if array[idx] not in ignore and (process.extractBests(array[idx], qualifierList, scorer=tsr, score_cutoff=86) or process.extractBests(array[idx],itemList,scorer=tsr,score_cutoff=85) or process.extractBests(array[idx],variantList,scorer=qRatio,score_cutoff=87)):
        transDict["service"] = "Trade"
        item2 = transactionObject.copy()
        if idx>0 and array[idx-1] == "sundry":
            idx = idx-1
        idx = QQI_Pattern(array,idx,item2,transReview,peopleArray,placesArray,otherItems)
        otherItems.append(item2)
    elif array[idx] not in ignore and (process.extractBests(array[idx+1],qualifierList, scorer=tsr, score_cutoff=86) or process.extractBests(array[idx+1],itemList,scorer=tsr,score_cutoff=85) or process.extractBests(array[idx+1],variantList,scorer=qRatio,score_cutoff=87)):
        transDict["service"] = "Trade"
        item2 = transactionObject.copy()
        idx = QQI_Pattern(array,idx,item2,transReview,peopleArray,placesArray,otherItems)
        otherItems.append(item2)
    elif array[idx] not in ignore and process.extractBests(array[idx], servicesList, scorer=tsr, score_cutoff=85):
        transDict["service"] = "Trade"
        item2 = transactionObject.copy()
        idx = serviceOf_Pattern(array,idx,item2,transReview,peopleArray,placesArray,otherItems)
        otherItems.append(item2)
    else:
        for_Keyword(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)

# Compares index values. Accounts for when a function returns 0
def compareIdx(idx,x):
    if x == 0 or x is None:
        return idx
    else:
        return x

# Determines if number is a total/unit cost then convets it to to pound-shilling-pence (If not a COST returns 0, else returns 1)
def getCost(array,idx,transDict,transReview,peopleArray,placesArray,otherItems):  # sourcery skip: low-code-quality
    money = moneyObject.copy()  # Creates new money dictionary
    flag = None  # Flag for UNIT COST
    
    if len(array)<=idx:
        return 0

    if idx>0 and array[idx-1] == "at":
        flag = True  # Flag to store COST as UNIT COST
    
    if len(array)>idx+1 and array[idx] in ["at","is"]:  # Checks if "at"/"is" precedes the COST
        if len(array)>idx+2 and array[idx] == "at" and array[idx+2] == "percent":
            array[idx] == ""   # Removes "at"
            transDict["quantity"] = array[idx+1]
            transDict["qualifier"] = "Percent"
            idx+=3
            if len(array)>idx and fuzz.ratio(array[idx], "exchange")>90:
                idx+=1
        elif idx>0 and array[idx] == "at" and fuzz.ratio(array[idx-1], "valued")>90: # Checks id "valued" precedes "at"
            array[idx] == ""   # Removes "at"
            array[idx-1] == ""   # Removes "valued"
            idx+=1
            x = getCost(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
            idx = compareIdx(idx,x)
            if len(array)>idx and array[idx] in ["currency","currencies","sterling","sterlings"]:
                idx+=1
                x = getCost(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
                idx = compareIdx(idx,x)
        else:
            idx+=1 
            flag = True  # Flag to store COST as UNIT COST

    # Acounts for "valued at" phrase
    if len(array)>idx+1 and fuzz.ratio(array[idx], "valued")>90:
        array[idx] == ""   # Removes "valued"
        idx+=1 
        if array[idx] == "at":
            array[idx] == ""   # Removes "at"
            idx+=1
            x = getCost(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
            idx = compareIdx(idx,x)
            if len(array)>idx and array[idx] in ["currency","currencies","sterling","sterlings"]:
                idx+=1
                x = getCost(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
                idx = compareIdx(idx,x)
            return idx

    # Acounts for if "cost" precedes price
    if fuzz.token_set_ratio(lem.lemmatize(array[idx]),"cost")>90 or fuzz.token_set_ratio(array[idx], "returned")>88:
        idx+=1
    
    # Makes sure that value is a price
    if len(array)>idx+1 and array[idx] not in ignore and ((process.extractBests(array[idx],itemList,scorer=qRatio,score_cutoff=85) or process.extractBests(array[idx],variantList,scorer=qRatio,score_cutoff=86) or process.extractBests(lem.lemmatize(array[idx]),qualifierList,scorer=qRatio,score_cutoff=88))):
        idx+=1
        idx = QQI_Pattern(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
        return idx
    
    # Checks if a items with numerical variants follows
    if len(array)>idx+1 and array[idx+1] not in ignore and process.extractBests(array[idx+1],numItems,scorer=fuzz.QRatio, score_cutoff=88):
        idx = QQI_Pattern(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
        return idx
    
    # Checks for "currency-at-QUANTITY-QUALIFIER" pattern
    if len(array)>idx+4 and array[idx+1] in ["currency","sterling"] and array[idx+2] =="at" and array[idx+3][0].isdigit()==True and array[idx+4] == "percent":
        transDict["quantity"] = array[idx+3]
        transDict["qualifier"] = "percent"
        idx+=5
        return idx

    
    if len(array)>idx+1 and array[idx][0].isdigit() and array[idx+1] in [":","/:"]:
        money["pounds"] = "0"
        money["shillings"] = array[idx]
        money["pence"] = "0"
        if flag == True or (len(array)>idx+2 and array[idx+1] == "per" and array[idx+2] == "order"):
            transDict["unitCost"] = money
        else:
            transDict["totalCost"] = money
        idx+=2
        return idx

    cost = nltk.word_tokenize(array[idx])   # Tokenizes the string
    if len(cost) == 1 and "/" in cost[0]:   # Checks for "2/1" COST pattern
        cost = cost[0].split("/")
        money["pounds"] = "0"
        money["shillings"] = cost[0]
        money["pence"] = cost[1]
    elif len(cost) == 2 and "/" in cost[0] and cost[1]==":":   # Checks for "8/:" COST pattern
        cost = cost[0].split("/")
        money["pounds"] = "0"
        money["shillings"] = cost[0]
        money["pence"] = "0"
    elif len(cost) == 1 and cost[0].isalnum() == True and cost[0][-1] in ["d","D"]:     # Checks for "6d" COST pattern
        p = cost[0].replace(cost[0][-1],"")  # Removes the "d"
        money["pounds"] = "0"
        money["shillings"] = "0"
        money["pence"] = p
    elif len(cost) == 5 and cost[1] == ".." and cost[3] == "..":  # Checks for "83..6..2" pattern
        money["pounds"] = cost[0]
        money["shillings"] = cost[2]
        money["pence"] = cost[4]
    elif len(cost) == 4 and cost[0] == ".." and cost[2] == "..":  # Checks for "..6..2" pattern
        for i, element in enumerate(cost):
            if element == "..." and  i+1 < len(cost):
                x=cost[i+1]
                cost[i+1] = f"0.{x}"
        money["pounds"] = "0"
        money["shillings"] = cost[1]
        money["pence"] = cost[3]
    elif len(cost) == 4 and cost[-1] == "..":  # Checks for "83..6.." pattern
        money["pounds"] = cost[0]
        money["shillings"] = cost[2]
        money["pence"] = "0"
    elif len(cost) == 1 and cost[0].isdigit()==True:
        money["pounds"] = "0"
        money["shillings"] = "0"
        money["pence"] = cost[0]

    if money["pence"] == "":
        return 0  # Returns 0 if COST not found
    if len(array)>idx+2 and array[idx+1] == "per" and array[idx+2] == "order":  # Checks if "per order" follows COST
        transDict["unitCost"] = money  # Saves COST as UNIT_COST
        array[idx+1] == ""   # Removes "per" keyword
        if len(array) > 0 and array[idx-1] == "at":
            array[idx-1] == ""   # Removes "at" keyword
        idx+=3
    elif flag == True:
        transDict["unitCost"] = money  # Saves COST as UNIT_COST
        idx+=1
        if len(array)>idx and array[idx][0].isdigit():
            x = getCost(array,idx,transDict,transReview,peopleArray,placesArray,otherItems) # Checks if TOTAL_COST follows UNIT_COST
            idx = compareIdx(idx,x)
        elif len(array)>idx and len(array[idx])>2 and array[idx][0:2] == "..":
            x = getCost(array,idx,transDict,transReview,peopleArray,placesArray,otherItems) # Checks if TOTAL_COST follows UNIT_COST
            idx = compareIdx(idx,x)
    else:
        transDict["totalCost"] = money  # Saves COST as TOTAL_COST
        idx+1
        if len(array)>idx+2 and array[idx] == "at" and array[idx+2] == "percent":
            array[idx] == ""   # Removes "at"
            transDict["quantity"] = array[idx+1]
            transDict["qualifier"] = "Percent"
            idx+=3
            return idx
    return idx  # Returns True and index if COST found

# Determines if word is a known PLACE name (If not found returns 0, else returns index of next word )
def getPlace(array,idx,placesArray):
    # Searches for place
    if array[idx] not in ignore and process.extractBests(array[idx],placesList,scorer=fuzz.token_set_ratio,score_cutoff=80):
        place = process.extractBests(array[idx], placesList, scorer=fuzz.token_set_ratio, score_cutoff=80)[0][0]
        i = len(place.split())
        idx = idx+i
        return [place, idx]
    # Searches for place alias and maps it to the real/full name
    elif array[idx] not in ignore and process.extractBests(array[idx],placeAliasList["alias"],scorer=fuzz.token_set_ratio,score_cutoff=80):
        pl = process.extractBests(array[idx], placeAliasList["alias"], scorer=fuzz.token_set_ratio, score_cutoff=80)[0][0]
        n = placeAliasList.index[placeAliasList['alias']==pl].tolist()
        realName = placeAliasList['location'].loc[n[0]]
        i = len(realName.split())
        idx = idx+i
        return [realName, idx]
    else:
        return 0

# Finds names & professions, and preceding/following prefixes/suffixes. Returns 0 if no names found, else it returns the index of next word
def findName(array,idx,peopleArray,transReview):
    newPplDict = peopleObject.copy()   # Initializes new people dictionary

    # Helper function to search for profession
    def professionFinder(idx,newPplDict):  # n = need distance from end of array
        if array[idx] not in ignore and process.extractBests(array[idx], professionList, scorer=qRatio, score_cutoff=90):
            match = process.extractBests(array[idx], professionList, scorer=qRatio, score_cutoff=90)[0][0]
            newPplDict["profession"] = match
            idx = idx + len(match.split())   # If profession is found, increments "i" by length of profession name
        elif idx>0 and array[idx] not in ignore and array[idx-1]=="the" and process.extractBests(array[idx], placesList, scorer=fuzz.QRatio, score_cutoff=90)==[]:
            newPplDict["profession"] = array[idx]
            transReview.append("Review: Confirm PLACE name,")
            idx+=1
        return idx
          
    # Checks for preceeding professions     
    idx = professionFinder(idx,newPplDict)

    # Checks if first name is in database
    while len(array) > idx:
        # Checks for prefixes
        if array[idx] in prefixList:
            newPplDict["prefix"] = array[idx]
            idx+=1

        # Checks if first name is in database
        if array[idx] not in ignore and process.extractBests(array[idx], firstNameList, scorer=qRatio, score_cutoff=90):
            newPplDict["firstName"] = process.extractBests(array[idx], firstNameList, scorer=qRatio, score_cutoff=90)[0][0]
            idx = idx + len(newPplDict["firstName"].split())
            if array[idx] not in ignore and process.extractBests(array[idx], lastNameList, scorer=qRatio, score_cutoff=90):   #Checks for last name
                newPplDict["lastName"] = process.extractBests(array[idx], lastNameList, scorer=qRatio, score_cutoff=90)[0][0]
                idx = idx + len(newPplDict["lastName"].split())
                if len(array)>idx and array[idx] not in ignore and process.extractBests(array[idx], suffixList, scorer=qRatio, score_cutoff=90):  # Checks for suffixes 
                    newPplDict["suffix"] = process.extractBests(array[idx], suffixList, scorer=qRatio, score_cutoff=90)[0][0]
                    idx = idx + len(newPplDict["suffix"].split())
                    professionFinder(idx,newPplDict) # Checks for following professions
            else: 
                newPplDict["lastName"] = "LNU"
            # Checks for suffixes following first name
            if len(array)>idx and array[idx] not in ignore and process.extractBests(array[idx], suffixList, scorer=tsr, score_cutoff=90):  # Checks for suffixes 
                newPplDict["suffix"] = process.extractBests(array[idx], suffixList, scorer=tsr, score_cutoff=90)[0][0]
                idx = idx + len(newPplDict["suffix"].split())
                idx = professionFinder(idx,newPplDict) # Checks for following professions
        elif array[idx] not in ignore and process.extractBests(array[idx], lastNameList, scorer=tsr, score_cutoff=90):  # Checks if name is a last name
            newPplDict["firstName"] = "FNU"
            newPplDict["lastName"] = process.extractBests(array[idx], lastNameList, scorer=tsr, score_cutoff=90)[0][0]
            idx = idx + len(newPplDict["lastName"].split())
            if len(array)>idx and array[idx] not in ignore and process.extractBests(array[idx], suffixList, scorer=tsr, score_cutoff=90):  # Checks for suffixes 
                newPplDict["suffix"] = process.extractBests(array[idx], suffixList, scorer=tsr, score_cutoff=90)[0][0]
                idx = idx + len(newPplDict["suffix"].split())
                idx = professionFinder(idx,newPplDict) # Checks for following professions
        # Checks is any person found
        if newPplDict == peopleObject:
            return 0  # Returns zero if no names found
        else:
            return [newPplDict, idx]

# Looks for a first name/prefix if a last name is found
def reverseFindName(array,idx,transReviwe,peopleArray):
    newPplDict = peopleObject.copy()   # Initializes new people dictionary
    while idx > 0: 
        if  array[idx] not in ignore and process.extractBests(array[idx], professionList, scorer=tsr, score_cutoff=86):  # Checks for professions following names
            job = process.extractBests(array[idx], professionList, scorer=tsr, score_cutoff=86)[0][0]
            newPplDict["profession"] = job
            idx = idx - len(job.split())
        
        if array[idx] == "the":  # Accounts for "the" preceding professions
            idx = idx-1

        if array[idx] in suffixList:    # Checks for suffix
            newPplDict["suffix"] = array[idx]
            idx = idx-1

        if array[idx] not in ignore and process.extractBests(array[idx], lastNameList, scorer=tsr, score_cutoff=88):  # Checks for last name
            LName = process.extractBests(array[idx-1], lastNameList, scorer=tsr, score_cutoff=88)[0][0]
            newPplDict["lastName"] = array[idx]  # Stores last name
            idx = idx - len(LName.split())

        if array[idx] not in ignore and process.extractBests(array[idx], firstNameList, scorer=tsr, score_cutoff=88):  # Checks for first Name
            fName = process.extractBests(array[idx], firstNameList, scorer=tsr, score_cutoff=88)[0][0]
            newPplDict["firstName"] = fName
            idx = idx - len(fName.split())

        if array[idx] in prefixList:   # Checks if prefix precedes first name
            newPplDict["prefix"] = array[idx]
            idx = idx-1

        if array[idx] not in ignore and process.extractBests(array[idx], professionList, scorer=tsr, score_cutoff=86):  # Checks for preceding profession
            job = process.extractBests(array[idx], professionList, scorer=tsr, score_cutoff=86)[0][0]
            newPplDict["profession"] = job
            idx = idx - len(job.split())

        if newPplDict != peopleObject:
            peopleArray.append(newPplDict)
            return 1
        else:
            return 0
        
# Reverse parses transactions
def reverseParse(array,idx,transDict,transReview,peopleArray,placesArray,otherItems):
    idx = len(array)-1

    # Helper Function to account for ADJECTIVES
    def getAdjs():
        transDict["qualifier"] = array[idx]
        idx = idx-1
        if idx>0 and array[idx][0].isdigit() == True and array[idx].isalnum() == False: # Checks for QUANTITY
            transDict["quantity"] = array[idx]
            if idx>0 and array[0] == "of":  # Checks if "of" precedes/Accounts for SERVICE-of pattern
                idx = idx-1
                # Checks for SERVICES
                if idx>0 and array[idx] not in ignore and process.extractBests(lem.lemmatize(array[idx]),servicesList,scorer=tsr,score_cutoff=95)==True:  
                    transDict["service"] = process.extractBests(lem.lemmatize(array[idx]),servicesList,scorer=tsr,score_cutoff=95)[0][0]
                elif len(array[idx])>3 and array[idx] not in ignore and (array[idx][-3:]== "ing" or array[idx][-4:]== "ings") and process.extractBests(array[idx],ingList,scorer=fuzz.QRatio,score_cutoff=90)==[]:
                    transDict["service"] = array[idx]
                else:
                    transDict["service"] = array[idx]
                    transReview.append("REVIEW: Confirm Service")

    # Helper Function to account for SERVICES
    def servicesHelper():
        if idx>0 and array[idx] not in ignore and process.extractBests(lem.lemmatize(array[idx]),servicesList,scorer=tsr,score_cutoff=90): 
            transDict["service"] = process.extractBests(lem.lemmatize(array[idx]),servicesList,scorer=tsr,score_cutoff=90)[0][0]
        elif len(array[idx])>3 and array[idx] not in ignore and (array[idx][-3:]== "ing" or array[idx][-4:]== "ings") and process.extractBests(array[idx],ingList,scorer=fuzz.QRatio,score_cutoff=90)==[]:
            transDict["service"] = array[idx]
        elif idx > 1 and array[idx] == "of" and array[idx-1] not in ignore and process.extractBests(lem.lemmatize(array[idx-1]),servicesList,scorer=tsr,score_cutoff=90) or lem.lemmatize(array[idx-1])[-3:]=="ing":
            transDict["service"] = array[idx-1]

    # Checks if last element is a COST
    if len(array)>3 and array[idx] == "order" and array[idx-1] == "per" and array[idx-2][0].isdigit()==True:
        idx = idx-3
    if getCost(array,idx,transDict,transReview,peopleArray,placesArray,otherItems) == 0: # Checks for COST and returns if not found 
        return idx
    idx = idx-1

    # Checks if "at" precedes COST
    if 0<idx and array[idx] == "at":
        idx = idx-1

    if 0<idx and array[idx] in qualifierList:   # Accounts for ITEMS that are also QUALIFIERS
        transDict["item"] = array[idx]
        idx = idx-1
        if 0<idx and array[0] in ["a", "an"]:  # Checks if "a" or "an" precedes QUALIFIER
            transDict["quantity"] = "1"    # Saves QUANTITY as 1
            idx = idx-1
            servicesHelper()  # Checks for SERVICES
        elif 0<idx and array[idx][0].isdigit()==True:  # Checks if a digit precedes QUALIFIER
            transDict["quantity"] = array[idx]   # Saves QUANTITY
            idx = idx-1
            servicesHelper()  # Checks for SERVICES
    elif idx>0 and array[idx] not in ignore and process.extractBests(lem.lemmatize(array[idx]), servicesList, scorer=tsr, score_cutoff=95)==True:  
        transDict["service"] = process.extractBests(lem.lemmatize(array[idx]), servicesList, scorer=tsr, score_cutoff=95)[0][0]
        idx = idx-1
        if idx>0 and lem.lemmatize(array[idx]) in qualifierList:  # Checks for QUALIFIER
                transDict["qualifier"] = array[idx]
                idx = idx-1
                if 0<idx and array[idx][0].isdigit()==True:  # Checks if a digit precedes QUALIFIER
                    transDict["quantity"] = array[idx]   # Saves QUANTITY
    elif len(array[idx])>3 and array[idx] not in ignore and (array[idx][-3:]== "ing" or array[idx][-4:]== "ings") and process.extractBests(array[idx],ingList,scorer=fuzz.QRatio,score_cutoff=90)==[]:
        transDict["service"] = array[idx]
        idx = idx-1
        if 0<idx and lem.lemmatize(array[idx]) in qualifierList:  # Checks for QUALIFIER
                transDict["qualifier"] = array[idx]
                idx = idx-1
                if 0<idx and array[idx][0].isdigit()==True:  # Checks if a digit precedes QUALIFIER
                    transDict["quantity"] = array[idx]   # Saves QUANTITY
    elif idx>0 and array[idx] not in ignore and process.extractBests(array[idx], itemList, scorer=tsr, score_cutoff=90):  # Checks for ITEM
        item = process.extractBests(lem.lemmatize(array[idx]), itemList, scorer=tsr, score_cutoff=90)
        transDict["item"] = item[0][0]
        i = len(item[0][0].split())
        idx = idx-i

        if 0<idx and array[0] == "of":  # Checks if "of" precedes/Accounts for QUALIFIER-of pattern
            idx = idx-1
            if 0<idx and lem.lemmatize(array[idx]) in qualifierList:  # Checks for QUALIFIER
                transDict["qualifier"] = array[idx]
                idx = idx-1
                if 0<idx and array[0] in ["a", "an"]:  # Checks if "a" or "an" precedes QUALIFIER
                    transDict["quantity"] = "1"    # Saves QUANTITY as 1
                    idx = idx-1
                    servicesHelper()  # Checks for SERVICES
                elif 0<idx and array[idx][0].isdigit()==True:  # Checks if a digit precedes QUALIFIER
                    transDict["quantity"] = array[idx]   # Saves QUANTITY
                    idx = idx-1
                    servicesHelper()  # Checks for SERVICES
        elif 0<idx and lem.lemmatize(array[idx]) in qualifierList:  # Checks for QUALIFIER
            transDict["qualifier"] = array[idx]
            idx = idx-1
            if 0<idx and array[idx][0].isdigit()==True and array[idx].isalnum()==False: # Checks for QUANTITY
                transDict["quantity"] = array[idx]
                if 0<idx and array[0] == "of":  # Checks if "of" precedes/Accounts for SERVICE-of pattern
                    idx = idx-1
                    # Checks for SERVICES
                    if idx>0 and array[idx] not in ignore and process.extractBests(lem.lemmatize(array[idx]), servicesList, scorer=tsr, score_cutoff=95)==True:  
                        transDict["service"] = process.extractBests(lem.lemmatize(array[idx]), servicesList, scorer=tsr, score_cutoff=95)[0][0]
                    elif len(array[idx])>3 and array[idx] not in ignore and (array[idx][-3:]== "ing" or array[idx][-4:]== "ings") and process.extractBests(array[idx],ingList,scorer=fuzz.QRatio,score_cutoff=90)==[]:
                        transDict["service"] = array[idx]
                    else:
                        transDict["service"] = array[idx]
                        transReview.append("REVIEW: Confirm Service")
        elif 0<idx-1 and lem.lemmatize(array[idx-1]) in qualifierList:  # Accounts for 1 ADJECTIVE
            idx = idx-1
            variantsArray = []
            variantsArray.append(array[idx])
            transReview.append("Review: Confirm ADJECTIVE.")
            getAdjs()
        elif 0<idx-2 and lem.lemmatize(array[idx-2]) in qualifierList:  # Accounts for 2 ADJECTIVEs
            idx = idx-2
            variantsArray = []
            variantsArray.append(array[idx])
            variantsArray.append(array[idx-1])
            transReview.append("Review: Confirm ADJECTIVES.")
            getAdjs()

# Runs the keyword functions
def searchAllKeywords(array,idx,transDict,transReview,peopleArray,placesArray,otherItems):

    if "per" in array:  # Checks for "per" keyword
        index = None
        per_Keyword(array,index,transDict,transReview,peopleArray,placesArray,otherItems)

    if "charge" in array:  # Checks for "charge" keyword
        charge_Keyword(array, transDict, transReview)

    if "returned" in array or "returnd" in array or "omitted" in array:   # Checks for "returned" and "omitted" keywords
        returnedOmitted_Keywords(array,transDict)

    if process.extractBests("expense", array, scorer=tsr, score_cutoff=90):   # Checks for "expense" keyword
        index = array.index(process.extractBests("expence", array, scorer=tsr, score_cutoff=90)[0][0])
        expense_Keyword(array,index,transDict,transReview,peopleArray,placesArray,otherItems)

    if process.extractBests("expence", array, scorer=tsr, score_cutoff=90):   # Checks for "expence" (misspelled) keyword
        index = array.index(process.extractBests("expence", array, scorer=tsr, score_cutoff=90)[0][0])
        expense_Keyword(array,index,transDict,transReview,peopleArray,placesArray,otherItems)

    if process.extractBests("account", array, scorer=tsr, score_cutoff=90):  # Checks for "account" keyword
        (array,idx,transDict,transReview,peopleArray,placesArray,otherItems)

    if "value" in array:   # Checks for "value" keyword
        value_Keyword(array, transReview,transDict)

    if "by" in array:    # Checks for "by" keyword
        by_Keyword(array, transReview,placesArray,peopleArray)

    if "for" in array:    # Checks for "for" keyword
        idx = None
        for_Keyword(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)

    if process.extractBests("ballance", array, scorer=tsr, score_cutoff=87):   # Checks for "balance" keyword
        balance_Keyword(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)

    for i in range(len(array) - 1):  # Checks for "on the" key phrase
        value = array[i:i+2]
        if value == ["on", "the"]:
            idx1 = i
            idx2 = i+1
            onThe_Keywords(array,idx1, idx2, placesArray, transReview)
            break

    for i in range(len(array) - 1):  # Checks for "in the" key phrase
        value = array[i:i+2]
        if value == ["in", "the"]:
            idx1 = i
            idx2 = i+1
            onThe_Keywords(array,idx1, idx2, placesArray, transReview)
            break

# Assists in obtaining possible ADJECTIVES/Variants
def findAdjectives(array,idx,transDict,transReview,peopleArray,placesArray,otherItems):
    excludeList = keywordList+conjunctions
        
    if len(array)>idx+1 and array[idx+1] not in ignore and process.extractBests(array[idx+1], itemList, scorer=tsr, score_cutoff=87):  # Accounts for 1 ADJECTIVE
        transDict["variants"].append(array[idx])  # Stores VARIANT
        idx+=1
        idx = getItem(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)# Gets ITEM
        if transDict["quantity"] == "" and process.extractBests(transDict["item"],nonItems,scorer=tsr,score_cutoff=95)==[]:
            transDict["quantity"] = "1"
        idx = afterItemCheck(array,idx,transDict,transReview,peopleArray,placesArray,otherItems,"item")
        return idx
    elif len(array)>idx+2 and array[idx+2] not in ignore and process.extractBests(array[idx+2], itemList, scorer=tsr, score_cutoff=87) and array[idx] not in excludeList and array[idx+1] not in excludeList:  # Accounts for 2 ADJECTIVES
        transDict["variants"].extend([array[idx],array[idx+1]])  # Stores VARIANT
        idx+=2
        idx = getItem(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)  # Gets ITEM
        if transDict["quantity"] == "" and process.extractBests(transDict["item"],nonItems,scorer=tsr,score_cutoff=95)==[]:
            transDict["quantity"] = "1"
        idx = afterItemCheck(array,idx,transDict,transReview,peopleArray,placesArray,otherItems,"item")
        return idx
    elif len(array)>idx+3 and array[idx+3] not in ignore and process.extractBests(array[idx+3], itemList, scorer=tsr, score_cutoff=87) and array[idx] not in excludeList and array[idx+1] not in excludeList and array[idx+2] not in excludeList:  # Accounts for 3 ADJECTIVES
        transDict["variants"].extend([array[idx],array[idx+1]])  # Stores VARIANT
        idx+=3
        idx = getItem(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)  # Gets ITEM
        if transDict["quantity"] == "" and process.extractBests(transDict["item"],nonItems,scorer=tsr,score_cutoff=95)==[]:
            transDict["quantity"] = "1"
        idx = afterItemCheck(array,idx,transDict,transReview,peopleArray,placesArray,otherItems,"item")
        return idx
    else:
        if transDict["quantity"] == "1":
            transDict["quantity"] = ""
        x = getCost(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
        idx = compareIdx(idx,x)
    return idx

# Variant Items Check
def variantItemCheck(array,idx,transDict,transReview,peopleArray,placesArray,otherItems):
    def secondCheck(array,idx,transDict):
        if len(array)>idx+2 and array[idx+2] not in ignore and process.extractBests(array[idx+2],variantItemsList, scorer=fuzz.QRatio, score_cutoff=86):    
            if len(array)>idx+4 and array[idx+3] not in ignore and process.extractBests(array[idx+3],variantItemsList, scorer=fuzz.QRatio, score_cutoff=86) and process.extractBests(array[idx+4],itemList, scorer=fuzz.QRatio, score_cutoff=86):
                v1 = process.extractBests(array[idx], variantItemsList, scorer=fuzz.QRatio, score_cutoff=86)[0][0]
                v2 = process.extractBests(array[idx+1], variantItemsList, scorer=fuzz.QRatio, score_cutoff=86)[0][0]
                v3 = process.extractBests(array[idx+2], variantItemsList, scorer=fuzz.QRatio, score_cutoff=86)[0][0]
                v4 = process.extractBests(array[idx+3], variantItemsList, scorer=fuzz.QRatio, score_cutoff=86)[0][0]
                transDict["variants"].extend([v1,v2,v3,v4])
                transDict["item"] = process.extractBests(array[idx+4],itemList, scorer=fuzz.QRatio, score_cutoff=86)[0][0]
                idx+=5
            elif len(array)>idx+3 and array[idx+3] not in ignore and process.extractBests(array[idx+3],itemList, scorer=fuzz.QRatio, score_cutoff=86):
                v1 = process.extractBests(array[idx], variantItemsList, scorer=fuzz.QRatio, score_cutoff=86)[0][0]
                v2 = process.extractBests(array[idx+1], variantItemsList, scorer=fuzz.QRatio, score_cutoff=86)[0][0]
                v3 = process.extractBests(array[idx+2], variantItemsList, scorer=fuzz.QRatio, score_cutoff=86)[0][0]
                transDict["variants"].extend([v1,v2,v3])
                transDict["item"] = process.extractBests(array[idx+3],itemList, scorer=fuzz.QRatio, score_cutoff=86)[0][0]
                idx+=4
            else:
                v1 = process.extractBests(array[idx], variantItemsList, scorer=fuzz.QRatio, score_cutoff=86)[0][0]
                v2 = process.extractBests(array[idx+1], variantItemsList, scorer=fuzz.QRatio, score_cutoff=86)[0][0]
                transDict["variants"].extend([v1,v2])
                transDict["item"] = process.extractBests(array[idx+2],variantItemsList, scorer=fuzz.QRatio, score_cutoff=86)[0][0]
                idx+=3
        else:
            transDict["variants"].append(process.extractBests(array[idx], variantItemsList, scorer=fuzz.QRatio, score_cutoff=86)[0][0])
            transDict["item"] = process.extractBests(array[idx+1],variantItemsList, scorer=fuzz.QRatio, score_cutoff=86)[0][0]
            idx+=2
        return idx

    # Checks if next word is an ITEM
    if len(array)>idx+1 and array[idx+1] not in ignore and process.extractBests(array[idx+1],variantItemsList, scorer=fuzz.QRatio, score_cutoff=86):
        idx = secondCheck(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
    else:
        transDict["item"] = process.extractBests(array[idx],variantItemsList, scorer=fuzz.QRatio, score_cutoff=86)[0][0]
        idx+=1
    return idx

# Checks for common keywords after ITEMS
def afterItemCheck(array,idx,transDict,transReview,peopleArray,placesArray,otherItems,flag):
    if transDict["item"] != "" and process.extractBests(transDict["item"], numItems, scorer=fuzz.QRatio, score_cutoff=88):
        numItemCheck(array,idx,transDict)
    if process.extractBests(transDict["item"], liquidItems, scorer=fuzz.QRatio, score_cutoff=88):
        liquids = {"0.25":["1","Quart"], "0.5":["2","Quart"], "0.125":["1","Pint"],"0.75":["3","Quart"], "0.375":["3","Pint"],"0.875":["7","Pint"]}
        if transDict["quantity"] in liquids:
            temp = liquids[transDict["quantity"]]
            transDict["quantity"] = temp[0]
            transDict["qualifier"] = temp[1]
        elif transDict["qualifier"] != "Quart":
            transDict["qualifier"] = "Quart"
    
    if len(array)>idx and array[idx] in ["&","and","with",","]:
        x = and_item(array,idx,transDict,transReview,peopleArray,placesArray,otherItems,flag)
        idx = compareIdx(idx,x)
        x = getCost(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
        idx = compareIdx(idx,x)
    elif len(array)>idx and array[idx] =="for":
        array[idx] = ""
        idx+=1 
        idx = tradefor(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
    else:
        x = getCost(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
        idx = compareIdx(idx,x)
        if len(array)>idx and array[idx] =="for":
            array[idx] = ""
            idx+=1
            idx = tradefor(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
    return idx

# Checks for common keywords after SERVICES
def afterServiceCheck(array,idx,transDict,transReview,peopleArray,placesArray,otherItems,flag):   
    if len(array)>idx and array[idx] in ["&","and","with",","]:
        idx = and_item(array,idx,transDict,transReview,peopleArray,placesArray,otherItems,flag)
        x = getCost(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
        idx = compareIdx(idx,x)
    elif len(array)>idx and array[idx] =="for":
        array[idx] = ""
        idx+=1 
        idx = tradefor(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
    else:
        x = getCost(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
        idx = compareIdx(idx,x)
        if len(array)>idx and array[idx] =="for":
            idx+=1
            idx = tradefor(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
    return idx

# Gets ITEM
def getItem(array,idx,transDict,transReview,peopleArray,placesArray,otherItems): # sourcery skip: low-code-quality
    if array[idx] not in ignore and process.extractBests(array[idx], [i.split(' ', 1)[0] for i in multiItemList], scorer=tsr, score_cutoff=85):
        temp =  process.extractBests(array[idx], [i.split(' ', 1)[0] for i in multiItemList], scorer=tsr, score_cutoff=85)[0][0]
        if len(array)>idx+2 and array[idx+1] not in ignore and process.extractBests(array[idx+1],conjunctions,scorer=fuzz.QRatio,score_cutoff=88):
            idx+=1
            seperate = [word for word in multiItemList if "&" in word]
            if len(seperate)>0:
                seperate = [i.split('&', 1)[1].strip() for i in seperate]
                if process.extractBests(array[idx], seperate,scorer=fuzz.QRatio,score_cutoff=88):
                    temp2 = process.extractBests(array[idx], seperate,scorer=fuzz.QRatio,score_cutoff=88)
                    transDict["item"] = temp+" with "+temp2
                    idx+=3
                else:
                    transDict["item"] = temp+" with "+array[idx]
                    transReview.append("Review: Confirm ITEM")
                    idx+=3
        elif len(array)>idx+3 and process.extractBests(f"{temp} {array[idx+1]} {array[idx+3]}", itemList, scorer=fuzz.QRatio, score_cutoff=85):
            transDict["item"] = process.extractBests(f"{temp} {array[idx+1]} {array[idx+3]}", itemList, scorer=fuzz.QRatio, score_cutoff=85)[0][0]
            idx+=4
        elif len(array)>idx+1 and process.extractBests(f"{temp} {array[idx+1]}", itemList, scorer=fuzz.QRatio, score_cutoff=85):
            temp2 = process.extractBests(array[idx+1], [i.split(' ', 1)[1] for i in multiItemList], scorer=fuzz.QRatio, score_cutoff=86)[0][0]
            if len(array)>idx+3 and array[idx+2] == "with" and process.extractBests(f"{temp} {temp2} with {array[idx+3]}", itemList, scorer=fuzz.QRatio, score_cutoff=85):
                transDict["item"] = process.extractBests(f"{temp} {temp2} with {array[idx+3]}", itemList, scorer=fuzz.QRatio, score_cutoff=85)[0][0]
                idx+4
            else:
                transDict["item"] = multiItemList[multiItemList.index(f"{temp} {temp2}")]
                idx = len(transDict["item"])+idx
        elif array[idx][:2] == "w/":
            array[idx] = array[idx][2:]
            if len(array)>idx+1 and process.extractBests(f"{temp} {array[idx+1]}", itemList, scorer=fuzz.QRatio, score_cutoff=85):
                transDict["item"] = process.extractBests(array[idx+1], [i.split(' ', 1)[1] for i in multiItemList], scorer=tsr, score_cutoff=86)[0][0]
                idx = len(transDict["item"])+idx
        else:
            transDict["item"] = temp
            idx = idx + len(transDict["item"].split())
    elif array[idx] not in ignore and process.extractBests(array[idx],variantItemsList, scorer=fuzz.QRatio, score_cutoff=86):
        idx = variantItemCheck(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
    else:
        transDict["item"] = process.extractBests(array[idx],itemList, scorer=fuzz.QRatio, score_cutoff=86)[0][0]
        idx+=len(transDict["item"].split())
    return idx

def numItemCheck(array,idx,transDict):  # sourcery skip: low-code-quality
    decimals = {"0.25":"¼", "0.33":"⅓", "0.5":"½", "0.2":"⅕", "0.167":"⅙", "0.143":"⅐", "0.125":"⅛", "0.111":"⅑", 
                "0.1":"⅒", "0.667":"⅔", "0.4":"⅖", "0.75":"¾", "0.6":"⅗", "0.375":"⅜", "0.8":"⅘", "0.833":"⅚", "0.875":"⅞"}

    if fuzz.QRatio(array[idx],"Check")>88:
        if transDict["quantity"] in decimals:
            transDict["quantity"] = decimals[transDict["quantity"]]
    elif fuzz.QRatio(array[idx],"Dowlass")>88:
        if transDict["quantity"] in decimals:
            transDict["quantity"] = decimals[transDict["quantity"]]
    elif fuzz.QRatio(array[idx],"Linen")>88:
        if transDict["quantity"] in decimals:
            transDict["quantity"] = decimals[transDict["quantity"]]
    elif fuzz.QRatio(array[idx],"Stays")>88:
        if len(array)>idx+1 and array[idx+1] == "N":
            if len(array)>idx+2 and array[idx+2].isdigit() == True:
                transDict["item"] = f"Stays N {array[idx+2]}"
            else:
                 transDict["item"] = "Stays N"
    elif fuzz.QRatio(array[idx],"Breeches")>88:
        if transDict["quantity"]=="" and transDict["qualifier"]=="":
            transDict["quantity"] = "1"
            transDict["qualifier"]= "Pair"

# Checks if string is an integer or float
def numType(x):
    def isInt(x):
        try:
            int(x)
            return True
        except ValueError:
            return False
    
    if isInt(x) == True:
        return int
    else:
        try:
            float(x)
            return float
        except ValueError:
            return False

# =========================================== #
#             Pattern Functions               #
# =========================================== #

# Handles the "QUANTITY-QUALIFIER-ITEM" pattern
def QQI_Pattern(array,idx,transDict,transReview,peopleArray,placesArray,otherItems):
    if len(array)<idx:
        return 0

    if transDict["quantity"]=="" and array[idx] in ["a","an"]:
        transDict["quantity"] = "1"   # Stores "1" as QUANTITY
        idx+=1
    elif transDict["quantity"]=="" and numType(array[idx])!= False:
        transDict["quantity"] = array[idx]  # Stores number as QUANTITY
        idx+=1
    elif "/" in array[idx] and len(array[idx].split('/')) == 2:
        transDict["quantity"] = array[idx]
        idx+=1
    
    # Removes "nett" that precedes ITEM
    if "nett" in array and "off" not in array:
        array.pop(array.index("nett"))
    
    while len(array)>idx:  
        # Qualifier check
        if array[idx] not in ignore and process.extractBests(array[idx],itemQualList,scorer=fuzz.QRatio,score_cutoff=88):  # Checks if QUALIFIER is also an ITEM
            idx+=1
            if array[idx] in ["&","and","with"]:
                transDict["item"] = process.extractBests(array[idx-1], itemList, scorer=fuzz.QRatio, score_cutoff=88)[0][0]
                idx = afterItemCheck(array,idx,transDict,transReview,peopleArray,placesArray,otherItems,"item")
            elif array[idx] not in ignore and process.extractBests(array[idx], itemList, scorer=fuzz.QRatio, score_cutoff=88):  # Checks if next word is an ITEM
                transDict["qualifier"] = process.extractBests(array[idx-1], itemQualList, scorer=fuzz.QRatio, score_cutoff=88)[0][0]
                idx = getItem(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
                if transDict["quantity"] == "" and process.extractBests(transDict["item"],nonItems,scorer=tsr,score_cutoff=95)==[]:
                    transDict["quantity"] = "1"
                idx = afterItemCheck(array,idx,transDict,transReview,peopleArray,placesArray,otherItems,"item")
            elif array[idx] == "of":
                transDict["qualifier"] = process.extractBests(array[idx-1], itemQualList, scorer=fuzz.QRatio, score_cutoff=88)[0][0]
                idx+=1
                if array[idx] not in ignore and process.extractBests(array[idx], variantList, scorer=fuzz.QRatio, score_cutoff=88):
                    transDict["item"] = process.extractBests(array[idx], variantList, scorer=fuzz.QRatio, score_cutoff=88)[0][0]
                    idx += len(transDict["item"].split())
                    if transDict["quantity"] == "" and process.extractBests(transDict["item"],nonItems,scorer=tsr,score_cutoff=95)==[]:
                        transDict["quantity"] = "1"
                    idx = afterItemCheck(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
                elif array[idx] not in ignore and process.extractBests(array[idx], itemList, scorer=fuzz.QRatio, score_cutoff=88):
                    idx = getItem(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
                    if transDict["quantity"] == "" and process.extractBests(transDict["item"],nonItems,scorer=tsr,score_cutoff=95)==[]:
                        transDict["quantity"] = "1"
                    idx = afterItemCheck(array,idx,transDict,transReview,peopleArray,placesArray,otherItems,"item")
                else:
                    transDict["item"] = array[idx]
                    transReview.append("Review: Confirm ITEM.")
                    idx+=1
                    if transDict["quantity"] == "" and process.extractBests(transDict["item"],nonItems,scorer=tsr,score_cutoff=95)==[]:
                        transDict["quantity"] = "1"
                    idx = afterItemCheck(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
            elif array[idx] not in ignore and process.extractBests(array[idx], variantList, scorer=fuzz.QRatio, score_cutoff=88):
                transDict["qualifier"] = process.extractBests(array[idx-1], itemQualList, scorer=fuzz.QRatio, score_cutoff=88)[0][0]
                transDict["item"] = process.extractBests(array[idx], variantList, scorer=fuzz.QRatio, score_cutoff=88)[0][0]
                idx += len(transDict["item"].split())
                if transDict["quantity"] == "" and process.extractBests(transDict["item"],nonItems,scorer=tsr,score_cutoff=95)==[]:
                    transDict["quantity"] = "1"
                idx = afterItemCheck(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
            else:
                transDict["item"] = process.extractBests(array[idx-1], itemQualList, scorer=fuzz.QRatio, score_cutoff=88)[0][0]
                idx += len(transDict["item"].split())
                if transDict["quantity"] == "" and process.extractBests(transDict["item"],nonItems,scorer=tsr,score_cutoff=95)==[]:
                    transDict["quantity"] = "1"
                idx = afterItemCheck(array,idx,transDict,transReview,peopleArray,placesArray,otherItems, "quantity")
        elif array[idx] not in ignore and process.extractBests(lem.lemmatize(array[idx]),qualifierList,scorer=fuzz.QRatio,score_cutoff=88):  # Checks for Qualifiers
            if fuzz.QRatio(array[idx],"dozen")>88:   # Converts "dozen" to numerical value
                num = 12
                if transDict["quantity"] in ["", "1"]:
                    transDict["quantity"]= 12
                elif numType(array[idx])!= "False":
                    convert = numType(array[idx])
                    num *= convert(array[idx])
                    transDict["quantity"] = num
                elif len(array[idx].split('/'))==2:
                    num = 1 * num
                else:
                    transReview.append("Error: QUANTITY is not a numnerical value.")
            
            if len(array)>idx+1 and fuzz.QRatio(array[idx+1],"weighing")>88:  # Checks if next word is "weighing"
                idx+1
                if fuzz.QRatio(array[idx],"gross")>88:
                    idx+=1 
                if len(array)>idx+1 and array[idx].isdigit()==True and process.extractBests(array[idx+1],qualifierList,scorer=fuzz.QRatio,score_cutoff=88):
                    if transDict["quantity"] != "":
                        transDict["variants"].extend([transDict["quantity"], process.extractBests(array[idx-1], qualifierList, scorer=fuzz.QRatio, score_cutoff=88)[0][0]])
                        transDict["quantity"] = array[idx]
                        transDict["qualifier"] = process.extractBests(array[idx+1],qualifierList,scorer=fuzz.QRatio,score_cutoff=88)[0][0]
                        idx+=2                
                        if len(array)>idx and process.extractBests("pounds",array[idx:],scorer=fuzz.QRatio,score_cutoff=88):
                            idx = array[idx:].index(process.extractBests("pounds",array[idx:],scorer=fuzz.QRatio,score_cutoff=88)[0][0])
                            if array[idx-1][0].isdigit():
                                transDict["quantity"] = array[idx-1]
                                idx+=1
            elif len(array)>idx+1 and array[idx] not in ignore and array[idx+1] not in ignore and fuzz.QRatio(array[idx],"percent")>88 and process.extractBests(array[idx+1],itemList,scorer=fuzz.QRatio,score_cutoff=88)==[]:
                if len(array)>idx+2 and fuzz.QRatio(array[idx+1],"advance")>90 and fuzz.QRatio(array[idx+2],"payable")>90:
                    transDict["item"] = "Advance Payable"
                    idx+=3
                    return idx
                elif fuzz.QRatio(array[idx+1],"advance")>90:
                    transDict["item"] = "Advance"
                    idx+=2
                    return idx
            else:
                transDict["qualifier"] = process.extractBests(lem.lemmatize(array[idx]),qualifierList,scorer=fuzz.QRatio,score_cutoff=88)[0][0] # Saves qualifier
                if fuzz.QRatio(lem.lemmatize(array[idx]),"pound")>88 and transDict["item"] == "": 
                    transDict["item"] = "Tobacco"
                    if len(array)==idx:
                        return idx

                idx+=1
                if array[idx] in conjunctions:
                    idx+=1
                    x = and_item(array,idx,transDict,transReview,peopleArray,placesArray,otherItems,"qualifier")
                    idx = compareIdx(idx,x)
                    x = getCost(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
                    idx = compareIdx(idx,x)
                    return idx
            
            # Checks for words after QUALIFIER found
            if array[idx-1] not in ignore and fuzz.QRatio(lem.lemmatize(array[idx-1]),"pound")>88 and transDict["item"] == "": 
                if len(array)==idx:
                    transDict["item"] = "Tobacco"
                    return idx
                else:
                    transDict["item"] = "Tobacco"
                    idx = afterItemCheck(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
                    idx = compareIdx(idx,x)
            elif len(array)>idx+1 and array[idx] == "of" and array[id+1] not in ignore and process.extractBests(lem.lemmatize(array[idx+1]), itemList, scorer=tsr, score_cutoff=95):
                idx+=1
                idx = getItem(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
                if transDict["quantity"] == "" and process.extractBests(transDict["item"],nonItems,scorer=tsr,score_cutoff=95)==[]:
                    transDict["quantity"] = "1"
                idx = afterItemCheck(array,idx,transDict,transReview,peopleArray,placesArray,otherItems,"item")
            elif array[idx] not in ignore and process.extractBests(lem.lemmatize(array[idx]),servicesList,scorer=fuzz.QRatio,score_cutoff=90):    # Checks for known services
                transDict["service"] = array[idx]
                idx+=1
                if len(array)>idx+1 and array[idx] == "of":  # Checks if "of" follows SERVICE
                    idx+=1
                    idx = serviceOf_Pattern(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
                else:
                    afterServiceCheck(array,idx,transDict,transReview,peopleArray,placesArray,otherItems,"service")
            elif len(array[idx])>3  and array[idx] not in ignore and (array[idx][-3:]== "ing" or array[idx][-4:]== "ings") and process.extractBests(array[idx],ingList,scorer=fuzz.QRatio,score_cutoff=90)==[]:
                transDict["service"] = array[idx]
                transReview.append("Review: Confirm SERVICE.")
                idx+=1
                if len(array)>idx+1 and array[idx] == "of":  # Checks if "of" follows SERVICE
                    idx+2
                    idx = serviceOf_Pattern(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
                else:
                    idx = afterServiceCheck(array,idx,transDict,transReview,peopleArray,placesArray,otherItems,"service")
            elif len(array)>idx+1 and  array[idx+1] == "of":  # Accounts for unknown services
                transDict["service"] = array[idx]
                transReview.append("Review: Confirm SERVICE.")
                idx+2
                idx = serviceOf_Pattern(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
            elif len(array)>idx+1 and array[idx] == "at":
                idx+=1
                if getPlace(array,idx,placesArray)!= 0:
                    temp = getPlace(array,idx,placesArray)
                    placesArray.append(temp[0])
                    idx = temp[1]
                elif len(array)>idx+1 and array[idx+1] not in ignore and process.extractBests(lem.lemmatize(array[idx+1]), servicesList, scorer=tsr, score_cutoff=95)==True:
                    transDict["service"] = array[idx+1]
                    placesArray.append(array[idx])
                    transReview.append("Review: Confirm PLACE.")
                    idx+=2
                    idx = serviceA_Pattern(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
            elif array[idx] not in ignore and process.extractBests(array[idx],itemList,scorer=tsr,score_cutoff=87):  # Checks for ITEM
                idx = getItem(array,idx,transDict,transReview,placesArray,peopleArray,otherItems)
                if transDict["quantity"] == "" and process.extractBests(transDict["item"],nonItems,scorer=tsr,score_cutoff=95)==[]:
                    transDict["quantity"] = "1"
                idx = afterItemCheck(array,idx,transDict,transReview,peopleArray,placesArray,otherItems,"item")
            else: 
                idx = findAdjectives(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
        elif array[idx] not in ignore and process.extractBests(array[idx], itemList, scorer=tsr, score_cutoff=87):  # Checks for ITEM if no QUALIFIER found
            idx = getItem(array,idx,transDict,transReview,placesArray,peopleArray,otherItems)
            if transDict["quantity"] == "" and process.extractBests(transDict["item"],nonItems,scorer=tsr,score_cutoff=95)==[]:
                transDict["quantity"] = "1"
            idx = afterItemCheck(array,idx,transDict,transReview,peopleArray,placesArray,otherItems,"item")
        else: 
            idx = findAdjectives(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
        break
    return idx

# Handles the "SERVICE-of" pattern
def serviceOf_Pattern(array,idx,transDict,transReview,peopleArray,placesArray,otherItems):# sourcery skip: low-code-quality    
    # Function for repeat code
    def rentFunction(array,idx,transDict,transReview,peopleArray,placesArray,otherItems):
        temp = getPlace(array,idx,placesArray)
        if temp != 0:
            placesArray.append(temp[0])
            idx = temp[1]
            x = getCost(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
            idx = compareIdx(idx,x)
        elif [x for x in array[idx:] if x[0].isdigit()]:  # Checks if digit follows near
            num = [x for x in array[idx:] if x[0].isdigit()]  
            if array.index(num[0])<4 :
                n = array.index(num[0])
                pName = ' '.join(array[idx:n+1])
                placesArray.append(pName)   # Assumes unknown PLACE
                transReview.append("Review: Confirm PLACE.")
                idx = n+1
                x = getCost(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
                idx = compareIdx(idx,x)
        else:
            transReview.append("Review: Possible nnknown PLACE mentioned.")
        return idx

    if idx>=2 and lem.lemmatize(array[idx-2]) == "rent" and array[idx] not in possessiveList: # Checks if service is "rent"
        idx = rentFunction(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
    elif len(array)>idx+1 and array[idx] in possessiveList:  # checks for relations
        idx+=1
        # Checks for relation
        if process.extractBests(lem.lemmatize(array[idx]), relationsList, scorer=tsr, score_cutoff=95):
            relation = lem.lemmatize(array[idx])
            idx+=1
            # Checks for names
            if len(array)>idx and findName(array,idx,peopleArray,transReview) !=0:  
                temp = findName(array,idx,peopleArray,transReview)
                peopleArray.append(temp[0])
                idx = temp[1]
            else:     # Saves unknown person if no name found
                person = peopleObject.copy()
                person["relations"] = relation
                person["firstName"] = "FNU"
                person["lastName"] = "LNU"
                peopleArray.append(person)

            if len(array)>idx and array[idx] in ["a","an"]:
                idx+=1

            # Checks for Item
            if len(array)>idx and array[idx] not in ignore and process.extractBests(lem.lemmatize(array[idx]), itemList, scorer=tsr, score_cutoff=95):
                idx = getItem(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
                if transDict["quantity"] == "" and process.extractBests(transDict["item"],nonItems,scorer=tsr,score_cutoff=95)==[]:
                    transDict["quantity"] = "1"
                idx = afterItemCheck(array,idx,transDict,transReview,peopleArray,placesArray,otherItems,"item")
            else: # Saves unknown ITEM
                transDict["item"] = array[idx]
                transReview.append("Review: Confirm ITEM.")
                idx+=1
                idx = afterItemCheck(array,idx,transDict,transReview,peopleArray,placesArray,otherItems,"item")
    elif len(array)>idx+1 and array[idx] in ["a", "an"]:  # Checks for SERVICE-of-a pattern
        idx+=1
        if lem.lemmatize(array[idx-3]) == "rent":  # Checks if service is "rent"
            idx = rentFunction(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
        if lem.lemmatize(array[idx-3]) == "hire":  # Checks if service is "hire"
            transDict["item"] = f"Hire of a {array[idx]}"
            transDict["service"] = ""
            transReview.append("Review: Confirm ITEM.")
            idx+=1
            if len(array)>idx:
                idx = afterItemCheck(array,idx,transDict,transReview,peopleArray,placesArray,otherItems,"item")
            
        elif array[idx][0].isdigit() == True: # Checks if next value is a digit
            idx = QQI_Pattern(array, idx, transDict, transReview,placesArray)  # Checks for QUANTITY-QUALIFIER-ITEM pattern
        elif array[idx] not in ignore and process.extractBests(array[idx], itemList, scorer=qRatio, score_cutoff=90) and process.extractBests(array[idx], nonItems, scorer=qRatio, score_cutoff=90)==[]:  # Checks if known ITEM
            transDict["quantity"] = "1"
            idx = getItem(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
            idx = afterItemCheck(array,idx,transDict,transReview,peopleArray,placesArray,otherItems,"item")
        else:
            transDict["item"] = array[idx] # Assumes word is unknown ITEM
            transReview.append("Review: Confirm ITEM.")
            idx+1
            idx = afterItemCheck(array,idx,transDict,transReview,peopleArray,placesArray,otherItems,"item")
    elif array[idx][0].isdigit() == True: # Checks if next value is a digit
        idx = QQI_Pattern(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)  # Checks for QUANTITY-QUALIFIER-ITEM pattern
    else:
        temp = findName(array,idx,peopleArray,transReview)
        if temp!=0:
            peopleArray.append(temp[0])
            idx = temp[1]
            idx = compareIdx(idx,x)
    return idx

# Handles the "SERVICE-{a, an}" pattern
def serviceA_Pattern(array,idx,transDict,transReview,peopleArray,placesArray,otherItems):    # sourcery skip: low-code-quality
    while len(array)>idx:# Prevents out of bounds index increments
        if array[idx] in ["a", "an"]:  # Checks is SERVICE is followed by "of" or "a"
            idx+=1
            if idx>1 and array[idx-2] == "rent":
                temp = getPlace(array,idx,placesArray)
                if temp != 0:
                    placesArray.append(temp[0])
                    idx = temp[1]
                    x = getCost(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
                    idx = compareIdx(idx,x)
                elif [x for x in array[idx:] if x[0].isdigit()]:  # Checks if digit follows near
                    num = [x for x in array[idx:] if x[0].isdigit()]  
                    if array.index(num[0])<4 :
                        n = array.index(num[0])
                        pName = ' '.join(array[idx:n+1])
                        placesArray.append(pName)   # Assumes unknown PLACE
                        transReview.append("Review: Confirm PLACE.")
                        idx = n+1
                        x = getCost(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
                        idx = compareIdx(idx,x)
                else:
                    transReview.append("Review: Possible unknown PLACE mentioned.")
            else:
                QQI_Pattern(array,idx-1,transDict,transReview,peopleArray,placesArray,otherItems)
        elif array[idx] == "of":
            idx+=1
            serviceOf_Pattern(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
        elif len(array)>idx+2 and array[idx] == "the" and array[idx+1] == "above":   # Acounts for "the above ___" pattern
            idx+2
            if len(array)>idx+1 and array[idx+1] == "a":   # Accounts for "the above a" pattern
                if array[idx] not in ignore and process.extractBests(array[idx],itemList, scorer=fuzz.QRatio, score_cutoff=86):
                    transDict["variants"].append(process.extractBests(array[idx],itemList, scorer=fuzz.QRatio, score_cutoff=86)[0][0])
                elif array[idx] not in ignore and process.extractBests(array[idx],variantList, scorer=fuzz.QRatio, score_cutoff=86):
                    transDict["variants"].append(process.extractBests(array[idx],variantList, scorer=fuzz.QRatio, score_cutoff=86)[0][0])
                else:
                    transDict["variants"].append(array[idx])
                idx+=1
            QQI_Pattern(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
        elif array[idx].isdigit() == True:
            idx = QQI_Pattern(array,idx,transDict,transReview,peopleArray,placesArray,otherItems) # Checks for QUANTITY-QUALIFIER-ITEM pattern
        elif findName(array,idx,peopleArray,transReview) != 0:
            temp = findName(array,idx,peopleArray,transReview)
            peopleArray.append(temp[0])
            idx = temp[1]
            if array[idx] not in ignore and process.extractBests(array[idx], itemList, scorer=tsr, score_cutoff=86):
                idx = getItem(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
                if transDict["quantity"] == "" and process.extractBests(transDict["item"],nonItems,scorer=tsr,score_cutoff=95)==[]:
                    transDict["quantity"] = "1"
                idx = afterItemCheck(array,idx,transDict,transReview,peopleArray,placesArray,otherItems,"item")
            elif len(array)>idx+2 and array[idx]=="to":
                idx+=1
                if getPlace(array,idx,placesArray) != 0:
                    temp = getPlace(array,idx,placesArray)
                    placesArray.append(temp[0])
                    idx = temp[1]
                    idx = compareIdx(idx,x)
        elif array[idx] in ["&","and","with",","]:
            idx = and_item(array,idx,transDict,transReview,peopleArray,placesArray,otherItems,"service")
        else:
            idx = QQI_Pattern(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
        break

# Handles the "PERSON-for" pattern
def peopleFor_Pattern(array,idx,transDict,transReview,peopleArray,placesArray,otherItems):
    if idx is None:
        idx = array.index('for')  # Gets the array index for "for"
    array[idx] = ""   # Removes "for"

    if idx+1 == len(array): # Checks if next element is end of array
        transDict["item"] = array[idx+1]  # Stores Item
        transReview.append("Review: Confirm Item.")
    elif len(array)>idx+2:
        idx+=1
        if array[idx] in ["a","the"]:  # Checks if following word is "a"/"the"
            idx+=1
            if array[idx] not in ignore and process.extractBests(lem.lemmatize(array[idx]),servicesList, scorer=fuzz.QRatio, score_cutoff=86):
                transDict["service"] = array[idx] 
                idx = idx+1 
                idx = serviceA_Pattern(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
            elif len(array[idx])>3 and array[idx] not in ignore and (array[idx][-3:]== "ing" or array[idx][-4:]== "ings") and process.extractBests(array[idx],ingList,scorer=fuzz.QRatio,score_cutoff=90)==[]:
                transDict["service"] = array[idx] 
                idx = idx+1 
                idx = serviceA_Pattern(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
            else:
                if array[idx-1] == "a":
                    idx = idx-1
                idx = QQI_Pattern(array,idx-1,transDict,transReview,peopleArray,placesArray,otherItems)
        elif array[idx] == "interest" and array[idx+1] == "on":  # checks if "interest" follows
            array[idx+1] == ""     # Removes "on"
            transDict["item"] = "interest"  # Stores ITEM
            idx+=2
        elif array[idx][0].isdigit() == True:
            idx = QQI_Pattern(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
    return idx

# =========================================== #
#             "Begins" Functions              #
# =========================================== #

# Handles transactions begining with "Charge"/"Charges"
def beginsCharge(array,idx,transDict,transReview,peopleArray,placesArray,otherItems): # sourcery skip: low-code-quality
    def servicesHelper(array,idx,transDict,transReview,peopleArray,placesArray,otherItems):
        if array[idx] in ["a", "an"]:
            idx+=1
            idx = serviceA_Pattern(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
        elif array[idx] == "of":
            idx+=1
            idx = serviceOf_Pattern(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
        elif array[idx] in conjunctions:
            idx = and_item(array,idx,transDict,transReview,peopleArray,placesArray,otherItems,"service")
        return idx
    
    if len(array)>idx+2 and array[idx+1] == "on" and fuzz.ratio(lem.lemmatize(array[idx+2]), "Merchandise") >= 87:
        idx+2
        if len(array) == idx+1:
            transDict["item"] = "Charges on Merchandise"
        elif len(array)>idx+1 and array[idx+1][0].isdigit():   # Checks if next value is a digit
            idx+=1
            temp = getCost(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)  # Checks for COST
            if temp == 0:
                idx = QQI_Pattern(array,idx,transDict,transReview,peopleArray,placesArray,otherItems) # checks for QUANTITY-QUALIFIER-ITEM pattern
            else:
                idx = temp
        elif len(array)>idx+2 and array[idx+1] == "for":  # checks if "for" is next word
            idx+=1
            idx = for_Keyword(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
    elif len(array)>idx+1 and array[idx+1][0].isdigit():   # Checks if next value is a digit
        idx+=1
        x = getCost(array,idx,transDict,transReview,peopleArray,placesArray,otherItems) # Checks for COST  
        idx = compareIdx(idx,x)    
    elif len(array)>idx+1 and findName(array,idx,peopleArray,transReview)!=0:
        temp = findName(array,idx,peopleArray,transReview)
        peopleArray.append(temp[0])
        idx = temp[1]
        # Checks for SERVICES following names
        if len(array)>idx and array[idx] not in ignore and process.extractBests(lem.lemmatize(array[idx]), servicesList, scorer=tsr, score_cutoff=95):
            transDict["service"] = process.extractBests(lem.lemmatize(array[idx]), servicesList, scorer=tsr, score_cutoff=95)[0][0]
            idx+1
            idx = servicesHelper(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
        elif len(array)>idx and len(array[idx])>3 and array[idx] not in ignore and (array[idx][-3:]== "ing" or array[idx][-4:]== "ings") and process.extractBests(array[idx],ingList,scorer=fuzz.QRatio,score_cutoff=90)==[]:
            transDict["service"] = lem.lemmatize(array[idx])
            idx+1
            idx = servicesHelper(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)

# Handles transactions begining with "allowance"
def beginsAllowance(array,idx,transDict,transReview,peopleArray,placesArray,otherItems):
    if array[idx] in ["an","a"]:
        idx+=1

    if len(array)>idx+1 and array[idx]=="on":
        idx+=1
        idx = QQI_Pattern(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
    else:
        transReview.append("Error: Unable to parse transaction format")
    return idx

# Handles transactions begining with "total"
def beginsTotal(array,transDict,transReview,peopleArray,placesArray,otherItems):
    # Skips transactions that consist of only "total"
    if len(array) == 1 and array[0] == "total":
        return
    # Skips transactions that consist of only "total tobacco"
    if len(array) == 2 and array[1] not in ignore and process.extractBests(array[1], "tobacco", scorer=tsr, score_cutoff=90):
        return
    # Skips transactions that start with "total carried"
    if len(array)>1 and array[1] == "carried":
        return

    if len(array) == 2 and array[1] in ["sterling", "currency"]:
        transDict["item"] = "Total"
    elif len(array)>2 and fuzz.QRatio(array[1], "tobacco")>90 and array[2][0].isdigit == True:
        transDict["quantity"] = array[2]
        idx = 3
        if len(array)>idx and array[idx] in ["at","is"]:
            transDict["qualifer"] = "pound"
            x = getCost(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
            idx = compareIdx(idx,x)
        elif len(array)>idx+1 and array[idx+1] in ["at","is"]:
            transDict["qualifer"] = array[idx]
            idx+=1
            x = getCost(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
            idx = compareIdx(idx,x)
    elif len(array) < 6 and ("account" in array or "accounts" in array):
        transDict["item"] = "Total"

# Handles transactions begining with "subtotal" or "sum"
def beginsSubtotalSum(array):
    # Skips transactions that begin with "sum being" or "sum from"
    if array[0] == "sum" and array[1] in ["being", "from"]:
        if array[1]=="from":
            array[1]==""
        return
    # Skips transactions that consist of only "subtotal"
    if len(array) == 1 and array[0] == "subtotal":
        return
    # Skips transactions that consist of only "subtotal tobacco"
    if len(array) == 2 and array[0] == "subtotal" and array[1] not in ignore and process.extractBests(array[1], "tobacco", scorer=tsr, score_cutoff=90):
        return

# Handles transactions begining with "ballance"/ "balance"
def beginsBalance(array,transDict,transReview):
    array[0] == ""   # Removes "balance", as it is a keyword
    if array[1] == "carried":    
        transDict["item"] = "Total"
    elif array[1] == "due":   # Handles transactions begining with "balance due"
        if array[3] == "contra" or array[2] == "contra":
            transDict["item"] = "Contra Balance"
        elif array[2] == "carried":
            transDict["item"] = "Total"
        else:
            transDict["item"] = "Balance Due"
    elif array[1] == "from" and array[2] == "liber" :   # Handles transactions begining with "balance from liber"
        array[1] == ""  # Removes "from", as it is a keyword
        transDict["item"] = "Balance from"
    elif array[1] == "to" and array[2] == "liber" :   # Handles transactions begining with "balance to liber"
        array[1] == ""  # Removes "to"
        transDict["item"] = "Balance to"
    elif "contra" in array:
        idx = array.index("contra")
        if len(array)>idx-1 and array[idx-1] in ["per","charged"]:
            transDict["item"] = "Contra Balance"
    else:
        transReview.append("Error: Unable to parse transaction format")

# Handles transactions begining with "expense"/"expence"
def beginsExpense(array,idx,transDict,transReview,peopleArray,placesArray,otherItems):
   # Helper function to excute repeated code
    def helper(idx):
        if len(array)>idx and array[idx] == "for" :
            idx = for_Keyword(array, idx, transDict, transReview, peopleArray,placesArray)
        elif len(array)>idx and array[idx][0].isdigit() == True:
            idx = QQI_Pattern(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
        elif len(array)>idx and array[idx] == "a":
            idx = QQI_Pattern(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
        elif len(array)>idx+1 and array[idx]=="its" and fuzz.token_set_ratio(array[idx+1],"value")>90:
            idx+=2
            x = getCost(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
            idx = compareIdx(idx,x)
        elif len(array)>idx+1 and array[idx] == "of":
            idx+=1
            if array[idx] not in ignore and process.extractBests(array[idx], itemList, scorer=tsr, score_cutoff=85):
                idx = getItem(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
                if transDict["quantity"] == "" and process.extractBests(transDict["item"],nonItems,scorer=tsr,score_cutoff=95)==[]:
                    transDict["quantity"] = "1"
                idx = afterItemCheck(array,idx,transDict,transReview,peopleArray,placesArray,otherItems,"item")
            elif array[idx] not in ignore and process.extractBests(lem.lemmatize(array[idx]), servicesList, scorer=tsr, score_cutoff=95)==True:
                idx+=1
                if len(array)>idx+1 and array[idx] in ["a","an","the"]:
                    transDict["service"] = f"{array[idx-1]} {array[idx]} {array[idx+1]}"
                    idx+=2
                    idx = afterServiceCheck(array,idx,transDict,transReview,peopleArray,placesArray,otherItems,"service")
                else:    
                    transDict["service"] = f"{array[idx-1]}"
                    idx = afterServiceCheck(array,idx,transDict,transReview,peopleArray,placesArray,otherItems,"service")
            elif len(array[idx])>3 and array[idx] not in ignore and (array[idx][-3:]== "ing" or array[idx][-4:]== "ings") and process.extractBests(array[idx],ingList,scorer=fuzz.QRatio,score_cutoff=90)==[]:
                idx+=1
                if len(array)>idx+1 and array[idx] in ["a","an","the"]:
                    transDict["service"] = f"{array[idx-1]} {array[idx]} {array[idx+1]}"
                    idx+=2
                    idx = afterServiceCheck(array,idx,transDict,transReview,peopleArray,placesArray,otherItems,"service")
                else:    
                    transDict["service"] = f"{array[idx-1]}"
                    idx = afterServiceCheck(array,idx,transDict,transReview,peopleArray,placesArray,otherItems,"service")
            elif array[idx] in ["a","an"]:
                idx = serviceA_Pattern(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
        return idx

    if len(array)>idx+1 and array[idx] == "for":
        array[idx] = ""  # Removes "for"
        idx+=1  
        temp = findName(array,idx,peopleArray,transReview)

        if temp != 0:
            peopleArray.append(temp[0])
            idx = temp[1]
            helper(idx)
        elif len(array)>idx+2 and array[idx+1] in ["for","its","a","of"] :
            accountName = peopleObject.copy()
            accountName["account"] = f"{array[idx]} Expenses"
            peopleArray.append(accountName)
            idx+=1
            helper(idx)
        elif len(array)>idx+3 and array[idx+2] ["for","its","a"] :
            accountName = peopleObject.copy()
            accountName["account"] = f"{array[idx]} {array[idx+1]} Expenses"
            peopleArray.append(accountName)
            idx+=2
            helper(idx)
    else:
        idx = helper(idx)
    return idx
    
# Handles transactions begining with "account"
def beginsAccount(array,idx,transDict,transReview,peopleArray,placesArray,otherItems):
    array[0] == ""  # Removes "account", as it'a also a keyword
    idx = 1
   # Helper function to excute repeated code
    def helper(idx):
        if len(array)>idx and array[idx] == "for" :
            array[idx] = ""  # Removes "for"
            for_Keyword(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
        elif array[idx][0].isdigit() == True:
            temp = getCost(array,idx,transDict)
            if temp == 0:
                idx = QQI_Pattern(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
            else:
                idx = temp
        

    if len(array)>idx+1 and array[idx] == "of":
        idx+=1  
        temp = findName(array,idx,peopleArray,transReview)
        if len(array)>idx+2 and temp == 0 and array[idx+1] == "for" :
            accountName = peopleObject.copy()
            accountName["account"] = f"{array[2]} Expenses"
            peopleArray.append(accountName)
            array[idx+1] = ""  # Removes "for"
            idx+=1
            helper(idx)
        elif len(array)>idx+1 and fuzz.ratio(lem.lemmatize(array[idx+1]), "furniture") >= 90:
            transDict["item"] = f"{array[idx]} Furniture"
            idx+=2
            helper(idx)
        elif len(array)>idx+3 and temp == 0 and array[idx+2] == "for" :
            accountName = peopleObject.copy()
            accountName["account"] = f"{array[2]} {array[3]}"
            peopleArray.append(accountName)
            idx+=2
            helper(idx)
        elif temp != 0:
            peopleArray.append(temp[0])
            idx = temp[1]
            helper(idx)

# Handles transactions begining with a "sterling"/"currency"
def beginsSterlingCurrency(array,idx,transDict,transReview,peopleArray,placesArray,otherItems):
    # Saves the initial currency
    if fuzz.ratio(array[idx], "currency") > 88:
        initial = "currency"
    elif fuzz.ratio(array[idx], "sterling") > 88:
        initial = "sterling"

    def checkForAt():   # Helper function for repeated code
        if len(array)>idx+1 and array[idx] == "at" and array[idx+1][0].isdigit==True:
            array[idx] == ""   # Removes "at"
            transDict["quantity"] = array[idx+1]
            idx+=2
            if len(array)>idx and array[idx]=="percent":
                transDict["qualifier"] = "Percent"
            else:
                transDict["qualifier"] = "Percent"
                transReview.append("Review: Confirm QUANTITY and QUALIFIER.")
        
 
    idx=1# Sets index
    if len(array)> idx and array[idx] == "for":   # Checks for "for"
        idx+=1

    while idx < len(array):
        if array[idx] == "the":  # Acounts for if "the" follows
            idx+=1
            if len(array)>idx+1 and array[idx] == "contra":  # Acounts for if "the contra" follows
                idx+=1
        
        if array[idx][0].isdigit()==True:
            num = array[idx]
            idx+=1
            # Checks is "tobacco" is in the array
            if process.extractBests("tobacco", array[idx:], scorer=fuzz.token_set_ratio, score_cutoff=85):
                QQI_Pattern(array,idx-1,transDict,transReview,peopleArray,placesArray,otherItems)
            elif len(array)>idx+2 and array[idx+1]=="per" and array[idx+2]=="contra":
                array[idx+1]= ""  # Removes "per"
                transDict["item"] = f"{array[idx]} to {initial} per Contra"
                transDict["totalCost"] = num
                idx+=3
                checkForAt()
            elif fuzz.ratio(array[idx], "currency") > 88  or fuzz.ratio(array[idx], "sterling") > 88:
                transDict["item"] = f"{array[idx]} to {initial}"
                transDict["totalCost"] = num
                idx+=1
                checkForAt()
        break
    return
# Handles transactions begining with a "Contra Balance"
def beginsContraBalance(array,transDict,transReview, peopleArray, placesArray):
    transDict["item"] = "Contra Balance"
    #Checks if "to" is in the transaction
    if "to" in array[2:]:
        idx = array.index("to")
        array[idx] = ""   # removes "to"
        idx+=1
        if len(array)>idx:
            if getPlace(array,idx,placesArray) != 0:
                temp = getPlace(array,idx,placesArray)
                placesArray.append(temp[0])
                idx = temp[1]
            elif findName(array,idx,peopleArray,transReview)!=0:
                temp =findName(array,idx,peopleArray,transReview)
                peopleArray.append(temp[0])
                idx = temp[1]
            else:
                unknown = peopleObject.copy()
                unknown["firstName"] = "FNU"
                unknown["lastName"] = "LNU"
                peopleArray.append(unknown)
                transReview.append("Review: Unknown person found.")
                
# Handles transactions begining with a PLACE
def beginsPlace(array,idx,transDict,transReview,peopleArray,placesArray,otherItems): # sourcery skip: low-code-quality

    if len(array)>idx+2 and array[idx] == "cash" and array[idx+1] == "paid":
        idx+=2
        if len(array)>idx+1 and array[idx] == "by":
            array[idx] = ""   # Removes "by"
            idx+=1
        temp = findName(array,idx,peopleArray,transReview)
        if temp!=0:
            peopleArray.append(temp[0])
            idx = temp[1]
    
    if len(array)<idx:
        return
        
    elif array[idx] not in ignore and process.extractBests(lem.lemmatize(array[idx]), servicesList, scorer=tsr, score_cutoff=88)==True:
        idx+=1
        idx = serviceA_Pattern(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
    elif len(array[idx])>3  and (array[idx][-3:]== "ing" or array[idx][-4:]== "ings") and process.extractBests(array[idx],ingList,scorer=fuzz.QRatio,score_cutoff=90)==[]:
        idx+=1
        idx = serviceA_Pattern(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
    elif len(array)>idx+3 and array[idx] in possessiveList and array[idx+2] == "of":
        idx+=3
        if array[idx] not in ignore and process.extractBests(lem.lemmatize(array[idx]), servicesList, scorer=tsr, score_cutoff=88)==True:
            transDict["service"] = array[idx]  # Stores service
            idx+=1
            serviceA_Pattern(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
        elif len(array[idx])>3  and array[idx] not in ignore and (array[idx][-3:]== "ing" or array[idx][-4:]== "ings") and process.extractBests(array[idx],ingList,scorer=fuzz.QRatio,score_cutoff=90)==[]:
            transDict["service"] = array[idx]  # Stores service
            idx+=1
            idx = serviceA_Pattern(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
        elif len(array)>idx+1 and array[idx] in ["a","an"]:
            idx = serviceA_Pattern(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
        elif len(array)>idx+2 and array[idx+1] in ["of","a","an"]:
            transDict["service"] = array[idx]  # Stores unknown service
            transReview.append("Review: Confirm SERVICE.")
            idx+=1
            idx = serviceA_Pattern(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
    elif len(array)>idx+3 and array[idx] == "for":
        if array[idx] not in ignore and process.extractBests(lem.lemmatize(array[idx]), servicesList, scorer=tsr, score_cutoff=88)==True:
            idx+=1
            idx = serviceA_Pattern(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
        elif len(array[idx])>3 and array[idx] not in ignore and (array[idx][-3:]== "ing" or array[idx][-4:]== "ings") and process.extractBests(array[idx],ingList,scorer=fuzz.QRatio,score_cutoff=90)==[]:
            idx+=1
            idx = serviceA_Pattern(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
    elif array[idx][0].isdigit() == True:
        temp = getCost(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
        if temp == 0:
            idx = QQI_Pattern(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
        else:
            idx = temp

# Handles transactions begining with a "cash"
def beginsCash(array,idx,transDict,transReview,peopleArray,placesArray,otherItems): 
    # Sets transaction ITEM to "cash" and creates 2nd transaction object
    transDict["item"] = "Cash"
    item2 = transactionObject.copy()

    if len(array) == 1:
        transDict["item"] = "Cash"
        return
    else:
        idx = 1

    # Adds "Cash Account" to accounts
    newAccount = peopleObject.copy()
    newAccount["account"] = "Cash Account"
    peopleArray.append(newAccount)

    # Helper function for repeat code (checks if item 2 found)
    def checkItem2():
        if item2 != transactionObject:
            otherItems.append(item2)
            transDict["includedItems"] = otherItems

    # Helper function for repeat cost code 
    def checkIfDigit():
        if array[idx][0].isdigit()==True:
            x = getCost(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
            idx = compareIdx(idx,x)

    # Helper function for repeat code (checks for unknown/new place name)
    def placeName():
        n = array.index(temp[0])
        pName = ' '.join(array[idx:n+1])
        placesArray.append(pName)   # Assumes unknown PLACE
        transReview.append("Review: Confirm PLACE.")
        idx = array.index(temp[0])+1
    
    while len(array) > idx:
        temp = findName(array,idx,peopleArray,transReview)
        if temp != 0:  # Checks for PERSON
            peopleArray.append(temp[0])
            idx = temp[1]
            if fuzz.ratio(lem.lemmatize(array[idx]), "expense") >= 90 or fuzz.ratio(array[idx], "expence") >= 90:
                array[idx] = ""
                idx+=1
                if array[idx][0].isdigit()==True:
                    idx = QQI_Pattern(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
        elif array[idx] not in ignore and process.extractBests(lem.lemmatize(array[idx]), servicesList, scorer=tsr, score_cutoff=90):
            item2["service"] = array[idx]
            idx+=1
            serviceA_Pattern(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
            checkItem2()
        elif fuzz.QRatio(array[idx],"paid")>90: # Checks for "paid"
            array[idx] = ""  # Removes "paid"
            idx+=1
            beginsCashPaid(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
        elif fuzz.QRatio(array[idx],"sales")>88: # Checks for "sales"
            idx+=1
            beginsCashPaid(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
        elif fuzz.ratio(lem.lemmatize(array[idx]), "expense") >= 90 or fuzz.ratio(array[idx], "expence") >= 90:  # Checks for "expense"/"expence"
            array[idx] = ""    # Removes "expense"/"expence"
            idx+=1
            if array[idx] in ["to","at"]:  # Checks for "to"/"at"
                array[idx] = ""    # Removes "to"/"at"
                idx+=1
                temp =  getPlace(array,idx,placesArray)  # Checks for PLACE
                if temp !=0:
                    placesArray.append(temp[0])
                    idx = temp[1]
                    if array[idx]=="to":
                        idx+=1
                        # Checks for SERVICE
                        if array[idx] not in ignore and process.extractBests(lem.lemmatize(array[idx]), servicesList, scorer=tsr, score_cutoff=90)==True:
                            item2["service"] = array[idx]
                            idx+=1
                            serviceA_Pattern(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
                            checkItem2()
                        elif len(array[idx])>3 and array[idx] not in ignore and (array[idx][-3:]== "ing" or array[idx][-4:]== "ings") and process.extractBests(array[idx],ingList,scorer=fuzz.QRatio,score_cutoff=90)==[]:
                            item2["service"] = array[idx]
                            idx+=1
                            serviceA_Pattern(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
                            checkItem2()
                    else:
                        x = getCost(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
                        idx = compareIdx(idx,x)
                elif [x for x in array[idx:] if x[0].isdigit()] != []:  # If PLACE not found checks if digit follows near
                    temp = [x for x in array[idx:] if x[0].isdigit()]  
                    if array.index(temp[0])<4 :
                        idx = placeName(array,idx,placesArray)
                        idx = compareIdx(idx,x)
                        x = getCost(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
                        idx = compareIdx(idx,x)
                elif [x for x in array[idx:] if x=="to"] != []:   # If PLACE not found checks if "to" follows near
                    temp = [x for x in array[idx:] if x=="to"]  
                    if array.index(temp[0])<4 :
                        placeName()
                        if array[idx] not in ignore and process.extractBests(lem.lemmatize(array[idx]),servicesList,scorer=tsr,score_cutoff=90)==True:
                            item2["service"] = process.extractBests(lem.lemmatize(array[idx]),servicesList,scorer=tsr,score_cutoff=90)[0][0]
                            idx+=1
                            serviceA_Pattern(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
                            checkItem2()
                        elif len(array[idx])>3  and (array[idx][-3:]== "ing" or array[idx][-4:]== "ings") and process.extractBests(array[idx],ingList,scorer=fuzz.QRatio,score_cutoff=90)==[]:
                            item2["service"] = array[idx]
                            idx+=1
                            serviceA_Pattern(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
                            checkItem2()
                else:
                    pName = ' '.join(array[idx:])  # Assumes unknown PLACE
                    placesArray.append(pName)
                    transReview.append("Review: Confirm PLACE.")
            elif array[idx] == "of":  # Checks for "of"
                array[idx] = ""    # Removes "of"
                idx+=1
                if array[idx] not in ignore and process.extractBests(lem.lemmatize(array[idx]), servicesList, scorer=tsr, score_cutoff=90)==True:
                    item2["service"] = process.extractBests(lem.lemmatize(array[idx]),servicesList,scorer=tsr,score_cutoff=90)[0][0]
                    idx+=1
                    serviceA_Pattern(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
                    checkItem2()
                elif len(array[idx])>3  and (array[idx][-3:]== "ing" or array[idx][-4:]== "ings") and process.extractBests(array[idx],ingList,scorer=fuzz.QRatio,score_cutoff=90)==[]:
                    item2["service"] = array[idx]
                    idx+=1
                    serviceA_Pattern(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
                    checkItem2()
            elif array[idx] == "by":   # Checks for "by"
                array[idx] = ""    # Removes "by"
                idx+=1
                temp = findName(array,idx,peopleArray,transReview)
                if temp !=0:
                    peopleArray.append(temp[0])
                    idx = temp[1]
                    if array[idx] == "at":
                        idx+=1
                        if getPlace(array,idx,placesArray) !=0:
                            temp = getPlace(array,idx,placesArray)
                            placesArray.append(temp[0])
                            idx = temp[1]
                            if array [idx][0].isdigit()==True:
                                x = getCost(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
                                idx = compareIdx(idx,x)
                            elif array [-1][0].isdigit()==True:
                                x = getCost(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
                                idx = compareIdx(idx,x)
                                transReview.appaend("Review: Confirm COST. ")
        elif len(array)>idx+1 and fuzz.ratio(lem.lemmatize(array[idx+1]), "expense") >= 90 or fuzz.ratio(array[idx+1], "expence") >= 90:  # Checks for "expense"/"expence":
            array[idx+1] = ""
            new = peopleObject.copy()
            new["account"] = f"{array[idx]} Expenses"
            peopleArray.append(new)
            idx+=2
            beginsPerson(array,idx,transDict,transReview, peopleArray, placesArray, otherItems)
        elif array[idx] == "per":   # Checks for "per"
            array[idx] = ""  # Removes "per"
            idx+=1
            temp = per_Keyword(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
            if temp !=0:
                idx = idx+temp
                x = getCost(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
                idx = compareIdx(idx,x)
        elif array[idx] == "for":   # Checks for "for"
            array[idx] = ""  # Removes "for"
            idx+=1
            if array[idx][0].isdigit()==True:
                idx = QQI_Pattern(array,idx,item2,transReview,peopleArray,placesArray,otherItems)
                checkItem2()
            elif array[idx] in ["a","an"]:
                idx = serviceA_Pattern(array,idx,item2,transReview,peopleArray,placesArray,otherItems)
                checkItem2()
            elif array[idx] not in ignore and process.extractBests(lem.lemmatize(array[idx]), servicesList, scorer=tsr, score_cutoff=90):
                item2["service"] = array[idx]
                idx+=1
                idx = serviceA_Pattern(array,idx,item2,transReview,peopleArray,placesArray,otherItems)
                checkItem2()
            elif len(array)>idx+1 and fuzz.ratio(lem.lemmatize(array[idx+1]), "expense") >= 90 or fuzz.ratio(array[idx+1], "expence") >= 90:  # Checks for "expense"/"expence":
                array[idx+1] = ""
                new = peopleObject.copy()
                new["account"] = f"{array[idx]} Expenses"
                peopleArray.append(new)
                idx+=2
                beginsPerson(array,idx,transDict,transReview, peopleArray, placesArray, otherItems)
            else:
                for_Keyword(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
        elif fuzz.ratio(array[idx], "advanced") > 86:   # Checks for "advanced"
            idx+=1
            temp = findName(array,idx,peopleArray,transReview)  # Checks for name
            if temp !=0:   
                peopleArray.append(temp[0])
                idx = temp[1]
                if lem.lemmatize(array[idx]) in relationsList:  # Checks for relation (girl/boy/son/mother/father/etc) if name found
                    idx+=1
                    name = findName(array,idx,peopleArray,transReview)   # Checks of name of relation
                    if name !=0:
                        peopleArray.append(name[0])
                        idx = name[1]
                        checkIfDigit()   # Checks for COST if name of relation IS found
                    else:
                        checkIfDigit() # Checks for COST if name of relation NOT found
                else:
                    checkIfDigit()   # Checks for COST if NO relation is found
            # checks for "your/his/her-RELATION" pattern         
            elif len(array)>idx+1 and array[idx] in possessiveList and lem.lemmatize(array[idx+1]) in relationsList:
                idx+=2
                name = findName(array,idx,peopleArray,transReview)   # Checks of name of relation
                if name !=0:
                    peopleArray.append(name[0])
                    idx = name[1]
                    checkIfDigit()  # Checks for COST if name of relation IS found
                else:
                    checkIfDigit()   # Checks for COST if name of relation NOT found
        elif len(array)>idx+1 and array[idx] not in ignore and fuzz.QRatio(array[idx],"received")>88 and array[idx+1] == "from":   # Checks for "received from "
            array[idx+1] = ""    # Removes "from"
            idx+=2
            temp = findName(array,idx,peopleArray,transReview)
            if temp != 0:
                peopleArray.append(temp[0])
                idx = temp[1]
                if array[idx] == "for":
                    idx+=1
                    serviceA_Pattern(array,idx,item2,transReview,peopleArray,placesArray,otherItems)
            else:
                temp = getPlace(array,idx,placesArray)
                if temp != 0:
                    placesArray.append(temp[0])
                    idx = temp[1]
                    if array[idx] == "for":
                        idx+=1
                        serviceA_Pattern(array,idx,item2,transReview,peopleArray,placesArray,otherItems)
        elif fuzz.ratio(array[idx], "lacking") > 90:   # Checks for "lacking"
            return
        elif fuzz.ratio(array[idx], "account") > 90:   # Checks for "account"
            array[idx] = ""  # Removes "account"
            idx+=1
            x = getCost(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
            idx = compareIdx(idx,x)
        elif array[idx] == "of":    # Checks for "of"
            idx+=1
            temp = findName(array,idx,peopleArray,transReview)
            if temp != 0:
                peopleArray.append(temp[0])
                idx = temp[1]
                x = getCost(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
                idx = compareIdx(idx,x)
        elif process.extractBests("club", array, scorer=tsr, score_cutoff=88):   # Checks if "club" is in the array
            item2["item"] = "Club Costs"
            idx = array.index(process.extractBests("club", array, scorer=tsr, score_cutoff=88)[0][0])+1
            if array[idx] == "at":  # Checks for "at"
                array[idx] = ""   # Removes "at"
                idx+=1
                temp = getPlace(array,idx,placesArray)
                if temp!=0:
                    placesArray.append(temp[0])
                    idx = temp[1]
                    x = getCost(array,idx,item2,transReview,peopleArray,placesArray,otherItems)
                    idx = compareIdx(idx,x)
        break

    # Moves TOTAL_COST from Item2 to "Cash" item
    if transDict["totalCost"] == "" and item2["totalCost"] != "":
        transDict["totalCost"] = item2["totalCost"]
        item2["totalCost"] = ""

# Handles transactions begining with a "cash paid"
def beginsCashPaid(array,idx,transDict,transReview, peopleArray, placesArray, otherItems):
    # Sets transaction ITEM to "cash" and creates 2nd transaction object
    transDict["item"] = "Cash"
    item2 = transactionObject.copy()

    # Helper function for repeat code (checks if item 2 found)
    def checkItem2(item2):
        if item2 != transactionObject:
            otherItems.append(item2)
            transDict["includedItems"] = otherItems
    while len(array) > idx:
        if idx>0 and fuzz.QRatio(array[idx-1], "sales")>88:
            if array[idx] == "for":
                idx+=1
            QQI_Pattern(array,idx,item2,transReview,peopleArray,placesArray,otherItems)
            checkItem2(item2)
        elif findName(array,idx,peopleArray,transReview) != 0:  # Checks for PERSON
            temp = findName(array,idx,peopleArray,transReview)
            peopleArray.append(temp[0])
            idx = temp[1]
            if fuzz.QRatio(lem.lemmatize(array[idx]), "expense") >= 90 or fuzz.ratio(array[idx], "expence") >= 90:  # Checks if "expense"/"expence" follows
                idx+=1
                if array[idx][0].isdigit()==True:  # Checks for QUALITY-QUANTITY-ITEM pattern
                    QQI_Pattern(array, idx, item2, transReview, placesArray, peopleArray)
                    checkItem2()
                elif array[idx] == "of":
                    idx+=1
                    if array[idx] not in ignore and process.extractBests(lem.lemmatize(array[idx]),servicesList,scorer=tsr,score_cutoff=90)==True:
                        item2["service"] = process.extractBests(lem.lemmatize(array[idx]),servicesList,scorer=tsr,score_cutoff=90)[0][0]
                        idx+=1
                        serviceA_Pattern(array, idx, item2, peopleArray, transReview, placesArray)
                        checkItem2()
                    elif len(array[idx])>3 and array[idx] not in ignore and (array[idx][-3:]== "ing" or array[idx][-4:]== "ings") and process.extractBests(array[idx],ingList,scorer=fuzz.QRatio,score_cutoff=90)==[]:
                        item2["service"] = array[idx]
                        idx+=1
                        serviceA_Pattern(array, idx, item2, peopleArray, transReview, placesArray)
                        checkItem2()
            elif array[idx] == "for":
                for_Keyword(array, idx, item2, transReview, peopleArray,placesArray)
                checkItem2()
            elif array[idx] == "at":
                array[idx] = ""    # Removes "at"
                idx+=1
                if getPlace(array,idx,placesArray) != 0:
                    temp = getPlace(array,idx,placesArray)
                    placesArray.append(temp[0])
                    idx = temp[1]
                else:
                    placesArray.append(array[idx])
                    transReview.append("Review: Confirm PLACE.")
        elif array[idx][0].isdigit() == True:
            QQI_Pattern(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
            checkItem2(item2)
        # Checks for SERVICE
        elif array[idx] not in ignore and process.extractBests(lem.lemmatize(array[idx]), servicesList, scorer=tsr, score_cutoff=90)==True or lem.lemmatize(array[idx])[-3:]== "ing":
            item2["service"] = array[idx]
            idx+=1
            serviceA_Pattern(array,idx,item2,transReview,peopleArray,placesArray,otherItems)
            checkItem2(item2)
        elif len(array)>idx+1 and array[idx+1] in ["of","a","an"]:  # Checks for unknown SERVICE
            item2["service"] = array[idx]
            transReview.append("Review: Confirm SERVICE.")
            idx+=1
            serviceA_Pattern(array,idx,item2,transReview,peopleArray,placesArray,otherItems)
            checkItem2(item2)
        elif array[idx] in possessiveList:  # Checks for "your"/"his"/"her"/etc
            idx+=2
            if array[idx][0].isdigit() == True:  # Checks for COST
                x = getCost(array,idx,item2,transReview,peopleArray,placesArray,otherItems)
                idx = compareIdx(idx,x)
            elif findName(array,idx,peopleArray,transReview) != 0:   # Checks for PERSOM
                temp = findName(array,idx,peopleArray,transReview)
                peopleArray.append(temp[0])
                idx = temp[1]
                x = getCost(array,idx,item2,transReview,peopleArray,placesArray,otherItems)
                idx = compareIdx(idx,x)
        elif len(array)>idx and array[idx] == "for":
            for_Keyword(array,idx,item2,transReview,peopleArray,placesArray,otherItems)
            checkItem2(item2)
        break

    # Moves TOTAL_COST from Item2 to "Cash" item
    if transDict["totalCost"] == "" and item2["totalCost"] != "":
        transDict["totalCost"] = item2["totalCost"]
        item2["totalCost"] = ""

# Handles transactions begining with an ITEM
def beginsItem(array,idx,transDict,transReview,peopleArray,placesArray,otherItems,item):

    #Checks if ITEM only and adds quantity as "1"
    if len(array) == idx:
        transDict["quantity"] = "1"
        return
    
    # Removes "on hand" phrase
    if "hand" in array:
        index = array.index("hand")
        if index>0 and array(index-1)=="on":
            array.pop(index)
            array.pop(index-1)

    while len(array) > idx:
        # Checks if next word is "at"
        if array[idx] == "at":
            x = getCost(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
            idx = compareIdx(idx,x)
            if x == 0 or (transDict["unitCost"] == "" and transDict["totalCost"] == ""):
                temp = getPlace(array,idx,placesArray)
                if temp != 0:
                    placesArray.append(temp[0])
                    idx = temp[1]
            array[idx] = ""  # Removes "at"
        elif len(array)>idx+1 and array[idx][0].isdigit() and lem.lemmatize(array[idx+1]) in qualifierList:
            transDict["quantity"] = array[idx]
            transDict["qualifier"] = lem.lemmatize(array[idx+1])
            idx+=2
            x = getCost(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
            idx = compareIdx(idx,x)
        elif array[idx] == "on":
            array[idx] = ""   # Removes "on"
            idx+=1
            if len(array)>idx+3 and array[idx+1]=="at" and array[idx+2].isdigit():
                transDict["item"] = f"{item} on {array[idx]}"
                uc = 100 + int(array[idx+2])
                transDict["unitCost"] = f"{uc}"
                idx+=3
                if len(array)>idx+1 and array[idx].isalpha():
                    idx+=1
                x = getCost(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
                idx = compareIdx(idx,x)
            elif getPlace(array,idx,placesArray) != 0:
                temp = getPlace(array,idx,placesArray)
                placesArray.append(temp[0])
                idx = temp[1]
        elif array[idx][0].isdigit():
            x = getCost(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
            idx = compareIdx(idx,x)
        elif array[idx] == "from":
            if getPlace(array,idx,placesArray) != 0:
                temp = getPlace(array,idx,placesArray)
                placesArray.append(temp[0])
                idx = temp[1]
                if len(array)> idx+1 and array[idx] == "to" and getPlace(array,idx+1,placesArray)!=0:
                    idx+=1
                    temp = getPlace(array,idx,placesArray)
                    placesArray.append(temp[0])
                    idx = temp[1]
                elif len(array)> idx+1 and array[idx] == "to" and findName(array,idx+1,peopleArray,transReview) != 0:
                    idx+=1
                    temp = findName(array,idx,peopleArray,transReview)
                    peopleArray.append(temp[0])
                    idx = temp[1]
                x = getCost(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
                idx = compareIdx(idx,x)
            elif findName(array,idx+1,peopleArray,transReview) != 0:
                temp = findName(array,idx+1,peopleArray,transReview)
                peopleArray.append(temp[0])
                idx = temp[1]
                if len(array)> idx+1 and array[idx] == "to" and getPlace(array,idx+1,placesArray)!=0:
                    idx+=1
                    temp = getPlace(array,idx,placesArray)
                    placesArray.append(temp[0])
                    idx = temp[1]
                elif len(array)> idx+1 and array[idx] == "to" and findName(array,idx+1,peopleArray,transReview) != 0:
                    idx+=1
                    temp = findName(array,idx,peopleArray,transReview)
                    peopleArray.append(temp[0])
                    idx = temp[1]
                x = getCost(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
                idx = compareIdx(idx,x)
        elif len(array)>idx+1 and fuzz.QRatio(array[idx],"received ")>90 and array[idx+1] in ["per", "by"]:
            idx+=2
            if array[idx] == "the":
                idx+=1
            temp = getPlace(array,idx,placesArray)
            if temp !=0:
                if array[idx-1] == "the":
                    array[idx-2] = ""  # Removes "per"/"by"
                else:
                    array[idx-1] = ""  # Removes "per"/"by"
                placesArray.append(temp[0])
                idx = temp[1]
                if array[idx] == "from":
                    idx+=1
                    place = getPlace(array,idx,placesArray)
                    if place !=0:
                        array[idx-1] = ""  # Removes "from"
                        placesArray.append(place[0])
                        idx = place[1]
                        x = getCost(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
                        idx = compareIdx(idx,x)
                elif array[idx] == "per":
                    idx+=1
                    temp = findName(array,idx,peopleArray,transReview)
                    if temp !=0:
                        array[idx-1] = ""  # Removes "per"
                        peopleArray.append(temp[0])
                        idx = temp[1]
                        x = getCost(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
                        idx = compareIdx(idx,x)
                    elif array[idx] == "the":
                        idx+=1
                        place = getPlace(array,idx,placesArray)
                        if place !=0:
                            array[idx-2] = ""  # Removes "per"
                            placesArray.append(place[0])
                            idx = place[1]
                            x = getCost(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
                            idx = compareIdx(idx,x)
            else:
                temp = findName(array,idx,peopleArray,transReview)
                if temp !=0:
                    if array[idx-1] == "the":
                        array[idx-2] = ""  # Removes "per"/"by"
                    else:
                        array[idx-1] = ""  # Removes "per"/"by"
                    peopleArray.append(temp[0])
                    idx = temp[1]
                    x = getCost(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
                    idx = compareIdx(idx,x)
        break

# Handles transactions begining with a PERSON name or profession
def beginsPerson(array,idx,transDict,transReview,peopleArray,placesArray,otherItems):
    if len(array) == idx+1:
        transDict["item"] = "Cash"
        return

    # Removes the word "the" from the array
    array = list(filter(("the").__ne__, array))

    if len(array)>idx+3 and array[idx] == "on" and array[idx+1] == "account" and array[idx+2] == "of":
        idx+=3
        name = findName(array,idx,peopleArray,transReview)
        if name != 0:
            peopleArray.append(name[0])
            idx = name[1]

    while len(array) > idx:
        if array[idx].isdigit()==True:
            if len(array)>idx+1 and array[idx+1] not in ignore and process.extractBests(lem.lemmatize(array[idx+1]), qualifierList, scorer=qRatio, score_cutoff=90):
                idx = QQI_Pattern(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
            else:
                x = getCost(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
                idx = compareIdx(idx,x)
        if len(array)>idx and array[idx] in ["a","an"]:
            idx = QQI_Pattern(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
        elif len(array)>idx and array[idx] not in ignore and process.extractBests(lem.lemmatize(array[idx]), servicesList, scorer=qRatio, score_cutoff=90)==True:
            idx+=1
            idx = serviceA_Pattern(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
        elif len(array)>idx and len(array[idx])>3 and array[idx] not in ignore and (array[idx][-3:]== "ing" or array[idx][-4:]== "ings") and process.extractBests(array[idx],ingList,scorer=fuzz.QRatio,score_cutoff=90)==[]:
            idx+=1
            idx = serviceA_Pattern(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
        elif len(array)>idx and array[idx] == "for":
            idx = for_Keyword(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
        elif len(array)>idx and array[idx] == "off" and "nett" in array[idx:]:
            idx+=1
            if "nett" in array:
                nIndex = array.index("nett")+1
            if "net" in array:
                nIndex = array.index("net")+1
            
            if len(array)>nIndex and array[idx].isdigit()==True and array[nIndex].isdigit()==True:
                quantity = int(array[nIndex]) - int(array[idx])
                transDict["quantity"] = quantity
                idx+=1
                nIndex+=1
            
                if len(array)>nIndex and lem.lemmatize(array[nIndex]) in qualifierList:
                    transDict["qualifier"] = lem.lemmatize(array[nIndex])
                    nIndex+=1
                    if array[nIndex] not in ignore and process.extractBests(array[nIndex], itemList, scorer=tsr, score_cutoff=80):
                        item = process.extractBests(array[nIndex], itemList, scorer=tsr, score_cutoff=80)[0][0]
                        transDict["item"] = item
                        i = len(item.split())
                        nIndex = nIndex+i
                        x = getCost(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
                        idx = compareIdx(idx,x)
                elif lem.lemmatize(array[idx]) in qualifierList:
                    transDict["qualifier"] = lem.lemmatize(array[idx])
                    if array[nIndex] not in ignore and process.extractBests(array[nIndex], itemList, scorer=tsr, score_cutoff=80):
                        item = process.extractBests(array[nIndex], itemList, scorer=tsr, score_cutoff=80)[0][0]
                        transDict["item"] = item
                        i = len(item.split())
                        nIndex = nIndex+i
                        x = getCost(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
                        idx = compareIdx(idx,x)
                else:
                    transDict["item"] = "Tobacco"
                    transDict["qualifier"] = "Pound"
                    transReview.append("Review: Confirm ITEM and QUALIFIER.")
        elif len(array) > idx+1 and array[idx]=="the" and fuzz.token_set_ratio(array[idx+1], "ballance")>87:
            transDict["item"] = "Balance from"
            array[idx] = ""
            idx+=2
            if len(array) > idx and array[idx] == "of":
                idx+=1
                if array[idx] in possessiveList:
                    idx+=1
                if len(array) > idx+1 and array[idx+1]=="account":
                    array[idx+1] = "" # Removes "account"
                    newPplDict = peopleObject.copy()   # Initializes new people dictionary
                    newPplDict["account"] = f"{array[idx]} Account" # Saves Account Name
                    transReview.append("Review: ACCOUNT name.")
        break
    return idx

# Handles transactions begining with "Sundry"/"Sundries"
def beginsSundry(array,transDict,transReview,peopleArray,placesArray,otherItems):
    idx = 1

    while len(array)>idx:
        if fuzz.QRatio(array[idx], "goods")>90:
            transDict["item"] = "Sundry Goods"
            if array[-1][0].isdigit():
                idx = len(array)-1
                x = getCost(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
                idx = compareIdx(idx,x)
        elif array[idx] == "for" or fuzz.QRatio(array[idx], "debts") >90:
            idx+=1
            if len(array)>idx+1 and fuzz.QRatio(array[idx],"debts")>90 and fuzz.QRatio(array[idx+1],"due")>90: # Checks for "debts due"
                idx+=2

            if array[idx] == "in":  # Checks for "in" (Pattern: Sundrys for Debts due by them in )
                idx+=1
            elif len(array)>idx+3 and array[idx+3] == "in":  # Checks for "in" (Pattern: Sundrys for Debts due by them in )
                idx+=3
            
            if array[idx] not in ignore and process.extractBests(array[idx],["currency","sterling"], scorer=fuzz.QRatio, score_cutoff=86):  # Acounts for "in currency"/"in sterling" pattern.
                if len(array)>idx+2 and array[idx+1]in ["&","and"] and array[idx+2] not in ignore and process.extractBests(array[idx+2],itemList, scorer=fuzz.QRatio, score_cutoff=86):  # Checkd for "&"/"and"
                    transDict["item"] = "Sundry Debts"
                    new = transactionObject.copy()
                    new["item"] = array[idx]+" & "+process.extractBests(array[idx],itemList, scorer=fuzz.QRatio, score_cutoff=86)[0][0]
                    new["service"] = "Sundry Debts"
                    transDict["otherItems"].append(new)
                else:
                    transDict["item"] = "Sundry Debts"

                if  array[-3] == "at" and array[-2][0].isdigit():
                    idx = len(array)-4
                    x = getCost(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
                    idx = compareIdx(idx,x)
                elif array[-1][0].isdigit():
                    idx = len(array)-1
                    x = getCost(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
                    idx = compareIdx(idx,x)
            elif array[idx] not in ignore and process.extractBests(array[idx],itemList, scorer=fuzz.QRatio, score_cutoff=86):   # Checks for ITEM
                transDict["item"] = "Sundry Debts"
                new = transactionObject.copy()
                new["item"] = process.extractBests(array[idx],itemList, scorer=fuzz.QRatio, score_cutoff=86)[0][0]
                idx+=1
                if len(array)>idx+1 and array[idx].isdigit() and process.extractBests(array[idx+1],qualifierList, scorer=fuzz.QRatio, score_cutoff=86):
                    transDict["quantity"] = array[idx]
                    transDict["qualifier"] = process.extractBests(array[idx+1],qualifierList, scorer=fuzz.QRatio, score_cutoff=86)[0][0]
                    transDict["service"] = "Sundry Debts"
                    transDict["otherItems"].append(new)
                    idx+=2
                    if  array[-3] == "at" and array[-2][0].isdigit():
                        idx = len(array)-4
                        x = getCost(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
                        idx = compareIdx(idx,x)
                    elif array[-1][0].isdigit():
                        idx = len(array)-1
                        x = getCost(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
                        idx = compareIdx(idx,x)

# Handles transactions begining with all other words.
def beginsOther(array,transDict,transReview,peopleArray,placesArray,otherItems):
    idx = 0

    if array[idx] in possessiveList:
        idx+=1
        if len(array[idx])>idx+1 and fuzz.QRatio(array[idx], "note")>90  and fuzz.QRatio(array[idx+1], "payable")>90:
            transDict["item"] = "Note Payable"
        elif len(array[idx])>idx and array[idx] not in ignore and fuzz.QRatio(array[idx], "bond")>90 and process.extractBests("contra",array, scorer=fuzz.QRatio, score_cutoff=87):
            transDict["item"] = "Contra Balance"
        elif process.extractBests("bond",array, scorer=fuzz.QRatio, score_cutoff=87):
            transDict["item"]="Bond"
            temp = findName(array,idx,peopleArray,transReview)
            if temp!=0:
                peopleArray.append(temp[0])
                idx = temp[1]
    elif fuzz.QRatio(array[idx],"difference")>90 and fuzz.QRatio(array[idx+2],"exchange")>90:
        idx+=3
        if len(array[idx])>idx and array[idx]=="of":
            idx+=1
        transDict["item"] = "Difference in Exchange"
        new = transactionObject.copy()
        new["service"] = "Sundry Debts"
        idx = QQI_Pattern(array,new,transDict,transReview,peopleArray,placesArray,otherItems)
        new["service"] = "Difference in Exchange"
        transDict["otherItems"].append(new)
  
# =========================================== #
#             Keyword Functions               #
# =========================================== #

# Handles the "per" keyword
def per_Keyword(array,idx,transDict,transReview,peopleArray,placesArray,otherItems):  
    if idx == None:
        indices = [i for i, x in enumerate(array) if x == "per"]
    else:
        indices = [idx]

    for idx in indices:
        if len(array)>idx+1:    # Prevents out of bounds index increments
            idx+=1
        else:
            return

        if len(array)>idx+1 and array[idx] == "the":   # Checks if next word is "the"
            idx+1
            name = findName(array,idx,peopleArray,transReview)  # Looks for profession/person
            if name == 0:      # If no person/profession found, saves next word as a place
                temp = getPlace(array,idx,placesArray)
                if temp != 0:
                    placesArray.append(temp[0])
                    idx = temp[1]
                else:
                    transReview.append("Review: New PERSON or PLACE possibly mentioned.")
            else:
                peopleArray.append(name[0])
                idx = name[1]
        elif len(array)>idx+1 and lem.lemmatize(array[idx]) in relationsList and findName(array,idx+1,peopleArray,transReview)!=0:	 # Checks if word is in relationList
            temp = findName(array,idx,peopleArray,transReview)
            peopleArray.append(temp[0])
            idx = temp[1]
            return idx
        elif len(array)>idx+2 and lem.lemmatize(array[idx+1]) in relationsList and findName(array,idx+2,peopleArray,transReview)!=0:    # accounts for if possessives precede the relation
            temp = findName(array,idx,peopleArray,transReview)
            peopleArray.append(temp[0])
            idx = temp[1]
            return idx
        elif findName(array,idx,peopleArray,transReview)!=0:
            temp = findName(array,idx,peopleArray,transReview)
            peopleArray.append(temp[0])
            idx = temp[1]
            return idx
        elif getPlace(array,idx,placesArray)!=0:
            temp = getPlace(array,idx,placesArray)
            placesArray.append(temp[0])
            idx = temp[1]
            return idx
        else:
            transReview.append("Review: New PERSON or PLACE possibly mentioned.")
            
# Handles the "from" keyword
def from_Keyword(array,transReview,peopleArray,placesArray):  
    indices = [i for i, x in enumerate(array) if x == "from"]

    for idx in indices:
        if len(array)>idx+1:    # Prevents out of bounds index increments
            idx+=1
        else:
            return

        if len(array)>idx+1 and array[idx] == "the":   # Checks if next word is "the"
            idx+1
            name = findName(array,idx,peopleArray,transReview)  # Looks for profession/person
            if name == 0:      # If no person/profession found, saves next word as a place
                temp = getPlace(array,idx,placesArray)
                if temp != 0:
                    placesArray.append(temp[0])
                    idx = temp[1]
                else:
                    transReview.append("Review: New PERSON or PLACE possibly mentioned.")
            else:
                peopleArray.append(name[0])
                idx = name[1]
        elif len(array)>idx+1 and lem.lemmatize(array[idx]) in relationsList and findName(array,idx+1,peopleArray,transReview)!=0:	 # Checks if word is in relationList
            temp = findName(array,idx,peopleArray,transReview)
            peopleArray.append(temp[0])
            idx = temp[1]
            return idx
        elif len(array)>idx+2 and lem.lemmatize(array[idx+1]) in relationsList and findName(array,idx+2,peopleArray,transReview)!=0:    # accounts for if possessives precede the relation
            temp = findName(array,idx,peopleArray,transReview)
            peopleArray.append(temp[0])
            idx = temp[1]
            return idx
        elif findName(array,idx,peopleArray,transReview)!=0:
            temp = findName(array,idx,peopleArray,transReview)
            peopleArray.append(temp[0])
            idx = temp[1]
            return idx
        elif getPlace(array,idx,placesArray)!=0:
            temp = getPlace(array,idx,placesArray)
            placesArray.append(temp[0])
            idx = temp[1]
            return idx
        else:
            transReview.append("Review: New PERSON or PLACE possibly mentioned.")

# Handles the "to" keyword
def to_Keyword(array,transReview,peopleArray,placesArray):  
    indices = [i for i, x in enumerate(array) if x == "to"]

    for idx in indices:
        if len(array)>idx+1:    # Prevents out of bounds index increments
            idx+=1
        else:
            return

        if len(array)>idx+1 and array[idx] == "the":   # Checks if next word is "the"
            idx+1
            name = findName(array,idx,peopleArray,transReview)  # Looks for profession/person
            if name == 0:      # If no PERSON/PROFESSION found, checks for PLACE
                temp = getPlace(array,idx,placesArray)
                if temp != 0:
                    placesArray.append(temp[0])
                    idx = temp[1]
                else:
                    transReview.append("Review: New PERSON or PLACE possibly mentioned.")
            else:
                peopleArray.append(name[0])
                idx = name[1]
        elif len(array)>idx+1 and lem.lemmatize(array[idx]) in relationsList and findName(array,idx+1,peopleArray,transReview)!=0:	 # Checks if word is in relationList
            temp = findName(array,idx,peopleArray,transReview)
            peopleArray.append(temp[0])
            idx = temp[1]
            return idx
        elif len(array)>idx+2 and lem.lemmatize(array[idx+1]) in relationsList and findName(array,idx+2,peopleArray,transReview)!=0:    # accounts for if possessives precede the relation
            temp = findName(array,idx,peopleArray,transReview)
            peopleArray.append(temp[0])
            idx = temp[1]
            return idx
        elif findName(array,idx,peopleArray,transReview)!=0:    # Checks for PERSON
            temp = findName(array,idx,peopleArray,transReview)
            peopleArray.append(temp[0])
            idx = temp[1]
            return idx
        elif getPlace(array,idx,placesArray)!=0:    # Checks for PLACE
            temp = getPlace(array,idx,placesArray)
            placesArray.append(temp[0])
            idx = temp[1]
            return idx
        else:
            transReview.append("Review: New PERSON or PLACE possibly mentioned.")

# Handles the "in" keyword
def in_Keyword(array,transReview,peopleArray,placesArray):  
    indices = [i for i, x in enumerate(array) if x == "in"]

    for idx in indices:
        if len(array)<idx+1:    # Prevents out of bounds index increments
            idx+=1
        else:
            return

        if len(array)>idx+1 and array[idx] == "the":   # Checks if next word is "the"
            idx+1
        
        temp = getPlace(array,idx,placesArray)   # Checks for PLACE
        if temp != 0:
            placesArray.append(temp[0])
            idx = temp[1]
          
# Handles the "at" keyword
def at_Keyword(array,transReview,peopleArray,placesArray):  
    indices = [i for i, x in enumerate(array) if x == "in"]

    for idx in indices:
        if len(array)<idx+1:    # Prevents out of bounds index increments
            idx+=1
        else:
            return

        if len(array)>idx+1 and array[idx] == "the":   # Checks if next word is "the"
            idx+1
            
        temp = getPlace(array,idx,placesArray)   # Checks for PLACE
        if temp != 0:
            placesArray.append(temp[0])
            idx = temp[1]
        
# Handles the "balance" keyword
def balance_Keyword(array,idx,transDict,transReview,peopleArray,placesArray,otherItems):
    if process.extractBests("balance",array,scorer=fuzz.QRatio,score_cutoff=86):
        idx = array.index(process.extractBests("balance",array,scorer=fuzz.QRatio,score_cutoff=86)[0][0])  # Gets the array index of "balance"
    if process.extractBests("ballance",array,scorer=fuzz.QRatio,score_cutoff=86):
        idx = array.index(process.extractBests("ballance",array,scorer=fuzz.QRatio,score_cutoff=86)[0][0])  # Gets the array index of "balance" 

    # Removes other keywords attached to "balance"   
    if idx > 0 and array[idx-1] == "the":
        array[idx-1] = ""
        if idx > 1 and array[idx-2] == "for":
            array[idx-2] = ""
        transDict["item"] = "balance from"
    if len(array)>idx+1  and array[idx+1] == "of":
        array[idx+1] = ""
        transDict["item"] = "balance from"
    if len(array)>idx+1 and array[idx+1].isdigit():
        x = getCost(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
        idx = compareIdx(idx,x)

# Handles the "on the" keyword pair
def onThe_Keywords(array,idx1,idx2,placesArray,transReview):
    array[idx1] = ""  # Removes "on"/"in"
    array[idx2] = ""  # Removes "the"
    
    if len(array)>idx2+1:
        idx2+=1
        temp = getPlace(array,idx2,placesArray)
        if temp != 0: 
            placesArray.append(temp[0])
        else:
            placesArray.append(array[idx2])
            transReview.append("Review: Confirm PLACE name.")

# Handles the "charge on/of" keyword pattern
def charge_Keyword(array,transDict,transReview,peopleArray,placesArray,otherItems):
    idx = array.index('charge')+1   # Gets the array index for "charge" and increments it to next word
    if idx>0 and array[idx-1] == "for":
        array[idx-1] = ""

    if len(array)>idx+1 and array[idx] in ["of", "on"]:
        array[idx]= ""
        idx+=1
        if array[idx] not in ignore and process.extractBests(array[idx], itemList, scorer=tsr, score_cutoff=86):   # Checks for ITEM
            idx = getItem(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
            if transDict["quantity"] == "" and process.extractBests(transDict["item"],nonItems,scorer=tsr,score_cutoff=95)==[]:
                transDict["quantity"] = "1"
            idx = afterItemCheck(array,idx,transDict,transReview,peopleArray,placesArray,otherItems,"item")
        elif array[idx].isdigit() or process.extractBests(array[idx], qualifierList, scorer=tsr, score_cutoff=86):  # Checks for QUANTITY-QUALIFIER-ITEM
            QQI_Pattern(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
        elif array[idx] not in ignore and process.extractBests(array[idx], variantList, scorer=tsr, score_cutoff=86):   # Checks for VARIANT-ITEM
            findAdjectives(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
        else:
            transReview.append("REVIEW: New ITEM possible mentioned")

# Handles the "paid" keyword pair
def paid_Keyword(array,transDict,transReview,peopleArray,placesArray,otherItems):
    idx = array.index('paid')

    if len(array)>idx+2 and array[idx+2] in ["a","an"]:
        idx+=2
        if array[idx] not in ignore and process.extractBests(array[idx],itemList,scorer=fuzz.QRatio,score_cutoff=86) == [] and transDict["item"]=="":
            transDict["item"] = array[idx]
    elif idx > 0 and (transDict["totalCost"] == "" or transDict["unitCost"] == "" ):
        idx+=1
        x = getCost(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
        idx = compareIdx(idx,x)
        transReview.append("Review: Confirm costs.")

# Handles the "expense"/"expence" keyword
def expense_Keyword(array,idx,transDict,transReview,peopleArray,placesArray,otherItems):             
    # Checks words preceding "Expense"
    if idx > 0:  # Prevents out of bounds index subtraction
        index = idx-1
        if reverseFindName(array,index,transReview, peopleArray) == 0:  # Checks for PEOPLE
            newPplDict = peopleObject.copy()   # Initializes new people dictionary
            newPplDict["Account"] = f"{array[index]} Expenses" # Saves Account Name
            transReview.append("Review: ACCOUNT name.")

    # Checks the following words
    if len(array)>idx+1 and array[idx+1] in ["for","of"]:
        idx+=1
        idx = beginsExpense(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
    return idx

# Function for "account" keyword
def account_Keyword(array,idx,transDict,transReview,peopleArray,placesArray,otherItems):
    idx = array.index('account')  # Gets the array index for "account"
        
    # Checks words preceding "account"
    if idx > 0:  # Prevents out of bounds index subtraction
        index = idx-1
        if idx > 0 and array[index] in ["on","in"]: # Checks if "on" precedes "account"
            array[idx-1] = ""   # Removes "on"/"in" keyword
            index = index-1
        if reverseFindName(array,index,transReview,peopleArray) == 0: # Checks for preceding names and professions
            newPplDict = peopleObject.copy()   # Initializes new people dictionary
            newPplDict["Account"] = f"{array[index]} Account" # Saves Account Name
            transReview.append("Review: ACCOUNT name.")
    
    idx+=1  
    # Checks the following word
    while len(array) > idx:
        if array[idx] == "of":  # Checks if "of" follows "account"
            array[idx] == ""  # Removes "of" keyword
            idx+=1
            if len(array)>idx+1 and fuzz.ratio(lem.lemmatize(array[idx+1]), "furniture") >= 90:
                transDict["item"] = f"{array[idx]} Furniture"
                idx+=2
                if array[idx] == "for":
                    for_Keyword(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
                else:
                    x = getCost(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
                    idx = compareIdx(idx,x)
            elif len(array)>idx+1 and (fuzz.ratio(lem.lemmatize(array[idx+1]), "expense") >= 90 or fuzz.ratio(array[idx+1], "expence") >= 90):
                transDict["item"] = f"{array[idx]} Expenses"
                idx+=2
                if array[idx] == "for":
                    for_Keyword(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
                else:
                    getCost(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
                    idx = compareIdx(idx,x)
            else:
                temp = findName(array,idx,peopleArray,transReview)   # Checks for name
                peopleArray.append(temp[0])
                idx = temp[1]
                idx = compareIdx(idx,x)
        break
                
# Handles the "value" keyword
def value_Keyword(array,idx,transDict,transReview,peopleArray,placesArray,otherItems):
    idx = array.index('value')  # Gets the array index for "value"

    # Checks if "the" is preceeding and removes it
    if idx > 0 and array[idx - 1] == "the":
        array[idx - 1] == ""

    idx+=1
    if len(array)>idx and array[idx] == "of":  # Checks if "of" follows "value"
        array[idx] == ""  # Removes "of"
        idx+=1
        if array[idx] not in ignore and process.extractBests(array[idx], itemList, scorer=tsr, score_cutoff=80):  # Checks if word is an item
            item = process.extractBests(array[idx], itemList, scorer=tsr, score_cutoff=80)[0][0]
            i = len(item.split())
            idx = idx+i
            transDict["item"] = f"Value of {item}"
        else:
            if len(array)>idx+2 and array[idx+1] in ["&","and"] and array[idx+2] not in ignore and process.extractBests(array[idx+2], itemList, scorer=tsr, score_cutoff=80)==[]:
                transDict["item"] = f"{array[idx]} & {array[idx+2]} Value"
                idx+=3
                x = getCost(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
                idx = compareIdx(idx,x)
            else:
                transDict["item"] = f"{array[idx]} Value"
                idx+=1
                x = getCost(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
                idx = compareIdx(idx,x)
            
# Handles the "returned" keyword
def returnedOmitted_Keywords(array,transDict):
    if "returned" in array and transDict["service"] == "":
        transDict["service"] = "returned"
    if "returnd" in array and transDict["service"] == "":
        transDict["service"] = "returned"
    if "omitted" in array and transDict["service"] == "":
        transDict["service"] = "omitted"

# Handles the "by" keyword
def by_Keyword(array, transReview,placesArray,peopleArray):
    idx = array.index('by')  # Gets the array index for "by"

    if len(array)>idx+1:
        idx+=1
    if array[idx] == "the":
        idx+=1

    if len(array)>idx+1 and array[idx] in possessiveList:  # Checks for proceeding pronouns
        idx+=1
        if array[idx] in relationsList:
            idx+=1
        temp = findName(array,idx,peopleArray,transReview)==0
        if temp ==0:  # Checks for PERSON
            transReview.append("Review: New/Unknown PERSON possibly mentioned.")
        else:
            peopleArray.append(temp[0])
            idx = temp[1]
    else:
        if getPlace(array,idx,placesArray) !=0:   # Checks for PLACE
            temp = getPlace(array,idx,placesArray)
            placesArray.append(temp[0])
            idx = temp[1]
        elif findName(array,idx,peopleArray,transReview) !=0:   # Checks for PERSON
            temp = findName(array,idx,peopleArray,transReview)
            peopleArray.append(temp[0])
            idx = temp[1]
        else:
            transReview.append("Review: New/Unkown PLACE/PERSON possibly mentioned.")

# Handles the "for" keyword
def for_Keyword(array,idx,transDict,transReview,peopleArray,placesArray,otherItems):    # sourcery skip: low-code-quality
    if idx is None:   # Sets index
        idx = array.index('for')  # Gets the array index for "for"
    array[idx] = ""  # Removes "for"

    # Accounts for "for part of" phrase
    if len(array)>idx+3 and fuzz.QRatio(array[idx+1],"part")>88 and array[idx+2]=="of":
        idx+=3
         
    while len(array)>idx:
        idx+=1
        if array[idx] == "the": # Checks if "the" follows
            idx+=1
            if (fuzz.QRatio(array[idx],"expense")>=90 or fuzz.QRatio(array[idx],"expence")>=90):
                array[idx] = "expense"
                expense_Keyword(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
            if array[idx] not in ignore and process.extractBests(array[idx], itemList, scorer=tsr, score_cutoff=95):  # Checks for ITEM
                idx = getItem(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
                if transDict["quantity"] == "" and process.extractBests(transDict["item"],nonItems,scorer=tsr,score_cutoff=95)==[]:
                    transDict["quantity"] = "1"
                idx = afterItemCheck(array,idx,transDict,transReview,peopleArray,placesArray,otherItems,"item")
            elif array[idx] in personsList:  # checks for people
                idx+=1
                temp = getCost(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
                if temp: #checks for cost
                    name = findName(array,idx,peopleArray,transReview)  # Checks for name if no COST found  
                    if name != 0:   # If name found checks for COST, accounts for length of name
                        peopleArray.append(name[0])
                        idx = name[1]    # Updates index with name count
                        x = getCost(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)  # checks for following COST
                        idx = compareIdx(idx,x)
                    # Stores unkown person and checks if COST follows
                    elif len(array)> idx+2 and (array[idx+1][0].isdigit() == True or array[idx+2][0].isdigit() == True):  
                        unknown = peopleObject.copy()
                        unknown["firstname"] = "FNU"  # Stores first name
                        unknown["lastname"] = "LNU"  # Stores stores 
                        peopleArray.append(unknown)
                        transReview.append("Review people names.")
                        idx+=1
                        x = getCost(array,idx,transDict,transReview,peopleArray,placesArray,otherItems) # checks for following COST
                        idx = compareIdx(idx,x)
                else:
                    idx = temp
            elif len(array)>idx+1 and array[idx][0].isdigit() == True:  # Checks for QUANTITY-QUALIFIER-ITEM pattern
                idx+=1
                x = QQI_Pattern(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
            elif array[idx] not in ignore and process.extractBests(array[idx],servicesList,scorer=tsr,score_cutoff=90):
                transDict["Service"] = process.extractBests(array[idx],servicesList,scorer=tsr,score_cutoff=90)[0][0]   # Stores SERVICE
                idx+=1
                x = serviceA_Pattern(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
            elif len(array[idx])>3  and (array[idx][-3:]== "ing" or array[idx][-4:]== "ings") and process.extractBests(array[idx],ingList,scorer=fuzz.QRatio,score_cutoff=90)==[]:
                transDict["Service"] = array[idx]   # Stores SERVICE
                idx+=1
                x = serviceA_Pattern(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
            elif fuzz.token_set_ratio(array[idx], "ballance")>87:
                transDict["item"] = "Balance from"
                array[idx] = ""
                idx+=1
           # Checks for SERVICES
        elif len(array)>idx+1 and fuzz.ratio(lem.lemmatize(array[idx+1]), "furniture") >= 90:
            transDict["item"] = f"{array[idx]} Furniture"
            idx+=2
            x = getCost(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
            idx = compareIdx(idx,x)
        elif array[idx] not in ignore and process.extractBests(array[idx], servicesList, scorer=tsr, score_cutoff=90):
            transDict["Service"] = process.extractBests(array[idx],servicesList,scorer=tsr,score_cutoff=90)[0][0]   # Stores SERVICE
            idx+=1
            idx = serviceA_Pattern(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
        elif len(array[idx])>3 and array[idx] not in ignore and (array[idx][-3:]== "ing" or array[idx][-4:]== "ings") and process.extractBests(array[idx],ingList,scorer=fuzz.QRatio,score_cutoff=90)==[]:
            transDict["Service"] = array[idx]   # Stores SERVICE
            idx+=1
            idx = serviceA_Pattern(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
        elif len(array)>idx+1 and array[idx][0].isdigit() == True or array[idx] in ["a","an"]:  # Checks for QUANTITY-QUALIFIER-ITEM pattern
            idx = QQI_Pattern(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
        elif findName(array,idx,peopleArray,transReview) !=0: # Checks for PERSON
            temp = findName(array,idx,peopleArray,transReview)
            peopleArray.append(temp[0])
            idx = temp[1]
            return idx
        elif len(array)>idx+1 and array[idx+1] in ["a", "an", "of"] :
            transDict["Service"] = array[idx]   # Stores possible SERVICE
            transReview.append("Review: Confirm SERVICE.")
            idx+=1
            idx = serviceA_Pattern(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
        elif len(array)>idx+2 and array[idx] == "fee" and array[idx+1] == "on":
            idx+=2
            if array[idx] not in ignore and process.extractBests(array[idx], itemList, scorer=tsr, score_cutoff=85):
                transDict["item"] = process.extractBests(array[idx], itemList, scorer=tsr, score_cutoff=85)[0][0]
                idx = idx+len(transDict["item"].split())
                x = getCost(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
                idx = compareIdx(idx,x)
            else:
                transDict["item"] = f"Fee On {array[idx]}"
                transReview.append("Review: Confirm ITEM.")
                idx+=1
                x = getCost(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
                idx = compareIdx(idx,x)
        break
    
    return idx

# Handles "&", "and", "with", and commas
def and_item(array,idx,transDict,transReview,peopleArray,placesArray,otherItems,flag):
    newItem = transactionObject.copy()
    def itemComparison(transDict,newItem,flag=None):
        if otherItems != [] and len(otherItems)>1:
            compareItem = otherItems[-1].copy()
        else:
            compareItem = transDict.copy()

        if compareItem["item"] == newItem["item"] and compareItem["quantity"]!="" and newItem["quantity"]!="" and compareItem["variants"]==[] and newItem["variants"]==[]:
            if compareItem["qualifier"] == newItem["qualifier"]!="" and numType(compareItem["quantity"])!=False and numType(newItem["quantity"])!=False:
                numType1 = numType(compareItem["quantity"])
                numType2 = numType(newItem["quantity"])
                value = numType1(compareItem["quantity"]) + numType2(newItem["quantity"])
                compareItem["quantity"] = f"{value}"
                return compareItem
        elif compareItem["item"]=="" and newItem["item"]!="" and flag=="qualifier":
            numType1 = numType(compareItem["quantity"])
            numType2 = numType(newItem["quantity"])
            value = numType1(compareItem["quantity"]) + numType2(newItem["quantity"])
            compareItem["quantity"] = f"{value}"
            return compareItem
        else:
            return 0
 
    if flag[0] == "service":
        # Service "&" Service
        if array[idx] not in ignore and process.extractBests(lem.lemmatize(array[idx]),servicesList,scorer=tsr,score_cutoff=95)==True:
            if transDict["item"] == "":
                transDict["service"] = f"{flag[1]} & {array[idx]}"
                idx+=1
                idx = serviceA_Pattern(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
                return idx
        elif len(array[idx])>3 and array[idx] not in ignore and (array[idx][-3:]== "ing" or array[idx][-4:]== "ings") and process.extractBests(array[idx],ingList,scorer=fuzz.QRatio,score_cutoff=90)==[]:
            if transDict["item"] == "":
                transDict["service"] = f"{flag[1]} & {array[idx]}"
                idx+=1
                idx = serviceA_Pattern(array,idx,transDict,transReview,peopleArray,placesArray,otherItems)
                return idx
        else:
            if transDict["item"] == "":
                transDict["item"] = f"{flag[1]} & {array[idx]}"
                idx+=1
                return idx

    if flag[0] == "item":
        
        if array[idx] not in ignore and process.extractBests(lem.lemmatize(array[idx]),servicesList,scorer=tsr,score_cutoff=95):  # Checks for known SERVICE
            newItem["service"] = process.extractBests(lem.lemmatize(array[idx]),servicesList,scorer=tsr,score_cutoff=95)[0][0]
            idx+=1
            idx = serviceA_Pattern(array,idx,newItem,transReview,peopleArray,placesArray,otherItems)
            if newItem != transactionObject:
                otherItems.append(newItem)
            return idx
        # Checks for unknown SERVICE
        elif len(array[idx])>3 and array[idx] not in ignore and (array[idx][-3:]== "ing" or array[idx][-4:]== "ings") and process.extractBests(array[idx],ingList,scorer=fuzz.QRatio,score_cutoff=90)==[]:
            newItem["service"] = array[idx]
            idx+=1
            idx = serviceA_Pattern(array,idx,newItem,transReview,peopleArray,placesArray,otherItems)
            if newItem != transactionObject:
                otherItems.append(newItem)
            return idx
        elif idx>0 and fuzz.QRatio(array[idx-1],"with")>90:
            if array[idx] not in ignore and process.extractBests(array[idx], variantList, scorer=tsr, score_cutoff=85):
                transDict["variant"].append(process.extractBests(array[idx], variantList, scorer=tsr, score_cutoff=85)[0][0])
                idx+=1
                return idx
        elif array[idx] not in ignore and process.extractBests(array[idx], itemList, scorer=tsr, score_cutoff=85):   # ITEM & ITEM
            idx = getItem(array,idx,newItem,transReview,peopleArray,placesArray,otherItems)
            newItem["quantity"] = 1
            idx = afterItemCheck(array,idx,newItem,transReview,peopleArray,placesArray,otherItems,flag)
            test = itemComparison(transDict,newItem)
            if test != 0:
                transDict = test
            elif newItem != transactionObject:
                otherItems.append(newItem)
        elif array[idx] in ["a","an"]:   # ITEM & {a,an} ITEM
            idx = QQI_Pattern(array,idx,newItem,transReview,peopleArray,placesArray,otherItems)
            if newItem != transactionObject:
                otherItems.append(newItem)
        elif array[idx][0].isdigit()==True and len(array)==idx+1:  # ITEM & PRICE
            x = getCost(array,idx,newItem,transReview,peopleArray,placesArray,otherItems)
            idx = compareIdx(idx,x)
        elif len(array)>idx+1 and array[idx][0].isdigit()==True and array[idx+1] in ["currency","sterling"]:  # ITEM & PRICE
            newItem["item"] = "Cash"
            x = getCost(array,idx,newItem,transReview,peopleArray,placesArray,otherItems)
            idx = compareIdx(idx,x)
            if newItem["totalCost"] != "" or newItem["unitCost"] != "":
                otherItems.append(newItem)
        else:
            idx = QQI_Pattern(array,idx,newItem,transReview,peopleArray,placesArray,otherItems)
            if newItem != transactionObject:
                idx = afterItemCheck(array,idx,newItem,transReview,peopleArray,placesArray,otherItems,flag)
                test = itemComparison(transDict,newItem)
                if test != 0:
                    transDict = test
                else:
                    otherItems.append(newItem)


    if flag[0] == "variant":
        if array[idx][0].isdigit()==True or array[idx] in ["a","an"] :  # ITEM & QUANTITY
            idx = QQI_Pattern(array,idx,newItem,transReview,peopleArray,placesArray,otherItems)
            if transDict["variants"][0] == newItem["variants"][0] and transDict["quantity"]!="" and newItem["quantity"]!="":
                n1=numType(newItem["quantity"])
                n2=numType(transDict["quantity"])
                quantity = n1(newItem["quantity"])+n2(transDict["quantity"])
                transDict["quantity"] = f"{quantity}"
            else:
                transDict["item"] = newItem["item"]
                transDict["qualifier"] = newItem["qualifier"]
    
    if flag[0] == "qualifier":  
        saveIndex = idx
        if len(array)>idx+1:
                saveNext = array[idx+1]  
        idx = QQI_Pattern(array,idx,newItem,transReview,peopleArray,placesArray,otherItems)
        test = itemComparison(transDict,newItem,"qualifier")
        if test != 0:
                transDict = test
        elif newItem != transactionObject:
            otherItems.append(newItem)
  
def convertPeopleObject(peopleArray):
    people = []
    name = ""
    for pplDict in peopleArray:
        for x in pplDict:
            if pplDict[x]!="":
                name = name + " " + pplDict[x]
    name = name.strip()
    if name not in people:
        people.append(name)
    return people

# ************************************************** VARIABLES USED *********************************************************** #
# entryError[] = List to store entry errors              |  transArray[] = Array of tokenizd transactions from Chip's function
# variantsArray = List of item's adjectives              |  otherItems[] = List of items associated with a single transaction
# transError[] = List to store entry errors              |  peopleArray[] = List of people mentioned in a transaction
# transReview = List of data to review in a transaction  |  placesArray[] = List of places mentioned in a transaction
# ***************************************************************************************************************************** #

# Function that parses the transactions from the entry column (transArr = array/list of transactions)
def parse_transaction(transArr):    # sourcery skip: hoist-statement-from-loop
 # Initializes error array for entry
    entryErrors = []
    parsedTransactionsArray = []

    i = 0
    # Converts all array elememts to lowercase
    # print(type(transArr))
    # print(transArr)

    while i < len(transArr):
        transArr[i] = [element.lower() for element in transArr[i]]
        i+=1

    # Checks the firt word of first tranaction to get entry's transaction type.
    if transArr[0][0] == "to":
        transactionType = "debit"
    elif transArr[0][0] == "by":
        transactionType = "credit"
    else:
        transactionType = "None"
        entryErrors.append("Error: No transaction type.")
    
    allTransactionsList = []
    # Iterating through array/list of transactions
    for transaction in transArr:

        # Checks for "vizt", deletes it and everything after it
        if "vizt" in transaction:
            x = transaction.index('vizt')  # Gets the array index for "vizt"
            if len(transaction)>x+1 and transaction[x+1] == "from":
                del transaction[x:]

        # Initializes lists for transaction values and errors
        otherItems, peopleArray, placesArray, transReview = [],[],[],[] 
        transDict = transactionObject.copy() # Initializies dictionary for transaction
        transDict["variants"]=[]
        transDict["includedItems"]=[]

        # Checks if first character of first string is a digit or letter
        if transaction[0][0].isdigit() == True:
            intFirstParse(transaction,transDict,transReview,peopleArray,placesArray,otherItems)
            parsedTransactionsArray.append(transDict)
        elif transaction[0][0].isalpha() == True or transaction[0][0]=="&":
            # Checks if first word in array is "to" or "by"
            if transaction[0] == "to" or transaction[0] == "by":
                transaction.pop(0)  # Removes "to" or "by" 
           
            if transaction[0][0].isdigit() == True:  # Checks again if first character of first string is a digit or letter
                intFirstParse(transaction,transDict,transReview,peopleArray,placesArray,otherItems)
                parsedTransactionsArray.append(transDict)
            else:
                alphaFirstParse(transaction,transDict,transReview,peopleArray,placesArray,otherItems)
                parsedTransactionsArray.append(transDict)
        else:
            entryErrors.append("Error: Entry does not begin with a letter or number")
        
        if len(peopleArray)>0:
            temp = convertPeopleObject(peopleArray)
            transDict["mentionedPpl"].append(temp)
        
        if len(placesArray)>0:
            transDict["mentionedPlaces"].append(placesArray)

        if len(transReview)>0:
            transDict["errorReview"].append(transReview)
        
        allTransactionsList.append(transDict)
        
              
    return [allTransactionsList, transactionType]

text = "the Sherriff 11 pounds tobacco & 4/:"
transactions = preprocess(text)
print(transactions)
results = parse_transaction(transactions)
print(results)
#for dict in results[0]:
#   print(dict, end="\n\n")
