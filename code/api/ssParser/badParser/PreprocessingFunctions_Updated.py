import itertools
import nltk
nltk.download('punkt')
import re

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
  tknzr = nltk.tokenize.TweetTokenizer(match_phone_numbers=False)
  tokens = tknzr.tokenize(text)
  print(tokens)
  #begin process of removing words that are replacements
  replaceWordIndex = []
  removenumbersIndex = []
  for idx, word in enumerate(tokens): #this basically means that 'current' index is accesed by variable idx inside loop
    if word == '[':
      if tokens[idx + 2] == ']' and (tokens[idx - 1][0] == tokens[idx + 1][0] or tokens[idx - 1] in ["ditto", "Ditto"]):
        replaceWordIndex.append(idx) #replace words based on matching first letter
      elif tokens[idx+1].isdigit() == True and tokens[idx+3] != "..":  # Accounts for prices in brackets
        removenumbersIndex.append(idx)
  for num in removenumbersIndex:#pop the numbers
    tokens.pop(num+1)
    for count in range(len(replaceWordIndex)): #adjust index accordingly
      if(replaceWordIndex[count] > num):
        replaceWordIndex[count] = replaceWordIndex[count]-1
    for count in range(len(removenumbersIndex)): #adjust index accordingly
      if(removenumbersIndex[count] > num):
        removenumbersIndex[count] = removenumbersIndex[count]-1
  for num in replaceWordIndex: #pop replaced word and brakets
    tokens.pop(num+2)
    tokens.pop(num)
    tokens.pop(num-1)
    for count in range(len(replaceWordIndex)): #adjust index accordingly
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
          if (tokens[idx-1][0] == tokens[idx+1][0] or tokens[idx-1] in ["ditto","Ditto"]):
            replaceWordIndex.append(idx)
          break
      break
  for num in replaceWordIndex:
    tokens.pop(num-1)
    for count in range(len(replaceWordIndex)): #adjust index accordingly
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
 
  fractions = {'¼': "0.25", "⅓": "0.33", '½': "0.5", '⅕': "0.2", '⅙': "0.167", '⅐': "0.143", '⅛':" 0.125", '⅑': "0.111", 
    '⅒': "0.1", '⅔': "0.667", '⅖': "0.4", '¾': "0.75", '⅗': "0.6", '⅜': "0.375", '⅘': "0.8", '⅚': "0.833", '⅞': "0.875" }
  
  for idx, word in enumerate(tokens):
    if word in fractions:
      tokens[idx] = fractions[word]
    
    # Removes periods
    if tokens[idx] == ".":
      tokens[idx] = ""


  # Removes periods and stitches prices back together
  for idx, word in enumerate(tokens):
    # Removes possessive 's
    if len(tokens[idx]) > 1 and tokens[idx][-1] == "s" and tokens[idx][-2] == "'":
      tokens[idx] = tokens[idx].replace("'","")
     
     # 0 .. 0 .. 0
     # 0  1 2  3 4
    # Stitches Prices
    if tokens[idx] == "..":
      if len(tokens)>idx+2 and tokens[idx+1][0].isdigit()==True and tokens[idx+2] == "..":
        tokens[idx] = tokens[idx]+tokens[idx+1]+tokens[idx+2]
        tokens[idx+1] = ""
        tokens[idx+2] = ""
        if idx>0 and tokens[idx-1][0].isdigit()==True:
          tokens[idx] = tokens[idx-1]+tokens[idx]
          tokens[idx-1] = ""
        if len(tokens)>idx+3 and tokens[idx+3][0].isdigit()==True:
          tokens[idx] = tokens[idx]+tokens[idx+3]
          tokens[idx+3] = ""

  #remove all character removals '^','[',']','<','>', '{', '}', '(', ')',  '&ct', 'etcetera', 'Etcetera', '£'
  removals = ['^','[',']','<','>', '{', '}', '(', ')', '&ct', 'etcetera', 'Etcetera', '£','?']  # add all characters to be removed to this list
  for count in range(0,len(tokens)):
    for character in removals:
      while character in tokens[count]:
        tokens[count] = tokens[count].replace(character,"")
        
  #remove "" tokens
  while ("" in tokens):
    tokens.remove('')

  #gluing preceding tokens and fractions together
  numberRE = re.compile("[0-9]+")
  decimalRE = re.compile("\.[0-9]+")
  for idx, word in enumerate(tokens):
    if (numberRE.match(tokens[idx])
        and idx + 1 < len(tokens)) and (decimalRE.match(tokens[idx + 1])):
      tokens[idx] = tokens[idx] + tokens[idx+1]
      tokens.pop(idx+1)

  #gluing 'per' and 'cent' together
  for idx, word in enumerate(tokens):
    if (word in ['per', 'Per'] and idx + 1 < len(tokens)) and tokens[idx + 1] in ["cent", "Cent"]:
      tokens[idx] = 'percent'
      tokens.pop(idx+1)

  saveIdx =[]
  #changing 'w' and 'wt' 'ws' to pounds
  for idx, word in enumerate(tokens):
    if tokens[idx] in ["w","wt","W", "WT", "Wt", 'ws', 'Ws', 'wS', 'WS']:
      tokens[idx] = "pound"
    # Accounts for "w" attached to numbers (Ex: "10w" becomes "10 pound") 
    if len(tokens[idx])>1 and tokens[idx][0].isdigit() == True and (tokens[idx][-1] in ["w","W"] or tokens[idx][-2] in ["w","W"] ):
        tokens[idx] = tokens[idx].replace(tokens[idx][-1],"")
        saveIdx.append(idx)
        if tokens[idx][-1] in ["w","W"]: tokens[idx] = tokens[idx].replace(tokens[idx][-1],"")
        
  for i in saveIdx:
    tokens.insert(i+1, 'pound')

  # two pound in a row
  poundSpellings = ['pound', 'pounds', 'Pound', 'Pounds']
  for idx, word in enumerate(tokens):
    for spelling in poundSpellings:
      if word == spelling:
        for spelling2 in poundSpellings:
          while (idx+1 < len(tokens)):
            if tokens[idx+1] == spelling2:
              tokens.pop(idx+1)
            break

  # 1 M thousand and 1M Thousand --> 1000
  print(tokens)
  for idx, word in enumerate(tokens):
    numMre = re.compile('\d+(M|m)')# any number of digits followed by a single 'M' or 'm'
    if word.isnumeric():
      while (idx+2 < len(tokens)):
        if tokens[idx+1] == 'M' or tokens[idx+1] == 'm':
          if tokens[idx+2] == 'thousand' or tokens[idx+2] == 'Thousand':
            tokens[idx] = tokens[idx]+'000'
            tokens.pop(idx+2)
            tokens.pop(idx+1)
        break
    elif re.match(numMre, word):
      while (idx+1 < len(tokens)):
        if tokens[idx+1] == 'thousand' or tokens[idx+1] == 'Thousand':
          tokens[idx] = tokens[idx].rstrip(tokens[idx][-1])
          tokens[idx] = tokens[idx] + '000'
          tokens.pop(idx+1)
        break
  return tokens


def preprocess(string):#function that is acctually called
  breakup = seperate(string)
  preprocessed = []
  for count in range(0, len(breakup)):
    preprocessed.append(editTokens(breakup[count]))
  while [] in preprocessed:
    preprocessed.remove([])
  return preprocessed