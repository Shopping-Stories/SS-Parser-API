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
