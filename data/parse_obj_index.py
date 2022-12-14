import pandas as pd
from re import sub
from json import dump

# All this program does is read in the object index. Maybe convert this into an api endpoint to update the object index at some point.

df = pd.read_excel("Object Index_1760-1761.xlsx")
items = df["Item"]

item_set = set([(" ".join(reversed(x))).strip() for x in items.str.split(",")])
item_set = {sub(r"\s+", " ", x) for x in item_set}


with open("obj_index.json", 'w') as file:
    dump(list(item_set), file)