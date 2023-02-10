from json import load
from typing import FrozenSet
from os.path import dirname, join

# Consider del-ing itemset and reimporting it when you need it to save memory when not in use.
# ^ This is a funny idea by past me

with open(join(dirname(__file__), "obj_index.json"), 'r') as file:
    global item_set 
    # Set of all items in the dataset.
    item_set: FrozenSet[str] = frozenset({x.lower() for x in load(file)})

with open(join(dirname(__file__), "amt_index.json"), 'r') as file:
    global amount_set 
    # Set of all items in the dataset.
    amount_set: FrozenSet[str] = frozenset({x.lower() for x in load(file)})

drink_set: FrozenSet[str] = frozenset(["brandy", "rum"])