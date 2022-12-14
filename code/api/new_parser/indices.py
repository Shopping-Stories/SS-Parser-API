from json import load
from typing import FrozenSet

# Consider del-ing itemset and reimporting it when you need it to save memory when not in use.

with open("obj_index.json", 'r') as file:
    global item_set 
    # Set of all items in the dataset.
    item_set: FrozenSet[str] = frozenset({x.lower() for x in load(file)})

with open("amt_index.json", 'r') as file:
    global amount_set 
    # Set of all items in the dataset.
    amount_set: FrozenSet[str] = frozenset({x.lower() for x in load(file)})
