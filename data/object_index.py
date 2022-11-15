from json import load
from typing import FrozenSet

# Consider del-ing itemset and reimporting it when you need it to save memory when not in use.

file = open("obj_index.json", 'r')

# Set of all items in the dataset.
item_set: FrozenSet[str] = frozenset({x.lower() for x in load(file)})

file.close()