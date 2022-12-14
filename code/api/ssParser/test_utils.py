from os import getcwd, path
from ss_parser import parse_file
from json import load
from copy import deepcopy

# Utility functions for integration testing

# If you decorate your function with easy_test, you don't have to manually specify the test, just
# make a function with the name of your test, and in the expected outputs folder put <yourfuncname>.json
# and in inputs folder put <yourfuncname.xlsx> and this will do the rest for you
def easy_test(f):
    fname = f.__name__
    excel_path = path.join(getcwd(), "inputs", f"{fname}.xlsx")
    expected_output_path = path.join(getcwd(), "expected outputs", f"{fname}.json")
    
    def wrap(self, *args, **kwargs):
        actual_output = parse_file(excel_path)
        expected_output = None
        with open(expected_output_path, 'r') as file:
            expected_output = load(file)
        
        expected_output = remove_created_at(expected_output)
        actual_output = remove_created_at(actual_output)
        
        f(self, *args, **kwargs, output=actual_output, expected_output=expected_output)
    
    return wrap


# Remove created at fields to allow tests to pass, and make sure entryID is a string
def remove_created_at(out):
    new_out = []
    for entry in out:
        newentry = deepcopy(entry)
        del newentry["createdAt"]
        if "updatedAt" in newentry:
            del newentry["updatedAt"]
        new_out.append(newentry)
        newentry["meta"]["entryID"] = str(newentry["meta"]["entryID"])
    return new_out