import re

def extract_tm(text):
    # can be expanded by appending regex for [int]..[int]..[int]
    # parenthises separate groups
    #tm_pattern = re.compile("\[TM: ([0-9]{4}|[0-9]{5}) ([a-zA-Z]{2}|[a-zA-Z]{3})\] [A-Z] ([0-9]*) ")
    tm_pattern = re.compile("\[TM: ([0-9]{4,5}) ([^\]]{1,4})\]( +[A-Z]? +([0-9]*) +([0-9]*)\. ?\.([0-9]*)\. ?\.([0-9]*))?")

    found_tm = re.search(tm_pattern, text)
    # more variables can be made for data values
    if (found_tm != None):
      TM_id = found_tm.group(1)
      Mark = found_tm.group(2)
      note = found_tm.group(4)
      wt_total = found_tm.group(5)
      wt_barrel = found_tm.group(6)
      wt_tobacco = found_tm.group(7)

      tm_parsed = {'TM_id' : TM_id, 'Mark' : Mark, 'note' : note, 'wt_total': wt_total, 'wt_barrel' : wt_barrel, 'wt_tobacco' : wt_tobacco}
    else:
      tm_parsed = None
    return tm_parsed
    # loop through entries and extract info into variables, and then push to db (need to figure out schema)
    # loop through entries and extract info into variables, and then push to db (need to figure out schema)
