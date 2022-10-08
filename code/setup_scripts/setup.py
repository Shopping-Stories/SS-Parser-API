import nltk
from os import path
nltk.download("all")

file = open("nginx.conf")
newfile = []
codedir = path.abspath(path.join(path.dirname(__file__), '..'))
for line in file.readlines():
    if "/path/to/app/current/public" in line:
        line = line.replace("/path/to/app/current/public", codedir)
    newfile.append(line)
file.close()

file = open("nginx.conf", 'w')
file.writelines(newfile)
file.close()