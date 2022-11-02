from os import listdir, path
from sys import argv
from json import load

if __name__ == "__main__":
    if len(argv) > 1:
        folder = argv[1]
        contents = listdir(folder)
        total_rows = 0
        explicit_error_rows = 0
        for filename in contents:
            if filename.split(".")[-1] == "json":
                file = open(path.join(folder, filename))
                data = load(file)
                file.close()
                for translist in data:
                    for row in translist:
                        if "errors" in row:
                            explicit_error_rows += 1
                        total_rows += 1
        print(f"Total explicit error rows: {explicit_error_rows}, total rows: {total_rows}")
