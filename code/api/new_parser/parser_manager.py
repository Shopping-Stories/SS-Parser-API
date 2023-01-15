from .new_parser import parse_folder
from os import makedirs, listdir, remove
from os.path import join, dirname, basename
from ..new_entry_manager import preparsed_lock
import traceback
import datetime
from threading import Lock

dump_folder = join(dirname(__file__), "ParseMe")

isparsing = False

_parsing_lock = Lock()

def start_parse(client):
    acquired = _parsing_lock.acquire(blocking=False)
    if not acquired:
        return

    isparsing = True

    out = client.list_objects_v2(Bucket="shoppingstories", Prefix="ParseMe")
    filenames = [x["Key"] for x in out["Contents"] if x["Key"] != "ParseMe/"]
    
    makedirs(dump_folder, exist_ok=True)
    
    todelete = []

    for filename in filenames:
        filename = filename.__str__()
        newfilename = join(dump_folder, filename.removeprefix("ParseMe/"))
        worked = True
        try:
            with open(newfilename, 'wb') as file:
                client.download_fileobj("shoppingstories", filename, file)
        except Exception:
            worked = False
            file = open("crashlog.txt", 'w+')
            file.write("\n")
            file.write("Crash log for " + datetime.datetime.now().__str__() + "\n")
            file.write(traceback.format_exc())
            file.write("\n")
            file.write(filename.__str__())
            file.write("\n")
            file.close()
            continue

        if worked:
            todelete.append(filename)
    
    client.delete_objects(Bucket="shoppingstories", Delete={"Objects": [{"Key": key} for key in todelete], "Quiet": True})

    parse_folder(dump_folder)
    _parsing_lock.release()

    upload_results(client)

def check_progress():
    try:
        filenames = listdir(dump_folder)
        if not filenames:
            if isparsing:
                return 0 
        tot_excel = 0
        tot_finished = 0
        toret = []
        for filename in filenames:
            if ".xls" in filename and ".json" not in filename and ".exception" not in filename:
                tot_excel += 1
                toret.append(filename)
            if ".json" in filename:
                tot_finished += 1
            if ".exception" in filename:
                tot_finished += 1
        
        return (tot_finished / tot_excel, toret)
   
    except Exception:
        return (1, [])

# Uploads parser results
def upload_results(client):
    acquired = _parsing_lock.acquire(blocking=True)
    if not acquired:
        return
    
    acquired = preparsed_lock.acquire(blocking=True)
    if not acquired:
        return

    isparsing = False
    filenames = None

    try:
        filenames = listdir(dump_folder)
    except Exception:
        _parsing_lock.release()
        preparsed_lock.release()
        return
    
    if filenames is None:
        _parsing_lock.release()
        preparsed_lock.release()
        return
    
    succeeded = []

    for filename in filenames:
        filename = join(dump_folder, filename)
        if ".json" in filename and ".exception" not in filename:
            try:
                with open(filename, 'rb') as file:
                    client.upload_fileobj(file, "shoppingstories", f"Parsed/{basename(filename)}")
                succeeded.append(filename)
            except Exception:
                pass
        else:
            succeeded.append(filename)

    for filename in succeeded:
        try:
            remove(filename)
        except Exception:
            pass

    _parsing_lock.release()
    preparsed_lock.release()