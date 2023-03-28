from .new_parser import parse_folder
from os import makedirs, listdir, remove
from os.path import join, dirname, basename
from ..new_entry_manager import preparsed_lock
import traceback
import datetime
from threading import Lock
import logging
from json import dumps
from io import BytesIO


dump_folder = join(dirname(__file__), "ParseMe")

isparsing = False

_parsing_lock = Lock()

# Parses everything in s3 bucket under prefix ParseMe
def start_parse(client):
    acquired = _parsing_lock.acquire(blocking=False)
    if not acquired:
        logging.info("Parser lock unacquired")
        return

    isparsing = True

    out = client.list_objects_v2(Bucket="shoppingstories", Prefix="ParseMe")
    filenames = [x["Key"] for x in out["Contents"] if x["Key"] != "ParseMe/"]
    
    makedirs(dump_folder, exist_ok=True)
    logging.info("makedirs done")

    todelete = []

    for filename in filenames:
        logging.info(f"doing filename {str(filename)}")
        filename = filename.__str__()
        newfilename = join(dump_folder, filename.removeprefix("ParseMe/"))
        worked = True
        try:
            with open(newfilename, 'wb') as file:
                client.download_fileobj("shoppingstories", filename, file)
        except Exception:
            logging.warning(f"filename {filename} failed! Exception: {traceback.format_exc()}")
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
            logging.info("file worked.")
            todelete.append(filename)
    
    if todelete:
        client.delete_objects(Bucket="shoppingstories", Delete={"Objects": [{"Key": key} for key in todelete], "Quiet": True})

    logging.info("Starting parse")
    set_progress(0, client)
    parse_folder(dump_folder, get_set_progress_for_parser(client))
    logging.info("finished parse.")
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
        logging.info("Parsing lock acquisition failed upload_results")
        return
    
    acquired = preparsed_lock.acquire(blocking=True)
    if not acquired:
        logging.info("Preparsing lock acquisition failed upload_results")
        return

    logging.info("Entered upload results")
    isparsing = False
    filenames = None

    try:
        filenames = listdir(dump_folder)
        logging.info(f"Found files {filenames}")
    except Exception:
        logging.warning("Exception in listdir.")
        _parsing_lock.release()
        preparsed_lock.release()
        return
    
    if filenames is None:
        logging.warning("No files found to upload.")
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

_progress_lock = Lock()
def set_progress(number: float, client):
    prog = {"progress": number}
    toUpload = BytesIO(dumps(prog).encode("UTF-8"))
    with _progress_lock:
        client.upload_fileobj(toUpload, "shoppingstories", "ParserProgress/progress.json")

def get_set_progress_for_parser(client):
    def sp(number: float):
        set_progress(number, client)
    
    return sp
