from fastapi import APIRouter
from boto3 import client
from .api_types import StringList, Message, IncomingFileUrls
from threading import Lock
from traceback import format_exc

router = APIRouter()

s3_lock = Lock()

url_prefix = "https://shoppingstories.s3.amazonaws.com/"

# TODO: Integrate this lock or a separate lock into the uploader, which is not here RN.

@router.get("/get_ready_URLs", tags=["Parser Management"], response_model=StringList)
def get_ready_files():
    """
    Returns a list of file URLs that are ready to be displayed and parsed/edited by the front end.
    If an error occurs, the first string in the string list retuned will have ERROR at the front of it.
    This method will block until URLs are returned.
    """
    s3 = client("s3")
    acquired = s3_lock.acquire()
    if not acquired:
        return StringList(strings=["ERROR: Unable to acquire s3 lock."])
    
    try:
        obj_list = s3.list_objects_v2(Bucket="shoppingstories", Prefix="Parsed")["Contents"]
    except Exception:
        s3_lock.release()
        return StringList(strings=["ERROR: Unable to get URLS.\n" + format_exc()])
    
    s3_lock.release()

    urls = [url_prefix + x["Key"] for x in obj_list if x["Key"] != "Parsed/"]

    return StringList(strings=urls)

@router.post("/del_ready_files", tags=["Parser Management"], response_model=Message)
def delete_ready_files(urls: IncomingFileUrls):
    """
    Deletes files from the list of files ready to be displayed/parsed by the front end.
    Call this after you upload the results from a file to the database via another endpoint.
    Do not modify the file URLs between when you get them and when you call this endpoint.
    If an error occurs, ERROR will be at the front of the message returned. Just wait a bit and call this again if that happens.
    """
    urls = urls.urls
    s3 = client("s3")

    s3_keys = {"Objects": [{"Key": x.removeprefix(url_prefix)} for x in urls]}

    acquired = s3_lock.acquire(blocking=False)
    if not acquired:
        return Message(message="ERROR: Unable to acquire s3 lock. Please wait for other operations to end.")    

    try:
        s3.delete_objects(Bucket="shoppingstories", Delete=s3_keys)
    except Exception:
        s3_lock.release()
        return Message(message="ERROR: Unable to delete objects.\n" + format_exc())

    s3_lock.release()

    return Message(message="Successfully deleted requested objects.")