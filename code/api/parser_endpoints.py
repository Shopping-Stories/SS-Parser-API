from fastapi import APIRouter, BackgroundTasks
from boto3 import client
from io import BytesIO
from .api_types import Message, IncomingFile, ParserProgress
from .new_parser.parser_manager import start_parse, check_progress
import traceback
from base64 import b64decode

router = APIRouter()

@router.post("/upload_and_parse/{name}", tags=["Parser Management"], response_model=Message)
def upload_file_and_parse(name: str, inc_file: IncomingFile, bg_tasks: BackgroundTasks) -> Message:
    """
    Uploads bytes data from data into s3 bucket as filename name. File is assumed to be base64 encoded.
    """

    data = b64decode(inc_file.file)
    file = BytesIO(data)
    s3_cli = client('s3')
    
    try:
        s3_cli.upload_fileobj(file, "shoppingstories", f"ParseMe/{name}")
    except Exception:
        print("Could not upload file to s3.")
        print(traceback.format_exc())
        return Message(message="Error uploading file to s3.")
    
    file.close()

    bg_tasks.add_task(start_parse, s3_cli)

    return Message(message="Successfully uploaded file to s3.")


@router.get("/upload_and_parse_debug/{name}", tags=["Parser Management"], response_model=Message)
def debug_upload_file_and_parse(name: str, bg_tasks: BackgroundTasks) -> Message:
    """
    Uploads test data as a file and attempts to download it.
    """

    data = b"ajsdifjaoisdjf\nsidjfoiajsdfoas\nisjdfoaisdjfoadsi\nxd\naoidsgfdso asjdfoij sdf as \nxd"
    file = BytesIO(data)
    s3_cli = client('s3')
    
    try:
        s3_cli.upload_fileobj(file, "shoppingstories", f"ParseMe/{name}")
    except Exception:
        print("Could not upload file to s3.")
        print(traceback.format_exc())
        return Message(message="Error uploading file to s3.")
    
    file.close()

    bg_tasks.add_task(start_parse, s3_cli)

    return Message(message="Successfully uploaded file to s3.")


@router.post("/upload/{name}", tags=["Parser Management"], response_model=Message)
def upload_file(name: str, inc_file: IncomingFile) -> Message:
    """
    Uploads bytes data from data into s3 bucket as filename name. File is assumed to be base64 encoded.
    """

    data = b64decode(inc_file.file)
    file = BytesIO(data)
    s3_cli = client('s3')
    
    try:
        s3_cli.upload_fileobj(file, "shoppingstories", f"ParseMe/{name}")
    except Exception:
        print("Could not upload file to s3.")
        print(traceback.format_exc())
        return Message(message="Error uploading file to s3.")
    
    file.close()

    return Message(message="Successfully uploaded file to s3.")

@router.get("/get_parser_progress", tags=["Parser Management"], response_model=ParserProgress)
def get_status():
    """
    Gets the progress of the parser, 1 is finished, 0 is starting.
    """
    progress, filenames = check_progress()
    return ParserProgress(progress=progress, filenames=filenames)