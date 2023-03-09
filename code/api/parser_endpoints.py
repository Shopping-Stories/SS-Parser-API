from fastapi import APIRouter, BackgroundTasks
from boto3 import client
from io import BytesIO
from .api_types import *
from .new_parser.parser_manager import start_parse, check_progress
from .new_parser.new_parser import parse_file_and_dump, parse_folder
import traceback
from base64 import b64decode

router = APIRouter()

@router.post("/upload_and_parse_single/", tags=["Parser Management"], response_model=Message)
def upload_file_and_parse(inc_file: IncomingFile, bg_tasks: BackgroundTasks) -> Message:
    """
    Uploads bytes data from data into s3 bucket as filename name. File is assumed to be base64 encoded.
    """

    name = inc_file.name
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

@router.post("/upload_and_parse_multi/", tags=["Parser Management"], response_model=FailedFiles)
def upload_files_and_parse(inc_files: IncomingFiles, bg_tasks: BackgroundTasks) -> FailedFiles:
    """
    Uploads multiple files into s3 and queues a parse. Slightly more efficient for batch uploads. Returns a list of all files for which the upload failed.
    File data is assumed to be base64 encoded.
    """

    s3_cli = client('s3')
    errors = []
    for inc_file in inc_files.files:
        name = inc_file.name
        data = b64decode(inc_file.file)
        file = BytesIO(data)
        
        try:
            s3_cli.upload_fileobj(file, "shoppingstories", f"ParseMe/{name}")
        except Exception:
            print("Could not upload file to s3.")
            print(traceback.format_exc())
            if traceback.format_exc() is not None:
                errors.append(FailedFile(name=name, reason=traceback.format_exc()))
            else:
                errors.append(FailedFile(name=name, reason="Reason Unknown, traceback was null."))
        
        file.close()

    bg_tasks.add_task(start_parse, s3_cli)

    return FailedFiles(files=errors)


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

@router.get("/test_parser", tags=["Parser Management"], response_model=Message)
def test_parsing(bg_tasks: BackgroundTasks) -> Message:
    """
    Tests parser on a hardcoded file for dev purposes.
    """
    folder = "..\\data\\Amelia\\"
    file = "C_1760_001_FINAL_.xlsx"
    task = parse_folder
    bg_tasks.add_task(task, folder)
    return Message(message="Started parser.")

@router.post("/upload/", tags=["Parser Management"], response_model=Message)
def upload_file(inc_file: IncomingFile) -> Message:
    """
    Uploads bytes data from data into s3 bucket as filename name. File is assumed to be base64 encoded.
    """

    name = inc_file.name
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


@router.get("/queue_parse", tags=["Parser Management"], response_model=Message)
def queue_parse(bg_tasks: BackgroundTasks) -> Message:
    """
    Queues a parse of files already in the staging area (i.e. uploaded via upload_file).
    This should not usually need to be called, only in case of files sticking in the staging area for some reason (e.g. error) or in case of using upload_file.
    """

    s3_cli = client('s3')
    bg_tasks.add_task(start_parse, s3_cli)
    return Message(message="Successfully queued a parse.")