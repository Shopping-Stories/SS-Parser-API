from fastapi import APIRouter, BackgroundTasks
from boto3 import client
from io import BytesIO
from .api_types import *
import traceback
from base64 import b64decode
from json import loads
from traceback import format_exc

router = APIRouter()

def do_parse():
    ecs = client("ecs")
    cluster_arn = "arn:aws:ecs:us-east-1:921328813402:cluster/shopParser"
    currTasks = ecs.list_tasks(cluster=cluster_arn)
    if len(currTasks["taskArns"]) > 0: 
        # If no task is currently running, start the parser, otherwise do nothing.
        ecs.run_task(taskDefinition="arn:aws:ecs:us-east-1:921328813402:task-definition/shopParser:3", launchType="FARGATE", cluster=cluster_arn, networkConfiguration={"awsvpcConfiguration": {"subnets": ["subnet-ef238dce", "subnet-8acc6fec", "subnet-38c7b475", "subnet-db46ea84"]}})
        return True
    else:
        return False
    

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

    bg_tasks.add_task(do_parse)

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

    bg_tasks.add_task(do_parse)

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

    bg_tasks.add_task(do_parse)

    return Message(message="Successfully uploaded file to s3.")

# Endpoints that can be used to test the parser locally, do not push to github while these are uncommented. Under if true so you can collapse this in editor.
if True:
    # @router.get("/test_parser", tags=["Parser Management"], response_model=Message)
    # def test_parsing(bg_tasks: BackgroundTasks) -> Message:
    #     """
    #     Tests parser on a hardcoded file for dev purposes.
    #     """
    #     folder = "..\\data\\Amelia\\"
    #     file = "C_1760_012_FINAL_.xls"
    #     task = parse_file_and_dump
    #     bg_tasks.add_task(task, folder, file)
    #     return Message(message="Started parser.")


    # @router.get("/parse_all", tags=["Parser Management"], response_model=Message)
    # def test_parsing(bg_tasks: BackgroundTasks) -> Message:
    #     """
    #     Parses all data in a hardcoded folder.
    #     """
    #     folders = ["..\\data\\Amelia\\", "..\\data\\Mahlon\\"]
    #     task = parse_file_and_dump
        
    #     filenames = []

    #     for folder in folders:
    #         files = os.listdir(folder)
    #         for file in files:
    #             if file.endswith(".xls") or file.endswith(".xlsx"):
    #                 filenames.append(file)
    #                 bg_tasks.add_task(task, folder, file)
        
    #     return Message(message=f"Parsing: {', '.join(filenames)}")

    # @router.get("/reparse_exceptions", tags=["Parser Management"], response_model=Message)
    # def parse_exceptions(bg_tasks: BackgroundTasks) -> Message:
    #     """
    #     Runs parser on any files resulting in exceptions from a previous parse.
    #     """

    #     folders = ["..\\data\\Amelia\\", "..\\data\\Mahlon\\"]
    #     task = parse_file_and_dump
        
    #     filenames = []

    #     for folder in folders:
    #         files = os.listdir(folder)
    #         for file in files:
    #             if file.endswith(".exception"):
    #                 filenames.append(file.removesuffix(".exception"))
    #                 bg_tasks.add_task(task, folder, file.removesuffix(".exception"))
        
    #     return Message(message=f"Parsing: {', '.join(filenames)}")
    pass


@router.post("/upload/", tags=["Parser Management"], response_model=Message)
def upload_file(inc_file: IncomingFile) -> Message:
    """
    Uploads bytes data from data into s3 bucket for parsing as filename name. File is assumed to be base64 encoded.
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

# Note: consider caching this result for like 5 minutes to reduce server load and s3 load
@router.get("/get_parser_progress", tags=["Parser Management"], response_model=ParserProgress)
def get_status():
    """
    Gets the progress of the parser, 1 is finished, 0 is starting.
    """
    s3_cli = client("s3")
    toOut = None
    try:
        out = s3_cli.list_objects_v2(Bucket="shoppingstories", Prefix="ParserProgress")
        filenames = [x["Key"] for x in out["Contents"] if x["Key"] != "ParserProgress/"]
        if "ParserProgress/progress.json" in filenames:
            file = BytesIO()
            s3_cli.download_fileobj("shoppingstories", "ParserProgress/progress.json", file)
            file.seek(0)
            progress = file.read()
            progress = progress.decode("UTF-8")
            progress = loads(progress)
            toOut = ParserProgress(progress=float(progress["progress"]), filenames=["no_errors.txt"])
    except: 
        toOut = ParserProgress(progress=1, filenames=[format_exc()])
    
    return toOut


@router.get("/queue_parse", tags=["Parser Management"], response_model=Message)
def queue_parse(bg_tasks: BackgroundTasks) -> Message:
    """
    Queues a parse of files already in the staging area (i.e. uploaded via upload_file).
    This should not usually need to be called, only in case of files sticking in the staging area for some reason (e.g. error) or in case of using upload_file.
    """
    
    bg_tasks.add_task(do_parse)
    return Message(message="Successfully queued a parse.")
