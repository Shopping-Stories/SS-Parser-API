from fastapi import APIRouter, Response, BackgroundTasks
from .api_types import *
import traceback
from base64 import b64decode, b64encode
from boto3 import client
from io import BytesIO
from .ssParser.items import item_upload

router = APIRouter()

@router.get("/test_upload_items", response_model=Message)
def test_up_items(bg_tasks: BackgroundTasks):
    a = None
    with open("api/ssParser/C_1760_Item_Master_List_Categories.xlsx", 'rb') as file:
        print("Yes")
        a = file.read()
    # print(a)
    toUp = IncomingFile(file=b64encode(a), name="itemIndex.xlsx")
    return upload_document(inc_file=toUp, bg_tasks=bg_tasks)
    

@router.post("/upload_document", tags=["Documentation"], response_model=Message)
def upload_document(inc_file: IncomingFile, bg_tasks: BackgroundTasks) -> Message:
    """
    uploads a document to the documentation folder
    """

    name = inc_file.name
    data = b64decode(inc_file.file)
    file = BytesIO(data)
    s3_cli = client('s3')

    try:
        if inc_file.name == "itemIndex.xlsx":
            bg_tasks.add_task(item_upload, inc_file)
            s3_cli.upload_fileobj(file, "shoppingstories", f"Documentation/{name}")
            return Message(message="Successfully uploaded file to s3.")
        else:
            s3_cli.upload_fileobj(file, "shoppingstories", f"Documentation/{name}")
    except Exception:
        print("Could not upload file to s3.")
        print(traceback.format_exc())
        return Message(message="Error uploading file to s3.")

    file.close()

    return Message(message="Successfully uploaded file to s3.")

@router.get("/list_all_documents", tags=["Documentation"])
def list_all_documents():
    """
    lists all documents in Documentation
    """

    s3_cli = client('s3')
    filenames = []

    try:
        result = s3_cli.list_objects_v2(Bucket="shoppingstories", Prefix="Documentation/")
    except Exception:
        print("Could not get files from s3.")
        print(traceback.format_exc())
        return Message(message="Error getting files from s3.")

    # for item in result['Contents']:
    #     files = item['Key']
    #     print(files)
    #     filenames.append(files)  # optional if you have more filefolders to got through.
    # return filenames

    return result

@router.get("/get_document/{name}", tags=["Documentation"])
def get_document(name: str):
    """
    gets the documentation to display
    """

    s3_cli = client('s3')

    try:
        res = s3_cli.get_object(Bucket="shoppingstories", Key=f"Documentation/{name}")
    except Exception:
        print("Could not get file from s3.")
        print(traceback.format_exc())
        return Message(message="Error getting file from s3.")
    # print(res)
    # return res['Body'].read().decode("ANSI")
    # Set the Content-Type header to 'application/pdf'
    headers = {'Content-Type': 'application/pdf'}

    # Return the binary data of the PDF file directly from the response body
    return Response(content=res['Body'].read(), headers=headers)

@router.get("/delete_document/{name}", tags=["Documentation"], response_model=Message)
def delete_document(name: str) -> Message:
    """
    deletes document
    """

    s3_cli = client('s3')

    try:
        s3_cli.delete_object(Bucket="shoppingstories", Key=f"Documentation/{name}")
    except Exception:
        print("Could not delete file from s3.")
        print(traceback.format_exc())
        return Message(message="Error deleting file from s3.")

    return Message(message="Successfully deleted file from s3.")
