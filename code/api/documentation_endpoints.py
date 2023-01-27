from fastapi import APIRouter
from api_types import *
import traceback
from base64 import b64decode
from boto3 import client
from io import BytesIO

router = APIRouter()

@router.post("/document", tags=["Documentation"], response_model=Message)
def upload_document(inc_file: IncomingFile) -> Message:
    """
    uploads a document to the documentation folder
    """

    name = inc_file.name
    data = b64decode(inc_file.file)
    file = BytesIO(data)
    s3_cli = client('s3')

    try:
        s3_cli.upload_fileobj(file, "shoppingstories", f"Documentation/{name}")
    except Exception:
        print("Could not upload file to s3.")
        print(traceback.format_exc())
        return Message(message="Error uploading file to s3.")

    file.close()

    return Message(message="Successfully uploaded file to s3.")

@router.get("/documents", tags=["Documentation"], response_model=Message)
def list_all_documents() -> Message:
    """
    lists all documents in Documentation
    """

    s3_cli = client('s3')

    try:
        res = s3_cli.list_objects_v2(Bucket="shoppingstories", Prefix="/Documentation")
    except Exception:
        print("Could not get files from s3.")
        print(traceback.format_exc())
        return Message(message="Error getting files from s3.")

    return res

@router.get("/document/{name}", tags=["Documentation"], response_model=Message)
def get_documents(name: str) -> Message:
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

    return res

@router.delete("/document/{name}", tags=["Documentation"], response_model=Message)
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
