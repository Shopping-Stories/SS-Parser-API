from fastapi import APIRouter
from boto3 import client
from io import BytesIO
from .api_types import Message, IncomingFile
import traceback
from base64 import b64decode

router = APIRouter()

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
