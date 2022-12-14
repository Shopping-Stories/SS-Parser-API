from fastapi import APIRouter
from boto3 import client
from io import BytesIO

def upload_file(name, data: bytes):
    file = BytesIO(data)
    s3_cli = client('s3')
    try:
        s3_cli.upload_fileobj(file, "shoppingstories", f"ParseMe/{name}")
    except Exception:
        return 
    file.close()