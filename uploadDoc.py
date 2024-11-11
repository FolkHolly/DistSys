import azure.functions as func
import logging
import base64
import io
import os
import uuid
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, ContainerClient, BlobBlock, BlobClient, StandardBlobTier

uploadDoc = func.Blueprint()

account_url = "https://vmhollygroupa19c.blob.core.windows.net"
credential = DefaultAzureCredential()
blob_service_client = BlobServiceClient(account_url, credential=credential)


@uploadDoc.route(route="upload_doc_http_trigger", auth_level=func.AuthLevel.ANONYMOUS)
def upload_doc_http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    try:
            req_body = req.get_body()
    except ValueError:
        logging.warning('Could not find document, returning with error.')
        return func.HttpResponse(
             "Please provide a document in the request body",
             status_code=400
        )
    else:
        doc = req_body
        logging.info(type(doc))
        logging.info('Found document, converting to base64.')
        try:
            b64_doc = base64.b64encode(doc)
        except:
            logging.warning('Could not convert document to base64, returning with error.')
            return func.HttpResponse(
                    "Error in processing document",
                    status_code=400
            )
    try:
        logging.info('Uploading document to blob storage.')
        blob_client = blob_service_client.get_blob_client(container='receipts', blob="sample-blob.txt")
        blob_client.upload_blob(b64_doc, blob_type="BlockBlob", length=len(b64_doc), overwrite=False)
    except:
            logging.warning('Could not upload document to blob storage.')
            return func.HttpResponse(
                    "Error in uploading document",
                    status_code=400
            )
    return func.HttpResponse(
            f"Your receipt has been uploaded successfully.",
            status_code=200)
