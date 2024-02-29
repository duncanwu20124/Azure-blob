# new imports
import pandas as pd
from dagster import get_dagster_logger, Output, MetadataValue
import requests,json
from dagster import asset,Definitions
# new asset
from dagster import job, op
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential

def upload_blob_op(context, container_name, blob_name, file_path):
    account_url = "https://iomanager.blob.core.windows.net/"
    credential = "KSWjAfPpsKFuAOGdCB5/BL4WiCFG89UT3kRAxw4/zx9MfQFJauJJ4DKkuudxFrJeRoA7AqmVV5i9+AStaAHCvA=="
    blob_service_client = BlobServiceClient(account_url, credential)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

    with open(file_path, "rb") as data:
        context.log.info(f"Uploading {file_path} to {blob_name} in {container_name}")
        blob_client.upload_blob(data)

def download_blob_op(context, container_name, blob_name, download_file_path):
    """
    Dagster operation to download a blob from Azure Blob Storage.
    """
    account_url = "https://iomanager.blob.core.windows.net/"
    credential = DefaultAzureCredential()
    blob_service_client = BlobServiceClient(account_url, credential)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

    with open(download_file_path, "wb") as download_file:
        context.log.info(f"Downloading {blob_name} from {container_name} to {download_file_path}")
        download_file.write(blob_client.download_blob().readall())

@asset
def health_emotion(context):
    container_name = "iomanager"  # Replace with your actual container name
    blob_name = "iomanager"
    file_path = "/tmp/health_emotion.csv"
    download_file_path = "/home/duncan/New Folder/health_emotion.csv"
    url = "http://data-api.eastus.cloudapp.azure.com/api/34d7b37c9b223d3ab3dbdab1635b578e"
    req = requests.get(url)
    item = json.loads(req.text)
    rows = item["rows"]
    columns = item["columns"]
    df = pd.DataFrame(rows, columns=columns)
    df['ID'] = df['ID'].astype('int32')
    df.to_csv(file_path, index=False)

    # Upload CSV to Azure Blob Storage
    upload_blob_op(context, container_name=container_name, blob_name=blob_name, file_path=file_path)
    download_blob_op(context, container_name=container_name, blob_name=blob_name, download_file_path=download_file_path)
    return Output(
        value=df,
        metadata={
            "num_records": len(df),
            "preview": MetadataValue.md(df.head().to_markdown()),
        },
    )

   
    