
from dagster import job, op
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
# new imports
import pandas as pd
from dagster import get_dagster_logger, Output, MetadataValue
import requests,json
from dagster import asset,Definitions
@op
def upload_blob_op(context, container_name, blob_name, file_path):
    
    #url = "http://data-api.eastus.cloudapp.azure.com/api/34d7b37c9b223d3ab3dbdab1635b578e"
    #req = requests.get(url)
    #item = json.loads(req.text)
    #rows =item["rows"]
    #columns = item["columns"]
    #df = pd.DataFrame(rows, columns=columns)
    #df['ID'] = df['ID'].astype('int32')
    # Export DataFrame to CSV
    #csv_file_path = "/tmp/health_emotion.csv"
    #df.to_csv(csv_file_path, index=False)

    account_url = "https://iomanager.blob.core.windows.net/"
    credential = DefaultAzureCredential()
    blob_service_client = BlobServiceClient(account_url, credential)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

    with open(file_path, "rb") as data:
        context.log.info(f"Uploading {file_path} to {blob_name} in {container_name}")
        blob_client.upload_blob(data)

@op
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

@job
def azure_blob_storage_job():
    """
    Dagster job to handle blob operations in Azure Blob Storage.
    """
    # Here you can define the workflow, e.g., first upload a file, then download it
    upload_blob_op()
    download_blob_op()

# To execute the job, you can use Dagster's command line tools or Dagit UI.
# Example command line execution: `dagster job execute -f your_script.py -j azure_blob_storage_job`
