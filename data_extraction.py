import requests
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

def fetch_facebook_ads_data():
    access_token = 'your_access_token'
    ad_account_id = 'act_your_ad_account_id'
    url = f'https://graph.facebook.com/v14.0/{ad_account_id}/campaigns'

    params = {
        'access_token': access_token,
        'fields': 'id,name,status'
    }

    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        upload_to_azure_blob(data, 'facebook_ads_campaigns.json')
    else:
        print(f"Error: {response.status_code}, {response.text}")

def upload_to_azure_blob(data, file_name):
    try:
        blob_service_client = BlobServiceClient(account_url="https://<your_storage_account>.blob.core.windows.net", credential=DefaultAzureCredential())
        blob_client = blob_service_client.get_blob_client(container="your_container", blob=file_name)
        blob_client.upload_blob(data, overwrite=True)
    except Exception as e:
        print(f"Error uploading to Azure Blob Storage: {e}")

fetch_facebook_ads_data()
