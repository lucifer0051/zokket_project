##for facebook ads
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



#### for google ads

from google.ads.google_ads.client import GoogleAdsClient
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

def fetch_google_ads_data():
    client = GoogleAdsClient.load_from_storage('google-ads.yaml')
    customer_id = 'your_customer_id'
    query = '''
        SELECT
            campaign.id,
            campaign.name,
            campaign.status
        FROM
            campaign
        LIMIT 10
    '''
    try:
        response = client.service.google_ads.search(customer_id=customer_id, query=query)
        data = [{'campaign_id': row.campaign.id, 'campaign_name': row.campaign.name, 'campaign_status': row.campaign.status} for row in response]
        upload_to_azure_blob(data, 'google_ads_campaigns.json')
    except Exception as ex:
        print(f"Error: {ex}")

def upload_to_azure_blob(data, file_name):
    try:
        blob_service_client = BlobServiceClient(account_url="https://<your_storage_account>.blob.core.windows.net", credential=DefaultAzureCredential())
        blob_client = blob_service_client.get_blob_client(container="your_container", blob=file_name)
        blob_client.upload_blob(json.dumps(data), overwrite=True)
    except Exception as e:
        print(f"Error uploading to Azure Blob Storage: {e}")

fetch_google_ads_data()

