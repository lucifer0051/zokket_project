import pandas as pd
import json

def process_data(data):
    campaigns = data['data']
    df = pd.json_normalize(campaigns)
    json_data = df.to_json(orient='records')
    upload_to_azure_blob(json_data, 'facebook_ads_campaigns.json')


###error handling 

def upload_to_azure_blob(data, file_name):
    try:
        blob_service_client = BlobServiceClient(account_url="https://<your_storage_account>.blob.core.windows.net", credential=DefaultAzureCredential())
        blob_client = blob_service_client.get_blob_client(container="your_container", blob=file_name)
        blob_client.upload_blob(data, overwrite=True)
    except Exception as e:
        print(f"Error uploading to Azure Blob Storage: {e}")
