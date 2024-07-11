import pandas as pd
import json

def process_data(data):
    campaigns = data['data']
    df = pd.json_normalize(campaigns)
    json_data = df.to_json(orient='records')
    upload_to_azure_blob(json_data, 'facebook_ads_campaigns.json')
