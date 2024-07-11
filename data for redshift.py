import json
from datetime import datetime

def transform_facebook_ads_data(json_data):
    """
    Transform JSON data from Facebook Ads into a structured format suitable for AWS Redshift.
    
    Args:
    - json_data (str): JSON string containing Facebook Ads data
    
    Returns:
    - list: List of dictionaries, each dictionary representing a row of data suitable for Redshift
    """
    try:
        data = json.loads(json_data)
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        return []
    
    transformed_data = []
    
    for entry in data:
        # Extract relevant fields from JSON
        ad_id = entry.get('ad_id')
        campaign_id = entry.get('campaign_id')
        ad_name = entry.get('ad_name')
        clicks = entry.get('clicks', 0)
        impressions = entry.get('impressions', 0)
        spend = entry.get('spend', 0.0)
        date_start = entry.get('date_start')
        date_stop = entry.get('date_stop')
        
        # Convert date strings to datetime objects
        try:
            start_date = datetime.strptime(date_start, '%Y-%m-%d')
            end_date = datetime.strptime(date_stop, '%Y-%m-%d')
        except ValueError as e:
            print(f"Error parsing dates: {e}")
            continue
        
        # Prepare data in a format suitable for Redshift
        transformed_entry = {
            'ad_id': ad_id,
            'campaign_id': campaign_id,
            'ad_name': ad_name,
            'clicks': int(clicks),
            'impressions': int(impressions),
            'spend': float(spend),
            'start_date': start_date.strftime('%Y-%m-%d'),
            'end_date': end_date.strftime('%Y-%m-%d')
        }
        
        transformed_data.append(transformed_entry)
    
    return transformed_data

# Example usage:
json_data = """
[
    {
        "ad_id": "ad_001",
        "campaign_id": "campaign_001",
        "ad_name": "Ad 1",
        "clicks": 100,
        "impressions": 1000,
        "spend": 50.0,
        "date_start": "2023-01-01",
        "date_stop": "2023-01-31"
    },
    {
        "ad_id": "ad_002",
        "campaign_id": "campaign_001",
        "ad_name": "Ad 2",
        "clicks": 150,
        "impressions": 1200,
        "spend": 75.0,
        "date_start": "2023-01-01",
        "date_stop": "2023-01-31"
    }
]
"""

# Transform JSON data
transformed_data = transform_facebook_ads_data(json_data)

# Print transformed data (for demonstration)
for row in transformed_data:
    print(row)
