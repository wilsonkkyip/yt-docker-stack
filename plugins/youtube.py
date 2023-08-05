from typing import List, Text
import requests
from math import ceil, log10, floor

def list_channels(
    id: Text,
    api_key: Text
):
    url = "https://www.googleapis.com/youtube/v3/channels"
    parts = [
        "brandingSettings", "contentDetails", "contentOwnerDetails", "id", 
        "localizations", "snippet", "statistics", "status", "topicDetails"
    ]
    params = {
        "id": id,
        "part": ",".join(parts), 
    }
    headers={"x-goog-api-key": api_key}
    response = requests.get(url, params=params, headers=headers)

    if not response.ok:
        print(response.json())
        response.raise_for_status()
    
    return response

def download_channels(
    id: Text,
    api_key: Text,
    output_file: Text
):
    response = list_channels(id, api_key)
    with open(output_file, "w") as f:
        f.write(response.text)
