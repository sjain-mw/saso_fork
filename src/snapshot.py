from config import STREAMS_TO_USE


import requests
from requests.auth import HTTPDigestAuth
from PIL import Image
from io import BytesIO

def fetch_snapshot(camera_id, file_path):
    DEFAULT_HEADERS = {
    'Content-Type': "application/xml; charset='UTF-8'",
    'Accept': "*/*"
    }
    camera_ip = STREAMS_TO_USE[camera_id].split('@')[-1].split(':')[0]
    snapshot_url = f'http://{camera_ip}/ISAPI/Streaming/channels/101/picture'
    response = requests.get(snapshot_url, auth=HTTPDigestAuth('admin', 'Q12345678q'), headers=DEFAULT_HEADERS)

    if response.status_code == 200:
        # Convert the response content (image bytes) to an image
        image = Image.open(BytesIO(response.content))
        image.save(file_path)
    else:
        print(f"Failed to fetch image, status code: {response.status_code}")