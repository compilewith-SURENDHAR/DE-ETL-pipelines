from requests import get 
from auth import get_auth_header

def get_latest_albums(token):
    url = "https://api.spotify.com/v1/browse/new-releases"
    headers = get_auth_header(token)
    params = {
    "limit": 50,   
    "offset": 50  
    }
    response = get(url, headers=headers, params=params)
    if response.status_code == 200:
        data = response.json()
        albums = data["albums"]["items"]
        return albums
    else:
        print("Error:", response.status_code, response.text)

    