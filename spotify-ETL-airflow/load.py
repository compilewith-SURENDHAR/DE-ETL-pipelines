import mysql.connector
from datetime import datetime
from auth import get_token
from extract import get_latest_albums

token = get_token()
print(token)
data = get_latest_albums(token)
print("data feteched")
print(len(data))

# Connect to your MySQL database
conn = mysql.connector.connect(
    host="172.26.128.1",
    user="root",
    password="root",
    database="spotifyetl"
)
print("mysql connection established")
cursor = conn.cursor()

def get_time():
    current_datetime = datetime.now()
    date = current_datetime.date()
    time = current_datetime.time()
    print(date, time)
    
def load_new_releases(data=data):
    # Create table
    create_table_query = """
    CREATE TABLE IF NOT EXISTS new_releases (
        spotify_id VARCHAR(50) PRIMARY KEY,
        album_type VARCHAR(50),
        artists VARCHAR(255),
        available_markets TEXT,
        external_url VARCHAR(255),
        href VARCHAR(255),
        name VARCHAR(255),
        release_date VARCHAR(50),
        release_date_precision VARCHAR(20),
        total_tracks INT,
        uri VARCHAR(100)
    )
    """
    cursor.execute(create_table_query)
    conn.commit()
    print("table created")

    for album in data:
        spotify_id = album["id"]
        album_type = album["album_type"]
        artists = ", ".join([artist["name"] for artist in album["artists"]])
        available_markets = ", ".join(album["available_markets"])
        external_url = album["external_urls"]["spotify"]
        href = album["href"]
        name = album["name"]
        release_date = album["release_date"]
        release_date_precision = album["release_date_precision"]
        total_tracks = album["total_tracks"]
        uri = album["uri"]
        
        insert_query = """
        INSERT INTO new_releases
        (spotify_id, album_type, artists, available_markets, external_url, href, name, release_date, release_date_precision, total_tracks, uri)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            album_type=VALUES(album_type),
            artists=VALUES(artists),
            available_markets=VALUES(available_markets),
            external_url=VALUES(external_url),
            href=VALUES(href),
            name=VALUES(name),
            release_date=VALUES(release_date),
            release_date_precision=VALUES(release_date_precision),
            total_tracks=VALUES(total_tracks),
            uri=VALUES(uri)
        """
        cursor.execute(insert_query, (spotify_id, album_type, artists, available_markets, external_url, href, name, release_date, release_date_precision, total_tracks, uri))

    conn.commit()
    cursor.close()
    conn.close()
    print("Albums inserted/updated successfully!")
    get_time()


load_new_releases()
