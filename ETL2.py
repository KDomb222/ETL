import sqlalchemy
import pandas as pd 
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import sqlite3
from sqlalchemy.sql.sqltypes import DateTime


DATABASE_LOCATION = "sqlite:///my_played_tracks.sqlite"
USER_ID = "kdombrowicki" # Spotify username 
TOKEN = "BQAod3WdvIqczsSpnJqPtlvOPbIwkSsYUOHSKeS308Y_JJmnpzOM9SrVqtaz0L-gSZQDkEvydvpEq713GlZHvXkhhDbU6DLGcZev7-silsPoSDw73yFO_M2m7zSDXodu9Csp22ttvnGIijBGMeX4FkE" # Spotify API token

#required info in headers, according to API instructions
if __name__=="__main__":

    headers = {
        "Accept" : "application/json",
        "Content-Type" : "application/json",
        "Authorization" : "Bearer {token}".format(token=TOKEN)
    }

#we need data from last 24h, the timestamp must be converted to unix milisec
today = datetime.datetime.now()
yesterday =  today - datetime.timedelta(days=1)
yesterday_unix_timestamp = int(yesterday.timestamp())*1000

#request library, lets download all we need from last 24h
r = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=yesterday_unix_timestamp), headers = headers)

data = r.json()

#starting some creation
song_names = []
artist_names = []
played_at_list = []
timestamps = []

# Extracting only the information that we are interested in, using little loop      
for song in data["items"]:
    song_names.append(song["track"]["name"])
    artist_names.append(song["track"]["album"]["artists"][0]["name"])
    played_at_list.append(song["played_at"])
    timestamps.append(song["played_at"][0:10])

#will be using the famous pandas, so lets prepare a dictionary
song_dict = {
    "song_name" : song_names,
    "artist_name": artist_names,
    "played_at" : played_at_list,
    "timestamp" : timestamps
}
    
#almighty pandas in use to create a dataframe
song_df = pd.DataFrame(song_dict, columns = ["song_name", "artist_name", "played_at", "timestamp"])

print(song_df)

#CHECKPOINT
#yay, we manged to finish the EXTRACT part of ETL.