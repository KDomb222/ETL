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

#we need data from last 24h, the timestamp is formerly in unix sec
today = datetime.datetime.now()
yesterday =  today - datetime.timedelta(days=1)
yesterday_unix_timestamp = int(yesterday.timestamp())*1000

#request library
r = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=yesterday_unix_timestamp), headers = headers)

data = r.json()

print(data)

#CHECKPOINT
#we did get some data. For now a little bit disordered.
