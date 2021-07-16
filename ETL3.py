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

#HERE STARTS TRANSFORM (I know, code isn't in order E->T->L)

#lets start the Transform part
#first, validate if data is fine for us to take into DB

def check_if_valid_data(df: pd.DataFrame) -> bool:
    # Check if dataframe is empty
    if df.empty:
        print("No songs downloaded. Finishing execution")
        return False 

#Now, lets create a constraint. We will choose play time as our primary key, since we can't listen to 2 songs at the same time. So, if we received data with multiple songs with same timestamp
#that means something is wrong there, and we don't want messy data in our DB
    # Primary Key Check
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception("Primary Key check is violated")

#empty data is not good for us, if we find some, let's raise an exception

    # Check for nulls
    if df.isnull().values.any():
        raise Exception("Null values found")

     # Check that all timestamps are of yesterday's date, we are using only last 24h of our data from Spotify
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

    timestamps = df["timestamp"].tolist()
    for timestamp in timestamps:
        if datetime.datetime.strptime(timestamp, '%Y-%m-%d') != yesterday:
            raise Exception("At least one of the returned songs does not have a yesterday's timestamp")

    return True


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

#The Transform part starts right after token part, go up there ^

 # Validate
if check_if_valid_data(song_df):
    print("Data valid, proceed to Load stage")

#Hurray we did it the Transform part! we might have gotten some exceptions in result, but thats fine, it means we won't get data we don't need
