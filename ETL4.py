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
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1) #HERE, WE CAN CHOOSE FROM HOW MANY DAYS WE EXTRACT DATA
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

#HERE, IF YOU WANT TO CHANGE THE STANDARD 1 DAY DATA, REMEMBER TO COMMENT THE WHOLE TIMESTAMP SECTION, SO IT WON'T GIVE ERRORS
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

#Now, let's start with Load part

#we need a database itself, so we build engine. We need to connect the database so we will make a conn, and a pointer to have access to specific data in our DB

engine = sqlalchemy.create_engine(DATABASE_LOCATION)
conn = sqlite3.connect('my_played_tracks.sqlite')
cursor = conn.cursor()

#ok, now lets use some SQL to create table in our DB
sql_query = """
    CREATE TABLE IF NOT EXISTS my_played_tracks(
        song_name VARCHAR(200),
        artist_name VARCHAR(200),
        played_at VARCHAR(200),
        timestamp VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
    )
    """

#here our magic cursor will execute our querry
cursor.execute(sql_query)
print("Opened database successfully")

#we will use pandas to insert our data directly to DB. If table exists, we want to add/append the data obviously
try:
    song_df.to_sql("my_played_tracks", engine, index=False, if_exists='append')
except:
    print("Data already exists in the database")

    conn.close()
    print("Close database successfully")

#Now let's celebrate. We completed our ETL process :)