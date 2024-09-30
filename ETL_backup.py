# this program can be ran to gather new or missing data as a backup for ETL_and_FeatEng.py


# user input dates
start_datetime = '2024-06-01'  # Enter start date
end_datetime = '2024-09-29'  # Enter end date

# user input candidate
q = "walz" # Enter search query
candidate = "Tim Walz" # Enter candidate name


import requests
import pandas as pd
from datetime import datetime
import time

from textblob import TextBlob
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

import sqlite3


def query(q, after_timestamp, before_timestamp, base_url='https://api.pullpush.io/reddit/search/submission/'):

    # Define your query parameters
    params = {
        'q': q,  # Search term
        #'subreddit': 'learnpython',  # Subreddit to search in
        'size': 100,  # Number of results to return
        'before': before_timestamp,  # Only search for comments before the specified date and time
        'after': after_timestamp,
        'sort_type': 'score', # Sort by score (use 'hot' to sort by hotness)
        'sort': 'desc'
    }

    # Make the request to the API
    response = requests.get(base_url, params=params)

    # parse and wrangle the data
    data = response.json()['data']
    df = pd.DataFrame(data)
    df = df[["author", "created_utc", "score", "title", "selftext"]]
    df = df[df["score"] > 1]
    df['created_utc'] = pd.to_datetime(df['created_utc'], unit='s')
    #df = df.sort_values("score", ascending=False)
    
    return df


def get_date_str(timestamp):
    
    # Convert the timestamp to a datetime object
    date_time_obj = datetime.fromtimestamp(timestamp)

    # Format the datetime object as a string
    date_string = date_time_obj.strftime('%Y-%m-%d')  # Adjust the format as needed

    return date_string  # Output: '2024-06-17' (for the given timestamp)


start_time = time.time()


start_timestamp = int(datetime.strptime(start_datetime, '%Y-%m-%d').timestamp())
end_timestamp = int(datetime.strptime(end_datetime, '%Y-%m-%d').timestamp()) + 86400
#counter = int(start_datetime[-2:].replace("-", ""))
counter = 1

after_timestamp = start_timestamp
before_timestamp = after_timestamp + 86400

df = [query(q, after_timestamp, before_timestamp)]

elapsed_time = time.time() - start_time
print(counter, get_date_str(after_timestamp), get_date_str(before_timestamp), f"{elapsed_time} seconds")

while before_timestamp != end_timestamp:
    after_timestamp += 86400
    before_timestamp = after_timestamp + 86400
    try:
        df.append(query(q, after_timestamp, before_timestamp))
    except:
        pass
    counter += 1
    elapsed_time = time.time() - start_time
    print(counter, get_date_str(after_timestamp), get_date_str(before_timestamp), f"{elapsed_time} seconds")
    
df = pd.concat(df)
df = df.drop_duplicates()
#df = df.sort_values("score", ascending=False)
df = df.sort_values(["score", "created_utc"], ascending=[False, True])
df = df.reset_index()
del df["index"]

elapsed_time = time.time() - start_time
print(f"{elapsed_time} seconds")


# instanstiate vader sentiment analyzer
vader_analyzer = SentimentIntensityAnalyzer()


df["selftext"] = df["selftext"].astype(str).str.replace("\[removed]", "").str.replace("\[deleted]", "")


texts = []
polarities = []
subjectivities = []

for title, selftext in zip(df["title"].values, df["selftext"].values):
    if len(selftext) > 0:
        text = f"{title}: {selftext}"
    else:
        text = title
    texts.append(text)
    
    # sentiment analysis:
    polarity = vader_analyzer.polarity_scores(text.lower())['compound']
    blob = TextBlob(text.lower())
    subjectivity = blob.sentiment.subjectivity
    polarities.append(polarity)
    subjectivities.append(subjectivity)
    
df["text"] = texts
df["polarity"] = polarities
df["subjectivity"] = subjectivities


df["created_utc"] = df["created_utc"].astype(str)
df["pk"] = df["author"] + "_" + df["created_utc"] + "_" + candidate
del df["title"], df["selftext"]
df["candidate"] = candidate
df = df[["author", "created_utc", "text", "score", "candidate", "polarity", "subjectivity", "pk"]]


conn = sqlite3.connect("database/reddit_political_sentiment_database.db")
cursor = conn.cursor()


from tqdm import tqdm

for row in tqdm(df.values):
    user_id, timestamp, text, votes, candidate, polarity, subjectivity, pk = row
    data_to_insert = (pk, user_id, timestamp, text, votes, candidate, polarity, subjectivity)
    insert_query = 'INSERT OR IGNORE INTO reddit_posts (pk, user_id, timestamp, text, votes, candidate, polarity, subjectivity) VALUES (?, ?, ?, ?, ?, ?, ?, ?)'
    cursor.execute(insert_query, data_to_insert)
    conn.commit()
