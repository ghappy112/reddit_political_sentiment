# ETL and Feature Engineering

# import packages
import praw
import pandas as pd
from datetime import datetime
from textblob import TextBlob
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import sqlite3
import mysql.connector

# connect to reddit api
reddit = praw.Reddit(
    client_id='client_id',
    client_secret='client_secret',
    user_agent="user_agent"
)

# connect to subreddit all (all contains all subreddits combined)
subreddit = reddit.subreddit('all')

# instanstiate vader sentiment analyzer
vader_analyzer = SentimentIntensityAnalyzer()

# reddit api search and sentiment analysis function. returns up to 250 posts that contain the given query in the post, and performs sentiment analysis using Vader and TextBLob
def search(query, candidate):
    
    search_results = subreddit.search(query=query, sort='new', syntax='plain', time_filter='all', limit=250)

    user_ids, timestamps, texts, votess, polarities, subjectivities = [], [], [], [], [], []
    for post in search_results:
        
        # extract reddit post features:
        user_id = post.author.name
        if len(post.selftext) > 1:
            text = f"{post.title}: {post.selftext}"
        else:
            text = post.title
        timestamp = datetime.utcfromtimestamp(post.created_utc)
        votes = post.score
        
        # sentiment analysis:
        polarity = vader_analyzer.polarity_scores(text.lower())['compound']
        blob = TextBlob(text.lower())
        subjectivity = blob.sentiment.subjectivity
        
        user_ids.append(user_id), timestamps.append(timestamp), texts.append(text),votess.append(votes), polarities.append(polarity), subjectivities.append(subjectivity)

    df = pd.DataFrame({"user_id": user_ids, "timestamp": timestamps, "text": texts, "votes": votes, "candidate": candidate, "polarity": polarities, "subjectivity": subjectivities})
    
    return df

# search for all candidates and perform sentiment analysis
candidates = pd.read_excel("candidates_queries.xlsx")
candidates = [tuple(list(r)[::-1]) for r in candidates.to_numpy()]
df = pd.concat([search(*candidate) for candidate in candidates])
df = df.drop_duplicates()
df["timestamp"] = df["timestamp"].astype(str) # convert timestamp column to string so it is compatabile with sql database
df["pk"] = df["user_id"] + "_" + df["timestamp"].astype(str) + "_" + df["candidate"] # create primary key column
df = df.drop_duplicates(["pk"])

# connect to sqlite database
conn = sqlite3.connect("database/reddit_political_sentiment_database.db")
cursor = conn.cursor()

# insert data into database
for row in df.values:
    user_id, timestamp, text, votes, candidate, polarity, subjectivity, pk = row
    data_to_insert = (pk, user_id, timestamp, text, votes, candidate, polarity, subjectivity)
    insert_query = 'INSERT OR IGNORE INTO reddit_posts (pk, user_id, timestamp, text, votes, candidate, polarity, subjectivity) VALUES (?, ?, ?, ?, ?, ?, ?, ?)'
    cursor.execute(insert_query, data_to_insert)
    conn.commit()

# create views
queries = ['DROP VIEW IF EXISTS reddit_posts_view',
            '''
            CREATE VIEW reddit_posts_view AS
            SELECT
                candidate,
                DATE(year || "-" || month || "-" || day) AS date,
                opinion,
                votes,
                votes_opinion,
                weighted_votes_opinion
            FROM
                (SELECT
                    candidate,
                    day,
                    month,
                    year,
                    opinion,
                    SUM(votes) AS votes,
                    SUM(votes_opinion) AS votes_opinion,
                    SUM(weighted_votes_opinion) AS weighted_votes_opinion
                FROM
                    (SELECT
                        candidate,
                        strftime("%d", timestamp) AS day,
                        strftime("%m", timestamp) AS month,
                        strftime("%Y", timestamp) AS year,
                        votes,
                        CASE WHEN polarity > 0 THEN "post approves of candidate" ELSE "post disapproves of candidate" END AS opinion,
                        (CASE WHEN polarity > 0 THEN 1 ELSE -1 END) * votes AS votes_opinion,
                        votes * polarity AS weighted_votes_opinion
                    FROM
                        reddit_posts
                    WHERE
                        polarity != 0
                    AND
                        subjectivity < 0.5
                    ) AS t1
                GROUP BY
                    candidate,
                    day,
                    month,
                    year,
                    opinion
                ) AS t2''',
            'DROP VIEW IF EXISTS reddit_posts_view2',
            '''
            CREATE VIEW reddit_posts_view2 AS
            SELECT
                reddit_posts_view.candidate,
                reddit_posts_view.date,
                opinion, votes,
                votes_opinion,
                ROUND(weighted_votes_opinion, 2) AS weighted_votes_opinion,
                total_votes,
                ROUND(total_weighted_votes, 2) AS total_weighted_votes,
                ROUND(CAST(votes_opinion AS FLOAT) / CAST(total_votes AS FLOAT), 3) AS votes_opinion_pct,
                ROUND(weighted_votes_opinion / total_weighted_votes, 3) AS weighted_votes_opinion_pct
            FROM
                reddit_posts_view
            INNER JOIN
                (SELECT
                    candidate,
                    date,
                    SUM(votes) AS total_votes,
                    SUM(ABS(weighted_votes_opinion)) AS total_weighted_votes
                FROM
                    reddit_posts_view
                GROUP BY
                    candidate,
                    date
                ) AS t
            ON
                reddit_posts_view.candidate = t.candidate
            AND
                reddit_posts_view.date = t.date''',
            'DROP VIEW IF EXISTS reddit_posts_view3',
            '''
            CREATE VIEW reddit_posts_view3 AS
            SELECT
                reddit_posts_view2.candidate,
                reddit_posts_view2.date,
                opinion,
                votes,
                votes_opinion,
                weighted_votes_opinion,
                total_votes,
                total_weighted_votes,
                t.votes_opinion_pct,
                t.weighted_votes_opinion_pct
            FROM
                reddit_posts_view2
            INNER JOIN
                (SELECT
                    candidate,
                    date,
                    MAX(CASE WHEN votes_opinion_pct < 0 THEN 0.00 ELSE votes_opinion_pct END) AS votes_opinion_pct,
                    MAX(CASE WHEN weighted_votes_opinion_pct < 0 THEN 0.00 ELSE weighted_votes_opinion_pct END) AS weighted_votes_opinion_pct
                FROM
                    reddit_posts_view2
                GROUP BY
                    candidate,
                    date
                ) AS t
            ON
                reddit_posts_view2.candidate = t.candidate
            AND
                reddit_posts_view2.date = t.date
            WHERE
                votes > 0''',
            'DROP VIEW IF EXISTS reddit_posts_view_weekly',
            '''
            CREATE VIEW reddit_posts_view_weekly AS
            SELECT
                candidate,
                start_of_week,
                ROUND(CAST(SUM(votes) AS FLOAT) / CAST(SUM(total_votes) AS FLOAT), 3) AS votes_opinion_pct,
                SUM(total_votes) AS total_votes
            FROM
                (SELECT
                    candidate,
                    date,
                    DATE(date, '-' || CAST(strftime('%w', date) AS TEXT) || ' day') AS start_of_week,
                    MAX(CASE WHEN votes_opinion < 0 THEN 0 ELSE votes END) as votes,
                    total_votes
                FROM
                    reddit_posts_view3
                GROUP BY
                    candidate,
                    date,
                    start_of_week,
                    total_votes
                ) AS t
            GROUP BY
                candidate,
                start_of_week''',
            'DROP VIEW IF EXISTS reddit_posts_view_all',
            'DROP VIEW IF EXISTS reddit_posts_view_monthly',
            '''
            CREATE VIEW reddit_posts_view_monthly AS
            SELECT
                candidate,
                month_year,
                ROUND(CAST(SUM(votes) AS FLOAT) / CAST(SUM(total_votes) AS FLOAT), 3) AS votes_opinion_pct,
                SUM(total_votes) AS total_votes
            FROM
                (SELECT
                    candidate,
                    date,
                    DATE(strftime('%Y', date) || '-' || strftime('%m', date) || '-01') AS month_year,
                    MAX(CASE WHEN votes_opinion < 0 THEN 0 ELSE votes END) as votes,
                    total_votes
                FROM
                    reddit_posts_view3
                GROUP BY
                    candidate,
                    date,
                    month_year,
                    total_votes
                ) AS t
            GROUP BY
                candidate,
                month_year''',
            '''
            CREATE VIEW reddit_posts_view_all AS
            SELECT
                candidate,
                date,
                opinion,
                votes,
                votes_opinion,
                weighted_votes_opinion,
                total_votes,
                total_weighted_votes,
                votes_opinion_pct,
                weighted_votes_opinion_pct,
                'Daily' AS time_frame
            FROM
                reddit_posts_view3
            UNION ALL
            SELECT
                candidate,
                start_of_week AS date,
                NULL AS opinion,
                total_votes AS votes,
                NULL AS votes_opinion,
                NULL AS weighted_votes_opinion,
                NULL total_votes,
                NULL AS total_weighted_votes,
                votes_opinion_pct,
                NULL weighted_votes_opinion_pct,
                'Weekly' AS time_frame
            FROM
                reddit_posts_view_weekly
            UNION ALL
            SELECT
                candidate,
                month_year AS date,
                NULL AS opinion,
                total_votes AS votes,
                NULL AS votes_opinion,
                NULL AS weighted_votes_opinion,
                NULL total_votes,
                NULL AS total_weighted_votes,
                votes_opinion_pct,
                NULL weighted_votes_opinion_pct,
                'Monthly' AS time_frame
            FROM
                reddit_posts_view_monthly''']
for query in queries:
    cursor.execute(query)
    conn.commit()

# select data from view all and clean data
query = 'SELECT * FROM reddit_posts_view_all'
df = pd.read_sql_query(query, conn)
df["opinion"] = df["opinion"].astype(str)
df["pk"] = df["candidate"] + "_" + df["date"].astype(str) + "_" + df["opinion"] + "_" + df["time_frame"]
df = df.fillna(0.0)

# close connection to database
conn.close()

# connect to mysql database
conn = mysql.connector.connect(
    host="host",
    user="user",
    password="password",
    database="database"
)
cursor = conn.cursor()

# create table
delete_table_query = "DROP TABLE IF EXISTS political_candidate_reddit_posts_summary"
create_table_query = '''
    CREATE TABLE political_candidate_reddit_posts_summary (
        pk VARCHAR(255),
        candidate TEXT,
        date DATETIME,
        opinion TEXT,
        votes INT,
        votes_opinion INT,
        weighted_votes_opinion FLOAT,
        total_votes INT,
        total_weighted_votes FLOAT,
        votes_opinion_pct FLOAT,
        weighted_votes_opinion_pct FLOAT,
        time_frame TEXT,
        PRIMARY KEY (pk)
    )'''
for query in [delete_table_query, create_table_query]:
    cursor.execute(query)
    conn.commit()

# insert data into database
for row in df.values:
    candidate, date, opinion, votes, votes_opinion, weighted_votes_opinion, total_votes, total_weighted_votes, votes_opinion_pct, weighted_votes_opinion_pct, time_frame, pk = row
    data_to_insert = (pk, candidate, date, opinion, votes, votes_opinion, weighted_votes_opinion, total_votes, total_weighted_votes, votes_opinion_pct, weighted_votes_opinion_pct, time_frame)
    insert_query = 'INSERT INTO political_candidate_reddit_posts_summary (pk, candidate, date, opinion, votes, votes_opinion, weighted_votes_opinion, total_votes, total_weighted_votes, votes_opinion_pct, weighted_votes_opinion_pct, time_frame) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
    cursor.execute(insert_query, data_to_insert)
    conn.commit()

# close connection to database
conn.close()



# record that ETL job ran successfully without error
with open("last_successful_etl_run.txt", 'w') as file:
    file.write(str(datetime.now()))
