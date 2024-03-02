import sqlite3

# connect to or create a databse (if it doesn't exist)
conn = sqlite3.connect("database/reddit_political_sentiment_database.db")

cursor = conn.cursor()

create_table_query = '''
    CREATE TABLE IF NOT EXISTS reddit_posts(
        pk TEXT PRIMARY KEY,
        user_id TEXT,
        timestamp DATETIME,
        text TEXT,
        votes INT,
        candidate TEXT,
        polarity FLOAT,
        subjectivity FLOAT
    )
'''

cursor.execute(create_table_query)

conn.commit()

conn.close()
