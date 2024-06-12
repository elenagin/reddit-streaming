'''
Team Members:
- Carlos Varela
- Elena Ginebra
- Matilde Bernocci
- Rafael Braga
'''

import time
import json
import sqlite3
from pyspark.sql.types import IntegerType
from pyspark.sql import SparkSession, functions as F
import re
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType
import warnings

warnings.filterwarnings("ignore")

# Create a Spark session
spark = SparkSession.builder.appName('reddit-streaming').getOrCreate()

# Function to save metrics to SQLite database
def save_to_sqlite(df, db_name, table_name):
    # Establish a connection to the SQLite database
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    # Create the table if it does not exist
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        mentioned_users INTEGER,
        referenced_posts INTEGER,
        urls_shared INTEGER,
        nvidia_count INTEGER,
        aapl_count INTEGER,
        elon_count INTEGER
    )
    """)
    
    # Prepare the data for insertion, converting lists to JSON strings
    data = df.rdd.map(lambda row: (
        row.mentioned_users,
        row.referenced_posts,
        row.urls_shared,
        row.nvidia_count,
        row.aapl_count,
        row.elon_count
    )).collect()

    # Insert the data into the table, replacing duplicates
    cursor.executemany(f"""
    INSERT OR REPLACE INTO {table_name} (mentioned_users, referenced_posts, urls_shared, nvidia_count, aapl_count, elon_count)
    VALUES (?, ?, ?, ?, ?, ?)
    """, data)

    conn.commit()
    conn.close()

# Initialize the last processed timestamp
last_processed_time = '2020-01-01 00:00:00'

def process_data():
    global last_processed_time

    # Read raw data from folder and create a DataFrame
    try:
        raw_data = spark.read.parquet('raw_data.parquet')
    except Exception as e:
        return 

    # Filter data based on last processed timestamp
    new_data = raw_data.filter(F.col("created_date") > last_processed_time)

    if new_data.count() == 0:
        print('Waiting for new data...')
        
        return

    # Update the last processed timestamp
    last_processed_time = new_data.agg(F.max("created_date")).collect()[0][0]
    print('inside loop processed time:', last_processed_time)

    # Define functions to extract mentions, subreddits, and URLs
    def extract_mentions(text):
        return re.findall(r'/u/(\w+)', text)

    def extract_subreddits(text):
        return re.findall(r'/r/(\w+)', text)

    def extract_urls(text):
        return re.findall(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', text)

    # Define functions to count words
    def count_word_occurrences(text, word):
        return len(re.findall(fr'\b{word}\b', text, re.IGNORECASE))

    # Register UDFs
    count_word_occurrences_udf = udf(lambda text, word: count_word_occurrences(text, word), IntegerType())
    extract_mentions_udf = udf(extract_mentions, ArrayType(StringType()))
    extract_subreddits_udf = udf(extract_subreddits, ArrayType(StringType()))
    extract_urls_udf = udf(extract_urls, ArrayType(StringType()))

    # Count instances of metrics and NVIDIA, AAPL, and Elon
    new_data = new_data.withColumn("mentions", extract_mentions_udf(new_data["text"]))
    new_data = new_data.withColumn("subreddits", extract_subreddits_udf(new_data["text"]))
    new_data = new_data.withColumn("urls", extract_urls_udf(new_data["text"]))
    new_data = new_data.withColumn("nvidia_count", count_word_occurrences_udf(new_data["text"], F.lit("NVDA")))
    new_data = new_data.withColumn("aapl_count", count_word_occurrences_udf(new_data["text"], F.lit("AAPL")))
    new_data = new_data.withColumn("elon_count", count_word_occurrences_udf(new_data["text"], F.lit("Elon")))

    # Aggregate the counts
    unique_users_count = new_data.selectExpr("explode(mentions) as user").select("user").distinct().count()
    unique_subreddits_count = new_data.selectExpr("explode(subreddits) as subreddit").select("subreddit").distinct().count()
    unique_urls_count = new_data.selectExpr("explode(urls) as url").select("url").distinct().count()
    nvidia_total_count = new_data.agg(F.sum("nvidia_count")).collect()[0][0]
    aapl_total_count = new_data.agg(F.sum("aapl_count")).collect()[0][0]
    elon_total_count = new_data.agg(F.sum("elon_count")).collect()[0][0]

    metrics_dict = [{
        'mentioned_users': unique_users_count,
        'referenced_posts': unique_subreddits_count,
        'urls_shared': unique_urls_count,
        'nvidia_count': nvidia_total_count,
        'aapl_count': aapl_total_count,
        'elon_count': elon_total_count
    }]

    metrics = spark.createDataFrame(metrics_dict)
    metrics.show()
    save_to_sqlite(metrics, 'reddit_streaming.db', 'metrics')

# Continuously check for new data and process it
while True:
    process_data()
    time.sleep(1)  # Wait before checking for new data
