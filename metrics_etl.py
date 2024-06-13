import time
import re
import sqlite3
import logging
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import IntegerType, ArrayType, StringType
from pyspark.sql.functions import udf

# Configure logging
logging.basicConfig(level=logging.INFO)

# Create a Spark session
spark = SparkSession.builder.appName('reddit-streaming').getOrCreate()

# Function to save metrics to the SQLite database
def save_to_sqlite(df, db_name, table_name):
    # Connect to the SQLite database
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # Create the table if it doesn't exist
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        mentioned_users INTEGER,
        referenced_posts INTEGER,
        urls_shared INTEGER,
        nvidia_count INTEGER,
        aapl_count INTEGER,
        elon_count INTEGER,
        post_text TEXT
    )
    """)

    # Prepare the data for insertion
    data = df.rdd.map(lambda row: (
        row.mentioned_users,
        row.referenced_posts,
        row.urls_shared,
        row.nvidia_count,
        row.aapl_count,
        row.elon_count,
        row.post_text
    )).collect()

    # Insert the data into the table
    cursor.executemany(f"""
    INSERT OR REPLACE INTO {table_name} 
    (mentioned_users, referenced_posts, urls_shared, nvidia_count, aapl_count, elon_count, post_text)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """, data)

    conn.commit()
    conn.close()

# Initialize the timestamp for the last processed data
last_processed_time = '2020-01-01 00:00:00'

def process_data():
    global last_processed_time

    # Read raw data from the folder
    try:
        # Read the Parquet files from the 'raw_data.parquet' folder
        raw_data = spark.read.parquet('raw_data.parquet')
        logging.info("Read raw data successfully.")
    except Exception as e:
        logging.error(f"Error reading raw data: {e}")
        return 

    # Filter new data based on the last processed timestamp
    new_data = raw_data.filter(F.col("created_date") > last_processed_time)

    if new_data.count() == 0:
        logging.info("Waiting for new data...")
        return

    # Update the last processed timestamp
    last_processed_time = new_data.agg(F.max("created_date")).collect()[0][0]
    logging.info(f"Updated last processed time: {last_processed_time}")

    # Functions to extract mentions, subreddits, and URLs
    def extract_mentions(text):
        return re.findall(r'/u/(\w+)', text)

    def extract_subreddits(text):
        return re.findall(r'/r/(\w+)', text)

    def extract_urls(text):
        return re.findall(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', text)

    def count_word_occurrences(text, word):
        return len(re.findall(fr'\b{word}\b', text, re.IGNORECASE))

    # Register the UDFs
    count_word_occurrences_udf = udf(lambda text, word: count_word_occurrences(text, word), IntegerType())
    extract_mentions_udf = udf(extract_mentions, ArrayType(StringType()))
    extract_subreddits_udf = udf(extract_subreddits, ArrayType(StringType()))
    extract_urls_udf = udf(extract_urls, ArrayType(StringType()))

    # Process and enrich the data
    new_data = new_data.withColumn("mentions", extract_mentions_udf(new_data["text"]))
    new_data = new_data.withColumn("subreddits", extract_subreddits_udf(new_data["text"]))
    new_data = new_data.withColumn("urls", extract_urls_udf(new_data["text"]))
    new_data = new_data.withColumn("nvidia_count", count_word_occurrences_udf(new_data["text"], F.lit("NVDA")))
    new_data = new_data.withColumn("aapl_count", count_word_occurrences_udf(new_data["text"], F.lit("AAPL")))
    new_data = new_data.withColumn("elon_count", count_word_occurrences_udf(new_data["text"], F.lit("Elon")))

    # Calculate unique counts
    unique_users_count = new_data.selectExpr("explode(mentions) as user").select("user").distinct().count()
    unique_subreddits_count = new_data.selectExpr("explode(subreddits) as subreddit").select("subreddit").distinct().count()
    unique_urls_count = new_data.selectExpr("explode(urls) as url").select("url").distinct().count()
    nvidia_total_count = new_data.agg(F.sum("nvidia_count")).collect()[0][0]
    aapl_total_count = new_data.agg(F.sum("aapl_count")).collect()[0][0]
    elon_total_count = new_data.agg(F.sum("elon_count")).collect()[0][0]

    # Concatenate all post texts
    post_texts = ' '.join(new_data.select("text").rdd.flatMap(lambda x: x).collect())
    logging.info(f"Aggregated post texts: {post_texts[:100]}...")  # Print only the first 100 characters

    # Prepare the data for saving
    metrics_dict = [{
        'mentioned_users': unique_users_count,
        'referenced_posts': unique_subreddits_count,
        'urls_shared': unique_urls_count,
        'nvidia_count': nvidia_total_count,
        'aapl_count': aapl_total_count,
        'elon_count': elon_total_count,
        'post_text': post_texts
    }]

    metrics = spark.createDataFrame(metrics_dict)
    metrics.show()
    save_to_sqlite(metrics, 'reddit_streaming.db', 'metrics')
    logging.info("Data saved to SQLite successfully.")

# Function to check data in the post_text column
def check_post_text(db_name):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    cursor.execute("SELECT post_text FROM metrics")
    results = cursor.fetchall()

    for row in results:
        print(row[0])  # Print the content of post_text

    conn.close()

# Continuous loop for processing data
while True:
    process_data()
    time.sleep(1)  # Wait before checking for new data

# Check the data after updating
check_post_text('reddit_streaming.db')
