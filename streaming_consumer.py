'''
Team Members:
- Carlos Varela
- Elena Ginebra
- Matilde Bernocci
- Rafael Braga
'''

import socket
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import time
import warnings

warnings.filterwarnings("ignore")

# Initialize a Spark session:
spark = SparkSession.builder.appName("reddit-streaming").getOrCreate()

# Define the schema for the incoming data:
schema = StructType([
    StructField("title", StringType(), True),
    StructField("created_utc", DoubleType(), True),
    StructField("text", StringType(), True)
])

def save_to_disk(df, path="raw_data.parquet"):
    df.write.mode("append").parquet(path)

# Define the socket client
def socket_client():
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect(('localhost', 9999))
    
    buffer = ""
    raw = spark.createDataFrame([], schema)  # Initialize an empty DataFrame
    batch_size = 1  # Number of records to collect before saving
    records = []

    try:
        while True:
            data = client_socket.recv(1024).decode('utf-8')
            if not data:
                break
            
            buffer += data
            while '\n' in buffer:
                line, buffer = buffer.split('\n', 1)
                if line.strip():
                    post_data = json.loads(line)
                    records.append(post_data)
                    
                    if len(records) >= batch_size:
                        new_rows = spark.createDataFrame(records, schema=schema)
                        records = []  # Reset the list after creating DataFrame

                        # Convert 'created_utc' to long and human-readable date:
                        new_rows = new_rows.withColumn("created_utc", col("created_utc").cast("long"))
                        new_rows = new_rows.withColumn("created_date", from_unixtime(col("created_utc")))
                        
                        # Save the DataFrame to disk:
                        save_to_disk(new_rows)

            time.sleep(1)  # Adjust sleep time if necessary to avoid spamming
    except (ConnectionResetError, BrokenPipeError):
        print("Connection has been closed.")
    finally:
        if records:  # Save any remaining records on exit
            new_rows = spark.createDataFrame(records, schema=schema)
            new_rows = new_rows.withColumn("created_utc", col("created_utc").cast("long"))
            new_rows = new_rows.withColumn("created_date", from_unixtime(col("created_utc")))
            save_to_disk(new_rows)
        client_socket.close()

# Run the socket client:
socket_client()
