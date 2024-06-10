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

    while True:
        data = client_socket.recv(1024).decode('utf-8')
        if not data:
            break
        
        buffer += data
        while '\n' in buffer:
            line, buffer = buffer.split('\n', 1)
            if line.strip():
                post_data = json.loads(line)
                
                # Append the new post data to the DataFrame:
                new_row = spark.createDataFrame([post_data], schema=schema)
                raw = raw.union(new_row)
                
                # Convert 'created_utc' to long and human-readable date:
                raw = raw.withColumn("created_utc", col("created_utc").cast("long"))
                raw = raw.withColumn("created_date", from_unixtime(col("created_utc")))
                
                # Save the DataFrame to disk:
                save_to_disk(raw)

                # Show the DataFrame (optional):
                #raw.show()
                
                # Reset raw DataFrame to avoid re-processing same data:
                raw = spark.createDataFrame([], schema)

        time.sleep(10)  # Wait before checking for new data...

    client_socket.close()

# Run the socket client:
socket_client()
