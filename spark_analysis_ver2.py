import time
from pyspark.sql import SparkSession, functions as F
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, ArrayType

# Create a Spark session:
spark = SparkSession \
    .builder \
    .appName('reddit-streaming') \
    .getOrCreate()

# Define a UDF to convert features to an array of (index, value) pairs:
def to_array(v):
    return [(i, float(v[i])) for i in range(len(v.toArray()))]

to_array_udf = F.udf(to_array, ArrayType(StructType([
    StructField("index", IntegerType(), False),
    StructField("value", DoubleType(), False)
])))

# Function to process data and update metrics
def process_data():
    global last_processed_time

    # Read raw data from folder and create a df:
    raw_data = spark.read.parquet('raw_data.parquet')

    # Filter data that is not yet processed
    new_data = raw_data.filter(F.col("created_date") > last_processed_time)

    if new_data.count() == 0:
        print('Waiting for new data...')
        return

    # Update the last processed timestamp
    last_processed_time = new_data.agg(F.max("created_date")).collect()[0][0]

    # Extract a specific group matched by a Java regex, from the specified string column for each metric:
    processed = new_data.withColumn('number_users_referenced', F.regexp_extract('text', r"(/u/\w+)", 0))
    processed = processed.withColumn('number_posts_referenced', F.regexp_extract('text', r"(/r/\w+)", 0))
    processed = processed.withColumn("external_urls_referenced", F.regexp_extract("text", r"(http[s]?://\S+)", 0))

    # Count occurrences in 60-second windows, sliding every 5 seconds:
    windowed_counts = processed.groupBy(
        F.window("created_date", "60 seconds", "5 seconds")
    ).agg(
        F.count("number_users_referenced").alias("user_ref_count"),
        F.count("number_posts_referenced").alias("post_ref_count"),
        F.count("external_urls_referenced").alias("url_count")
    )

    print('Obtaining metrics...')

    # Tokenize the text for TF-IDF:
    tokenizer = Tokenizer(inputCol="text", outputCol="words")
    words_data = tokenizer.transform(processed)

    # Apply TF:
    hashing_tf = HashingTF(inputCol="words", outputCol="raw_features", numFeatures=20000)
    featurized_data = hashing_tf.transform(words_data)

    # Compute the IDF:
    idf = IDF(inputCol="raw_features", outputCol="features")
    idf_model = idf.fit(featurized_data)
    rescaled_data = idf_model.transform(featurized_data)

    # Add window column to rescaled_data:
    rescaled_data = rescaled_data.withColumn("window", F.window("created_date", "60 seconds", "5 seconds"))

    # Convert TF-IDF features to an array of (index, value) pairs
    rescaled_data = rescaled_data.withColumn("features_array", to_array_udf(F.col("features")))

    # Explode the features_array into individual rows:
    exploded_data = rescaled_data.select("window", "words", F.explode("features_array").alias("feature"))

    # Define a window specification:
    window_spec = Window.partitionBy("window").orderBy(F.desc("feature.value"))

    # Add row numbers to each word within the window, ordered by TF-IDF score:
    ranked_data = exploded_data.withColumn("rank", F.row_number().over(window_spec))

    # Filter out the top 10 words for each window:
    top_words = ranked_data.filter(ranked_data["rank"] <= 10)

    # Aggregate the top words back into a list for each window:
    top_words_agg = top_words.groupBy("window").agg(F.collect_list("words").alias("top_words"))

    # Join the counts and top words data into a single metrics DataFrame:
    metrics = windowed_counts.join(top_words_agg, "window")

    # Show the metrics DataFrame:
    metrics.show()

    # Saving metrics to disk:
    metrics.write.mode("append").parquet("metrics_data.parquet")

# Initialize the last processed timestamp
last_processed_time = spark.read.parquet('raw_data.parquet').agg(F.max("created_date")).collect()[0][0]
print('Last post at: ',last_processed_time)
# Continuously check for new data and process it
while True:
    process_data()
    time.sleep(10)  # Wait before checking for new data
