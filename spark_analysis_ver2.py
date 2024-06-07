import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, col
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from textblob import TextBlob
from pyspark.sql.types import DoubleType
from pyspark.sql import functions as F

# Get the current working directory
current_dir = os.path.dirname(os.path.abspath(__file__))
save_path = os.path.join(current_dir, "raw_data")

# Path to the SQLite JDBC driver and SLF4J API
jdbc_driver_path = os.path.join(current_dir, "db_drivers/sqlite-jdbc-3.46.0.0.jar")
slf4j_api_path = os.path.join(current_dir, "db_drivers/slf4j-api-1.7.36.jar")

# Initialize Spark session with the JDBC driver and SLF4J API
spark = SparkSession.builder \
    .appName("RedditStreamingAnalysis") \
    .config("spark.jars", f"{jdbc_driver_path},{slf4j_api_path}") \
    .getOrCreate()

print("JARs included in Spark context:")
print(spark.sparkContext._conf.get("spark.jars"))

try:
    # Read the raw data
    print("Reading raw data from:", save_path)
    raw_df = spark.read.parquet(save_path)

    print("Schema of raw data:")
    raw_df.printSchema()

    print("Sample data from raw data:")
    raw_df.show(5)

    # Define a UDF for sentiment analysis
    def get_sentiment(text):
        blob = TextBlob(text)
        return blob.sentiment.polarity

    get_sentiment_udf = F.udf(get_sentiment, DoubleType())

    # Extract references
    print("Extracting references...")
    references_df = raw_df.select(
        explode(split(raw_df.text, " ")).alias("word")
    ).select(
        F.when(F.col("word").rlike(r"/u/\w+"), "user").otherwise(
        F.when(F.col("word").rlike(r"/r/\w+"), "post").otherwise(
        F.when(F.col("word").rlike(r"https?://\S+"), "url")
    )).alias("reference_type")
    ).groupBy("reference_type").count()

    print("References count:")
    references_df.show()

    # Tokenize the text
    print("Tokenizing text...")
    tokenizer = Tokenizer(inputCol="text", outputCol="words")
    words_data = tokenizer.transform(raw_df)

    print("Sample tokenized data:")
    words_data.select("text", "words").show(5)

    # Compute term frequencies
    print("Computing term frequencies...")
    hashing_tf = HashingTF(inputCol="words", outputCol="raw_features", numFeatures=20)
    featurized_data = hashing_tf.transform(words_data)

    # Compute IDF
    print("Computing IDF...")
    idf = IDF(inputCol="raw_features", outputCol="features")
    idf_model = idf.fit(featurized_data)
    rescaled_data = idf_model.transform(featurized_data)

    print("Top 10 words with highest TF-IDF scores:")
    rescaled_data.select("words", "features").show(truncate=False)

    # Compute sentiment analysis
    print("Computing sentiment analysis...")
    sentiment_df = raw_df.withColumn("sentiment", get_sentiment_udf(raw_df.text))

    print("Sentiment scores:")
    sentiment_df.select("title", "sentiment").show()

    # Save sentiment data to a SQLite database
    sqlite_url = "jdbc:sqlite:" + os.path.join(current_dir, "spark_streaming.db")
    
    # Write the DataFrame to SQLite
    sentiment_df.write \
        .format("jdbc") \
        .option("url", sqlite_url) \
        .option("dbtable", "sentiment_analysis") \
        .option("driver", "org.sqlite.JDBC") \
        .mode("overwrite") \
        .save()

    print("Analysis complete.")
except Exception as e:
    print('Howdy... looks like we have an error: ', e)
finally:
    # Stop the Spark session
    print("Stopping the Spark session...")
    spark.stop()
    print("Spark session stopped.")
