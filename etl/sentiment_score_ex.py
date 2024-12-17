from datetime import datetime
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import unix_timestamp, current_timestamp

def create_spark_session():
    spark = SparkSession.builder \
        .appName("MongoDB Extraction") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0") \
        .config("spark.mongodb.input.uri", "mongodb://root:root@localhost:27017/") \
        .config("spark.mongodb.output.uri", "mongodb://root:root@localhost:27017/") \
        .getOrCreate()
#   spark.sparkContext.setLogLevel("INFO")

    return spark

def extract_from_mongodb(spark, collection):

    # Calculate the timestamp for 3 days ago
    current_time = current_timestamp()

    # Calculate the timestamp for 3 days ago
    three_days_ago = unix_timestamp(current_time) - (3 * 24 * 60 * 60) 

    # MongoDB URI for connection
    uri = f"mongodb://root:root@localhost:27017/"

    # Define the filter to only include records from the last 3 days
    query = {"datetime": {"$gte": three_days_ago}}  # Fixed this line

    # Read the data with the filter applied
    df = spark.read.format("mongodb") \
        .option("uri", uri) \
        .option("database","market_screener") \
        .option("collection",collection) \
        .option("pipeline", f"[{{'$match': {str(query)}}}]") \
        .load()

    return df


