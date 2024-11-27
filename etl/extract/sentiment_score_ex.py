from pyspark.sql import SparkSession
from pyspark.sql.functions import unix_timestamp, current_timestamp



def create_spark_session():
    spark = SparkSession.builder \
        .appName("MongoDB Extraction") \
        .config("spark.mongodb.input.uri", "mongodb://localhost:27017/market_screener") \
        .config("spark.mongodb.output.uri", "mongodb://localhost:27017/market_screener") \
        .getOrCreate()
    return spark

def extract_from_mongodb(spark, collection):
    # Calculate the timestamp for 3 days ago
    current_time = current_timestamp()

    # Calculate the timestamp for 3 days ago
    three_days_ago = unix_timestamp(current_time) - (3 * 24 * 60 * 60) 

    # MongoDB URI for connection
    uri = f"mongodb://localhost:27017/market_screener.{collection}"

    # Define the filter to only include records from the last 3 days
    # Adjust the field name `datetime` as per your MongoDB schema
    query = {"datetime": {"$gte": three_days_ago_timestamp}}  # Filtering by the `datetime` field

    # Read the data with the filter applied
    df = spark.read.format("mongo") \
        .option("uri", uri) \
        .option("pipeline", f"[{{'$match': {str(query)}}}]") \
        .load()

    return df
