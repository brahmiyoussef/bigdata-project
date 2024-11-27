
def load_to_mongodb(df,collection):
    # MongoDB URI
    mongo_uri = f"mongodb://localhost:27017/market_screener.{collection}"
    
    # Write the DataFrame to the specified MongoDB collection
    df.write.format("mongo").option("uri", mongo_uri).mode("append").save()