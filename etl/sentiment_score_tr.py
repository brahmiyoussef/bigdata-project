from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, col, udf
from pyspark.sql.types import StringType, FloatType, StructType, StructField
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from transformers import pipeline


# Initialize the sentiment analysis pipeline
local_model_path = "C:/Users/HP/.cache/huggingface/hub/models--Sigma--financial-sentiment-analysis/snapshots/d78ca172e07e94390f615739cee98a2154381f7e"
sentiment_pipeline = pipeline("sentiment-analysis", model=local_model_path)

# Function to concatenate 'headline' and 'summary'
def concatenate_headline_and_summary(df):
    df_with_concat = df.withColumn("headline_summary", concat_ws(" ", col("headline"), col("summary")))
    df_result = df_with_concat.select(["headline_summary", "related","datetime"])
    return df_result

# UDF for sentiment analysis
def apply_sentiment_analysis(text):
    inputs = tokenizer(text, return_tensors="pt", truncation=True, padding=True, max_length=512)
    outputs = model(**inputs)
    logits = outputs.logits
    predictions = logits.argmax(axis=-1)
    label = model.config.id2label[predictions.item()]  # Map to label
    score = logits.softmax(dim=-1).max().item()  # Get the confidence score
    return label, score

# Define schema for sentiment
sentiment_schema = StructType([
    StructField("label", StringType(), True),
    StructField("score", FloatType(), True)
])

# Register the UDF with PySpark
sentiment_udf = udf(apply_sentiment_analysis, sentiment_schema)

# Function to apply sentiment to the DataFrame
def apply_sentiment_to_dataframe(df):
    df_with_sentiment = df.withColumn("sentiment", sentiment_udf(col("headline_summary")))
    df_with_sentiment = df_with_sentiment.select(
        "headline_summary",
        col("sentiment.label").alias("label"),
        col("sentiment.score").alias("score")
    )
    return df_with_sentiment
