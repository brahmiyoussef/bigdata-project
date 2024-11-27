from pyspark.sql.functions import concat_ws, col,  udf
from pyspark.sql.types import StringType, FloatType
from transformers import pipeline

# Initialize the sentiment analysis pipeline
pipe = pipeline("text-classification", model="Sigma/financial-sentiment-analysis")



def concatenate_headline_and_summary(df):
    # Create a new column 'headline_summary' that concatenates 'headline' and 'summary'
    df_with_concat = df.withColumn("headline_summary", concat_ws(" ", col("headline"), col("summary")))
    
    df_result = df_with_concat.select(["headline_summary","related"])
    
    return df_result


def apply_sentiment_analysis(text):
    result = pipe(text)
    label = result[0]['label']  # 'LABEL_0' for negative, 'LABEL_1' for neutral, 'LABEL_2' for positive
    score = result[0]['score']  # Confidence score (float)
    return label, score


sentiment_udf = udf(apply_sentiment_analysis, StringType())

# Function to apply sentiment analysis and create the 'score' column
def apply_sentiment_to_dataframe(df):
    # Apply the sentiment analysis UDF to the 'headline_summary' column
    df_with_sentiment = df.withColumn("sentiment", sentiment_udf(col("headline_summary")))
    
    # Split the sentiment into two columns: 'label' and 'score'
    df_with_sentiment = df_with_sentiment.select(
        "headline_summary",
        col("sentiment")[0].alias("label"),
        col("sentiment")[1].alias("score")
    )
    
    return df_with_sentiment