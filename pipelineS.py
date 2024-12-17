
from etl.sentiment_score_ex import extract_from_mongodb,create_spark_session
from etl.sentiment_score_tr import concatenate_headline_and_summary, apply_sentiment_to_dataframe
from etl.sentiment_score_lo import load_to_mongodb , df_to_pandas, pd_to_collection
from data_science.sentimental_analysis import apply_sentiment_model




spark = create_spark_session()
print("created session successfully")

df= extract_from_mongodb(spark,'news')
df_cleaned= concatenate_headline_and_summary(df)
pd_news=df_to_pandas(df_cleaned)
'''print(pd_news.head())
'''
news_sentiment=apply_sentiment_model(pd_news)
print(news_sentiment.head())
pd_to_collection(news_sentiment)
