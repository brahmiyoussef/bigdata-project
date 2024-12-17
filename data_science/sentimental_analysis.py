from transformers import AutoTokenizer, AutoModelForSequenceClassification
from transformers import pipeline
from datetime import datetime


def apply_sentiment_model(df):
    # Load model directly

    tokenizer = AutoTokenizer.from_pretrained("mrm8488/distilroberta-finetuned-financial-news-sentiment-analysis")
    model = AutoModelForSequenceClassification.from_pretrained("mrm8488/distilroberta-finetuned-financial-news-sentiment-analysis")
    sentiment_pipeline = pipeline("sentiment-analysis", model=model, tokenizer=tokenizer)
    try:
        
        sentiment_results = df['headline_summary'].apply(lambda text: sentiment_pipeline(text)[0])

        # Extract sentiment label and score
        df['sentiment'] = sentiment_results.apply(lambda result: result['label'])
        df['score'] = sentiment_results.apply(lambda result: result['score'])

        return df
    except Exception as e:
        print(f"Error while applying sentiment model: {e}")
        return df
