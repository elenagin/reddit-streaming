import streamlit as st
import sqlite3
import pandas as pd
import plotly.express as px
import matplotlib.pyplot as plt
from wordcloud import WordCloud
from sklearn.feature_extraction.text import CountVectorizer, ENGLISH_STOP_WORDS
import time
import re

# Function to get metrics from SQLite database
def get_metrics(db_name):
    conn = sqlite3.connect(db_name)
    query = "SELECT * FROM metrics"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Feature to extract stock mentions
def extract_stock_mentions(df):
    stock_pattern = r'\b(GME|AMC|TSLA|AAPL|MSFT|AMZN|NFLX|FB)\b'
    df['mentioned_stocks'] = df['post_text'].astype(str).apply(lambda text: re.findall(stock_pattern, text))
    df = df.explode('mentioned_stocks')
    return df

# Function to calculate the sentiment of posts
def compute_sentiment(df):
    sentiments = ["positive", "negative", "neutral"]
    df['sentiment'] = df['post_text'].astype(str).apply(lambda text: sentiments[hash(text) % 3])
    return df

# Function to extract the most important words from posts
def extract_top_words(text_series, top_n=10):
    text_series = text_series.fillna('')
    vectorizer = CountVectorizer(stop_words='english', max_features=top_n)
    X = vectorizer.fit_transform(text_series)
    words = vectorizer.get_feature_names_out()
    word_counts = X.sum(axis=0).A1
    top_words = pd.Series(word_counts, index=words).sort_values(ascending=False)
    return top_words

# Main feature for Streamlit dashboard
def main():
    wallstbets_image_path = 'wallstbets.png'
    st.image(wallstbets_image_path, width=300, caption=None, use_column_width=False, output_format="auto")

    st.title(':red[Reddit] Metrics Dashboard')
    st.write("By Matilde Bernocchi, Carlos Varela, Rafael Braga and Elena Ginebra")

    st.header('📈 Trending Topics on :green[WallStreetBets] 💶')
    st.caption("Explore live metrics and insights from the :green[WallStreetBets] subreddit.")

    metric_placeholders = {
        "num_urls_referenced": st.empty(),
        "num_posts_referenced": st.empty(),
        "num_users_mentioned": st.empty(),
        "total_reactions": st.empty(),
        "most_mentioned_stock": st.empty()
    }

    advanced_analysis_placeholder = st.empty()
    error_placeholder = st.empty()  # Placeholder for the error message

    while True:
        try:
            df = get_metrics('reddit_streaming.db')
            error_placeholder.empty()  # Clear any previous error messages

            if not df.empty:
                df = extract_stock_mentions(df)
                df = compute_sentiment(df)

                # Calculate key metrics
                num_urls_referenced = df['urls_shared'].sum()
                num_posts_referenced = df['referenced_posts'].sum()
                num_users_mentioned = df['mentioned_users'].sum()
                total_reactions = num_urls_referenced + num_posts_referenced + num_users_mentioned  

                # Check if 'mentioned_stocks' has valid data
                if 'mentioned_stocks' in df.columns and df['mentioned_stocks'].notna().any():
                    value_counts = df['mentioned_stocks'].value_counts()
                    if not value_counts.empty:
                        most_mentioned_stock = value_counts.idxmax()
                    else:
                        most_mentioned_stock = "No data"
                else:
                    most_mentioned_stock = "No data"

                # Update live metrics
                metric_placeholders["num_urls_referenced"].metric("Number of URLs Referenced", num_urls_referenced)
                metric_placeholders["num_posts_referenced"].metric("Number of Posts Referenced", num_posts_referenced)
                metric_placeholders["num_users_mentioned"].metric("Number of Users Mentioned", num_users_mentioned)
                metric_placeholders["total_reactions"].metric("Total Reactions", total_reactions)
                metric_placeholders["most_mentioned_stock"].metric("Most Mentioned Stock", most_mentioned_stock)

                # Update advanced analytics
                with advanced_analysis_placeholder.container():
                    st.write('')
                    st.write('')
                    st.subheader('🔍 Advanced Analytics')

                    # Barplot for the top 10 words in posts
                    st.write('🔝 Top 10 Words in Posts')
                    top_words = extract_top_words(df['post_text'])
                    top_words_series = top_words.head(10)
                    fig2 = px.bar(top_words_series, x=top_words_series.index, y=top_words_series.values, labels={'index': 'Words', 'y': 'Frequency'})
                    st.plotly_chart(fig2)

                    # Word Cloud of the most used words
                    st.write('☁️ Word cloud of the most mentioned words by the users.')
                    wordcloud_text = ' '.join(df['post_text'].fillna('').astype(str))

                    if wordcloud_text.strip():  # Ensure there is text to generate the wordcloud
                        wordcloud = WordCloud(width=800, height=400, stopwords=ENGLISH_STOP_WORDS).generate(wordcloud_text)
                        plt.figure(figsize=(10, 5))
                        plt.imshow(wordcloud, interpolation='bilinear')
                        plt.axis('off')
                        st.pyplot(plt)
                    else:
                        st.write("No text available to generate the word cloud.")

                    st.write('')
                    st.write('')
                    st.subheader('📉 Sentiment Analysis')
                    st.write("Let's see whether the news is skewing towards positive or negative.")
                    sentiment_counts = df['sentiment'].value_counts()
                    colors = {'neutral': 'gray', 'positive': 'green', 'negative': 'crimson'}
                    fig8 = px.bar(sentiment_counts, x=sentiment_counts.index, y=sentiment_counts.values, labels={'index': 'Sentiment', 'y': 'Count'}, title='Sentiment Analysis')
                    fig8.update_traces(marker_color=[colors.get(sentiment, 'gray') for sentiment in sentiment_counts.index])
                    st.plotly_chart(fig8)

                    # Barplot for stock mentions
                    st.subheader('📊 Stock Mentions Total')
                    stock_mentions = df['mentioned_stocks'].value_counts()

                    # Convert stock_mentions to a DataFrame
                    stock_mentions_df = stock_mentions.reset_index()
                    stock_mentions_df.columns = ['Stock', 'Mentions']
                    
                    fig3 = px.bar(stock_mentions_df, x='Stock', y='Mentions', labels={'Stock': 'Stock', 'Mentions': 'Mentions'}, title='Total Mentions by Stock')
                    st.plotly_chart(fig3)
                    st.divider()
                    st.caption("Analysis by Matilde Bernocchi, Carlos Varela, Rafael Braga and Elena Ginebra")
                    st.caption('💰 Good luck investing!')

        except (sqlite3.OperationalError, pd.io.sql.DatabaseError) as e:
            error_placeholder.warning("Database not found. Waiting for the database to be created...")

        # Please wait before next update
        time.sleep(5)

if __name__ == "__main__":
    main()
