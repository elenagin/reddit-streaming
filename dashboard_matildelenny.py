import os
import pandas as pd
import pyarrow.parquet as pq
import streamlit as st
import plotly.express as px
import re
import matplotlib.pyplot as plt
from wordcloud import WordCloud
from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS
import streamlit as st
import sqlite3
import pandas as pd
import time


# Function to get metrics from the SQLite database
def get_metrics(db_name):
    conn = sqlite3.connect(db_name)
    query = "SELECT * FROM metrics"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Function to compute metrics
def compute_metrics(df):
    num_users_mentioned = df['user_ref_count'].sum()
    num_posts_mentioned = df['post_ref_count'].sum()
    num_urls_mentioned = df['url_count'].sum()
    return num_users_mentioned, num_posts_mentioned, num_urls_mentioned

# Main function for the Streamlit app
def main():
    st.image('reddit.png', width=200)
    st.title(':orange[Reddit] Metrics Dashboard ')
    st.write("By Matilde Bernocchi, Carlos Varela, Rafael Braga and Elena Ginebra")

    st.header('💁🏻‍♀️ :violet[Female Fashion] Advice ')
    st.caption("Find below some live metrics regarding :violet[Female Fashion] Advice posts on reddit.")


    placeholder = st.empty()

    while True:
        try:
            # Retrieve and compute metrics
            df = get_metrics('reddit_streaming.db')
            num_users_mentioned, num_posts_mentioned, num_urls_mentioned = compute_metrics(df)

            # Display metrics
            with placeholder.container():
                st.metric("Number of Users Mentioned", num_users_mentioned)
                st.metric("Number of Posts Mentioned", num_posts_mentioned)
                st.metric("Number of URLs Mentioned", num_urls_mentioned)

        except (sqlite3.OperationalError, pd.io.sql.DatabaseError) as e:
            st.warning("Database not found. Waiting for the database to be created...")

        # Wait for 5 seconds before updating
        time.sleep(5)

# Function to load data
@st.cache_data(ttl=60)
def load_data(filepath):
    try:
        data = pd.read_parquet(filepath)
        st.write("Columns in the dataset:", data.columns)
        
        # Extract timestamps from the window column
        if 'window' in data.columns:
            data['window_start'] = pd.to_datetime(data['window'].apply(lambda x: x['start']), unit='ns')
            data['window_end'] = pd.to_datetime(data['window'].apply(lambda x: x['end']), unit='ns')
            data['timestamp'] = data['window_start']
        else:
            st.error("The 'window' column is missing in the dataset.")
        return data
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return pd.DataFrame()

# Define the folder path
folder_path = 'metrics_data.parquet/'
# List all parquet files in the folder
parquet_files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith('.parquet')]
# Load all parquet files into a single DataFrame
dfs = []

for file in parquet_files:
    table = pq.read_table(file)
    dfs.append(table.to_pandas())


# Load and combine the data
data = pd.concat(dfs, ignore_index=True)

# Debugging step to verify the data columns
st.write("Data columns after loading and before filtering", data.columns)
st.write("First few rows of the data", data.head())

if not data.empty:
    # Dashboard title
    st.title('Reddit Streaming Analytics Dashboard')

    # Sidebar filters
    st.sidebar.header('Filters')
    # Remove date filters as we're not using timestamps
    # start_date = st.sidebar.date_input('Start date', data['timestamp'].min())
    # end_date = st.sidebar.date_input('End date', data['timestamp'].max())

    # filtered_data = data.loc[(data['timestamp'] >= pd.Timestamp(start_date)) & (data['timestamp'] <= pd.Timestamp(end_date))]
    filtered_data = data

    # Display raw data
    st.subheader('Raw Data')
    st.write(filtered_data)

    # Top 10 words by frequency
    st.subheader('Top 10 Important Words')
    filtered_data['top_words'] = filtered_data['top_words'].apply(lambda x: [item for sublist in x for item in sublist])  # Flatten nested lists
    top_words_flat = [word for sublist in filtered_data['top_words'] for word in sublist if word.strip().lower() not in ENGLISH_STOP_WORDS and word.isalpha()]  # Filter out empty words, stop words, and non-alphabetic tokens
    top_words_series = pd.Series(top_words_flat).value_counts().head(10)
    fig2 = px.bar(top_words_series, x=top_words_series.index, y=top_words_series.values, labels={'index': 'Words', 'y': 'Frequency'}, title='Top 10 Important Words by Frequency')
    st.plotly_chart(fig2)

    # Word Cloud
    st.subheader('Word Cloud of Important Words')
    wordcloud = WordCloud(width=800, height=400, stopwords=ENGLISH_STOP_WORDS).generate(' '.join(top_words_flat))
    plt.figure(figsize=(10, 5))
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis('off')
    st.pyplot(plt)

    # Sentiment Analysis (if available)
    if 'sentiment' in filtered_data.columns:
        st.subheader('Sentiment Analysis')
        fig3 = px.histogram(filtered_data, x='sentiment', nbins=50, title='Sentiment Analysis of Posts')
        st.plotly_chart(fig3)

    # Additional Metrics
    st.subheader('Additional Metrics')
    st.write('Total posts analyzed:', len(filtered_data))
    st.write('Total user references:', filtered_data['user_ref_count'].sum())
    st.write('Total post references:', filtered_data['post_ref_count'].sum())
    st.write('Total external links:', filtered_data['url_count'].sum())

    # Auto-refresh button
    if st.button('Refresh Data'):
        st.experimental_rerun()
else:
    st.error("No data available or the 'timestamp' column is missing.")

if __name__ == "__main__":
    main()