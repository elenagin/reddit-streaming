import os
import pandas as pd
import pyarrow.parquet as pq
import streamlit as st
import plotly.express as px
import re
import matplotlib.pyplot as plt
from wordcloud import WordCloud
from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS

# Display the combined DataFrame and metrics in a Streamlit dashboard
st.image('reddit.png', width=200)
st.title(':orange[Reddit] Metrics Dashboard ')
st.write("By Matilde Bernocchi, Carlos Varela, Rafael Braga and Elena Ginebra")

st.header('💁🏻‍♀️ :violet[Female Fashion] Advice ')
st.caption("Find below some live metrics regarding :violet[Female Fashion] Advice posts on reddit.")


# # Define the folder path
# folder_path = '/Users/lenn/Desktop/BTS/RDA - Real-time Data Analysis/metrics_data.parquet/'

# # List all parquet files in the folder
# parquet_files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith('.parquet')]

# # Load all parquet files into a single DataFrame
# df_list = []
# for file in parquet_files:
#     table = pq.read_table(file)
#     df_list.append(table.to_pandas())

# # Concatenate all DataFrames
# combined_df = pd.concat(df_list, ignore_index=True)

# # Calculate some example metrics
# total_rows = combined_df.shape[0]
# total_columns = combined_df.shape[1]
# summary_stats = combined_df.describe().to_dict()

# # Display metrics
# st.header('Metrics')
# st.metric(label="Total Rows", value=total_rows)
# st.metric(label="Total Columns", value=total_columns)

# # Display summary statistics
# st.header('Summary Statistics')
# st.json(summary_stats)

# # Display the DataFrame
# st.header('Data Table')
# st.dataframe(combined_df)

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

# Paths to the Parquet files
parquet_files = [
    'metrics_data.parquet/part-00000-82dad062-a401-45c8-ae31-95f63edd30c1-c000.snappy.parquet',
    'metrics_data.parquet/part-00000-436be57b-f84d-45a0-84f0-6a84375441b5-c000.snappy.parquet',
    'metrics_data.parquet/part-00000-cc041379-e338-4b8f-8fe6-b841c204d711-c000.snappy.parquet'
]

# Load and combine the data
dfs = [load_data(file) for file in parquet_files]
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
