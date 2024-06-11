import os
import pandas as pd
import pyarrow.parquet as pq
import streamlit as st
import plotly.express as px
import re
import matplotlib.pyplot as plt
from wordcloud import WordCloud

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

# Display the combined DataFrame and metrics in a Streamlit dashboard
st.image('reddit.png', width=200)
st.title(':orange[Reddit] Metrics Dashboard ')
st.write("By Matilde Bernocchi, Carlos Varela, Rafael Braga and Elena Ginebra")

st.header('💁🏻‍♀️ :violet[Female Fashion] Advice ')
st.caption("Find below some live metrics regarding :violet[Female Fashion] Advice posts on reddit.")


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

# Load the processed data (assuming the DataFrame is saved as 'reddit_metrics.csv')
df = pd.read_csv("reddit_metrics.csv")

# Function to parse the 'window' column
def parse_window(window):
    # Use regex to find the start timestamp
    match = re.search(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", window)
    if match:
        return match.group(0)
    return None

# Extract 'start' timestamps from the 'window' column
df['start'] = df['window'].apply(parse_window)

# Convert the extracted 'start' timestamps to datetime
df['start'] = pd.to_datetime(df['start'])

# Calculate user_ref_count, post_ref_count, and url_count
aggregated_counts = df.groupby('start').agg({
    'user_ref_count': 'sum',
    'post_ref_count': 'sum',
    'url_count': 'sum'
}).reset_index()

# Display the DataFrame
st.subheader("Reddit Data Metrics")
st.write(df)

# Line chart for user, post, and URL counts over time
st.subheader("User, Post, and URL Counts Over Time")
fig_counts = px.line(aggregated_counts, x='start', y=['user_ref_count', 'post_ref_count', 'url_count'], 
                     labels={'value': 'Count', 'variable': 'Metric', 'start': 'Time Window'},
                     title='Counts Over Time')

fig_counts.update_layout(
    xaxis=dict(
        title='Time Window',
        tickformat='%Y-%m-%d %H:%M',  # Adjust the date format as needed
        tickmode='linear',  # Use linear mode for better spacing
        nticks=10  # Adjust the number of ticks
    ),
    yaxis_title='Count'
)
st.plotly_chart(fig_counts)

# Top words visualization
st.subheader("Top Words")
# Flatten the top_words column for better visualization
df['top_words'] = df['top_words'].apply(eval)  # Convert string representation of list to actual list
df_top_words = df.explode('top_words')
df_top_words_count = df_top_words['top_words'].value_counts().reset_index()
df_top_words_count.columns = ['index', 'count']  # Rename columns appropriately

# Filter out common stop words (optional)
common_stop_words = {'or', 'so', 'in', 'the', 'and', 'of', 'to', 'a', 'is', 'that', 'it', 'on', 'for', 'with', 'as', 'are', 'this', 'but', 'not'}
df_top_words_count = df_top_words_count[~df_top_words_count['index'].isin(common_stop_words)]

# Create the bar plot
fig_top_words = px.bar(df_top_words_count, x='index', y='count',
                       labels={'index': 'Word', 'count': 'Count'},
                       title='Top Words Frequency')
fig_top_words.update_layout(xaxis_tickangle=-45)
st.plotly_chart(fig_top_words)

# Display the most recent top_words
most_recent_window = df['start'].max()
recent_top_words_series = df[df['start'] == most_recent_window]['top_words']
recent_top_words_flat = [word for sublist in recent_top_words_series for word in sublist]

# Get the count of the top words
recent_top_words_count = pd.Series(recent_top_words_flat).value_counts().reset_index()
recent_top_words_count.columns = ['Word', 'Count']

st.table(recent_top_words_count)

# Function to load data
@st.cache_data(ttl=60)
def load_data(filepath):
    try:
        data = pd.read_parquet(filepath)
        st.write("Columns in the dataset:", data.columns)
        
        # Extract timestamps from the window column
        if 'window' in data.columns:
            data['window_start'] = data['window'].apply(lambda x: pd.to_datetime(x['start'], unit='ns'))
            data['window_end'] = data['window'].apply(lambda x: pd.to_datetime(x['end'], unit='ns'))
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

if not data.empty and 'timestamp' in data.columns:
    # Dashboard title
    st.title('Reddit Streaming Analytics Dashboard')

    # Sidebar filters
    st.sidebar.header('Filters')
    start_date = st.sidebar.date_input('Start date', data['timestamp'].min())
    end_date = st.sidebar.date_input('End date', data['timestamp'].max())

    filtered_data = data[(data['timestamp'] >= pd.Timestamp(start_date)) & (data['timestamp'] <= pd.Timestamp(end_date))]

    # Display raw data
    st.subheader('Raw Data')
    st.write(filtered_data)

    # Time series plot
    st.subheader('Time Series of Metrics')
    long_format = filtered_data.melt(id_vars='timestamp', value_vars=['user_ref_count', 'post_ref_count', 'url_count'], 
                                     var_name='Metric', value_name='Count')
    fig = px.line(long_format, x='timestamp', y='Count', color='Metric', title='Time Series of User References, Post References, and External Links')
    st.plotly_chart(fig)

    # Top 10 words by frequency
    st.subheader('Top 10 Important Words')
    filtered_data['top_words'] = filtered_data['top_words'].apply(lambda x: [item for sublist in x for item in sublist])  # Flatten nested lists
    top_words_flat = [word for sublist in filtered_data['top_words'] for word in sublist if word.strip()]  # Filter out empty words
    top_words_series = pd.Series(top_words_flat).value_counts().head(10)
    fig2 = px.bar(top_words_series, x=top_words_series.index, y=top_words_series.values, labels={'index': 'Words', 'y': 'Frequency'}, title='Top 10 Important Words by Frequency')
    st.plotly_chart(fig2)

    # Word Cloud
    st.subheader('Word Cloud of Important Words')
    wordcloud = WordCloud(width=800, height=400).generate(' '.join(top_words_flat))
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
