import streamlit as st
import pandas as pd
import pyarrow.parquet as pq
import os
import time

st.set_page_config(page_title="Reddit Metrics Dashboard", layout="wide")

def read_new_metrics(metrics_path):
    while True:
        if os.path.exists(metrics_path):
            break  # If metrics_path exists, exit the loop
        else:
            print(f"Waiting for metrics directory at {metrics_path} to be created...")
            time.sleep(5)  # Wait for 5 seconds before retrying
    
    metrics_files = [os.path.join(metrics_path, f) for f in os.listdir(metrics_path) if f.endswith('.parquet')]
    if not metrics_files:
        return pd.DataFrame()
    
    df_list = [pq.read_table(file).to_pandas() for file in metrics_files]
    df = pd.concat(df_list, ignore_index=True)
    return df

# Initialize an empty DataFrame to store metrics data
metrics_df = pd.DataFrame()

# Path to the metrics.parquet directory
metrics_path = 'metrics_data.parquet'

# Main loop to continuously check for new data
while True:
    new_data = read_new_metrics(metrics_path)
    if not new_data.empty:
        metrics_df = pd.concat([metrics_df, new_data], ignore_index=True)
        metrics_df.drop_duplicates(inplace=True)

        # Convert window column to string for proper plotting
        metrics_df['window'] = metrics_df['window'].astype(str)

        # Display cumulative counts over time
        cumulative_df = metrics_df.groupby('window').agg({
            'user_ref_count': 'sum',
            'post_ref_count': 'sum',
            'url_count': 'sum'
        }).cumsum().reset_index()

        st.line_chart(cumulative_df.set_index('window'))

        # Display the most recent top_words
        most_recent_window = metrics_df['window'].max()
        recent_top_words = metrics_df[metrics_df['window'] == most_recent_window]['top_words'].values[0]

        st.table(pd.DataFrame({'Top Words': recent_top_words}))

    # Wait for some time before checking for new data
    time.sleep(10)
