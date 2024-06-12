'''
Team Members:
- Carlos Varela
- Elena Ginebra
- Matilde Bernocci
- Rafael Braga
'''

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
    num_users_mentioned = df['mentioned_users'].sum()
    num_posts_mentioned = df['referenced_posts'].sum()
    num_urls_mentioned = df['urls_shared'].sum()
    num_nvdia_mentioned = df['nvidia_count'].sum()
    num_appl_mentioned = df['aapl_count'].sum()
    num_elon_mentioned = df['elon_count'].sum()
    return num_users_mentioned, num_posts_mentioned, num_urls_mentioned, num_nvdia_mentioned, num_appl_mentioned, num_elon_mentioned

# Main function for the Streamlit app
def main():
    st.title("Reddit Streaming Metrics")

    placeholder = st.empty()

    while True:
        try:
            # Retrieve and compute metrics
            df = get_metrics('reddit_streaming.db')
            num_users_mentioned, num_posts_mentioned, num_urls_mentioned, num_nvdia_mentioned, num_appl_mentioned, num_elon_mentioned = compute_metrics(df)

            # Display metrics
            with placeholder.container():
                st.metric("Number of Users Mentioned", num_users_mentioned)
                st.metric("Number of Posts referenced", num_posts_mentioned)
                st.metric("Number of URLs shared", num_urls_mentioned)
                st.metric("Number of times people mentioned NVDIA", num_nvdia_mentioned)
                st.metric("Number of times people mentioned APPL", num_appl_mentioned)
                st.metric("Number of times people mentioned Elon Musk", num_elon_mentioned)

        except (sqlite3.OperationalError, pd.io.sql.DatabaseError) as e:
            st.warning("Waiting for database")
            time.sleep(10)

        # Wait for 5 seconds before updating
        time.sleep(5)

if __name__ == "__main__":
    main()
