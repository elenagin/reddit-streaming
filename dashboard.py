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
    num_users_mentioned = df['user_ref_count'].sum()
    num_posts_mentioned = df['post_ref_count'].sum()
    num_urls_mentioned = df['url_count'].sum()
    return num_users_mentioned, num_posts_mentioned, num_urls_mentioned

# Main function for the Streamlit app
def main():
    st.title("Reddit Streaming Metrics")

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

if __name__ == "__main__":
    main()
