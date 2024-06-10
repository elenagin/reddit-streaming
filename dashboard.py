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
import os
import glob
import time

# Function to create SQLite database and table
def create_database_and_table():
    conn = sqlite3.connect('metrics.db')  
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS real_time_metrics 
                      (window TIMESTAMP,
                      user_ref_count INTEGER, post_ref_count INTEGER, 
                      url_count INTEGER, top_words TEXT)''')
    conn.commit()
    conn.close()

# Function to read Parquet files and insert data into SQLite table
def read_parquet_and_insert_to_db():
    parquet_files = glob.glob('metrics_data.parquet/*.parquet')
    if not parquet_files:
        st.write("No Parquet files found. Waiting for data...")
        return

    for file in parquet_files:
        df = pd.read_parquet(file)
        conn = sqlite3.connect('metrics.db')  
        df.to_sql('real_time_metrics', conn, if_exists='append', index=False)
        conn.close()
        st.write(f"Data from {file} inserted into database.")

# Function to get the sum of values from the specified columns
def get_sum_of_columns():
    conn = sqlite3.connect('metrics.db')  
    cursor = conn.cursor()
    cursor.execute('''SELECT SUM(user_ref_count), SUM(post_ref_count), SUM(url_count) 
                      FROM real_time_metrics''')
    sums = cursor.fetchone()
    conn.close()
    return sums

# Function to update and display the sum in real-time
def update_sum():
    while True:
        sums = get_sum_of_columns()
        st.write("Sum of user_ref_count:", sums[0])
        st.write("Sum of post_ref_count:", sums[1])
        st.write("Sum of url_count:", sums[2])
        time.sleep(10)

# Streamlit app
def main():
    st.title("Real-time Data from Parquet Files")
    st.write("This app displays real-time data from Parquet files stored in the 'metrics_data.parquet' directory.")

    # Create SQLite database and table
    create_database_and_table()

    # Start reading Parquet files and inserting data into SQLite table
    read_parquet_and_insert_to_db()

    # Start updating and displaying sum in real-time
    update_sum()

if __name__ == "__main__":
    main()
