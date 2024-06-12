''' 
Team Members:
- Carlos Varela
- Elena Ginebra
- Matilde Bernocci
- Rafael Braga
'''

import os
import dotenv
import praw
import socket
import json
import threading
import time
import warnings

warnings.filterwarnings("ignore")

# Load environment variables:
env_file = 'creds.sh'
dotenv.load_dotenv(env_file, override=True)

CLIENT_ID = os.environ['CLIENT_ID']
SECRET_TOKEN = os.environ['SECRET_TOKEN']
USERNAME = os.environ['USERNAME']
PASSWORD = os.environ['PASSWORD']
USER_AGENT = 'MyBot/0.0.1'

# Initialize PRAW:
reddit = praw.Reddit(
    client_id=CLIENT_ID,
    client_secret=SECRET_TOKEN,
    username=USERNAME,
    password=PASSWORD,
    user_agent=USER_AGENT
)

# Function to handle each client connection:
def handle_client(client_socket, subreddit='wallstreetbets'):
    seen_posts = set()  # Track seen posts by getting their IDs...
    
    try:
        for post in reddit.subreddit(subreddit).stream.submissions():
            post_id = post.id
            if post_id not in seen_posts:  # If it is a new post...
                seen_posts.add(post_id)  # Add id to our IDs registry...
                post_data = {
                    'title': post.title,
                    'created_utc': post.created_utc,
                    'text': post.selftext
                }
                print(post_data)  # Verify the data before sending!
                client_socket.sendall((json.dumps(post_data) + '\n').encode('utf-8'))
            
            time.sleep(1)  # Adjust sleep time if necessary to avoid spamming
    except (ConnectionResetError, BrokenPipeError):
        print("Connection has been closed.")
    finally:
        client_socket.close()

# Define the socket server:
def socket_server():
    print('Opening wormhole')
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(('localhost', 9999))
    server_socket.listen(5)
    print("Ready on port 9999")
    
    while True:
        client_socket, addr = server_socket.accept()
        print(f"Connection from {addr} has been established.")
        client_thread = threading.Thread(target=handle_client, args=(client_socket,))
        client_thread.start()

# Run the socket server and handle concurrent requests:
server_thread = threading.Thread(target=socket_server)
server_thread.start()
