''' 
Team Members:
- Carlos Varela
- Elena Ginebra
- Matilde Bernocci
- Rafael Braga
'''

import os
import dotenv
import requests
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

# Reddit API authentication:
auth = requests.auth.HTTPBasicAuth(CLIENT_ID, SECRET_TOKEN)
data = {'grant_type': 'password', 'username': USERNAME, 'password': PASSWORD}
header_ua = {'User-Agent': USER_AGENT}

res = requests.post('https://www.reddit.com/api/v1/access_token', auth=auth, data=data, headers=header_ua)
TOKEN = res.json()['access_token']
header_auth = {'Authorization': f"bearer {TOKEN}"}
headers = {**header_ua, **header_auth}

# Function to fetch posts from a subreddit:
def fetch_posts(subreddit='wallstreetbets'):#wallstreetbets
    res_posts = requests.get(f"https://oauth.reddit.com/r/{subreddit}/hot", headers=headers)
    posts = res_posts.json().get('data', {}).get('children', [])
    return posts

# Function to handle each client connection:
def handle_client(client_socket):
    seen_posts = set()  # Track seen posts by getting their IDs...

    try:
        while True: # Keep on checking for new posts every minute...
            posts = fetch_posts()
            new_posts = []

            for post in posts:
                post_id = post['data']['id']
                if post_id not in seen_posts: # If it is a new post...
                    seen_posts.add(post_id) # Add id to our IDs registry...
                    post_data = {
                        'title': post['data']['title'],
                        'created_utc': post['data'].get('created_utc'),
                        'text': post['data'].get('selftext', '')
                    }
                    new_posts.append(post_data) # Add new post to our post data list...

            if new_posts:
                for post_data in new_posts:
                    print(post_data)  # Verify the data before sending!
                    client_socket.sendall((json.dumps(post_data) + '\n').encode('utf-8'))

            time.sleep(60)  # Wait for 1 minute before checking for new posts...
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
