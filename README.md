# Reddit Streaming
Welcome to the Reddit Streaming project! This repository provides tools and scripts for streaming, processing, and analyzing Reddit data in real-time. The project is designed for developers interested in exploring and utilizing Reddit data for various analytical and monitoring purposes.

### Features
- Real-Time Data Fetching: Stream Reddit data in real-time using the PRAW SubredditStream library.
- Data Processing: Stream data through sockets, process, and analyze it to extract useful insights.
- Dashboard Visualization: Visualize metrics and trends using a web-based dashboard in near real-time.
- Scalable Architecture: Built with scalability in mind, allowing for large-scale data handling.

### Table of Contents
1. Installation
2. Usage
3. Project Structure
4. Contributing
5. License


## Installation
To get started with the Reddit Streaming project, follow these steps:

Clone the repository:

_bash_
Copy code
_git clone https://github.com/elenagin/reddit-streaming.git_
_cd reddit-streaming_
Install dependencies:

Ensure you have Python installed on your machine.
Install the required Python packages:


_bash_
Copy code
_pip install -r requirements.txt_

Configure API keys:
Create a cred.sh file in the root directory.
Add your Reddit API credentials to the cred.sh file:

_bash_
Copy code

CLIENT_ID=your_client_id


CLIENT_SECRET=your_client_secret


USER_AGENT=your_user_agent


## Usage
Start the app:

_bash_
_streamlit run app.py_

This script manages and runs multiple scripts through processes. App.py will start by running the streaming_consumer.py script. Then, it will run streaming_consumer.py which will receive the Reddit data and store it in a parquet format for further processing. After that, it will run metrics_etl.py which will extract data from the raw_data.parquet directory, process it with pyspark to compute metrics, and store it in a sqlite database. This will start a web-based dashboard where you can visualize the data and metrics using streamlit.


## Project Structure
- app.py: Main application script.
- streaming_producer.py: Script for fetching data from Reddit.
- streaming_consumer.py: Client for storing the fetched data.
- metrics_etl.py: ETL pipeline to generate metrics with pyspark and store them in a db.
- dashboard.py: Streamlit dashboard for visualizing the data.
- requirements.txt: List of dependencies required for the project.
  

## Contributing
We welcome contributions from the community! To contribute:

1. Fork the repository.
2. Create a new branch for your feature or bugfix.
3. Commit your changes and push to your fork.
4. Submit a pull request with a description of your changes.
_Please ensure your code adheres to the project's coding standards and includes tests where applicable._

---
---
## License
This project is licensed under the MIT License. See the LICENSE file for more details.
Feel free to reach out if you have any questions or need further assistance!
