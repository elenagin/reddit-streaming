''' 
Team Members:
- Carlos Varela
- Elena Ginebra
- Matilde Bernocci
- Rafael Braga
'''

import subprocess
import time
import warnings

warnings.filterwarnings("ignore")

def run_script(script_name):
    try:
        return subprocess.Popen(['python', script_name])
    except Exception as e:
        print(f"Error running script {script_name}: {e}")

def run_streamlit_app(app_name):
    try:
        return subprocess.Popen(['streamlit', 'run', app_name])
    except Exception as e:
        print(f"Error running Streamlit app {app_name}: {e}")

# Run the data fetching script
print("Running streaming producer")
fetching_process = run_script('streaming_server.py')

# Wait for 10 seconds
time.sleep(5)

# Run the data processing script
print("Extracting data from subreddit...")
processing_process = run_script('streaming_consumer.py')

# Wait for 10 seconds
time.sleep(15)

# Run the data analysis script
print("Running Metrics ETL pipeline...")
analysis_process = run_script('metrics_etl.py')

# Wait for 15 seconds
print('preparing to display metrics...')
time.sleep(20)

# Run the Streamlit app
print("Launching metrics dashboard...")
streamlit_process = run_streamlit_app('dashboard.py')

print("All scripts have been started.")
print("Monitoring scripts...")

# Optionally, wait for each process to complete (if desired)
try:
    fetching_process.wait()
    processing_process.wait()
    analysis_process.wait()
    streamlit_process.wait()
except KeyboardInterrupt:
    print("Terminating scripts...")
    fetching_process.terminate()
    processing_process.terminate()
    analysis_process.terminate()
    streamlit_process.terminate()
    fetching_process.wait()
    processing_process.wait()
    analysis_process.wait()
    streamlit_process.wait()
    print("All scripts terminated.")
