import subprocess
import time

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
print("Running data_fetching.py...")
fetching_process = run_script('data_fetching_server.py')

# Wait for 15 seconds
time.sleep(15)

# Run the data processing script
print("Running data_processing.py...")
processing_process = run_script('data_processing_client.py')

# Wait for 15 seconds
time.sleep(15)

# Run the data analysis script
print("Running data_analysis.py...")
analysis_process = run_script('spark_analysis_ver2.py')

# Wait for 15 seconds
time.sleep(15)

# Run the Streamlit app
print("Running streamlit_app.py...")
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
