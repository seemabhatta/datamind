import subprocess
import time
import os
import sys
import signal

def run_app():
    """
    Run both the FastAPI server and Streamlit app in parallel
    """
    print("Starting NL2SQL Chat application...")
    
    # Start the FastAPI server
    api_process = subprocess.Popen(
        [sys.executable, "api.py"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    print("API server starting...")
    
    # Give the API server time to start
    time.sleep(2)
    
    # Start the Streamlit app
    streamlit_process = subprocess.Popen(
        [sys.executable, "-m", "streamlit", "run", "nl2sqlchatv4.py"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    print("Streamlit app starting...")
    
    try:
        # Keep the script running until interrupted
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        # Terminate both processes
        api_process.terminate()
        streamlit_process.terminate()
        
        # Wait for processes to terminate
        api_process.wait()
        streamlit_process.wait()
        
        print("Application shut down successfully")

if __name__ == "__main__":
    run_app()
