#!/usr/bin/env python3

# --- 1. IMPORTS & CONFIGURATION ---
# All imports are grouped at the top.
import os
import subprocess
import json
import psycopg2
import schedule
import time


# Load all DB connection info from environment variables.
# This makes the script portable and secure, perfect for Docker.
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST") # Use the service name 'db' in Docker
DB_PORT = os.getenv("DB_PORT", "5432") # Default to 5432 if not set
SPEEDTEST_SERVER_ID = os.getenv("SPEEDTEST_SERVER_ID")

print(f"Database: {DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT} {DB_NAME}")

# --- 2. CORE FUNCTIONS ---

def run_speed_test():
    """Runs the Speedtest CLI and returns the JSON output."""
    print("Running speed test...")
    try:
        command = ['speedtest', '--format=json', '--accept-license', '--accept-gdpr']

        # Add server, if specified
        if SPEEDTEST_SERVER_ID:
            command.extend(['--server-id', SPEEDTEST_SERVER_ID])
        # The '--format=json' flag is key
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=True
        )
        print("Speed test completed.")
        return json.loads(result.stdout)
    except subprocess.CalledProcessError as e:
        # --- MODIFIED FOR DEBUGGING ---
        # Print the standard error output from the command to see the real error.
        print(f"Error running speedtest: {e}")
        print(f"STDERR from speedtest command:\n---_BEGIN_STDERR_---\n{e.stderr}\n---_END_STDERR_---")
        return None
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON output: {e}")
        return None

def store_results(data):
    """Connects to the PostgreSQL DB and inserts the speed test data."""
    if not data:
        print("No data to store.")
        return

    # Check if all necessary DB config is present
    if not all([DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT]):
        print("Database configuration is incomplete. Check environment variables.")
        return

    try:
        download_mbps = data['download']['bandwidth'] * 8 / 1_000_000
        upload_mbps = data['upload']['bandwidth'] * 8 / 1_000_000
        ping_ms = data['ping']['latency']
        server_name = data['server']['name']
        server_location = data['server']['location']

        # --- REFINED: Use a 'with' statement for robust connection handling ---
        # This automatically opens, commits, and closes the connection.
        with psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT) as conn:
            with conn.cursor() as cursor:
                sql = """
                    INSERT INTO tracker.speed_result 
                    (download_mbps, upload_mbps, ping_ms, server_name, server_location) 
                    VALUES (%s, %s, %s, %s, %s);
                """
                cursor.execute(sql, (download_mbps, upload_mbps, ping_ms, server_name, server_location))
        
        print(f"Successfully stored result: DL: {download_mbps:.2f} Mbps, UL: {upload_mbps:.2f} Mbps, Ping: {ping_ms} ms")

    except (Exception, psycopg2.Error) as error:
        print(f"Error while connecting to PostgreSQL or inserting data: {error}")

def run_speed_test_job():
    """The complete job to be scheduled."""
    print("Scheduler starting speed test job...")
    speed_data = run_speed_test()
    store_results(speed_data)
    print("Speed test job finished.")

# --- 3. SCHEDULER EXECUTION ---

if __name__ == "__main__":
    print("ISP Tracker Collector starting up.")
    
    schedule.every(5).minutes.do(run_speed_test_job)
    
    print(f"Job scheduled to run every 5 minutes. Waiting for the first run...")

    # --- REFINED: Add a try/except block for the initial run for startup safety ---
    try:
        run_speed_test_job()
    except Exception as e:
        print(f"Initial run failed: {e}")

    # Main loop to run the scheduler
    while True:
        schedule.run_pending()
        time.sleep(1)