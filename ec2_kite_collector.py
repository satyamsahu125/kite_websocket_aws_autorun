import os
import datetime
import pandas as pd
import threading
import time
import json
import logging
import pytz # For timezone handling
import pyarrow.parquet as pq
import pyarrow as pa
import boto3 # For S3 and Secrets Manager integration
from dotenv import load_dotenv
load_dotenv()
from kiteconnect import KiteConnect, KiteTicker

# --- Configuration ---
AWS_REGION = "ap-south-1" # Ensure this matches your AWS region
SECRETS_MANAGER_SECRET_NAME = "KiteConnectBankniftyData" # Must match the secret name used by your local script

# AWS S3 Configuration
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "kitebanknifty20250808") # Set your S3 bucket name
S3_PREFIX = os.getenv("S3_PREFIX", "banknifty_data/") # Prefix for objects within the bucket
SAVE_TO_S3 = os.getenv("SAVE_TO_S3", "True").lower() == "true" # "True" to save to S3, "False" for local only

# Local file storage directories on EC2
TEMP_DATA_DIR = "temp_kite_data"
FINAL_DATA_DIR = "final_kite_data"

# Market Hours (in IST - Indian Standard Time)
# Adjust these times based on actual market hours and buffer for EOD processing
MARKET_OPEN_HOUR = 9
MARKET_OPEN_MINUTE = 15 # Market opens at 9:15 AM IST
MARKET_CLOSE_HOUR = 15 # Official Equity/F&O close is 3:30 PM IST
MARKET_CLOSE_MINUTE = 30
EOD_PROCESSING_HOUR = 15 # Start EOD processing slightly after close
EOD_PROCESSING_MINUTE = 45 # e.g., 3:45 PM IST

IST = pytz.timezone('Asia/Kolkata') # Define Indian Standard Timezone for accurate timing

# Ensure local directories exist on EC2 instance
os.makedirs(TEMP_DATA_DIR, exist_ok=True)
os.makedirs(FINAL_DATA_DIR, exist_ok=True)

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    handlers=[logging.StreamHandler()])

# Global variables for data collection and control
in_memory_ticks = []
data_lock = threading.Lock() # For thread-safe access to in_memory_ticks
shutdown_event = threading.Event() # Event to signal graceful shutdown to all threads

# --- Function to fetch credentials from AWS Secrets Manager ---
def get_kite_credentials():
    """Fetches Kite Connect API credentials from AWS Secrets Manager."""
    try:
        secrets_client = boto3.client('secretsmanager', region_name=AWS_REGION)
        get_secret_value_response = secrets_client.get_secret_value(
            SecretId=SECRETS_MANAGER_SECRET_NAME
        )
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
            credentials = json.loads(secret)
            logging.info("Kite credentials fetched from AWS Secrets Manager.")
            return credentials
        else:
            logging.error("Secret is not a string type in Secrets Manager. It must be a JSON string.")
            return None
    except Exception as e:
        logging.error(f"Error retrieving Kite credentials from Secrets Manager: {e}")
        return None

# --- WebSocket Callbacks ---
# These functions handle real-time data from Kite Connect WebSocket
def on_connect(ws, response):
    logging.info("Kite WebSocket connected.")
    try:
        # Fetch instrument tokens for Bank Nifty futures and options from Kite API
        instruments = kite.instruments("NFO")
        banknifty_futures_options_tokens = []

        # IMPORTANT: Refine this filtering logic to subscribe only to desired instruments.
        # Subscribing to too many instruments can lead to rate limits or data overload.
        # Example: Filter for current month future and relevant weekly/monthly options strikes.
        # This example is broad; tailor it to your needs.
        for instrument in instruments:
            if instrument['segment'] == 'NFO' and 'BANKNIFTY' in instrument['tradingsymbol']:
                # Example: Filtering for specific expiry or strike range (add your logic here)
                # if instrument['instrument_type'] == 'FUT' and "24AUGFUT" in instrument['tradingsymbol']:
                # if instrument['instrument_type'] in ['CE', 'PE'] and instrument['strike'] > 45000 and instrument['strike'] < 47000:
                banknifty_futures_options_tokens.append(instrument['instrument_token'])
                # logging.debug(f"Adding instrument: {instrument['tradingsymbol']}")

        logging.info(f"Attempting to subscribe to {len(banknifty_futures_options_tokens)} Bank Nifty F&O instruments.")

        if not banknifty_futures_options_tokens:
            logging.warning("No Bank Nifty F&O instruments found based on filter. Please refine instrument selection logic.")
            ws.stop() # Stop WebSocket if no instruments found
            return

        # Subscribe to tokens and set mode to FULL for comprehensive data (including market depth)
        # Process subscriptions in batches if the list is very large to avoid API rate limits
        batch_size = 300 # A reasonable batch size
        for i in range(0, len(banknifty_futures_options_tokens), batch_size):
            batch = banknifty_futures_options_tokens[i:i + batch_size]
            ws.subscribe(batch)
            ws.set_mode(ws.MODE_FULL, batch) # MODE_FULL provides 10-level market depth [9], [16], [18]
            logging.info(f"Subscribed to batch of {len(batch)} instruments.")
            time.sleep(0.1) # Small delay to be polite to the API

        logging.info(f"Successfully subscribed to {len(banknifty_futures_options_tokens)} instruments in MODE_FULL.")
    except Exception as e:
        logging.error(f"Error subscribing to instruments: {e}")
        ws.stop() # Stop WebSocket on error

def on_ticks(ws, ticks):
    """Callback for receiving new market data ticks."""
    timestamp = datetime.datetime.now(IST) # Record timestamp in IST
    with data_lock: # Ensure thread-safe access to in_memory_ticks
        for tick in ticks:
            # Extract relevant data from the tick and add a local timestamp
            # Tick structure: https://kite.trade/docs/connect/v3/websocket/#market-data
            processed_tick = {
                'timestamp': timestamp,
                'instrument_token': tick.get('instrument_token'),
                'last_price': tick.get('last_price'),
                'ohlc_open': tick.get('ohlc', {}).get('open'), # OHLC data for the current candle
                'ohlc_high': tick.get('ohlc', {}).get('high'),
                'ohlc_low': tick.get('ohlc', {}).get('low'),
                'ohlc_close': tick.get('ohlc', {}).get('close'),
                'volume': tick.get('volume'),
                'oi': tick.get('oi'), # Open Interest
                # Market depth data - storing as JSON string to easily save to CSV/Parquet
                'depth_buy': json.dumps(tick.get('depth', {}).get('buy', [])),
                'depth_sell': json.dumps(tick.get('depth', {}).get('sell', []))
            }
            in_memory_ticks.append(processed_tick)
    # logging.debug(f"Received {len(ticks)} ticks. Total in memory: {len(in_memory_ticks)}")

def on_close(ws, code, reason):
    """Callback for WebSocket close event."""
    logging.info(f"Kite WebSocket closed. Code: {code}, Reason: {reason}")
    # Signal the main thread to initiate EOD processing if not already triggered
    if not shutdown_event.is_set():
        logging.info("WebSocket closed. Triggering End-of-Day processing.")
        shutdown_event.set() # Set the event to signal all threads to shutdown gracefully

def on_error(ws, code, reason):
    """Callback for WebSocket error event."""
    logging.error(f"Kite WebSocket error. Code: {code}, Reason: {reason}")
    # Depending on error code, you might want to stop or retry connection.

def on_reconnect(ws, attempt_count):
    """Callback for WebSocket reconnection attempt."""
    logging.warning(f"Kite WebSocket reconnecting: Attempt {attempt_count}")

def on_noreconnect(ws):
    """Callback if WebSocket cannot reconnect."""
    logging.error("Kite WebSocket could not reconnect. Giving up.")
    shutdown_event.set() # If no reconnect is possible, signal EOD and exit

# Assign the callbacks to the KiteTicker instance
kws = None # Initialize kws to None; it will be set after credentials are loaded
# Callbacks will be assigned to the actual kws instance later in __main__

# --- Periodic Saving Function ---
def save_periodic_data():
    """Periodically saves accumulated in-memory ticks to a temporary CSV file."""
    global in_memory_ticks
    while not shutdown_event.is_set(): # Continue running until shutdown is signaled
        time.sleep(20) # Save every 20 seconds

        data_to_save = []
        with data_lock:
            if in_memory_ticks:
                data_to_save = list(in_memory_ticks) # Take a snapshot of current data
                in_memory_ticks.clear() # Clear the in-memory buffer
                logging.info(f"Flushing {len(data_to_save)} ticks to temporary file.")
            else:
                logging.debug("No new ticks to save periodically.")
                continue

        if data_to_save:
            try:
                df = pd.DataFrame(data_to_save)
                # Generate a unique filename based on current timestamp
                filename = os.path.join(TEMP_DATA_DIR, f"ticks_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S_%f')}.csv")
                df.to_csv(filename, index=False)
                logging.info(f"Saved {len(data_to_save)} ticks to {filename}")
            except Exception as e:
                logging.error(f"Error saving periodic data: {e}")

# --- End-of-Day (EOD) Processing and Parquet Conversion ---
def process_eod_data():
    """Consolidates all temporary data, cleans it, and saves it as a clean Parquet file."""
    logging.info("Starting End-of-Day data processing...")
    all_day_data = []
    temp_files = [f for f in os.listdir(TEMP_DATA_DIR) if f.endswith('.csv')]
    temp_files.sort() # Process files in chronological order

    # First, flush any remaining ticks from memory to ensure all data is captured
    remaining_ticks = []
    with data_lock:
        if in_memory_ticks:
            remaining_ticks = list(in_memory_ticks)
            in_memory_ticks.clear()
            logging.info(f"Flushing {len(remaining_ticks)} remaining ticks from memory for EOD.")

    if remaining_ticks:
        try:
            df_remaining = pd.DataFrame(remaining_ticks)
            remaining_filename = os.path.join(TEMP_DATA_DIR, f"ticks_last_flush_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S_%f')}.csv")
            df_remaining.to_csv(remaining_filename, index=False)
            temp_files.append(os.path.basename(remaining_filename)) # Add this file to the list for processing
            logging.info(f"Saved remaining ticks to {remaining_filename}")
        except Exception as e:
            logging.error(f"Error saving remaining in-memory ticks for EOD: {e}")

    # Re-scan temp files to ensure the last flushed file is included
    temp_files = [f for f in os.listdir(TEMP_DATA_DIR) if f.endswith('.csv')]
    temp_files.sort()

    # Load all temporary CSV files into a list of DataFrames
    for fname in temp_files:
        filepath = os.path.join(TEMP_DATA_DIR, fname)
        try:
            df = pd.read_csv(filepath)
            all_day_data.append(df)
            os.remove(filepath) # Clean up temporary file after reading
            logging.debug(f"Processed and removed temporary file: {filepath}")
        except Exception as e:
            logging.error(f"Error reading or deleting temporary file {filepath}: {e}")

    if all_day_data:
        consolidated_df = pd.concat(all_day_data, ignore_index=True)
        logging.info(f"Consolidated {len(consolidated_df)} total ticks from temporary files.")

        # --- Data Cleaning and Type Conversion ---
        # Convert timestamp column to datetime objects and localize to IST
        consolidated_df['timestamp'] = pd.to_datetime(consolidated_df['timestamp'])
        consolidated_df['timestamp'] = consolidated_df['timestamp'].dt.tz_convert(IST)
        # Sort data by timestamp and instrument token for chronological order
        consolidated_df.sort_values(by=['timestamp', 'instrument_token'], inplace=True)
        consolidated_df.drop_duplicates(inplace=True) # Remove any duplicate ticks
        logging.info(f"Cleaned data, total unique ticks: {len(consolidated_df)}")

        # Convert numeric columns to appropriate types, handling potential errors and NaNs
        for col in ['last_price', 'ohlc_open', 'ohlc_high', 'ohlc_low', 'ohlc_close', 'volume', 'oi']:
            if col in consolidated_df.columns:
                consolidated_df[col] = pd.to_numeric(consolidated_df[col], errors='coerce').fillna(0) # Fill NaNs with 0
        if 'instrument_token' in consolidated_df.columns:
            consolidated_df['instrument_token'] = consolidated_df['instrument_token'].astype('int64')

        # --- Save to Parquet File ---
        # Generate a daily-based filename
        eod_filename = os.path.join(FINAL_DATA_DIR, f"banknifty_fo_data_{datetime.date.today().strftime('%Y%m%d')}.parquet")
        try:
            # Save the DataFrame to a Parquet file for efficient storage and querying
            consolidated_df.to_parquet(eod_filename, index=False, engine='pyarrow')
            logging.info(f"Daily Parquet file saved locally: {eod_filename}")

            # --- Upload to S3 (if enabled) ---
            if SAVE_TO_S3:
                upload_to_s3(eod_filename, S3_BUCKET_NAME, S3_PREFIX)

        except Exception as e:
            logging.error(f"Error saving Parquet file or uploading to S3: {e}")
    else:
        logging.info("No data frames to consolidate for End-of-Day processing.")

def upload_to_s3(local_filepath, bucket_name, s3_prefix=""):
    """Uploads a file to an AWS S3 bucket."""
    s3_client = boto3.client('s3')
    object_name = s3_prefix + os.path.basename(local_filepath) # Full path in S3 bucket
    try:
        s3_client.upload_file(local_filepath, bucket_name, object_name)
        logging.info(f"Successfully uploaded {local_filepath} to s3://{bucket_name}/{object_name}")
        # Optionally, remove the local file after successful upload if you don't need it on EC2 disk
        os.remove(local_filepath)
        logging.info(f"Removed local file: {local_filepath}")
    except Exception as e:
        logging.error(f"Error uploading {local_filepath} to S3: {e}")

# --- Market Session Control and Shutdown Logic ---
def market_session_manager():
    """Manages the market session, triggering EOD processing at close."""
    while True:
        now_ist = datetime.datetime.now(IST)

        # Log a warning if past market open but WebSocket is not connected
        if not kws.is_connected() and not shutdown_event.is_set() and \
           (now_ist.hour > MARKET_OPEN_HOUR or \
           (now_ist.hour == MARKET_OPEN_HOUR and now_ist.minute >= MARKET_OPEN_MINUTE)):
            logging.warning(f"Market is open ({now_ist.strftime('%H:%M')}) but Kite WebSocket is not connected. This might indicate an issue with initial connection or reconnection.")

        # Check if it's time for End-of-Day processing
        if now_ist.hour > EOD_PROCESSING_HOUR or \
           (now_ist.hour == EOD_PROCESSING_HOUR and now_ist.minute >= EOD_PROCESSING_MINUTE):
            if not shutdown_event.is_set(): # Only trigger EOD once
                logging.info(f"EOD processing time detected ({now_ist.strftime('%H:%M')}). Stopping WebSocket and initiating data processing.")
                kws.stop() # Disconnect WebSocket gracefully
                shutdown_event.set() # Signal all other threads to prepare for shutdown
                break # Exit the market_session_manager loop
            else:
                logging.debug("EOD processing already triggered. Waiting for application shutdown.")

        # Sleep until the next minute mark or for a short interval
        time_to_sleep = (60 - now_ist.second) % 60
        if time_to_sleep == 0: time_to_sleep = 60 # Sleep for a full minute if at 0 second
        time.sleep(time_to_sleep)

# --- Main Script Execution Block ---
if __name__ == "__main__":
    logging.info("Starting Kite BankNifty F&O Data Collector Application...")

    # Step 1: Fetch credentials from AWS Secrets Manager
    credentials = get_kite_credentials()
    if not credentials:
        logging.error("Failed to retrieve Kite credentials from Secrets Manager. Exiting application.")
        exit(1) # Cannot proceed without valid credentials

    API_KEY = credentials.get("API_KEY")
    API_SECRET = credentials.get("API_SECRET")
    ACCESS_TOKEN = credentials.get("ACCESS_TOKEN")

    if not all([API_KEY, API_SECRET, ACCESS_TOKEN]):
        logging.error("Missing API_KEY, API_SECRET, or ACCESS_TOKEN in Secrets Manager. Please ensure the secret is correctly populated. Exiting.")
        exit(1)

    # Step 2: Initialize KiteConnect and KiteTicker with fetched credentials
    try:
        kite = KiteConnect(api_key=API_KEY)
        kite.set_access_token(ACCESS_TOKEN)
        # Initialize kws (KiteTicker) only after successful credential load
        kws = KiteTicker(API_KEY, ACCESS_TOKEN)
        # Assign callbacks to the kws instance
        kws.on_connect = on_connect
        kws.on_ticks = on_ticks
        kws.on_close = on_close
        kws.on_error = on_error
        kws.on_reconnect = on_reconnect
        kws.on_noreconnect = on_noreconnect

        logging.info("KiteConnect and KiteTicker initialized successfully with Secrets Manager credentials.")
    except Exception as e:
        logging.error(f"Error initializing KiteConnect or KiteTicker with fetched credentials: {e}")
        exit(1) # Exit if API client cannot be initialized

    # Step 3: Start background threads
    # The WebSocket connection thread (kws.connect() is blocking)
    websocket_thread = threading.Thread(target=kws.connect, daemon=True)
    websocket_thread.start()
    logging.info("Kite WebSocket connection attempt initiated in background thread.")

    # The periodic data saving thread
    periodic_saver_thread = threading.Thread(target=save_periodic_data, daemon=True)
    periodic_saver_thread.start()
    logging.info("Periodic data saver thread started.")

    # The market session manager thread (to control start/stop logic based on market hours)
    session_manager_thread = threading.Thread(target=market_session_manager, daemon=True)
    session_manager_thread.start()
    logging.info("Market session manager thread started.")

    # Step 4: Keep the main thread alive and wait for shutdown signal
    try:
        logging.info("Main thread waiting for shutdown signal (e.g., market close time or KeyboardInterrupt)...")
        shutdown_event.wait() # This will block until shutdown_event.set() is called by other threads

    except KeyboardInterrupt:
        logging.info("KeyboardInterrupt detected. Signaling for graceful shutdown.")
        shutdown_event.set() # If user presses Ctrl+C, initiate shutdown
    except Exception as e:
        logging.error(f"An unexpected error occurred in main thread: {e}")
        shutdown_event.set() # On any other unexpected error, attempt graceful shutdown

    # Step 5: Perform End-of-Day processing after shutdown signal is received
    logging.info("Shutdown event received. Giving a small buffer for threads to cease activities.")
    time.sleep(5) # Give a few seconds for background threads to react to shutdown_event

    process_eod_data() # Ensure EOD data processing runs after the shutdown signal

    logging.info("Application shutdown complete. Exiting.")