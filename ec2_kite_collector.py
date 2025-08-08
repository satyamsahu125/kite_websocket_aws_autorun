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

from kiteconnect import KiteConnect, KiteTicker

from dotenv import load_dotenv
load_dotenv()

# --- Configuration ---
AWS_REGION = "ap-south-1" # Ensure this matches your AWS region
SECRETS_MANAGER_SECRET_NAME = "KiteConnectBankniftyData" # Must match the secret name used by your local script

# AWS S3 Configuration
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "kitebanknifty20250808") # Set your S3 bucket name
S3_PREFIX = os.getenv("S3_PREFIX", "banknifty_data/") # Prefix for objects within the bucket
SAVE_TO_S3 = os.getenv("SAVE_TO_S3", "True").lower() == "true" 

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
    logging.info("Kite WebSocket connected. Subscribing to instruments...")
    try:
        # Fetch instrument tokens for Bank Nifty futures and options from Kite API
        # This can be heavy, consider caching or filtering more precisely
        instruments = kite.instruments("NFO")
        banknifty_futures_options_tokens = []

        # --- REFINED INSTRUMENT FILTERING LOGIC ---
        today = datetime.date.today()
        # You'll need to define how you want to select the current expiry for Futures and Options.
        # Example: Get the current (nearest) monthly future and current week's options.
        # This requires more robust logic for production to handle rollovers, weekly expiries accurately.

        # Simplified example: Get all BANKNIFTY Futures and Options for demonstration.
        # This will be a large list, so batching is important.
        # For a practical solution, you should identify specific expiries (e.g., nearest Thursday for options).
        # Example for getting instruments for current week (Bank Nifty Options expire Thursday)
        # Find the next Thursday
        days_until_thursday = (3 - today.weekday() + 7) % 7 # Thursday is weekday 3
        if days_until_thursday == 0: # If today is Thursday, it's this week's expiry
            current_expiry_date = today
        else:
            current_expiry_date = today + datetime.timedelta(days=days_until_thursday)

        for instrument in instruments:
            if instrument['segment'] == 'NFO' and 'BANKNIFTY' in instrument['tradingsymbol']:
                # Filter for Futures
                if instrument['instrument_type'] == 'FUT':
                    # Add current month future, or next month if near expiry
                    # For simplicity, adding all BANKNIFTY futures for now.
                    # YOU MUST REFINE THIS TO GET THE CORRECT CONTRACT
                    if instrument['name'] == 'BANKNIFTY' and instrument['instrument_type'] == 'FUT':
                         banknifty_futures_options_tokens.append(instrument['instrument_token'])
                         # logging.info(f"Adding Future: {instrument['tradingsymbol']}")
                # Filter for Options (CE/PE)
                elif instrument['instrument_type'] in ['CE', 'PE']:
                    # Filter by expiry date for current week options, and a reasonable strike range
                    # Example: strikes within +/- 2000 points of a hypothetical ATM (e.g., 47000)
                    # You'd typically calculate ATM dynamically based on current NIFTY/BANKNIFTY index.
                    if instrument['expiry'] == current_expiry_date: # Only current week's options
                        # Adjust strike range as needed. This is a broad range.
                        if instrument['strike'] >= 45000 and instrument['strike'] <= 50000: # Example strike range
                            banknifty_futures_options_tokens.append(instrument['instrument_token'])
                            # logging.info(f"Adding Option: {instrument['tradingsymbol']}")

        logging.info(f"Identified {len(banknifty_futures_options_tokens)} Bank Nifty F&O instruments for subscription.")

        if not banknifty_futures_options_tokens:
            logging.warning("No Bank Nifty F&O instruments found based on current filter logic. Data will not be collected.")
            # Do not stop ws here, let market_session_manager handle shutdown if needed
            return

        # Subscribe to tokens and set mode to FULL for comprehensive data (including market depth)
        batch_size = 300 # A reasonable batch size to prevent API rate limits
        for i in range(0, len(banknifty_futures_options_tokens), batch_size):
            batch = banknifty_futures_options_tokens[i:i + batch_size]
            ws.subscribe(batch)
            ws.set_mode(ws.MODE_FULL, batch) # MODE_FULL provides 10-level market depth
            logging.info(f"Subscribed to batch of {len(batch)} instruments.")
            time.sleep(0.1) # Small delay to be polite to the API

        logging.info(f"Successfully subscribed to {len(banknifty_futures_options_tokens)} instruments in MODE_FULL.")
    except Exception as e:
        logging.error(f"Error subscribing to instruments: {e}", exc_info=True) # Print traceback
        ws.stop() # Stop WebSocket on error

def on_ticks(ws, ticks):
    """Callback for receiving new market data ticks."""
    timestamp = datetime.datetime.now(IST) # Record timestamp in IST
    with data_lock: # Ensure thread-safe access to in_memory_ticks
        for tick in ticks:
            processed_tick = {
                'timestamp': timestamp,
                'instrument_token': tick.get('instrument_token'),
                'last_price': tick.get('last_price'),
                'ohlc_open': tick.get('ohlc', {}).get('open'),
                'ohlc_high': tick.get('ohlc', {}).get('high'),
                'ohlc_low': tick.get('ohlc', {}).get('low'),
                'ohlc_close': tick.get('ohlc', {}).get('close'),
                'volume': tick.get('volume'),
                'oi': tick.get('oi'),
                'depth_buy': json.dumps(tick.get('depth', {}).get('buy', [])),
                'depth_sell': json.dumps(tick.get('depth', {}).get('sell', []))
            }
            in_memory_ticks.append(processed_tick)
    # logging.debug(f"Received {len(ticks)} ticks. Total in memory: {len(in_memory_ticks)}")

def on_close(ws, code, reason):
    """Callback for WebSocket close event."""
    logging.info(f"Kite WebSocket closed. Code: {code}, Reason: {reason}")
    if not shutdown_event.is_set():
        logging.info("WebSocket closed. Triggering End-of-Day processing.")
        shutdown_event.set()

def on_error(ws, code, reason):
    """Callback for WebSocket error event."""
    logging.error(f"Kite WebSocket error. Code: {code}, Reason: {reason}")

def on_reconnect(ws, attempt_count):
    """Callback for WebSocket reconnection attempt."""
    logging.warning(f"Kite WebSocket reconnecting: Attempt {attempt_count}")

def on_noreconnect(ws):
    """Callback if WebSocket cannot reconnect."""
    logging.error("Kite WebSocket could not reconnect. Giving up.")
    shutdown_event.set()

# Initialize kws to None; it will be set after credentials are loaded in main
kws = None
# kite will also be set in main

# --- Periodic Saving Function ---
def save_periodic_data():
    """Periodically saves accumulated in-memory ticks to a temporary CSV file."""
    global in_memory_ticks
    while not shutdown_event.is_set():
        time.sleep(20)

        data_to_save = []
        with data_lock:
            if in_memory_ticks:
                data_to_save = list(in_memory_ticks)
                in_memory_ticks.clear()
                logging.info(f"Flushing {len(data_to_save)} ticks to temporary file.")
            else:
                logging.debug("No new ticks to save periodically.")
                continue

        if data_to_save:
            try:
                df = pd.DataFrame(data_to_save)
                filename = os.path.join(TEMP_DATA_DIR, f"ticks_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S_%f')}.csv")
                df.to_csv(filename, index=False)
                logging.info(f"Saved {len(data_to_save)} ticks to {filename}")
            except Exception as e:
                logging.error(f"Error saving periodic data: {e}", exc_info=True)

# --- End-of-Day (EOD) Processing and Parquet Conversion ---
def process_eod_data():
    """Consolidates all temporary data, cleans it, and saves it as a clean Parquet file."""
    logging.info("Starting End-of-Day data processing...")
    all_day_data = []
    temp_files = [f for f in os.listdir(TEMP_DATA_DIR) if f.endswith('.csv')]
    temp_files.sort()

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
            temp_files.append(os.path.basename(remaining_filename))
            logging.info(f"Saved remaining ticks to {remaining_filename}")
        except Exception as e:
            logging.error(f"Error saving remaining in-memory ticks for EOD: {e}", exc_info=True)

    temp_files = [f for f in os.listdir(TEMP_DATA_DIR) if f.endswith('.csv')]
    temp_files.sort()

    for fname in temp_files:
        filepath = os.path.join(TEMP_DATA_DIR, fname)
        try:
            df = pd.read_csv(filepath)
            all_day_data.append(df)
            os.remove(filepath)
            logging.debug(f"Processed and removed temporary file: {filepath}")
        except Exception as e:
            logging.error(f"Error reading or deleting temporary file {filepath}: {e}", exc_info=True)

    if all_day_data:
        consolidated_df = pd.concat(all_day_data, ignore_index=True)
        logging.info(f"Consolidated {len(consolidated_df)} total ticks from temporary files.")

        consolidated_df['timestamp'] = pd.to_datetime(consolidated_df['timestamp'])
        consolidated_df['timestamp'] = consolidated_df['timestamp'].dt.tz_convert(IST)
        consolidated_df.sort_values(by=['timestamp', 'instrument_token'], inplace=True)
        consolidated_df.drop_duplicates(inplace=True)
        logging.info(f"Cleaned data, total unique ticks: {len(consolidated_df)}")

        for col in ['last_price', 'ohlc_open', 'ohlc_high', 'ohlc_low', 'ohlc_close', 'volume', 'oi']:
            if col in consolidated_df.columns:
                consolidated_df[col] = pd.to_numeric(consolidated_df[col], errors='coerce').fillna(0)
        if 'instrument_token' in consolidated_df.columns:
            consolidated_df['instrument_token'] = consolidated_df['instrument_token'].astype('int64')

        eod_filename = os.path.join(FINAL_DATA_DIR, f"banknifty_fo_data_{datetime.date.today().strftime('%Y%m%d')}.parquet")
        try:
            consolidated_df.to_parquet(eod_filename, index=False, engine='pyarrow')
            logging.info(f"Daily Parquet file saved locally: {eod_filename}")

            if SAVE_TO_S3:
                upload_to_s3(eod_filename, S3_BUCKET_NAME, S3_PREFIX)

        except Exception as e:
            logging.error(f"Error saving Parquet file or uploading to S3: {e}", exc_info=True)
    else:
        logging.info("No data frames to consolidate for End-of-Day processing.")

def upload_to_s3(local_filepath, bucket_name, s3_prefix=""):
    """Uploads a file to an AWS S3 bucket."""
    s3_client = boto3.client('s3')
    object_name = s3_prefix + os.path.basename(local_filepath)
    try:
        s3_client.upload_file(local_filepath, bucket_name, object_name)
        logging.info(f"Successfully uploaded {local_filepath} to s3://{bucket_name}/{object_name}")
        os.remove(local_filepath)
        logging.info(f"Removed local file: {local_filepath}")
    except Exception as e:
        logging.error(f"Error uploading {local_filepath} to S3: {e}", exc_info=True)

# --- Market Session Control and Shutdown Logic ---
def market_session_manager():
    """Manages the market session, triggering EOD processing at close."""
    global kws # Allow global kws access to potentially stop it

    while not shutdown_event.is_set(): # Keep running until a shutdown is signaled
        now_ist = datetime.datetime.now(IST)

        # Log a warning if past market open but WebSocket is not connected
        # This check is more relevant if kws.connect() is blocking the main thread
        # The main thread starts kws.connect() after market_session_manager thread.
        # This warning might fire briefly until kws connects.
        if (not kws or not kws.is_connected()) and \
           (now_ist.hour > MARKET_OPEN_HOUR or \
           (now_ist.hour == MARKET_OPEN_HOUR and now_ist.minute >= MARKET_OPEN_MINUTE)) and \
           (now_ist.hour < EOD_PROCESSING_HOUR or \
           (now_ist.hour == EOD_PROCESSING_HOUR and now_ist.minute < EOD_PROCESSING_MINUTE)):
            logging.warning(f"Market is open ({now_ist.strftime('%H:%M')}) but Kite WebSocket is not connected. This might indicate an issue.")


        # Check if it's time for End-of-Day processing
        if now_ist.hour > EOD_PROCESSING_HOUR or \
           (now_ist.hour == EOD_PROCESSING_HOUR and now_ist.minute >= EOD_PROCESSING_MINUTE):
            if not shutdown_event.is_set():
                logging.info(f"EOD processing time detected ({now_ist.strftime('%H:%M')}). Stopping WebSocket and initiating data processing.")
                if kws and kws.is_connected():
                    kws.stop() # Disconnect WebSocket gracefully
                shutdown_event.set() # Signal all other threads to prepare for shutdown
                break # Exit the market_session_manager loop
            else:
                logging.debug("EOD processing already triggered. Waiting for application shutdown.")

        # Sleep until the next minute mark or for a short interval
        time_to_sleep = (60 - now_ist.second) % 60
        if time_to_sleep == 0: time_to_sleep = 60 # Ensure we sleep at least a minute if at 0 second
        time.sleep(time_to_sleep)

# --- Main Script Execution Block ---
if __name__ == "__main__":
    logging.info("Starting Kite BankNifty F&O Data Collector Application...")

    # Step 1: Fetch credentials from AWS Secrets Manager
    credentials = get_kite_credentials()
    if not credentials:
        logging.error("Failed to retrieve Kite credentials from Secrets Manager. Exiting application.")
        exit(1)

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
        kws = KiteTicker(API_KEY, ACCESS_TOKEN)
        
        # Assign callbacks to the kws instance (now that kws is initialized)
        kws.on_connect = on_connect
        kws.on_ticks = on_ticks
        kws.on_close = on_close
        kws.on_error = on_error
        kws.on_reconnect = on_reconnect
        kws.on_noreconnect = on_noreconnect

        logging.info("KiteConnect and KiteTicker initialized successfully with Secrets Manager credentials.")
    except Exception as e:
        logging.error(f"Error initializing KiteConnect or KiteTicker with fetched credentials: {e}", exc_info=True)
        exit(1)

    # Step 3: Start background threads (before kws.connect() which will block main thread)
    periodic_saver_thread = threading.Thread(target=save_periodic_data, daemon=True)
    periodic_saver_thread.start()
    logging.info("Periodic data saver thread started.")

    session_manager_thread = threading.Thread(target=market_session_manager, daemon=True)
    session_manager_thread.start()
    logging.info("Market session manager thread started.")

    # Step 4: Connect Kite WebSocket in the main thread (this is blocking)
    logging.info("Attempting to connect Kite WebSocket in the main thread...")
    try:
        kws.connect() # This call blocks until WebSocket disconnects or error
    except KeyboardInterrupt:
        logging.info("KeyboardInterrupt detected in main thread. Signaling for graceful shutdown.")
        shutdown_event.set()
    except Exception as e:
        logging.error(f"An unexpected error occurred during WebSocket connection: {e}", exc_info=True)
        shutdown_event.set() # Signal shutdown on any unexpected error

    # Step 5: After kws.connect() returns (i.e., WebSocket closed), proceed to EOD processing
    logging.info("WebSocket connection terminated. Proceeding with shutdown sequence.")
    shutdown_event.set() # Ensure all threads know to shut down

    # Give a small buffer for background threads to react to shutdown_event
    time.sleep(5)

    process_eod_data() # Ensure EOD data processing runs after the WebSocket stops

    logging.info("Application shutdown complete. Exiting.")