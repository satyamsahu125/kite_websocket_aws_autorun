
import os
import datetime
import pandas as pd
import threading
import time
import json
import logging
import pytz # For timezone handling
import pyarrow.parquet as pq # For Parquet file format
import pyarrow as pa # Dependency for pyarrow.parquet
import boto3 # For S3 and Secrets Manager integration

from kiteconnect import KiteConnect, KiteTicker # Zerodha Kite Connect API library

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
# These define when the script should attempt to collect data and perform EOD processing
MARKET_OPEN_HOUR = 9
MARKET_OPEN_MINUTE = 15 # Market opens at 9:15 AM IST
MARKET_CLOSE_HOUR = 15 # Official Equity/F&O close is 3:30 PM IST
MARKET_CLOSE_MINUTE = 30
EOD_PROCESSING_HOUR = 15 # Time to start End-of-Day processing (e.g., 3:45 PM IST)
EOD_PROCESSING_MINUTE = 45 

# Timezone for market hours calculation
IST = pytz.timezone('Asia/Kolkata') 

# Ensure local data directories exist on EC2 instance's file system
os.makedirs(TEMP_DATA_DIR, exist_ok=True)
os.makedirs(FINAL_DATA_DIR, exist_ok=True)

# --- Logging Setup ---
# Configures logging to print messages to the console (which systemd redirects to /var/log/kite_collector.log)
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    handlers=[logging.StreamHandler()])

# Global variables for real-time data storage and control signals
in_memory_ticks = [] # List to temporarily hold incoming tick data
data_lock = threading.Lock() # A lock to ensure thread-safe access to in_memory_ticks
shutdown_event = threading.Event() # A flag to signal graceful shutdown across threads

# Initialize KiteConnect and KiteTicker as global variables, set in main execution block
kite = None 
kws = None 

# --- Function to fetch credentials from AWS Secrets Manager ---
def get_kite_credentials():
    """
    Fetches Kite Connect API credentials (API_KEY, API_SECRET, ACCESS_TOKEN)
    from AWS Secrets Manager.
    """
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
            logging.error("Secret retrieved from Secrets Manager is not a string type. It must be a JSON string.")
            return None
    except Exception as e:
        logging.error(f"Error retrieving Kite credentials from Secrets Manager: {e}", exc_info=True)
        return None

def on_connect(ws, response):
    """
    Callback function executed when the WebSocket connection is established.
    It subscribes to the desired Bank Nifty F&O instruments.
    """
    logging.info("Kite WebSocket connected. Attempting to subscribe to instruments...")
    try:  
        # Fetch all F&O instruments from Kite Connect
        instruments = kite.instruments("NFO") 
        banknifty_futures_options_tokens = []
        today_date = datetime.date.today()
        
        logging.info(f"Total NFO instruments fetched: {len(instruments)}")
        logging.info(f"Current date: {today_date}")
        
        # --- Enhanced Diagnostic Block ---
        logging.info("--- Enhanced Diagnostic: BANKNIFTY instruments ---")
        futures_found = []
        options_by_expiry = {}
        
        for instrument in instruments:
            if instrument.get('name') == 'BANKNIFTY':
                if instrument['instrument_type'] == 'FUT':
                    futures_found.append(instrument)
                elif instrument['instrument_type'] in ['CE', 'PE']:
                    expiry = instrument['expiry']
                    if expiry not in options_by_expiry:
                        options_by_expiry[expiry] = []
                    options_by_expiry[expiry].append(instrument)
        
        logging.info(f"Found {len(futures_found)} futures contracts:")
        for fut in futures_found:
            logging.info(f"  Future: {fut['tradingsymbol']} | Expiry: {fut['expiry']} | Token: {fut['instrument_token']}")
        
        logging.info(f"Found options for {len(options_by_expiry)} different expiry dates:")
        for expiry, opts in sorted(options_by_expiry.items()):
            logging.info(f"  Expiry {expiry}: {len(opts)} options")
            if len(opts) <= 10:  # Show details for small groups
                for opt in opts[:5]:  # Show first 5
                    logging.info(f"    {opt['tradingsymbol']} | Strike: {opt['strike']}")
        
        logging.info("--- End Enhanced Diagnostic ---")

        # --- IMPROVED FUTURES FILTERING ---
        logging.info("=== FUTURES FILTERING ===")
        
        # Sort futures by expiry to get nearest contracts
        futures_found.sort(key=lambda x: x['expiry'])
        
        if futures_found:
            # Add the first (nearest expiry) future
            nearest_future = futures_found[0]
            banknifty_futures_options_tokens.append(nearest_future['instrument_token'])
            logging.info(f"✓ Added nearest Future: {nearest_future['tradingsymbol']} (Expiry: {nearest_future['expiry']})")
            
            # Add next month future if available
            if len(futures_found) > 1:
                next_future = futures_found[1]
                banknifty_futures_options_tokens.append(next_future['instrument_token'])
                logging.info(f"✓ Added next Future: {next_future['tradingsymbol']} (Expiry: {next_future['expiry']})")
        else:
            logging.warning("❌ No futures found after enhanced filtering")

        # --- GET ATM PRICE ---
        logging.info("=== FETCHING ATM PRICE ===")
        atm_price_rough = None
        
        try:
            symbols_to_try = ["NSE:NIFTY BANK", "NSE:BANKNIFTY"]
            for symbol in symbols_to_try:
                try:
                    ltp_data = kite.ltp([symbol])
                    spot_price = ltp_data[symbol]["last_price"]
                    atm_price_rough = round(spot_price / 100) * 100
                    logging.info(f"✓ Fetched {symbol}: {spot_price}, ATM: {atm_price_rough}")
                    break
                except Exception as e:
                    logging.warning(f"Failed {symbol}: {e}")
                    continue
        except Exception as e:
            logging.error(f"Price fetch failed: {e}")
        
        if not atm_price_rough:
            atm_price_rough = 55000  # Fallback based on current market levels
            logging.warning(f"Using fallback ATM: {atm_price_rough}")

        # --- IMPROVED OPTIONS FILTERING ---
        logging.info("=== OPTIONS FILTERING ===")
        
        # Define strike range
        strike_range_points = 2000  # Increased range for better coverage
        min_strike = atm_price_rough - strike_range_points
        max_strike = atm_price_rough + strike_range_points
        
        logging.info(f"ATM: {atm_price_rough}, Strike range: {min_strike} to {max_strike}")
        
        # Strategy 1: Find options expiring in the next 7-14 days (current week or next week)
        target_date_min = today_date + datetime.timedelta(days=1)  # Tomorrow
        target_date_max = today_date + datetime.timedelta(days=14)  # Two weeks
        
        weekly_options_added = 0
        for expiry, options_list in options_by_expiry.items():
            if target_date_min <= expiry <= target_date_max:
                logging.info(f"Checking expiry {expiry} ({len(options_list)} options)")
                
                options_for_this_expiry = 0
                for opt in options_list:
                    if min_strike <= opt['strike'] <= max_strike:
                        banknifty_futures_options_tokens.append(opt['instrument_token'])
                        options_for_this_expiry += 1
                        if options_for_this_expiry <= 5:  # Log first few
                            logging.info(f"  ✓ Added: {opt['tradingsymbol']} (Strike: {opt['strike']})")
                
                if options_for_this_expiry > 5:
                    logging.info(f"  ✓ Added {options_for_this_expiry - 5} more options for expiry {expiry}")
                
                weekly_options_added += options_for_this_expiry
                
                # Limit to avoid too many subscriptions
                if weekly_options_added >= 100:
                    logging.info(f"Reached weekly options limit (100), stopping at expiry {expiry}")
                    break
        
        logging.info(f"Total weekly/near-term options added: {weekly_options_added}")
        
        # Strategy 2: If insufficient weekly options, add monthly options
        monthly_options_added = 0
        if weekly_options_added < 20:  # If we have fewer than 20 options, add monthly
            logging.info("Adding monthly options as supplement...")
            
            # Find expiries 15-45 days out (monthly contracts)
            monthly_min = today_date + datetime.timedelta(days=15)
            monthly_max = today_date + datetime.timedelta(days=45)
            
            for expiry, options_list in options_by_expiry.items():
                if monthly_min <= expiry <= monthly_max:
                    logging.info(f"Adding monthly options for expiry {expiry}")
                    
                    # Be more selective for monthly - closer to ATM
                    monthly_min_strike = atm_price_rough - 1000
                    monthly_max_strike = atm_price_rough + 1000
                    
                    for opt in options_list:
                        if monthly_min_strike <= opt['strike'] <= monthly_max_strike:
                            banknifty_futures_options_tokens.append(opt['instrument_token'])
                            monthly_options_added += 1
                            if monthly_options_added <= 10:  # Log first few monthly
                                logging.info(f"  ✓ Monthly: {opt['tradingsymbol']} (Strike: {opt['strike']})")
                    
                    if monthly_options_added >= 50:  # Limit monthly options
                        break
        
        logging.info(f"Total monthly options added: {monthly_options_added}")

        # --- FINAL FALLBACK ---
        total_instruments = len(banknifty_futures_options_tokens)
        logging.info(f"=== SUMMARY ===")
        logging.info(f"Total instruments selected: {total_instruments}")
        
        if total_instruments == 0:
            logging.warning("No instruments found with normal logic, using emergency fallback...")
            
            # Emergency fallback: Add any BANKNIFTY instruments available
            fallback_count = 0
            for instrument in instruments:
                if (instrument.get('name') == 'BANKNIFTY' and 
                    instrument.get('instrument_type') in ['FUT', 'CE', 'PE']):
                    banknifty_futures_options_tokens.append(instrument['instrument_token'])
                    fallback_count += 1
                    logging.info(f"Emergency fallback: {instrument.get('tradingsymbol')}")
                    if fallback_count >= 20:  # Limit emergency fallback
                        break
            
            logging.info(f"Emergency fallback added: {fallback_count} instruments")
        
        # --- SUBSCRIPTION ---
        if banknifty_futures_options_tokens:
            logging.info(f"Proceeding to subscribe to {len(banknifty_futures_options_tokens)} instruments")
            
            # Subscribe in batches
            batch_size = 200  # Reduced batch size for stability
            for i in range(0, len(banknifty_futures_options_tokens), batch_size):
                batch = banknifty_futures_options_tokens[i:i + batch_size]
                ws.subscribe(batch)
                ws.set_mode(ws.MODE_FULL, batch)
                logging.info(f"Subscribed to batch {i//batch_size + 1}: {len(batch)} instruments")
                time.sleep(0.2)  # Small delay between batches
            
            logging.info(f"✅ Successfully subscribed to {len(banknifty_futures_options_tokens)} instruments")
        else:
            logging.error("❌ No instruments to subscribe to!")
        
    except Exception as e:
        logging.error(f"❌ Error in on_connect: {e}", exc_info=True)
        ws.stop()
def on_ticks(ws, ticks):
    """
    Callback function executed when new market data ticks are received.
    It processes and stores the ticks in memory.
    """
    # Record the local timestamp in IST when ticks are received
    timestamp = datetime.datetime.now(IST) 
    with data_lock: # Use lock for thread-safe access to the shared list
        for tick in ticks:
            # Extract relevant data points from the tick object
            # Tick structure reference: https://kite.trade/docs/connect/v3/websocket/#market-data
            processed_tick = {
                'timestamp': timestamp,
                'instrument_token': tick.get('instrument_token'),
                'last_price': tick.get('last_price'),
                'ohlc_open': tick.get('ohlc', {}).get('open'), 
                'ohlc_high': tick.get('ohlc', {}).get('high'),
                'ohlc_low': tick.get('ohlc', {}).get('low'),
                'ohlc_close': tick.get('ohlc', {}).get('close'),
                'volume': tick.get('volume'),
                'oi': tick.get('oi'), # Open Interest
                # Store market depth as JSON strings for easier storage in CSV/Parquet
                'depth_buy': json.dumps(tick.get('depth', {}).get('buy', [])),
                'depth_sell': json.dumps(tick.get('depth', {}).get('sell', []))
            }
            in_memory_ticks.append(processed_tick)
    # logging.debug(f"Received {len(ticks)} ticks. Total in memory: {len(in_memory_ticks)}") # Use debug for high volume logs

def on_close(ws, code, reason):
    """Callback function executed when the WebSocket connection is closed."""
    logging.info(f"Kite WebSocket closed. Code: {code}, Reason: {reason}")
    # Signal the main thread to initiate EOD processing if closure is not planned
    if not shutdown_event.is_set():
        logging.info("WebSocket closed. Triggering End-of-Day processing.")
        shutdown_event.set() # Set the event to signal all threads to shutdown gracefully

def on_error(ws, code, reason):
    """Callback function executed when a WebSocket error occurs."""
    logging.error(f"Kite WebSocket error. Code: {code}, Reason: {reason}")

def on_reconnect(ws, attempt_count):
    """Callback function executed when a WebSocket reconnection attempt occurs."""
    logging.warning(f"Kite WebSocket reconnecting: Attempt {attempt_count}")

def on_noreconnect(ws):
    """Callback function executed if WebSocket cannot reconnect after multiple attempts."""
    logging.error("Kite WebSocket could not reconnect. Giving up.")
    shutdown_event.set() # Signal shutdown as data collection cannot continue

# --- Periodic Saving Function ---
def save_periodic_data():
    """
    Runs in a separate thread. Periodically saves accumulated in-memory ticks
    to a temporary CSV file and clears the memory buffer.
    """
    global in_memory_ticks
    # Continue running until a shutdown signal is received
    while not shutdown_event.is_set(): 
        time.sleep(20) # Save every 20 seconds

        data_to_save = []
        with data_lock: # Acquire lock before accessing shared memory
            if in_memory_ticks:
                data_to_save = list(in_memory_ticks) # Take a snapshot
                in_memory_ticks.clear() # Clear the buffer
                logging.info(f"Flushing {len(data_to_save)} ticks to temporary file.")
            else:
                logging.debug("No new ticks to save periodically.")
                continue # Skip if no new data

        if data_to_save:
            try:
                df = pd.DataFrame(data_to_save)
                # Generate a unique filename based on current timestamp
                filename = os.path.join(TEMP_DATA_DIR, f"ticks_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S_%f')}.csv")
                df.to_csv(filename, index=False)
                logging.info(f"Saved {len(data_to_save)} ticks to {filename}")
            except Exception as e:
                logging.error(f"Error saving periodic data: {e}", exc_info=True)

# --- End-of-Day (EOD) Processing and Parquet Conversion ---
def process_eod_data():
    """
    Consolidates all temporary CSV files collected throughout the day,
    performs data cleaning, and saves the consolidated data as a clean Parquet file.
    Optionally uploads the Parquet file to AWS S3.
    """
    logging.info("Starting End-of-Day data processing...")
    all_day_data = []
    # Get all temporary CSV files in the directory
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
            # Save the last flush to a distinct temporary file
            remaining_filename = os.path.join(TEMP_DATA_DIR, f"ticks_last_flush_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S_%f')}.csv")
            df_remaining.to_csv(remaining_filename, index=False)
            temp_files.append(os.path.basename(remaining_filename)) # Add this file to the list for processing
            logging.info(f"Saved remaining ticks to {remaining_filename}")
        except Exception as e:
            logging.error(f"Error saving remaining in-memory ticks for EOD: {e}", exc_info=True)

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
            logging.error(f"Error reading or deleting temporary file {filepath}: {e}", exc_info=True)

    if all_day_data:
        # Concatenate all DataFrames into one master DataFrame for the day
        consolidated_df = pd.concat(all_day_data, ignore_index=True)
        logging.info(f"Consolidated {len(consolidated_df)} total ticks from temporary files.")

        # --- Data Cleaning and Type Conversion ---
        # Convert timestamp column to datetime objects and localize to IST
        consolidated_df['timestamp'] = pd.to_datetime(consolidated_df['timestamp'])
        consolidated_df['timestamp'] = consolidated_df['timestamp'].dt.tz_convert(IST)
        # Sort data by timestamp and instrument token for chronological order
        consolidated_df.sort_values(by=['timestamp', 'instrument_token'], inplace=True)
        # Remove any potential duplicate ticks (e.g., from reconnections or re-processing)
        consolidated_df.drop_duplicates(inplace=True) 
        logging.info(f"Cleaned data, total unique ticks: {len(consolidated_df)}")

        # Convert numeric columns to appropriate types, handling potential errors and NaNs
        for col in ['last_price', 'ohlc_open', 'ohlc_high', 'ohlc_low', 'ohlc_close', 'volume', 'oi']:
            if col in consolidated_df.columns:
                # Use errors='coerce' to turn unparseable values into NaN, then fill NaN with 0
                consolidated_df[col] = pd.to_numeric(consolidated_df[col], errors='coerce').fillna(0) 
        if 'instrument_token' in consolidated_df.columns:
            consolidated_df['instrument_token'] = consolidated_df['instrument_token'].astype('int64')

        # --- Save to Parquet File ---
        # Generate a daily-based filename for the Parquet file
        eod_filename = os.path.join(FINAL_DATA_DIR, f"banknifty_fo_data_{datetime.date.today().strftime('%Y%m%d')}.parquet")
        try:
            # Save the DataFrame to a Parquet file for efficient storage and querying
            consolidated_df.to_parquet(eod_filename, index=False, engine='pyarrow')
            logging.info(f"Daily Parquet file saved locally: {eod_filename}")

            # --- Upload to S3 (if enabled) ---
            if SAVE_TO_S3:
                upload_to_s3(eod_filename, S3_BUCKET_NAME, S3_PREFIX)

        except Exception as e:
            logging.error(f"Error saving Parquet file or uploading to S3: {e}", exc_info=True)
    else:
        logging.info("No data frames to consolidate for End-of-Day processing.")

def upload_to_s3(local_filepath, bucket_name, s3_prefix=""):
    """
    Uploads a local file to a specified AWS S3 bucket.
    """
    s3_client = boto3.client('s3')
    # Construct the S3 object key (path in S3)
    object_name = s3_prefix + os.path.basename(local_filepath) 
    try:
        s3_client.upload_file(local_filepath, bucket_name, object_name)
        logging.info(f"Successfully uploaded {local_filepath} to s3://{bucket_name}/{object_name}")
        # Optionally, remove the local file after successful upload to save disk space on EC2
        os.remove(local_filepath) 
        logging.info(f"Removed local file: {local_filepath}")
    except Exception as e:
        logging.error(f"Error uploading {local_filepath} to S3: {e}", exc_info=True)

# --- Market Session Control and Shutdown Logic ---
def market_session_manager():
    """
    Manages the market session. It continuously checks the current time
    and triggers the End-of-Day processing at the specified market close time.
    """
    global kws # Access the global KiteTicker instance
    while not shutdown_event.is_set(): # Keep running until a shutdown is signaled
        now_ist = datetime.datetime.now(IST)

        # Log a warning if past market open but WebSocket is not connected
        # This check is primarily to alert if connection fails during active market hours
        if (not kws or not kws.is_connected()) and \
           (now_ist.hour > MARKET_OPEN_HOUR or \
           (now_ist.hour == MARKET_OPEN_HOUR and now_ist.minute >= MARKET_OPEN_MINUTE)) and \
           (now_ist.hour < MARKET_CLOSE_HOUR or \
           (now_ist.hour == MARKET_CLOSE_HOUR and now_ist.minute < MARKET_CLOSE_MINUTE)):
            logging.warning(f"Market is open ({now_ist.strftime('%H:%M')}) but Kite WebSocket is not connected. This might indicate an issue with connection or reconnection.")


        # Check if it's time for End-of-Day processing
        if now_ist.hour > EOD_PROCESSING_HOUR or \
           (now_ist.hour == EOD_PROCESSING_HOUR and now_ist.minute >= EOD_PROCESSING_MINUTE):
            if not shutdown_event.is_set(): # Ensure EOD is only triggered once
                logging.info(f"EOD processing time detected ({now_ist.strftime('%H:%M')}). Stopping WebSocket and initiating data processing.")
                if kws and kws.is_connected():
                    kws.stop() # Disconnect WebSocket gracefully
                shutdown_event.set() # Signal all other threads to prepare for shutdown
                break # Exit the market_session_manager loop
            else:
                logging.debug("EOD processing already triggered. Waiting for application shutdown.")

        # Sleep until the next minute mark or for a short interval
        time_to_sleep = (60 - now_ist.second) % 60
        if time_to_sleep == 0: time_to_sleep = 60 # Ensure we sleep at least a minute if currently at 0 second
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

    # Validate that all required credentials were retrieved
    if not all([API_KEY, API_SECRET, ACCESS_TOKEN]):
        logging.error("Missing API_KEY, API_SECRET, or ACCESS_TOKEN in Secrets Manager. Please ensure the secret is correctly populated. Exiting.")
        exit(1)

    # Step 2: Initialize KiteConnect and KiteTicker with fetched credentials
    try:
        kite = KiteConnect(api_key=API_KEY)
        kite.set_access_token(ACCESS_TOKEN)
        kws = KiteTicker(API_KEY, ACCESS_TOKEN)
        
        # Assign WebSocket callbacks to the KiteTicker instance
        kws.on_connect = on_connect
        kws.on_ticks = on_ticks
        kws.on_close = on_close
        kws.on_error = on_error
        kws.on_reconnect = on_reconnect
        kws.on_noreconnect = on_noreconnect

        logging.info("KiteConnect and KiteTicker initialized successfully with Secrets Manager credentials.")
    except Exception as e:
        logging.error(f"Error initializing KiteConnect or KiteTicker with fetched credentials: {e}", exc_info=True)
        exit(1) # Exit if API client cannot be initialized

    # Step 3: Start background threads (These must start BEFORE kws.connect())
    
    # Thread for periodic data saving
    periodic_saver_thread = threading.Thread(target=save_periodic_data, daemon=True)
    periodic_saver_thread.start()
    logging.info("Periodic data saver thread started.")

    # Thread for managing market session times and triggering EOD
    session_manager_thread = threading.Thread(target=market_session_manager, daemon=True)
    session_manager_thread.start()
    logging.info("Market session manager thread started.")

    # Step 4: Connect Kite WebSocket in the main thread (this is blocking)
    # kws.connect() must be in the main thread to avoid 'signal only works in main thread' error
    logging.info("Attempting to connect Kite WebSocket in the main thread...")
    try:
        kws.connect() # This call blocks until the WebSocket disconnects or an unhandled error occurs
    except KeyboardInterrupt:
        logging.info("KeyboardInterrupt detected in main thread. Signaling for graceful shutdown.")
        shutdown_event.set() # Set the event to signal immediate shutdown if Ctrl+C is pressed
    except Exception as e:
        logging.error(f"An unexpected error occurred during WebSocket connection: {e}", exc_info=True)
        shutdown_event.set() # Signal shutdown on any unexpected error

    # Step 5: After kws.connect() returns (i.e., WebSocket closed), proceed to EOD processing
    logging.info("WebSocket connection terminated. Proceeding with shutdown sequence.")
    shutdown_event.set() # Ensure all threads know to shut down before final processing

    # Give a small buffer for background threads to react to the shutdown_event
    time.sleep(5) 

    process_eod_data() # Perform End-of-Day data processing and saving

    logging.info("Application shutdown complete. Exiting.")