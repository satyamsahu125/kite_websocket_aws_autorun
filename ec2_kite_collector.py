#satyamsahu
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

instrument_mapping = {}

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

def is_market_open():


    now_ist = datetime.datetime.now(IST)
    current_time = now_ist.time()

    market_open_time = datetime.time(MARKET_OPEN_HOUR,MARKET_OPEN_MINUTE)
    market_close_time = datetime.time(MARKET_CLOSE_HOUR, MARKET_CLOSE_MINUTE)


    is_open = market_open_time <= current_time <= market_close_time

    is_weekday = now_ist.weekday() < 5


    return is_open and is_weekday

def is_eod_time():

    now_ist = datetime.datetime.now(IST)
    eod_time = datetime.time (EOD_PROCESSING_HOUR , EOD_PROCESSING_MINUTE)
    current_time = now_ist.time()

    return current_time > eod_time

def calculate_days_to_expiry(expiry_date):

    today = datetime.date.today()

    if isinstance(expiry_date, datetime.date):
        return(expiry_date - today).days
    return 0



def on_connect(ws, response):
    


    if not is_market_open():
        logging.warning("Market is currently closed. Skipping instrument subscription .")
        logging.info(f"Current time : {datetime.datetime.now(IST).strftime('%H:%M:%S')}")
        logging.info(f"Market hours : {MARKET_OPEN_HOUR:02d}:{MARKET_OPEN_MINUTE:02d} to {MARKET_CLOSE_HOUR:02d}:{MARKET_CLOSE_MINUTE:02d}")
        return
    
    logging.info("Kite WebSocket connected. Market is open .Attempting to subscribe to instruments...")
    
    global instrument_mapping

    try:  
        # Fetch all F&O instruments from Kite Connect
        instruments = kite.instruments("NFO") 
        banknifty_futures_options_tokens = []
        today_date = datetime.date.today()
        
        logging.info(f"Total NFO instruments fetched: {len(instruments)}")
        logging.info(f"Current date: {today_date}")
        
        logging.info("Building instrument mapping dictionary...")
        for instrument in instruments:
            if instrument.get('name') == 'BANKNIFTY':
                instrument_mapping[instrument['instrument_token']] = {

                    'trading_symbol': instrument.get('tradingsymbol', ''),
                    'instrument_type': instrument.get('instrument_type', ''),
                    'strike': instrument.get('strike', 0),
                    'expiry': instrument.get('expiry'),
                    'exchange': instrument.get('exchange', ''),
                    'name': instrument.get('name', ''),
                    'days_to_expiry': calculate_days_to_expiry(instrument.get('expiry')) if instrument.get('expiry') else 0
                }

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
        
        

        monthly_futures = []

        for fut in futures_found:

            monthly_futures.append(fut)

        monthly_futures.sort(key=lambda x : x['expiry'])

        if monthly_futures:

            nearest_future = monthly_futures[0]
            banknifty_futures_options_tokens.append(nearest_future['instrument_token'])
            logging.info(f"✓ Added nearest Monthly Future: {nearest_future['tradingsymbol']} (Expiry: {nearest_future['expiry']})")

            if len(monthly_futures) > 1:
                next_future = monthly_futures[1]
                banknifty_futures_options_tokens.append(next_future['instrument_token'])
                logging.info(f" Added next Monthly Future: {next_future['tradingsymbol']} (Expiry: {next_future['expiry']})")
                
        else:
            logging.warning(" No monthly futures found")


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
                    logging.info(f"Fetched {symbol}: {spot_price}, ATM: {atm_price_rough}")
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
        
        # Strategy 2: If insufficient weekly options, add monthly options
        monthly_options_added = 0
        monthly_min = today_date + datetime.timedelta(days=7)  # At least a week out
        monthly_max = today_date + datetime.timedelta(days=45)
        for expiry, options_list in options_by_expiry.items():
            if monthly_min <= expiry <= monthly_max:
                logging.info(f"Processing monthly options for expiry {expiry} ({len(options_list)} options)")
                
                options_for_this_expiry = 0
                for opt in options_list:
                    if min_strike <= opt['strike'] <= max_strike:
                        banknifty_futures_options_tokens.append(opt['instrument_token'])
                        options_for_this_expiry += 1
                        if options_for_this_expiry <= 5:  # Log first few
                            logging.info(f"  ✓ Added: {opt['tradingsymbol']} (Strike: {opt['strike']}, Days to expiry: {calculate_days_to_expiry(opt['expiry'])})")
                
                if options_for_this_expiry > 5:
                    logging.info(f"  ✓ Added {options_for_this_expiry - 5} more options for expiry {expiry}")
                
                monthly_options_added += options_for_this_expiry
                
                # Limit to avoid too many subscriptions
                if monthly_options_added >= 150:
                    logging.info(f"Reached monthly options limit (150), stopping at expiry {expiry}")
                    break
        
        logging.info(f"Total monthly options added: {monthly_options_added}")

        # --- FINAL FALLBACK ---
        total_instruments = len(banknifty_futures_options_tokens)
        logging.info(f"=== SUBSCRIPTION SUMMARY ===")
        logging.info(f"Total instruments selected: {total_instruments}")
        
        if total_instruments == 0:
            logging.warning("⚠️ No instruments found with normal logic, using emergency fallback...")
            
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
            logging.info(f" Proceeding to subscribe to {len(banknifty_futures_options_tokens)} instruments")
            
            # Subscribe in batches
            batch_size = 200  # Batch size for stability
            for i in range(0, len(banknifty_futures_options_tokens), batch_size):
                batch = banknifty_futures_options_tokens[i:i + batch_size]
                ws.subscribe(batch)
                ws.set_mode(ws.MODE_FULL, batch)
                logging.info(f" Subscribed to batch {i//batch_size + 1}: {len(batch)} instruments")
                time.sleep(0.2)  # Small delay between batches
            
            logging.info(f" Successfully subscribed to {len(banknifty_futures_options_tokens)} instruments")
        else:
            logging.error(" No instruments to subscribe to!")
        
    except Exception as e:
        logging.error(f" Error in on_connect: {e}", exc_info=True)
        ws.stop()
        
def on_ticks(ws, ticks):
    
    timestamp = datetime.datetime.now(IST) 
    with data_lock: 
        for tick in ticks:
            # Extract relevant data points from the tick object
            # Tick structure reference: https://kite.trade/docs/connect/v3/websocket/#market-data
            instrument_token = tick.get('instrument_token')
            instrument_details = instrument_mapping.get(instrument_token, {})
            
            # Extract relevant data points from the tick object and add instrument details
            processed_tick = {
                'timestamp': timestamp,
                'instrument_token': instrument_token,
                # ADDED: Include instrument details in each tick
                'trading_symbol': instrument_details.get('trading_symbol', ''),
                'instrument_type': instrument_details.get('instrument_type', ''),
                'strike': instrument_details.get('strike', 0),
                'expiry': instrument_details.get('expiry'),
                'days_to_expiry': instrument_details.get('days_to_expiry', 0),
                'exchange': instrument_details.get('exchange', ''),
                'name': instrument_details.get('name', ''),
                # Market data
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
             
    #logging.debug(f"Received {len(ticks)} ticks. Total in memory: {len(in_memory_ticks)}") # Use debug for high volume logs

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
    Memory-efficient EOD processing that handles large datasets in chunks.
    Drop-in replacement for the original function.
    """
    logging.info("Starting End-of-Day data processing...")
    
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
            logging.info(f"Saved remaining ticks to {remaining_filename}")
            del df_remaining, remaining_ticks  # Free memory immediately
        except Exception as e:
            logging.error(f"Error saving remaining in-memory ticks for EOD: {e}", exc_info=True)

    # Scan temp files to process final 
    temp_files = [f for f in os.listdir(TEMP_DATA_DIR) if f.endswith('.csv')]
    temp_files.sort()

    if not temp_files:
        logging.info("No data frames to consolidate for End-of-Day processing.")
        return

    logging.info(f"Found {len(temp_files)} temporary files to process")
    
    # Generate a daily-based filename for the Parquet file
    eod_filename = os.path.join(FINAL_DATA_DIR, f"banknifty_fo_data_{datetime.date.today().strftime('%Y%m%d')}.parquet")
    
    try:
        # Process files in chunks to avoid memory overload
        CHUNK_SIZE = 15  # Process 15 files at a time (adjust based on your system)
        total_rows_processed = 0
        
        # Create a temporary parquet writer for efficient chunk-by-chunk writing
        writer = None
        schema = None
        
        for i in range(0, len(temp_files), CHUNK_SIZE):
            chunk_files = temp_files[i:i + CHUNK_SIZE]
            chunk_num = i//CHUNK_SIZE + 1
            total_chunks = (len(temp_files) - 1) // CHUNK_SIZE + 1
            
            logging.info(f"Processing chunk {chunk_num}/{total_chunks}: {len(chunk_files)} files")
            
            # Load this chunk of files
            chunk_data = []
            chunk_rows = 0
            
            for fname in chunk_files:
                filepath = os.path.join(TEMP_DATA_DIR, fname)
                try:
                    df = pd.read_csv(filepath)
                    chunk_data.append(df)
                    chunk_rows += len(df)
                    logging.debug(f"Loaded {fname}: {len(df)} rows")
                except Exception as e:
                    logging.error(f"Error reading temporary file {filepath}: {e}", exc_info=True)
                    continue
            
            if not chunk_data:
                logging.warning(f"No valid data in chunk {chunk_num}")
                continue
            
            # Process this chunk
            try:
                # Concatenate chunk data
                logging.info(f"Consolidating chunk {chunk_num}: {chunk_rows} rows")
                chunk_df = pd.concat(chunk_data, ignore_index=True)
                del chunk_data  # Free memory immediately
                
                # --- Data Cleaning and Type Conversion for this chunk ---
                logging.info(f"Cleaning chunk {chunk_num} data...")
                
                # Convert timestamp column to datetime objects and localize to IST
                chunk_df['timestamp'] = pd.to_datetime(chunk_df['timestamp'])
                if chunk_df['timestamp'].dt.tz is not None:
                    chunk_df['timestamp'] = chunk_df['timestamp'].dt.tz_convert(IST)
                else:
                    chunk_df['timestamp'] = chunk_df['timestamp'].dt.tz_localize(IST)
                
                # Sort data by timestamp and instrument token for chronological order
                chunk_df.sort_values(by=['timestamp', 'instrument_token'], inplace=True)
                
                # Remove any potential duplicate ticks within this chunk
                before_dedup = len(chunk_df)
                chunk_df.drop_duplicates(inplace=True)
                after_dedup = len(chunk_df)
                if before_dedup != after_dedup:
                    logging.info(f"Removed {before_dedup - after_dedup} duplicates from chunk {chunk_num}")
                
                # Convert numeric columns to appropriate types, handling potential errors and NaNs
                numeric_columns = ['last_price', 'ohlc_open', 'ohlc_high', 'ohlc_low', 'ohlc_close', 'volume', 'oi', 'strike']
                for col in numeric_columns:
                    if col in chunk_df.columns:
                        chunk_df[col] = pd.to_numeric(chunk_df[col], errors='coerce').fillna(0)
                
                if 'instrument_token' in chunk_df.columns:
                    chunk_df['instrument_token'] = chunk_df['instrument_token'].astype('int64')
                
                # ADDED: Convert days_to_expiry to integer
                if 'days_to_expiry' in chunk_df.columns:
                    chunk_df['days_to_expiry'] = pd.to_numeric(chunk_df['days_to_expiry'], errors='coerce').fillna(0).astype('int32')
                
                logging.info(f" Chunk {chunk_num} cleaned: {len(chunk_df)} unique rows")
                total_rows_processed += len(chunk_df)
                
                # Convert to PyArrow table for efficient writing
                table = pa.Table.from_pandas(chunk_df)
                
                # Initialize writer with first chunk's schema
                if writer is None:
                    schema = table.schema
                    writer = pq.ParquetWriter(eod_filename, schema)
                    logging.info(f"Initialized Parquet writer for: {eod_filename}")
                
                # Write this chunk to the parquet file
                writer.write_table(table)
                logging.info(f"Written chunk {chunk_num} to parquet. Total rows so far: {total_rows_processed}")
                
                # Free memory
                del chunk_df, table
                
            except Exception as e:
                logging.error(f"Error processing chunk {chunk_num}: {e}", exc_info=True)
                continue
        
        # Close the parquet writer
        if writer:
            writer.close()
            logging.info(f"Daily Parquet file saved locally: {eod_filename}")
            logging.info(f"Total unique rows processed: {total_rows_processed}")
            
            # --- Upload to S3 (if enabled) ---
            if SAVE_TO_S3:
                upload_to_s3(eod_filename, S3_BUCKET_NAME, S3_PREFIX)
                
            logging.info("EOD processing completed successfully!")
            
        else:
            logging.error("No data was successfully processed - Parquet file not created")
            
    except Exception as e:
        logging.error(f"Error saving Parquet file or uploading to S3: {e}", exc_info=True)
        # Clean up partial parquet file if it exists
        if os.path.exists(eod_filename):
            try:
                os.remove(eod_filename)
                logging.info(f"Cleaned up partial parquet file: {eod_filename}")
            except:
                pass
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
    logging.info(" Market session manager started")
    
    while not shutdown_event.is_set(): # Keep running until a shutdown is signaled
        now_ist = datetime.datetime.now(IST)


        if now_ist.weekday() >= 5:
            logging.info(f"Today is a weekend ({now_ist.strftime('%A')}). Market is closed.")
            time.sleep(3600)
            continue

        market_is_open = is_market_open()
        eod_time_reached = is_eod_time()
    
        if now_ist.minute % 15 == 0 and now_ist.second < 30:
            if market_is_open:
                logging.info(f"Market is open - Current time : {now_ist.strftime('%H:%M:%S')}")
                if kws and kws.is_connected():
                    logging.info(f"Websocket is connected , data collection is active")
                else:
                    logging.warning(f"Websocket is not Connected during market hours!")
            else:
                logging.info(f"Market is close - Current time: {now_ist.strftime('%H:%M:%S')}")

        if market_is_open:
            if not kws or not kws.is_connected():
                logging.warning(f" Market is open ({now_ist.strftime('%H:%M')}) but Kite WebSocket is not connected.")
                logging.warning("This might indicate a connection issue that needs attention.")
        
        # MODIFIED: Enhanced EOD processing trigger
        if eod_time_reached:
            if not shutdown_event.is_set(): # Ensure EOD is only triggered once
                logging.info(f" EOD processing time detected ({now_ist.strftime('%H:%M')})")
                logging.info(" Initiating End-of-Day data processing sequence...")
                
                # Gracefully disconnect WebSocket if connected
                if kws and kws.is_connected():
                    logging.info(" Disconnecting WebSocket for EOD processing...")
                    
                    kws.stop()
                
                shutdown_event.set() # Signal all other threads to prepare for shutdown
                break # Exit the market_session_manager loop
            else:
                logging.debug("EOD processing already triggered. Waiting for application shutdown.")
        
        # ADDED: Different sleep intervals based on market status
        if market_is_open:
            # During market hours, check more frequently
            time.sleep(30)  # Check every 30 seconds during market hours
        else:
            # Outside market hours, check less frequently
            time.sleep(300)  # Check every 5 minutes outside market hours
def wait_for_market_open():
   
    while True:
        now_ist = datetime.datetime.now(IST)
        
        # Check if it's a weekend
        if now_ist.weekday() >= 5:  # Saturday=5, Sunday=6
            # Calculate time until next Monday
            days_until_monday = 7 - now_ist.weekday()
            monday_morning = now_ist.replace(hour=MARKET_OPEN_HOUR, minute=MARKET_OPEN_MINUTE, second=0, microsecond=0) + datetime.timedelta(days=days_until_monday)
            time_until_monday = (monday_morning - now_ist).total_seconds()
            
            logging.info(f"Weekend detected. Market will open on {monday_morning.strftime('%A %Y-%m-%d at %H:%M')}")
            logging.info(f"Sleeping for {time_until_monday/3600:.1f} hours until market opens")
            
            # Sleep in chunks to allow for graceful shutdown
            while time_until_monday > 0 and not shutdown_event.is_set():
                sleep_time = min(3600, time_until_monday)  # Sleep max 1 hour at a time
                time.sleep(sleep_time)
                time_until_monday -= sleep_time
            continue
        
        # Check if market is open
        if is_market_open():
            logging.info(f"Market is now open! Current time: {now_ist.strftime('%H:%M:%S')}")
            break
        else:
            # Market is closed, calculate time until market opens
            current_time = now_ist.time()
            market_open_time = datetime.time(MARKET_OPEN_HOUR, MARKET_OPEN_MINUTE)
            
            if current_time < market_open_time:
                # Market hasn't opened yet today
                market_open_today = now_ist.replace(hour=MARKET_OPEN_HOUR, minute=MARKET_OPEN_MINUTE, second=0, microsecond=0)
                time_until_open = (market_open_today - now_ist).total_seconds()
                
                logging.info(f" Market opens at {market_open_time.strftime('%H:%M')}. Waiting {time_until_open/60:.0f} minutes...")
            else:
                # Market has closed for today, wait until tomorrow
                tomorrow_open = now_ist.replace(hour=MARKET_OPEN_HOUR, minute=MARKET_OPEN_MINUTE, second=0, microsecond=0) + datetime.timedelta(days=1)
                time_until_open = (tomorrow_open - now_ist).total_seconds()
                
                logging.info(f"Market closed for today. Opens tomorrow at {tomorrow_open.strftime('%H:%M')}. Waiting {time_until_open/3600:.1f} hours...")
            
            # Sleep in chunks to allow for graceful shutdown
            while time_until_open > 0 and not shutdown_event.is_set():
                sleep_time = min(300, time_until_open)  # Sleep max 5 minutes at a time
                time.sleep(sleep_time)
                time_until_open -= sleep_time

# --- Main Script Execution Block ---
if __name__ == "__main__":
    logging.info("Starting Kite BankNifty F&O Data Collector Application...")
    logging.info(f"Application started at: {datetime.datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S %Z')}")
    

    now_ist = datetime.datetime.now(IST)
    if now_ist.weekday() >= 5:
        logging.info(f"Today is {now_ist.strftime('%A')} - Weekend detected")
    elif is_market_open():
        logging.info(f"Market is currently OPEN")
    elif is_eod_time():
        logging.info(f"Market has closed, it's past EOD time")
    else:
        logging.info(f" Market is currently CLOSED")
    
    # Step 1: Fetch credentials from AWS Secrets Manager
    logging.info(" Fetching credentials from AWS Secrets Manager...")
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


    # ADDED: Wait for market to open before starting data collection
    logging.info(" Checking market status before starting data collection...")
    
    # If it's already past EOD time, just run EOD processing and exit
    if is_eod_time():
        logging.info("It's already past EOD time. Running EOD processing and exiting...")
        try:
            process_eod_data()
            logging.info("EOD processing completed successfully.")
        except Exception as e:
            logging.error(f" Critical error during EOD processing: {e}", exc_info=True)
        logging.info(" Application shutdown complete. Exiting.")
        exit(0)
    
    # Wait for market to open if it's not open yet
    wait_for_market_open()
    
    # Double-check that we should proceed (market might have closed while waiting)
    if is_eod_time():
        logging.info(" Market closed while waiting. Running EOD processing and exiting...")
        try:
            process_eod_data()
            logging.info(" EOD processing completed successfully.")
        except Exception as e:
            logging.error(f"Critical error during EOD processing: {e}", exc_info=True)
        logging.info(" Application shutdown complete. Exiting.")
        exit(0)

    # Step 3: Start background threads (These must start BEFORE kws.connect())
    logging.info(" Starting background threads...")

    # Thread for managing market session times and triggering EOD
    session_manager_thread = threading.Thread(target=market_session_manager, daemon=True)
    session_manager_thread.start()
    logging.info("Market session manager thread started.")

    # Thread for periodic data saving
    periodic_saver_thread = threading.Thread(target=save_periodic_data, daemon=True)
    periodic_saver_thread.start()
    logging.info(" Periodic data saver thread started.")

    # Step 4: Connect Kite WebSocket in the main thread (this is blocking)
    # kws.connect() must be in the main thread to avoid 'signal only works in main thread' error
    logging.info(" Attempting to connect Kite WebSocket in the main thread...")
    logging.info(f" Connection attempt at: {datetime.datetime.now(IST).strftime('%H:%M:%S')}")
    
    try:
        kws.connect() # This call blocks until the WebSocket disconnects or an unhandled error occurs
    except KeyboardInterrupt:
        logging.info(" KeyboardInterrupt detected in main thread. Signaling for graceful shutdown.")
        shutdown_event.set() # Set the event to signal immediate shutdown if Ctrl+C is pressed
    except Exception as e:
        logging.error(f" An unexpected error occurred during WebSocket connection: {e}", exc_info=True)
        shutdown_event.set() # Signal shutdown on any unexpected error

    # Step 5: After kws.connect() returns (i.e., WebSocket closed), proceed to EOD processing
    logging.info("WebSocket connection terminated. Proceeding with shutdown sequence.")
    shutdown_event.set() # Ensure all threads know to shut down before final processing

    # Give a small buffer for background threads to react to the shutdown_event
    logging.info("Waiting for background threads to complete...")
    time.sleep(5) 

    # Final EOD processing
    try:
        logging.info(" Starting final End-of-Day processing...")
        process_eod_data()
        logging.info(" EOD processing completed successfully.")
    except Exception as e:
        logging.error(f" Critical error during EOD processing: {e}", exc_info=True)

    logging.info(" Application shutdown complete. Exiting.")
    #satyam