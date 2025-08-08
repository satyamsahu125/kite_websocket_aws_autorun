Okay, this is the complete, consolidated guide for setting up your automated Kite Connect Bank Nifty F&O data collector on AWS, incorporating all the fixes and best practices we've identified. At the end, I'll provide a section for your README with the common errors and their solutions.

---

# Automated Kite Connect Bank Nifty F&O Data Collector on AWS

This guide provides step-by-step instructions to set up a Python script on an AWS EC2 instance that collects real-time Bank Nifty Futures & Options data from Zerodha's Kite Connect WebSocket API, stores it, and saves it to AWS S3 daily. It includes automation for EC2 instance lifecycle and daily access token management.

## System Architecture

The system leverages:
*   **Zerodha Kite Connect API:** For real-time data via WebSocket.
*   **AWS Secrets Manager:** Securely stores Kite API keys, secret, and the daily-refreshed access token.
*   **AWS EC2:** Runs the Python data collection script during market hours.
*   **AWS S3:** Stores the collected daily data in Parquet format.
*   **AWS Lambda:** Starts and stops the EC2 instance on a schedule.
*   **Amazon EventBridge (CloudWatch Events):** Triggers Lambda functions on a daily schedule.
*   **IAM:** Manages permissions for all AWS components.

## Prerequisites

Before you start, ensure you have:

1.  **AWS Account:** With administrative access to create IAM users/roles, EC2 instances, S3 buckets, Secrets Manager secrets, Lambda functions, and EventBridge rules.
2.  **Zerodha Kite Connect Developer Account:**
    *   Your **API Key** and **API Secret**.
    *   A configured **Redirect URL** for your app (e.g., `http://localhost:3000`).
3.  **Python 3.9+:** Installed on both your local machine and your EC2 instance.
4.  **Required Python Libraries:**
    *   `kiteconnect`
    *   `pandas`
    *   `pyarrow`
    *   `boto3`
    *   `pytz`
    *   `python-dotenv` (for local script, `pip install python-dotenv`)
5.  **AWS CLI:** Installed and configured on your **local machine** for `aws configure`.

---

## Part 1: AWS Console Setup

### Step 1: Create an IAM Role for your EC2 Instance

This role grants your EC2 instance permissions to interact with AWS S3 and Secrets Manager.

1.  Go to **AWS Management Console** > Search for "IAM" > Click **"IAM"**.
2.  In the left navigation pane, click **"Roles"**.
3.  Click the **"Create role"** button.
    *   **Trusted entity type:** Select `AWS service`.
    *   **Use case:** Select `EC2`.
    *   Click **"Next"**.
4.  **Add Permissions:**
    *   **Attach Policies:**
        *   Search for `AmazonS3FullAccess` and select it. (For production, narrow this to specific bucket permissions).
        *   Search for `SecretsManagerReadWrite` and select it. (For production, create a custom policy as described below for more specific access).
    *   Click **"Next"**.
5.  **Name, Review, and Create Role:**
    *   **Role name:** Enter `KiteDataCollectorEC2Role`.
    *   (Optional) Add a description.
    *   Click **"Create role"**.

### Step 2: Create an S3 Bucket for Your Data

This is where your final Parquet files will be stored.

1.  Go to **AWS Management Console** > Search for "S3" > Click **"S3"**.
2.  Click the **"Create bucket"** button.
    *   **Bucket name:** Enter a **globally unique** name (e.g., `your-kite-banknifty-data-2025`).
    *   **AWS Region:** Select your preferred region (e.g., `Asia Pacific (Mumbai) ap-south-1`).
    *   **Object Ownership:** Keep `ACLs enabled` and `Recommended: Bucket owner preferred`.
    *   **Block Public Access settings:** Keep all options **checked** (highly recommended for security).
    *   Keep other settings as default.
    *   Click **"Create bucket"**.

### Step 3: Create a Secret in AWS Secrets Manager

This securely stores your Kite Connect API credentials and the daily-updated access token.

1.  Go to **AWS Management Console** > Search for "Secrets Manager" > Click **"Secrets Manager"**.
2.  Click the **"Store a new secret"** button.
    *   **Secret type:** Select `Other type of secret`.
    *   **Key/value pairs:** Enter the following JSON. This structure is what your Python script expects.
        ```json
        {
          "API_KEY": "YOUR_KITE_API_KEY",        
          "API_SECRET": "YOUR_KITE_API_SECRET",  
          "ACCESS_TOKEN": "DUMMY_INITIAL_TOKEN"  
        }
        ```
        **IMPORTANT:** Replace `YOUR_KITE_API_KEY` and `YOUR_KITE_API_SECRET` with your actual Kite credentials. The `ACCESS_TOKEN` is a placeholder for now.
    *   Click **"Next"**.
3.  **Configure secret:**
    *   **Secret name:** Enter `KiteConnectBankniftyData`. **This name is case-sensitive and must match exactly in your scripts and IAM policies.**
    *   (Optional) Add a description.
    *   Click **"Next"**.
4.  **Review and Store:**
    *   Review the settings.
    *   Click **"Store"**.

### Step 4: Create IAM User for Local Token Updater

This IAM user will be used by your local script to update the access token in Secrets Manager.

1.  Go to **IAM** > Click **"Users"** in the left navigation.
2.  Click **"Create user"**.
    *   **User name:** Enter `KiteDataUploaderLocal`.
    *   **AWS credential type:** Check **"Access key - Programmatic access"**.
    *   Click **"Next"**.
3.  **Set Permissions:**
    *   Select **"Attach policies directly"**.
    *   Click **"Create inline policy"**.
    *   **Visual editor:**
        *   **Service:** Search for `Secrets Manager` and select it.
        *   **Actions:** Expand "Read" and select `GetSecretValue`. Expand "Write" and select `PutSecretValue`.
        *   **Resources:** Select `Specific`. Click "Add ARN".
            *   **Region:** `ap-south-1` (or your chosen region).
            *   **Account ID:** Your AWS Account ID (from previous errors, `492683309164`).
            *   **Secret name:** `KiteConnectBankniftyData` (must match exactly).
            *   The ARN should look like: `arn:aws:secretsmanager:ap-south-1:YOUR_ACCOUNT_ID:secret:KiteConnectBankniftyData-*` (The `*` is crucial!).
            *   Click **"Add ARNs"**.
    *   Review, name the policy (e.g., `LocalSecretsManagerUpdatePolicy`), and create it.
    *   Go back to the "Create user" tab and continue.
4.  **Review and Create User:**
    *   Click **"Create user"**.
    *   **IMPORTANT:** On the success screen, you will see the **Access Key ID** and **Secret Access Key**. **COPY BOTH IMMEDIATELY AND SECURELY.** This is the only time the Secret Access Key will be displayed. You'll use these with `aws configure` locally.

### Step 5: Create AWS Lambda Functions for EC2 Control

These functions will start and stop your EC2 instance.

1.  Go to **AWS Management Console** > Search for "Lambda" > Click **"Lambda"**.
2.  **Create Function 1: `start_kite_ec2`**
    *   Click **"Create function"**.
    *   **Author from scratch:**
        *   **Function name:** `start_kite_ec2`
        *   **Runtime:** `Python 3.9` (or latest stable).
        *   **Architecture:** `x86_64`.
        *   **Execution role:** Click "Create new role with basic Lambda permissions". Then, under "Permissions", add an inline policy:
            *   **Service:** EC2
            *   **Actions:** Expand "Write" and select `StartInstances`. Expand "List" and select `DescribeInstances`.
            *   **Resources:** Select "Specific" and add the ARN of your EC2 instance (e.g., `arn:aws:ec2:ap-south-1:YOUR_ACCOUNT_ID:instance/i-xxxxxxxxxxxxxxxxx`).
            *   Create the policy and finish creating the Lambda function.
    *   **Function code:** Replace the default code with:
        ```python
        import boto3
        import os
        import logging

        logger = logging.getLogger()
        logger.setLevel(logging.INFO)

        def lambda_handler(event, context):
            instance_id = os.environ['EC2_INSTANCE_ID']
            region = os.environ['AWS_REGION']

            ec2 = boto3.client('ec2', region_name=region)

            try:
                ec2.start_instances(InstanceIds=[instance_id])
                logger.info(f"Started EC2 instance: {instance_id}")
                return {
                    'statusCode': 200,
                    'body': f"Started instance {instance_id}"
                }
            except Exception as e:
                logger.error(f"Error starting instance {instance_id}: {e}")
                raise e
        ```
    *   **Environment variables:** Add `EC2_INSTANCE_ID` (e.g., `i-0abcdef1234567890`) and `AWS_REGION` (e.g., `ap-south-1`).
    *   Click **"Deploy"**.

3.  **Create Function 2: `stop_kite_ec2`**
    *   Repeat the process for a new function.
    *   **Function name:** `stop_kite_ec2`.
    *   **Runtime:** `Python 3.9`.
    *   **Execution role:** Create a new role (or re-use `start_kite_ec2`'s role and modify its policy) with permissions for EC2: `StopInstances` and `DescribeInstances` for your specific EC2 instance ARN.
    *   **Function code:** Replace the default code with:
        ```python
        import boto3
        import os
        import logging

        logger = logging.getLogger()
        logger.setLevel(logging.INFO)

        def lambda_handler(event, context):
            instance_id = os.environ['EC2_INSTANCE_ID']
            region = os.environ['AWS_REGION']

            ec2 = boto3.client('ec2', region_name=region)

            try:
                ec2.stop_instances(InstanceIds=[instance_id])
                logger.info(f"Stopped EC2 instance: {instance_id}")
                return {
                    'statusCode': 200,
                    'body': f"Stopped instance {instance_id}"
                }
            except Exception as e:
                logger.error(f"Error stopping instance {instance_id}: {e}")
                raise e
        ```
    *   **Environment variables:** Add `EC2_INSTANCE_ID` and `AWS_REGION`.
    *   Click **"Deploy"**.

### Step 6: Create EventBridge Rules for Scheduling

These rules will trigger your Lambda functions on a daily schedule. **Times for cron expressions are in UTC.**

*   **Market Open (IST):** 9:15 AM IST = 3:45 AM UTC
*   **Market Close + EOD Processing Buffer (IST):** 3:45 PM IST = 10:15 AM UTC

1.  Go to **AWS Management Console** > Search for "EventBridge" > Click **"Amazon EventBridge"**.
2.  In the left navigation pane, click **"Rules"**.
3.  **Create Rule 1: `StartKiteCollectorDaily`**
    *   Click **"Create rule"**.
    *   **Name:** `StartKiteCollectorDaily`.
    *   **Description:** "Starts the EC2 instance for Kite data collection daily."
    *   **Define pattern:** Select `Schedule`.
    *   **Schedule pattern:** Choose `Cron expression`.
    *   Enter the cron expression for starting EC2 before market open (e.g., `cron(0 3 * * ? *)` for 3:00 AM UTC, which is 8:30 AM IST). This gives a 45-minute buffer for boot and script initialization.
    *   **Select targets:**
        *   **Target:** `Lambda function`.
        *   **Function:** Select `start_kite_ec2`.
    *   Click **"Create"**.

4.  **Create Rule 2: `StopKiteCollectorDaily`**
    *   Click **"Create rule"**.
    *   **Name:** `StopKiteCollectorDaily`.
    *   **Description:** "Stops the EC2 instance for Kite data collection daily after market close and EOD processing."
    *   **Define pattern:** `Schedule`.
    *   **Schedule pattern:** `Cron expression`.
    *   Enter the cron expression for stopping EC2 after EOD processing (e.g., `cron(30 10 * * ? *)` for 10:30 AM UTC, which is 4:00 PM IST). This provides a buffer after your script's EOD processing time (3:45 PM IST).
    *   **Select targets:**
        *   **Target:** `Lambda function`.
        *   **Function:** Select `stop_kite_ec2`.
    *   Click **"Create"**.

---

## Part 2: Local Setup (Your Machine)

### Step 1: Prepare Local Python Script (`local_token_updater.py`)

Create a file named `local_token_updater.py` with the following code.

```python
import os
import json
import boto3
from kiteconnect import KiteConnect
import logging
from dotenv import load_dotenv # Used for loading .env file

# Load environment variables from .env file (if present)
load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Configuration for Local Script ---
# These should be set as environment variables (e.g., in a .env file or system-wide)
# KITE_API_KEY and KITE_API_SECRET
LOCAL_KITE_API_KEY = os.getenv("KITE_API_KEY")
LOCAL_KITE_API_SECRET = os.getenv("KITE_API_SECRET")
REDIRECT_URL = "http://localhost:3000" # This must match your Redirect URL configured in Kite Connect app

AWS_REGION = "ap-south-1" # Set your AWS region (e.g., ap-south-1 for Mumbai)
SECRETS_MANAGER_SECRET_NAME = "KiteConnectBankniftyData" # Must match the secret name in AWS console

# --- AWS Secrets Manager Client ---
# boto3 will automatically pick up AWS credentials from ~/.aws/credentials,
# environment variables, or IAM roles.
secrets_client = boto3.client('secretsmanager', region_name=AWS_REGION)

def get_current_secret_data():
    """Fetches existing secret data from AWS Secrets Manager."""
    try:
        response = secrets_client.get_secret_value(SecretId=SECRETS_MANAGER_SECRET_NAME)
        if 'SecretString' in response:
            return json.loads(response['SecretString'])
        else:
            logging.error("Secret is not a string type. It must be a JSON string.")
            return {}
    except secrets_client.exceptions.ResourceNotFoundException:
        logging.warning(f"Secret '{SECRETS_MANAGER_SECRET_NAME}' not found. It will be created/updated with new data.")
        # Attempt to create a new secret if not found. Note: PutSecretValue can also create but GetSecretValue would fail first.
        # This warning means the initial manual creation was likely missed/typoed.
        # For a truly robust script, you'd add logic to handle initial creation with put_secret_value(CreateNewSecret=True)
        return {}
    except Exception as e:
        logging.error(f"Error fetching secret '{SECRETS_MANAGER_SECRET_NAME}': {e}")
        raise

def update_secret(new_data):
    """Updates the secret in AWS Secrets Manager with the provided data."""
    try:
        # Check if the secret exists first, to avoid ResourceNotFoundException
        try:
            secrets_client.get_secret_value(SecretId=SECRETS_MANAGER_SECRET_NAME)
            # If exists, update
            secrets_client.put_secret_value(
                SecretId=SECRETS_MANAGER_SECRET_NAME,
                SecretString=json.dumps(new_data)
            )
        except secrets_client.exceptions.ResourceNotFoundException:
            # If not found, create it with initial dummy data, then update
            logging.info(f"Secret '{SECRETS_MANAGER_SECRET_NAME}' not found during update, attempting to create.")
            secrets_client.create_secret(
                Name=SECRETS_MANAGER_SECRET_NAME,
                SecretString=json.dumps(new_data), # Use the new_data directly for creation
                Description=f"Kite Connect API credentials for {SECRETS_MANAGER_SECRET_NAME}"
            )
        
        logging.info(f"Secret '{SECRETS_MANAGER_SECRET_NAME}' updated successfully.")
    except Exception as e:
        logging.error(f"Error updating secret '{SECRETS_MANAGER_SECRET_NAME}': {e}")
        raise

def generate_access_token_and_update_secret(request_token):
    """Generates a new access token and updates the AWS Secrets Manager."""
    if not LOCAL_KITE_API_KEY or not LOCAL_KITE_API_SECRET:
        logging.error("KITE_API_KEY or KITE_API_SECRET environment variables are not set. Please set them in your .env file or system.")
        return

    kite = KiteConnect(api_key=LOCAL_KITE_API_KEY)

    try:
        data = kite.generate_session(request_token, api_secret=LOCAL_KITE_API_SECRET)
        access_token = data["access_token"]
        public_token = data.get("public_token") # Optional: store if needed

        logging.info(f"Generated new Access Token: {access_token[:5]}... (first 5 characters for verification)")

        existing_secret_data = {}
        try:
            existing_secret_data = get_current_secret_data()
        except secrets_client.exceptions.ResourceNotFoundException:
            # This is okay, it means the secret doesn't exist, and update_secret will create it.
            logging.info("Secret not found during initial fetch, will attempt to create during update.")
        except Exception as e:
            logging.error(f"Failed to fetch existing secret data: {e}", exc_info=True)
            return # Cannot proceed if cannot fetch

        existing_secret_data['API_KEY'] = LOCAL_KITE_API_KEY
        existing_secret_data['API_SECRET'] = LOCAL_KITE_API_SECRET
        existing_secret_data['ACCESS_TOKEN'] = access_token
        
        update_secret(existing_secret_data)
        logging.info("Access Token successfully updated in AWS Secrets Manager.")

    except Exception as e:
        logging.error(f"Error generating session or updating secret: {e}", exc_info=True) # Print full traceback
        logging.error("Please ensure your request_token is correct and valid (used only once).")

if __name__ == "__main__":
    logging.info("--- Kite Connect Token Updater (Local Script) ---")
    
    # Check if credentials loaded from .env
    if not LOCAL_KITE_API_KEY or not LOCAL_KITE_API_SECRET:
        logging.error("KITE_API_KEY and KITE_API_SECRET are not loaded from environment variables.")
        logging.error("Please create a .env file in the same directory as this script with:")
        logging.error("KITE_API_KEY=\"YOUR_API_KEY_HERE\"")
        logging.error("KITE_API_SECRET=\"YOUR_API_SECRET_HERE\"")
        exit(1)

    print("\n--- Manual Step Required Daily ---")
    print(f"1. Open your web browser and navigate to the Kite Connect login URL:")
    kite_login_url = f"https://kite.zerodha.com/connect/login?v=3&api_key={LOCAL_KITE_API_KEY}&redirect_uri={REDIRECT_URL}"
    print(f"   {kite_login_url}")
    print("2. Log in with your Zerodha credentials and grant permissions.")
    print(f"3. After successful login, your browser will redirect to '{REDIRECT_URL}'")
    print("   The URL will contain 'request_token=YOUR_REQUEST_TOKEN'.")
    print("4. Copy ONLY the 'YOUR_REQUEST_TOKEN' part from the redirected URL.")
    
    request_token_input = input("\nEnter the request_token you copied: ").strip()

    if request_token_input:
        generate_access_token_and_update_secret(request_token_input)
    else:
        logging.error("No request token entered. Exiting.")

```

### Step 2: Configure Local AWS Credentials

You need to tell `boto3` (used by your script) how to authenticate with your AWS account.

1.  **Install AWS CLI:** If not already installed, download and install the AWS CLI.
2.  **Configure AWS CLI:** Open your terminal/command prompt and run:
    ```bash
    aws configure
    ```
    *   **AWS Access Key ID:** Paste the Access Key ID of your `KiteDataUploaderLocal` IAM user.
    *   **AWS Secret Access Key:** Paste the Secret Access Key for that user.
    *   **Default region name:** Enter `ap-south-1` (or your chosen region).
    *   **Default output format:** Press Enter (accept `json`).

### Step 3: Run the Local Token Updater Script (Daily Manual Step)

1.  **Create a `.env` file** in the same directory as `local_token_updater.py` with your Kite API key and secret:
    ```
    KITE_API_KEY="your_actual_kite_api_key_here"
    KITE_API_SECRET="your_actual_kite_api_secret_here"
    ```
2.  **Execute the script:**
    ```bash
    python3 local_token_updater.py
    ```
3.  Follow the script's prompts:
    *   Open the provided URL in a browser.
    *   Log in to Kite.
    *   Copy the `request_token` from the redirected URL.
    *   Paste the `request_token` into the terminal prompt.

    The script will then generate a new access token and update the `KiteConnectBankniftyData` secret in AWS Secrets Manager.

---

## Part 3: EC2 Instance Setup

### Step 1: Launch EC2 Instance

1.  Go to **AWS Management Console** > Search for "EC2" > Click **"EC2"**.
2.  Click **"Launch instances"**.
3.  **Step 1: Choose an AMI:** Select `Amazon Linux 2 AMI` (HVM) or `Ubuntu Server`.
4.  **Step 2: Choose Instance Type:** Select `t2.micro` (free tier eligible) or `t3.micro`.
5.  **Step 3: Configure Instance Details:**
    *   **IAM role:** Select the `KiteDataCollectorEC2Role` you created earlier. **This is critical.**
    *   **User data:** In the "Advanced details" section, expand "User data", select "As text", and paste this script. This runs **once** on first boot to set up the environment.
        ```bash
        #!/bin/bash
        sudo yum update -y # For Amazon Linux; use apt-get for Ubuntu
        sudo yum install -y python3 python3-pip git

        mkdir -p /home/ec2-user/kite_collector
        cd /home/ec2-user/kite_collector

        # For initial setup, if you are not using Git:
        # You will SCP the ec2_kite_collector.py file after the instance is launched.
        # Otherwise, if your script is in a public Git repo:
        # git clone https://github.com/your-username/your-kite-repo.git .

        pip3 install kiteconnect pandas pyarrow boto3 pytz

        mkdir -p temp_kite_data
        mkdir -p final_kite_data
        ```
6.  **Step 4: Add Storage:** Default 8GB is usually sufficient.
7.  **Step 5: Configure Security Group:**
    *   Create a new security group.
    *   Allow **SSH (Port 22)** from `My IP` for initial setup and troubleshooting.
    *   Ensure outbound HTTPS (port 443) is allowed (default is usually all outbound, which is fine).
8.  **Step 6: Review and Launch:**
    *   Choose an existing key pair or create a new one. Download the `.pem` file and keep it secure.
    *   Launch the instance.

### Step 2: Deploy EC2 Python Script (`ec2_kite_collector.py`)

This is your main data collection script.

**`ec2_kite_collector.py`**

```python
import os
import datetime
import pandas as pd
import threading
import time
import json
import logging
import pytz
import pyarrow.parquet as pq
import pyarrow as pa
import boto3

from kiteconnect import KiteConnect, KiteTicker

# --- Configuration ---
AWS_REGION = "ap-south-1"
SECRETS_MANAGER_SECRET_NAME = "KiteConnectBankniftyData"

S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "your-kite-data-bucket-unique-name")
S3_PREFIX = os.getenv("S3_PREFIX", "banknifty_data/")
SAVE_TO_S3 = os.getenv("SAVE_TO_S3", "True").lower() == "true"

TEMP_DATA_DIR = "temp_kite_data"
FINAL_DATA_DIR = "final_kite_data"

MARKET_OPEN_HOUR = 9
MARKET_OPEN_MINUTE = 15
MARKET_CLOSE_HOUR = 15
MARKET_CLOSE_MINUTE = 30
EOD_PROCESSING_HOUR = 15
EOD_PROCESSING_MINUTE = 45

IST = pytz.timezone('Asia/Kolkata')

os.makedirs(TEMP_DATA_DIR, exist_ok=True)
os.makedirs(FINAL_DATA_DIR, exist_ok=True)

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    handlers=[logging.StreamHandler()])

in_memory_ticks = []
data_lock = threading.Lock()
shutdown_event = threading.Event()

def get_kite_credentials():
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

def on_connect(ws, response):
    logging.info("Kite WebSocket connected. Subscribing to instruments...")
    try:
        instruments = kite.instruments("NFO")
        banknifty_futures_options_tokens = []

        today = datetime.date.today()
        # Find the next Thursday for weekly options expiry (Thursday is weekday 3)
        days_until_thursday = (3 - today.weekday() + 7) % 7
        if days_until_thursday == 0: # If today is Thursday, it's this week's expiry
            current_expiry_date = today
        else:
            current_expiry_date = today + datetime.timedelta(days=days_until_thursday)

        # Dynamic ATM calculation for options - this is a simplification
        # In real trading, you'd fetch the current Bank Nifty index via another API or from a live feed
        # For demonstration, let's assume a hypothetical ATM range
        hypothetical_atm = 47000 # Replace with actual logic to get current Bank Nifty index
        strike_range = 2000 # +/- 2000 points from ATM

        for instrument in instruments:
            if instrument['segment'] == 'NFO' and 'BANKNIFTY' in instrument['tradingsymbol']:
                # Filter for Futures
                if instrument['instrument_type'] == 'FUT':
                    # Add current month future. Refine this logic for exact current future contract.
                    # Example: Filter by trading symbol for current month (e.g., 'BANKNIFTY24AUGFUT')
                    # This example is broad, you need to be precise here.
                    if instrument['name'] == 'BANKNIFTY' and instrument['instrument_type'] == 'FUT':
                        # Simple check for active month future, requires more robust expiry logic for production
                        banknifty_futures_options_tokens.append(instrument['instrument_token'])
                        # logging.debug(f"Adding Future: {instrument['tradingsymbol']}")
                # Filter for Options (CE/PE)
                elif instrument['instrument_type'] in ['CE', 'PE']:
                    if instrument['expiry'] == current_expiry_date:
                        if instrument['strike'] >= (hypothetical_atm - strike_range) and \
                           instrument['strike'] <= (hypothetical_atm + strike_range):
                            banknifty_futures_options_tokens.append(instrument['instrument_token'])
                            # logging.debug(f"Adding Option: {instrument['tradingsymbol']} - {instrument['strike']} - {instrument['expiry']}")

        logging.info(f"Identified {len(banknifty_futures_options_tokens)} Bank Nifty F&O instruments for subscription.")

        if not banknifty_futures_options_tokens:
            logging.warning("No Bank Nifty F&O instruments found based on current filter logic. Data will not be collected.")
            return

        batch_size = 300
        for i in range(0, len(banknifty_futures_options_tokens), batch_size):
            batch = banknifty_futures_options_tokens[i:i + batch_size]
            ws.subscribe(batch)
            ws.set_mode(ws.MODE_FULL, batch)
            logging.info(f"Subscribed to batch of {len(batch)} instruments.")
            time.sleep(0.1)

        logging.info(f"Successfully subscribed to {len(banknifty_futures_options_tokens)} instruments in MODE_FULL.")
    except Exception as e:
        logging.error(f"Error subscribing to instruments: {e}", exc_info=True)
        ws.stop()

def on_ticks(ws, ticks):
    timestamp = datetime.datetime.now(IST)
    with data_lock:
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

def on_close(ws, code, reason):
    logging.info(f"Kite WebSocket closed. Code: {code}, Reason: {reason}")
    if not shutdown_event.is_set():
        logging.info("WebSocket closed. Triggering End-of-Day processing.")
        shutdown_event.set()

def on_error(ws, code, reason):
    logging.error(f"Kite WebSocket error. Code: {code}, Reason: {reason}")

def on_reconnect(ws, attempt_count):
    logging.warning(f"Kite WebSocket reconnecting: Attempt {attempt_count}")

def on_noreconnect(ws):
    logging.error("Kite WebSocket could not reconnect. Giving up.")
    shutdown_event.set()

kite = None
kws = None

def save_periodic_data():
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

def process_eod_data():
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
    s3_client = boto3.client('s3')
    object_name = s3_prefix + os.path.basename(local_filepath)
    try:
        s3_client.upload_file(local_filepath, bucket_name, object_name)
        logging.info(f"Successfully uploaded {local_filepath} to s3://{bucket_name}/{object_name}")
        os.remove(local_filepath)
        logging.info(f"Removed local file: {local_filepath}")
    except Exception as e:
        logging.error(f"Error uploading {local_filepath} to S3: {e}", exc_info=True)

def market_session_manager():
    global kws
    while not shutdown_event.is_set():
        now_ist = datetime.datetime.now(IST)

        if (not kws or not kws.is_connected()) and \
           (now_ist.hour > MARKET_OPEN_HOUR or \
           (now_ist.hour == MARKET_OPEN_HOUR and now_ist.minute >= MARKET_OPEN_MINUTE)) and \
           (now_ist.hour < EOD_PROCESSING_HOUR or \
           (now_ist.hour == EOD_PROCESSING_HOUR and now_ist.minute < EOD_PROCESSING_MINUTE)):
            logging.warning(f"Market is open ({now_ist.strftime('%H:%M')}) but Kite WebSocket is not connected. This might indicate an issue.")

        if now_ist.hour > EOD_PROCESSING_HOUR or \
           (now_ist.hour == EOD_PROCESSING_HOUR and now_ist.minute >= EOD_PROCESSING_MINUTE):
            if not shutdown_event.is_set():
                logging.info(f"EOD processing time detected ({now_ist.strftime('%H:%M')}). Stopping WebSocket and initiating data processing.")
                if kws and kws.is_connected():
                    kws.stop()
                shutdown_event.set()
                break
            else:
                logging.debug("EOD processing already triggered. Waiting for application shutdown.")

        time_to_sleep = (60 - now_ist.second) % 60
        if time_to_sleep == 0: time_to_sleep = 60
        time.sleep(time_to_sleep)

if __name__ == "__main__":
    logging.info("Starting Kite BankNifty F&O Data Collector Application...")

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

    try:
        kite = KiteConnect(api_key=API_KEY)
        kite.set_access_token(ACCESS_TOKEN)
        kws = KiteTicker(API_KEY, ACCESS_TOKEN)
        
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

    periodic_saver_thread = threading.Thread(target=save_periodic_data, daemon=True)
    periodic_saver_thread.start()
    logging.info("Periodic data saver thread started.")

    session_manager_thread = threading.Thread(target=market_session_manager, daemon=True)
    session_manager_thread.start()
    logging.info("Market session manager thread started.")

    logging.info("Attempting to connect Kite WebSocket in the main thread...")
    try:
        kws.connect()
    except KeyboardInterrupt:
        logging.info("KeyboardInterrupt detected in main thread. Signaling for graceful shutdown.")
        shutdown_event.set()
    except Exception as e:
        logging.error(f"An unexpected error occurred during WebSocket connection: {e}", exc_info=True)
        shutdown_event.set()

    logging.info("WebSocket connection terminated. Proceeding with shutdown sequence.")
    shutdown_event.set()

    time.sleep(5)

    process_eod_data()

    logging.info("Application shutdown complete. Exiting.")

```

### Step 3: Configure `systemd` Service on EC2

This makes your script run automatically every time the EC2 instance starts.

1.  **SSH into your EC2 Instance.**
    ```bash
    ssh -i /path/to/your-key.pem ec2-user@YOUR_EC2_PUBLIC_IP
    ```
2.  **Copy `ec2_kite_collector.py`:**
    ```bash
    scp -i /path/to/your-key.pem /path/to/local/ec2_kite_collector.py ec2-user@YOUR_EC2_PUBLIC_IP:/home/ec2-user/kite_collector/
    ```
3.  **Create Service File:**
    ```bash
    sudo nano /etc/systemd/system/kite_data_collector.service
    ```
    Paste the following content. **Remember to update `User`, `WorkingDirectory`, `ExecStart` paths, and `Environment` variables for your S3 bucket.**
    ```ini
    [Unit]
    Description=Kite BankNifty F&O Data Collector
    After=network.target

    [Service]
    User=ec2-user # Or 'ubuntu' if you chose Ubuntu AMI
    WorkingDirectory=/home/ec2-user/kite_collector # Path where your script is
    ExecStart=/usr/bin/python3 /home/ec2-user/kite_collector/ec2_kite_collector.py
    Restart=on-failure # Automatically restart if the script crashes
    StandardOutput=append:/var/log/kite_collector.log # Redirect stdout to log file
    StandardError=append:/var/log/kite_collector.log # Redirect stderr to same log file

    # Environment variables for your S3 bucket
    Environment="S3_BUCKET_NAME=your-kite-data-bucket-unique-name" # YOUR S3 BUCKET NAME
    Environment="S3_PREFIX=banknifty_data/"
    Environment="SAVE_TO_S3=True" # Set to "False" if you only want local storage

    [Install]
    WantedBy=multi-user.target
    ```
4.  **Save the file** (Ctrl+X, Y, Enter for Nano).
5.  **Reload systemd and Enable the Service:**
    ```bash
    sudo systemctl daemon-reload
    sudo systemctl enable kite_data_collector.service
    ```
    *If you previously masked the service, you might need to `sudo systemctl unmask kite_data_collector.service` first.*
6.  **Start and Monitor the Service:**
    ```bash
    sudo systemctl start kite_data_collector.service
    sudo systemctl status kite_data_collector.service
    tail -f /var/log/kite_collector.log
    ```

---

## Part 4: Testing and Troubleshooting

The primary test is to run the system **during live Indian market hours (9:15 AM - 3:30 PM IST)** on a trading day.

1.  **Daily Manual Step:** Ensure your `local_token_updater.py` is run first thing in the morning (IST) to update the `ACCESS_TOKEN` in AWS Secrets Manager.
2.  **Monitor EC2 Startup:** At 8:30 AM IST (3:00 AM UTC), your EC2 instance should start automatically via EventBridge and Lambda.
3.  **Monitor Script Logs:** After instance boots, check `/var/log/kite_collector.log` via SSH.
    *   Look for `Kite credentials fetched from AWS Secrets Manager.`
    *   Look for `KiteConnect and KiteTicker initialized successfully.`
    *   **During market hours, look for `Kite WebSocket connected.` and `Successfully subscribed to X instruments`.**
    *   You should then see `Received X ticks. Total in memory: Y` and `Flushing Z ticks to temporary file.` periodically.
4.  **Monitor EC2 Shutdown:** At 4:00 PM IST (10:30 AM UTC), your EC2 instance should stop automatically.
5.  **Verify Data in S3:** Check your S3 bucket (`your-kite-data-bucket-unique-name`) for daily Parquet files (e.g., `banknifty_fo_data_YYYYMMDD.parquet`).

---

## Part 5: Common Errors and Resolutions (For your README)

This section summarizes the errors we encountered during setup and their fixes, which you can use for your project's README.

### Troubleshooting: Common Errors

Here are some common issues you might encounter during setup and execution, along with their solutions.

#### 1. `Failed to enable unit: Unit file /etc/systemd/system/kite_data_collector.service is masked.`

*   **Cause:** The `systemd` service was previously "masked," which forcefully prevents it from being enabled or started.
*   **Resolution:** Unmask the service, then reload `systemd` daemon, then enable.
    ```bash
    sudo systemctl unmask kite_data_collector.service
    sudo systemctl daemon-reload
    sudo systemctl enable kite_data_collector.service
    ```

#### 2. `Failed to enable unit: Unit file kite_data_collector.service does not exist.`

*   **Cause:** The `systemd` service file is not found at the expected path (`/etc/systemd/system/`). This usually means a typo in the filename or it was saved incorrectly/deleted.
*   **Resolution:** Verify the file's exact name and location (`ls -l /etc/systemd/system/`). If it's missing or misnamed, recreate/rename it ensuring the name is precisely `kite_data_collector.service`.

#### 3. `status=217/USER` in `systemctl status` output

*   **Cause:** The user specified in the `User=` directive in your `systemd` service file (`/etc/systemd/system/kite_data_collector.service`) either doesn't exist, or `systemd` cannot properly set up the execution environment for that user. Even if the user exists, permissions on paths or environment issues can trigger this.
*   **Resolution:**
    1.  **Verify User:** Confirm the actual username on your EC2 instance (`whoami`).
    2.  **Edit Service File:** Ensure `User=` in `kite_data_collector.service` matches this user (e.g., `User=ec2-user` or `User=ubuntu`). Also, check `WorkingDirectory` and `ExecStart` paths are correct for that user's home directory.
    3.  **Get Detailed Logs:** Run the script manually as the target user to capture Python-specific errors:
        ```bash
        cd /home/ec2-user/kite_collector/ # Or your WorkingDirectory
        sudo -u ec2-user /usr/bin/python3 /home/ec2-user/kite_collector/ec2_kite_collector.py > test_run.log 2>&1
        cat test_run.log
        ```
        The `test_run.log` will reveal the specific Python traceback or environment error.

#### 4. `ValueError: signal only works in main thread of the main interpreter`

*   **Cause:** This error arises when the `kws.connect()` method (which uses `twisted` internally for signal handling) is called from a non-main thread. Python's `signal` module restricts certain operations to the main thread.
*   **Resolution:** The `kws.connect()` call *must* be executed directly in the main thread of your `ec2_kite_collector.py` script. All other long-running tasks (periodic saving, market session management) should run in separate background threads, launched *before* `kws.connect()`. The provided `ec2_kite_collector.py` in this guide includes this fix.

#### 5. `ERROR - Error retrieving Kite credentials from Secrets Manager: An error occurred (AccessDeniedException) when calling the GetSecretValue operation: User: ... is not authorized...`

*   **Cause:** The IAM user/role trying to access Secrets Manager (either your `KiteDataUploaderLocal` user locally or your `KiteDataCollectorEC2Role` on EC2) does not have the necessary `secretsmanager:GetSecretValue` or `secretsmanager:PutSecretValue` permissions for your specific secret.
*   **Resolution:**
    1.  Go to **IAM** in AWS Console.
    2.  Navigate to **Users** (for local script) or **Roles** (for EC2 instance).
    3.  Select the relevant identity (`KiteDataUploaderLocal` or `KiteDataCollectorEC2Role`).
    4.  Go to the **"Permissions" tab**.
    5.  **Add/Edit an inline policy** (or attach a managed policy) to grant the required permissions.
    6.  The policy JSON **must** look like this (ensure `Action` has both `GetSecretValue` and `PutSecretValue` for the local user, and only `GetSecretValue` for EC2 role):
        ```json
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "secretsmanager:GetSecretValue",
                        "secretsmanager:PutSecretValue"
                    ],
                    "Resource": "arn:aws:secretsmanager:ap-south-1:YOUR_ACCOUNT_ID:secret:KiteConnectBankniftyData-*"
                }
            ]
        }
        ```
        **Crucially, verify:**
        *   `YOUR_ACCOUNT_ID` is correct.
        *   `KiteConnectBankniftyData` **EXACTLY matches the secret name (case-sensitive).**
        *   The `Resource` ends with `*-` to allow access to all versions.
    7.  Save changes.

#### 6. `ERROR - Error fetching secret 'KiteConnectBankNiftyData': An error occurred (ResourceNotFoundException) when calling the PutSecretValue operation: Secrets Manager can't find the specified secret.`

*   **Cause:** The secret with the specified name (`KiteConnectBankNiftyData`) does not exist in AWS Secrets Manager in the specified region. `PutSecretValue` typically updates an existing secret, it doesn't create one if not found. This also happens if the secret name in the script/policy is case-sensitive different from the one in AWS.
*   **Resolution:**
    1.  Go to **Secrets Manager** in the AWS Console.
    2.  **Manually create the secret** named `KiteConnectBankniftyData` (ensuring **exact case matching**). Populate it with initial dummy JSON values for `API_KEY`, `API_SECRET`, and `ACCESS_TOKEN`.
    3.  Ensure the `SECRETS_MANAGER_SECRET_NAME` in both your Python scripts matches this exact name.

#### 7. `ERROR - Connection error: 1006 - connection was closed uncleanly (WebSocket connection upgrade failed (403 - Forbidden))`

*   **Cause:** The Kite Connect WebSocket server refused the connection.
    *   **Most Common:** Attempting to connect outside live Indian market hours.
    *   Expired or invalid `ACCESS_TOKEN`.
    *   EC2 instance's Public IP address is not whitelisted in your Kite Connect app settings on Zerodha's developer portal.
*   **Resolution:**
    1.  **Test During Market Hours:** Run the script ONLY during Indian market hours (9:15 AM - 3:30 PM IST) on a trading day.
    2.  **Fresh Access Token:** Ensure you've run your `local_token_updater.py` script on the current day to refresh the `ACCESS_TOKEN` in Secrets Manager before attempting to connect from EC2.
    3.  **IP Whitelisting:** Log in to your Kite Connect Developer Console, go to your API App settings, and add the Public IP address of your EC2 instance to the whitelist.

#### 8. `WARNING - No Bank Nifty F&O instruments found based on filter. Please refine instrument selection logic.`

*   **Cause:** Your `on_connect` function's filtering logic for `kite.instruments("NFO")` is not returning any instrument tokens. While the WebSocket might connect, you won't subscribe to any data.
*   **Resolution:** Carefully review and refine the instrument filtering logic within your `on_connect` function in `ec2_kite_collector.py`. Ensure it correctly identifies the current Bank Nifty Future contract (e.g., current month's expiry) and a reasonable range of Bank Nifty Options for the current expiry (e.g., current weekly expiry, based on strike prices around the ATM). You might need to adjust `current_expiry_date` or `hypothetical_atm` logic for accurate filtering.

---

This comprehensive guide and troubleshooting section should cover all the necessary steps for a successful deployment and maintenance of your Kite Connect data collector on AWS!
