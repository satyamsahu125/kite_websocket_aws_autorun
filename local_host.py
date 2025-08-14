import os
import json
import boto3
from kiteconnect import KiteConnect
import logging
from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Configuration for Local Script ---
# These should ideally be set as environment variables on your local machine
# Example: export LOCAL_KITE_API_KEY="your_api_key"
LOCAL_API_KEY = os.getenv("KITE_API_KEY")
LOCAL_API_SECRET = os.getenv("KITE_API_SECRET")
REDIRECT_URL = "http://localhost:3000" # This must match your Redirect URL configured in Kite Connect app

AWS_REGION = "ap-south-1" # Set your AWS region (e.g., ap-south-1 for Mumbai)
SECRETS_MANAGER_SECRET_NAME = "KiteConnectBankniftyData" # Choose a unique name for your secret

# --- AWS Secrets Manager Client ---
# boto3 will automatically pick up AWS credentials from ~/.aws/credentials,
# environment variables, or IAM roles (if running on EC2).
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
        return {}
    except Exception as e:
        logging.error(f"Error fetching secret '{SECRETS_MANAGER_SECRET_NAME}': {e}")
        return {}

def update_secret(new_data):
    """Updates the secret in AWS Secrets Manager with the provided data."""
    try:
        secrets_client.put_secret_value(
            SecretId=SECRETS_MANAGER_SECRET_NAME,
            SecretString=json.dumps(new_data)
        )
        logging.info(f"Secret '{SECRETS_MANAGER_SECRET_NAME}' updated successfully.")
    except Exception as e:
        logging.error(f"Error updating secret '{SECRETS_MANAGER_SECRET_NAME}': {e}")
        raise

def generate_access_token_and_update_secret(request_token):
    """Generates a new access token and updates the AWS Secrets Manager."""
    if not LOCAL_API_KEY or not LOCAL_API_SECRET:
        logging.error("LOCAL_KITE_API_KEY or LOCAL_KITE_API_SECRET environment variables are not set. Please set them.")
        return

    kite = KiteConnect(api_key=LOCAL_API_KEY)

    try:
        # Generate session using the request token and API secret
        data = kite.generate_session(request_token, api_secret=LOCAL_API_SECRET)
        print(data)
        access_token = data["access_token"]
        # Store if needed, not strictly for KWS

        logging.info(f"Generated new Access Token: {access_token[:5]}... (first 5 characters for verification)")

        # Fetch existing secret data to preserve other fields if any
        existing_secret_data = get_current_secret_data()

        # Update the specific fields
        existing_secret_data['API_KEY'] = LOCAL_API_KEY
        existing_secret_data['API_SECRET'] = LOCAL_API_SECRET
        existing_secret_data['ACCESS_TOKEN'] = access_token
        # You can add other static info here, e.g., 'USER_ID': data.get('user_id')

        # Update the secret in AWS Secrets Manager
        update_secret(existing_secret_data)
        logging.info("Access Token successfully updated in AWS Secrets Manager.")

    except Exception as e:
        logging.error(f"Error generating session or updating secret: {e}")
        logging.error("Please ensure your request_token is correct and valid (used only once).")

if __name__ == "__main__":
    # Ensure you have AWS credentials configured locally for boto3 (e.g., ~/.aws/credentials or environment variables)
    # and they have permissions to `secretsmanager:GetSecretValue` and `secretsmanager:PutSecretValue`
    # for the secret: arn:aws:secretsmanager:YOUR_REGION:YOUR_ACCOUNT_ID:secret:KiteConnectBankNiftyData-*

    logging.info("--- Kite Connect Token Updater (Local Script) ---")
    print("\n--- Manual Step Required Daily ---")
    print(f"1. Open your web browser and navigate to the Kite Connect login URL:")
    # You need to manually construct this URL or use kite.login_url() from KiteConnect instance.
    # Example:
    kite_login_url = f"https://kite.zerodha.com/connect/login?v=3&api_key={LOCAL_API_KEY}&redirect_uri={REDIRECT_URL}"
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