Okay, this is the complete, consolidated, and step-by-step guide for setting up your automated Kite Connect Bank Nifty F&O data collector on AWS, including all the fixes and best practices we've identified. It's structured for easy understanding and includes a dedicated troubleshooting section and testing instructions.

---

# Automated Kite Connect Bank Nifty F&O Data Collector on AWS

This guide provides comprehensive, step-by-step instructions to deploy a Python script on an AWS EC2 instance that collects real-time Bank Nifty Futures & Options data from Zerodha's Kite Connect WebSocket API. The solution is designed for automation, cost-efficiency, and reliability, including daily access token management, scheduled EC2 instance lifecycle, and robust data storage in AWS S3.

## System Architecture

The solution integrates several AWS services with your Python application:

*   **Zerodha Kite Connect API:** Provides real-time market data via WebSocket.
*   **AWS Secrets Manager:** Securely stores your Kite API key, API secret, and the daily-refreshed access token.
*   **AWS EC2:** Hosts and executes the Python data collection script only during Indian market hours.
*   **AWS S3:** Serves as the durable storage for your collected daily data in efficient Parquet format.
*   **AWS Lambda:** Executes Python functions to programmatically start and stop the EC2 instance.
*   **Amazon EventBridge (CloudWatch Events):** Triggers the Lambda functions on a predefined daily schedule.
*   **AWS Identity and Access Management (IAM):** Manages fine-grained permissions for all AWS components to interact securely.

## Prerequisites

Before you begin, ensure you have:

1.  **An AWS Account:** With sufficient permissions to create and manage IAM users/roles, EC2 instances, S3 buckets, Secrets Manager secrets, Lambda functions, and EventBridge rules.
2.  **A Zerodha Kite Connect Developer Account:**
    *   Your unique **API Key** and **API Secret**.
    *   A configured **Redirect URL** for your app (e.g., `http://localhost:3000`).
3.  **Python 3.9+:** Installed on both your local machine (Windows/macOS/Linux) and the AWS EC2 instance.
4.  **Required Python Libraries:**
    *   **Local Machine:** `pip install kiteconnect pandas pyarrow boto3 pytz python-dotenv`
    *   **EC2 Instance:** `pip install kiteconnect pandas pyarrow boto3 pytz`
5.  **AWS CLI:** Installed and configured on your **local machine**. Running `aws configure` is essential for your local script to interact with AWS.
6.  **SSH Client:** For connecting to your EC2 instance (e.g., PuTTY for Windows, built-in SSH for macOS/Linux).
7.  **`scp` Client:** For securely copying files to your EC2 instance (usually comes with SSH).

---

## Part 1: AWS Console Setup

This phase involves configuring all the necessary AWS services through the AWS Management Console.

### Step 1: Create an IAM Role for Your EC2 Instance (`KiteDataCollectorEC2Role`)

This role grants your EC2 instance permissions to interact with AWS S3 and Secrets Manager.

1.  Go to **AWS Management Console** > Search for "IAM" > Click **"IAM"**.
2.  In the left navigation pane, click **"Roles"**.
3.  Click the **"Create role"** button.
    *   **Trusted entity type:** Select `AWS service`.
    *   **Use case:** Select `EC2`.
    *   Click **"Next"**.
4.  **Add Permissions:**
    *   **Attach Policies:**
        *   Search for `AmazonS3FullAccess` and select it. (For production, consider creating a custom policy with more restricted `s3:PutObject`, `s3:ListBucket` permissions for your specific bucket).
        *   Search for `SecretsManagerReadWrite` and select it. (For precise control, we will add an inline policy later, but selecting this managed policy initially is fine).
    *   Click **"Next"**.
5.  **Name, Review, and Create Role:**
    *   **Role name:** Enter `KiteDataCollectorEC2Role`.
    *   (Optional) Add a description, e.g., "Role for EC2 instance to collect Kite data and store in S3/Secrets Manager."
    *   Click **"Create role"**.

### Step 2: Create an S3 Bucket for Your Data

This bucket will store your collected daily market data in Parquet format.

1.  Go to **AWS Management Console** > Search for "S3" > Click **"S3"**.
2.  Click the **"Create bucket"** button.
    *   **Bucket name:** Enter a **globally unique** name (e.g., `your-kite-banknifty-data-2025-08-08`). Choose a name you'll remember!
    *   **AWS Region:** Select your preferred region (e.g., `Asia Pacific (Mumbai) ap-south-1`).
    *   **Object Ownership:** Keep `ACLs enabled` and `Recommended: Bucket owner preferred`.
    *   **Block Public Access settings for this bucket:** Keep all options **checked** (highly recommended for security).
    *   Keep other settings as default.
    *   Click **"Create bucket"**.

### Step 3: Create a Secret in AWS Secrets Manager (`KiteConnectBankniftyData`)

This secret will securely store your Kite Connect API credentials and the daily-refreshed access token.

1.  Go to **AWS Management Console** > Search for "Secrets Manager" > Click **"Secrets Manager"**.
2.  **Verify Region:** Ensure you are in the correct AWS Region (e.g., `ap-south-1`) in the top right corner of the console.
3.  Click the **"Store a new secret"** button.
    *   **Secret type:** Select `Other type of secret`.
    *   **Key/value pairs:** Enter the following JSON structure. **This is critical for your scripts.**
        ```json
        {
          "API_KEY": "YOUR_KITE_API_KEY",        
          "API_SECRET": "YOUR_KITE_API_SECRET",  
          "ACCESS_TOKEN": "DUMMY_INITIAL_TOKEN"  
        }
        ```
        **IMPORTANT:**
        *   Replace `YOUR_KITE_API_KEY` and `YOUR_KITE_API_SECRET` with your **actual** Kite Connect API Key and Secret.
        *   `ACCESS_TOKEN` is just a placeholder; your local script will update this daily.
    *   Click **"Next"**.
4.  **Configure secret:**
    *   **Secret name:** Enter `KiteConnectBankniftyData`. **This name is case-sensitive and must match exactly in your Python scripts and IAM policies.** (Note the lowercase 'n' in 'Banknifty' for consistency with our previous fixes).
    *   (Optional) Add a description, e.g., "Stores Kite Connect API credentials for automated data collection."
    *   Click **"Next"**.
5.  **Review and Store:**
    *   Review all the settings.
    *   Click **"Store"**.

### Step 4: Create an IAM User for Local Token Updater (`KiteDataUploaderLocal`)

This IAM user will be used by your local Python script (`local_token_updater.py`) to update the `ACCESS_TOKEN` in Secrets Manager daily.

1.  Go to **IAM** > Click **"Users"** in the left navigation.
2.  Click **"Create user"**.
    *   **User name:** Enter `KiteDataUploaderLocal`.
    *   **AWS credential type:** Check the box for **"Access key - Programmatic access"**. (You don't need console password access if this user is only for scripts).
    *   Click **"Next"**.
3.  **Set Permissions:**
    *   Select **"Attach policies directly"**.
    *   Click **"Create inline policy"**.
    *   **Visual editor:**
        *   **Service:** Search for `Secrets Manager` and select it.
        *   **Actions:**
            *   Expand the **"Read"** section and check `GetSecretValue`.
            *   Expand the **"Write"** section and check `PutSecretValue`.
        *   **Resources:** Select `Specific`. Click **"Add ARN"** next to "secret".
            *   **Region:** Your AWS region (e.g., `ap-south-1`).
            *   **Account ID:** Your AWS Account ID (e.g., `492683309164` from previous logs).
            *   **Secret name:** Type or paste `KiteConnectBankniftyData`.
            *   The ARN should look like: `arn:aws:secretsmanager:ap-south-1:YOUR_ACCOUNT_ID:secret:KiteConnectBankniftyData-*`. The `*` at the end is crucial to allow access to different versions of the secret.
            *   Click **"Add ARNs"**.
    *   Click **"Next: Tags"** (optional, then "Next: Review").
    *   **Policy name:** Give it a clear name, e.g., `LocalSecretsManagerUpdatePolicy`.
    *   Click **"Create policy"**.
    *   Go back to the "Create user" tab (if it's still open) and finish the wizard.
4.  **Review and Create User:**
    *   Click **"Create user"**.
    *   **CRITICAL:** On the success screen, you will see your **Access Key ID** and **Secret Access Key**. **THIS IS THE ONLY TIME THE SECRET ACCESS KEY IS SHOWN.** Copy both values immediately and store them securely (e.g., in a password manager or a `.csv` file downloaded from the console). You will use these with `aws configure` locally.

### Step 5: Create IAM Roles for Lambda Functions

Each Lambda function (`start_kite_ec2` and `stop_kite_ec2`) needs an IAM role with specific permissions and a trust policy that allows Lambda to assume it.

#### a. Role for `start_kite_ec2` Lambda (`start_kite_ec2_lambda_role`)

1.  Go to **IAM** > **Roles** > **Create role**.
2.  **Trusted entity type:** `AWS service` > **Use case:** `Lambda`. Click **"Next"**.
3.  **Add Permissions Policies:**
    *   Search for and select **`AWSLambdaBasicExecutionRole`** (for CloudWatch logs).
    *   Search for `AmazonEC2FullAccess` and select it. (For stricter security, you would use an inline policy allowing only `ec2:StartInstances` and `ec2:DescribeInstances` for your specific instance ARN).
    *   Click **"Next"**.
4.  **Name the Role:** Enter `start_kite_ec2_lambda_role`. Click **"Create role"**.
5.  **Verify Trust Policy:** After creation, click on the role name, go to **"Trust relationships"** tab, click **"Edit trust policy"**. Ensure it matches:
    ```json
    {
      "Version": "2012-10-17",
      "Statement": [
        {
          "Effect": "Allow",
          "Principal": {
            "Service": "lambda.amazonaws.com"
          },
          "Action": "sts:AssumeRole"
        }
      ]
    }
    ```
    Click **"Update policy"** if you made changes.

#### b. Role for `stop_kite_ec2` Lambda (`stop_kite_ec2_lambda_role`)

1.  Go to **IAM** > **Roles** > **Create role**.
2.  **Trusted entity type:** `AWS service` > **Use case:** `Lambda`. Click **"Next"**.
3.  **Add Permissions Policies:**
    *   Search for and select **`AWSLambdaBasicExecutionRole`**.
    *   **For EC2 stop permissions (use inline for least privilege):**
        *   Click **"Create inline policy"**.
        *   **Service:** EC2
        *   **Actions:** Expand "Write" and select `StopInstances`. Expand "List" and select `DescribeInstances`.
        *   **Resources:** Select `Specific`. Add the ARN of your specific EC2 instance (e.g., `arn:aws:ec2:ap-south-1:YOUR_ACCOUNT_ID:instance/i-xxxxxxxxxxxxxxxxx`).
        *   Review, name (e.g., `LambdaStopEC2InstancePolicy`), and create the policy.
    *   Click **"Next"** on the main "Add permissions" page.
4.  **Name the Role:** Enter `stop_kite_ec2_lambda_role`. Click **"Create role"**.
5.  **Verify Trust Policy:** After creation, click on the role name, go to **"Trust relationships"** tab, click **"Edit trust policy"**. Ensure it matches the same `lambda.amazonaws.com` trust policy as above. Click **"Update policy"** if changed.

### Step 6: Create and Configure AWS Lambda Functions

These functions will execute the start/stop commands for your EC2 instance.

1.  Go to **AWS Management Console** > Search for "Lambda" > Click **"Lambda"**.

#### a. `start_kite_ec2` Lambda Function

1.  Click **"Create function"**.
    *   **Function name:** `start_kite_ec2`
    *   **Runtime:** `Python 3.9` (or latest stable).
    *   **Architecture:** `x86_64`.
    *   **Execution role:** Choose `Use an existing role` and select `start_kite_ec2_lambda_role`.
    *   Click **"Create function"**.
2.  **Code:** In the "Code source" section, replace the default code with:
    ```python
    import boto3
    import os
    import logging

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    def lambda_handler(event, context):
        try:
            instance_id = os.environ['EC2_INSTANCE_ID']
            # AWS_REGION is automatically set by Lambda
            region = os.environ['AWS_REGION'] 

            ec2 = boto3.client('ec2', region_name=region)

            logger.info(f"Attempting to start EC2 instance: {instance_id} in region: {region}")
            ec2.start_instances(InstanceIds=[instance_id])
            logger.info(f"Successfully sent start command for EC2 instance: {instance_id}")

            return { 'statusCode': 200, 'body': f"Started instance {instance_id}" }

        except Exception as e:
            logger.error(f"Error starting EC2 instance {instance_id}: {e}")
            raise e
    ```
    Click **"Deploy"**.
3.  **Configuration:**
    *   **Environment variables:** Go to **"Configuration" > "Environment variables"**. Click "Edit".
        *   Add: **Key:** `EC2_INSTANCE_ID` , **Value:** `i-0f0bb17f687a74fc9` (Replace with your actual EC2 Instance ID).
        *   **Do NOT add `AWS_REGION` here; Lambda sets it automatically.**
        *   Click **"Save"**.
    *   **Timeout:** Go to **"Configuration" > "General configuration"**. Click "Edit".
        *   Set **"Timeout"** to `10 sec` (or `30 sec` to be safe).
        *   Click **"Save"**.

#### b. `stop_kite_ec2` Lambda Function

1.  Click **"Create function"**.
    *   **Function name:** `stop_kite_ec2`
    *   **Runtime:** `Python 3.9`.
    *   **Architecture:** `x86_64`.
    *   **Execution role:** Choose `Use an existing role` and select `stop_kite_ec2_lambda_role`.
    *   Click **"Create function"**.
2.  **Code:** In the "Code source" section, replace the default code with:
    ```python
    import boto3
    import os
    import logging

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    def lambda_handler(event, context):
        try:
            instance_id = os.environ['EC2_INSTANCE_ID']
            region = os.environ['AWS_REGION'] 

            ec2 = boto3.client('ec2', region_name=region)

            logger.info(f"Attempting to stop EC2 instance: {instance_id} in region: {region}")
            ec2.stop_instances(InstanceIds=[instance_id])
            logger.info(f"Successfully sent stop command for EC2 instance: {instance_id}")

            return { 'statusCode': 200, 'body': f"Stop command sent for instance {instance_id}" }

        except Exception as e:
            logger.error(f"Error stopping EC2 instance {instance_id}: {e}")
            raise e
    ```
    Click **"Deploy"**.
3.  **Configuration:**
    *   **Environment variables:** Go to **"Configuration" > "Environment variables"**. Click "Edit".
        *   Add: **Key:** `EC2_INSTANCE_ID` , **Value:** `i-0f0bb17f687a74fc9` (Replace with your actual EC2 Instance ID).
        *   Click **"Save"**.
    *   **Timeout:** Go to **"Configuration" > "General configuration"**. Click "Edit".
        *   Set **"Timeout"** to `10 sec` (or `30 sec`).
        *   Click **"Save"**.

### Step 7: Create EventBridge Rules for Scheduling

These rules trigger your Lambda functions on a daily schedule. **Cron expressions use UTC time.**

*   **Indian Market Open (9:15 AM IST) = 3:45 AM UTC**
*   **Indian Market Close + EOD Processing Buffer (3:45 PM IST) = 10:15 AM UTC**

1.  Go to **AWS Management Console** > Search for "EventBridge" > Click **"Amazon EventBridge"**.
2.  In the left navigation pane, click **"Rules"**.

#### a. `StartKiteCollectorDaily` Rule

1.  Click **"Create rule"**.
    *   **Name:** `StartKiteCollectorDaily`.
    *   **Description:** "Starts the EC2 instance for Kite data collection daily."
    *   **Status:** Ensure `Enabled` is selected.
    *   **Rule type:** `Scheduled rule`.
    *   Click **"Next"**.
2.  **Build Schedule:**
    *   **Event pattern:** Select `Schedule`.
    *   **Schedule pattern:** Choose `Cron expression`.
    *   Enter the cron expression for starting EC2 before market open (e.g., `cron(0 3 * * ? *)` for 3:00 AM UTC, which is 8:30 AM IST). This provides a 45-minute buffer for boot and script initialization.
    *   Click **"Next"**.
3.  **Select Target(s):**
    *   **Target:** `Lambda function`.
    *   **Function:** From the dropdown, select your `start_kite_ec2` Lambda function.
    *   Click **"Next"**.
4.  **Configure Tags (Optional):** Click **"Next"**.
5.  **Review and Create:** Click **"Create rule"**.

#### b. `StopKiteCollectorDaily` Rule

1.  Click **"Create rule"**.
    *   **Name:** `StopKiteCollectorDaily`.
    *   **Description:** "Stops the EC2 instance for Kite data collection daily after market close and EOD processing."
    *   **Status:** Ensure `Enabled` is selected.
    *   **Rule type:** `Scheduled rule`.
    *   Click **"Next"**.
2.  **Build Schedule:**
    *   **Event pattern:** `Schedule`.
    *   **Schedule pattern:** `Cron expression`.
    *   Enter the cron expression for stopping EC2 after EOD processing (e.g., `cron(30 10 * * ? *)` for 10:30 AM UTC, which is 4:00 PM IST). This provides a buffer after your script's EOD processing (3:45 PM IST).
    *   Click **"Next"**.
3.  **Select Target(s):**
    *   **Target:** `Lambda function`.
    *   **Function:** From the dropdown, select your `stop_kite_ec2` Lambda function.
    *   Click **"Next"**.
4.  **Configure Tags (Optional):** Click **"Next"**.
5.  **Review and Create:** Click **"Create rule"**.

---

## Part 2: Local Setup (Your Machine)

This phase configures your local machine to refresh the Kite Connect access token daily.

### Step 1: Prepare Local Python Script (`local_token_updater.py`)

Create a file named `local_token_updater.py` with the following code. Save it in a convenient directory.

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
        # This means the secret doesn't exist yet, we'll handle it in update_secret
        logging.warning(f"Secret '{SECRETS_MANAGER_SECRET_NAME}' not found during fetch. It will be created/updated.")
        raise # Re-raise to signal to generate_access_token_and_update_secret to handle creation
    except Exception as e:
        logging.error(f"Error fetching secret '{SECRETS_MANAGER_SECRET_NAME}': {e}")
        raise

def update_secret(new_data):
    """Updates the secret in AWS Secrets Manager with the provided data."""
    try:
        # Check if the secret exists first, to decide whether to update or create
        try:
            secrets_client.get_secret_value(SecretId=SECRETS_MANAGER_SECRET_NAME)
            # If exists, update
            secrets_client.put_secret_value(
                SecretId=SECRETS_MANAGER_SECRET_NAME,
                SecretString=json.dumps(new_data)
            )
        except secrets_client.exceptions.ResourceNotFoundException:
            # If not found, create it
            logging.info(f"Secret '{SECRETS_MANAGER_SECRET_NAME}' not found, attempting to create it.")
            secrets_client.create_secret(
                Name=SECRETS_MANAGER_SECRET_NAME,
                SecretString=json.dumps(new_data), # Use the new_data directly for creation
                Description=f"Kite Connect API credentials for {SECRETS_MANAGER_SECRET_NAME}"
            )
        
        logging.info(f"Secret '{SECRETS_MANAGER_SECRET_NAME}' updated successfully.")
    except Exception as e:
        logging.error(f"Error updating secret '{SECRETS_MANAGER_SECRET_NAME}': {e}", exc_info=True)
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
            logging.info("Secret not found on initial fetch, will proceed to create/update.")
            # This is fine, proceed with new_data containing only the access token
            pass 
        except Exception as e:
            logging.error(f"Failed to fetch existing secret data for unknown reason: {e}", exc_info=True)
            return # Cannot proceed if cannot fetch/create

        existing_secret_data['API_KEY'] = LOCAL_KITE_API_KEY
        existing_secret_data['API_SECRET'] = LOCAL_KITE_API_SECRET
        existing_secret_data['ACCESS_TOKEN'] = access_token
        # Add other relevant info if needed, e.g., existing_secret_data['USER_ID'] = data.get('user_id')

        update_secret(existing_secret_data)
        logging.info("Access Token successfully updated in AWS Secrets Manager.")

    except Exception as e:
        logging.error(f"Error generating session or updating secret: {e}", exc_info=True)
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

Your local script needs programmatic access to AWS.

1.  **Install AWS CLI:** If not already installed, download and install the AWS CLI for your operating system.
2.  **Configure AWS CLI:** Open your terminal/command prompt and run:
    ```bash
    aws configure
    ```    *   **AWS Access Key ID:** Paste the Access Key ID of your `KiteDataUploaderLocal` IAM user (created in Part 1, Step 4).
    *   **AWS Secret Access Key:** Paste the Secret Access Key for that user.
    *   **Default region name:** Enter `ap-south-1` (or your chosen AWS region).
    *   **Default output format:** Press Enter (accept `json`).

### Step 3: Run the Local Token Updater Script (Daily Manual Step)

This is the **only manual step** you'll perform daily.

1.  **Create a `.env` file** in the same directory as `local_token_updater.py`. This file will store your Kite API Key and Secret, preventing them from being hardcoded in the script.
    ```
    KITE_API_KEY="your_actual_kite_api_key_here"
    KITE_API_SECRET="your_actual_kite_api_secret_here"
    ```
2.  **Execute the script:**
    ```bash
    python3 local_token_updater.py
    ```
3.  Follow the script's prompts:
    *   Open the provided URL in a web browser.
    *   Log in to your Zerodha Kite account.
    *   Copy **ONLY** the `request_token` part from the redirected URL.
    *   Paste the `request_token` into your terminal prompt.

    The script will then generate a new Kite `access_token` and update the `KiteConnectBankniftyData` secret in AWS Secrets Manager. You should see success messages in your terminal.

---

## Part 3: EC2 Instance Setup and Script Deployment

This phase involves configuring your EC2 instance and deploying your main data collection script.

### Step 1: Launch EC2 Instance

1.  Go to **AWS Management Console** > Search for "EC2" > Click **"EC2"**.
2.  Click **"Launch instances"**.
3.  **Step 1: Choose an AMI:** Select `Amazon Linux 2 AMI` (HVM) or `Ubuntu Server`.
4.  **Step 2: Choose Instance Type:** Select `t2.micro` (free tier eligible) or `t3.micro`.
5.  **Step 3: Configure Instance Details:**
    *   **IAM role:** Select the `KiteDataCollectorEC2Role` you created in Part 1, Step 1. **This is crucial.**
    *   **User data:** In the "Advanced details" section, expand "User data", select "As text", and paste this script. This runs **once** on first boot to set up the environment.
        ```bash
        #!/bin/bash
        sudo yum update -y # For Amazon Linux; use apt-get for Ubuntu
        sudo yum install -y python3 python3-pip git

        mkdir -p /home/ec2-user/kite_collector
        cd /home/ec2-user/kite_collector

        # For initial deployment, we will SCP the ec2_kite_collector.py file after launch.
        # So, no 'git clone' needed here unless you manage your script with a public git repo.

        pip3 install kiteconnect pandas pyarrow boto3 pytz

        mkdir -p temp_kite_data
        mkdir -p final_kite_data
        ```
6.  **Step 4: Add Storage:** Default 8GB is usually sufficient. Increase if you expect very high data volumes over long periods.
7.  **Step 5: Configure Security Group:**
    *   Create a new security group.
    *   Allow **SSH (Port 22)** from `My IP` for initial setup and troubleshooting.
    *   Ensure outbound HTTPS (port 443) is allowed. (Default EC2 security groups usually allow all outbound traffic, which is fine for this).
8.  **Step 6: Review and Launch:**
    *   Choose an existing key pair or create a new one. Download the `.pem` file and keep it secure.
    *   Launch the instance.

### Step 2: Deploy EC2 Python Script (`ec2_kite_collector.py`)

This is your main data collection script.

1.  **Save the following code** as `ec2_kite_collector.py` on your local machine.

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
AWS_REGION = "ap-south-1" # Ensure this matches your AWS region
SECRETS_MANAGER_SECRET_NAME = "KiteConnectBankniftyData" # Must match the secret name in AWS console

S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "your-kite-data-bucket-unique-name") # Set your S3 bucket name
S3_PREFIX = os.getenv("S3_PREFIX", "banknifty_data/") # Prefix for objects within the bucket
SAVE_TO_S3 = os.getenv("SAVE_TO_S3", "True").lower() == "true" # "True" to save to S3, "False" for local only

TEMP_DATA_DIR = "temp_kite_data"
FINAL_DATA_DIR = "final_kite_data"

MARKET_OPEN_HOUR = 9
MARKET_OPEN_MINUTE = 15 # Market opens at 9:15 AM IST
MARKET_CLOSE_HOUR = 15 # Official Equity/F&O close is 3:30 PM IST
MARKET_CLOSE_MINUTE = 30
EOD_PROCESSING_HOUR = 15 # Start EOD processing slightly after close
EOD_PROCESSING_MINUTE = 45 # e.g., 3:45 PM IST

IST = pytz.timezone('Asia/Kolkata') # Define Indian Standard Timezone for accurate timing

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
        # For production, you'd fetch the current Bank Nifty index via another API or from a live feed
        # For demonstration, let's assume a hypothetical ATM range
        hypothetical_atm = 47000 # This needs to be dynamic for real use cases
        strike_range = 2000 # +/- 2000 points from ATM (adjust as needed)

        for instrument in instruments:
            if instrument['segment'] == 'NFO' and 'BANKNIFTY' in instrument['tradingsymbol']:
                # Filter for Futures
                if instrument['instrument_type'] == 'FUT':
                    # Add current month future. This requires robust logic to select the correct contract.
                    # Example: Filter by 'name' and 'instrument_type' for main future contract.
                    if instrument['name'] == 'BANKNIFTY' and instrument['instrument_type'] == 'FUT':
                         # Further refine: check instrument['expiry'] for nearest month.
                         banknifty_futures_options_tokens.append(instrument['instrument_token'])
                         # logging.debug(f"Adding Future: {instrument['tradingsymbol']}")
                # Filter for Options (CE/PE)
                elif instrument['instrument_type'] in ['CE', 'PE']:
                    if instrument['expiry'] == current_expiry_date: # Only current week's options
                        if instrument['strike'] >= (hypothetical_atm - strike_range) and \
                           instrument['strike'] <= (hypothetical_atm + strike_range):
                            banknifty_futures_options_tokens.append(instrument['instrument_token'])
                            # logging.debug(f"Adding Option: {instrument['tradingsymbol']} - {instrument['strike']} - {instrument['expiry']}")

        logging.info(f"Identified {len(banknifty_futures_options_tokens)} Bank Nifty F&O instruments for subscription.")

        if not banknifty_futures_options_tokens:
            logging.warning("No Bank Nifty F&O instruments found based on current filter logic. Data will not be collected.")
            return

        batch_size = 300 # A reasonable batch size for subscription to prevent API rate limits
        for i in range(0, len(banknifty_futures_options_tokens), batch_size):
            batch = banknifty_futures_options_tokens[i:i + batch_size]
            ws.subscribe(batch)
            ws.set_mode(ws.MODE_FULL, batch) # MODE_FULL provides 10-level market depth
            logging.info(f"Subscribed to batch of {len(batch)} instruments.")
            time.sleep(0.1) # Small delay to be polite to the API

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

        # Only warn if during market hours but not connected
        if (not kws or not kws.is_connected()) and \
           (now_ist.hour > MARKET_OPEN_HOUR or \
           (now_ist.hour == MARKET_OPEN_HOUR and now_ist.minute >= MARKET_OPEN_MINUTE)) and \
           (now_ist.hour < MARKET_CLOSE_HOUR or \
           (now_ist.hour == MARKET_CLOSE_HOUR and now_ist.minute < MARKET_CLOSE_MINUTE)):
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
    shutdown_event.set() # Ensure all threads know to shut down

    time.sleep(5) # Give a small buffer for background threads to react to shutdown_event

    process_eod_data()

    logging.info("Application shutdown complete. Exiting.")

```

2.  **Copy the script to your EC2 Instance:**
    *   Get your EC2 instance's Public IP address.
    *   Open your local terminal/command prompt.
    *   Use `scp` to copy the file:
        ```bash
        scp -i /path/to/your-key.pem /path/to/local/ec2_kite_collector.py ec2-user@YOUR_EC2_PUBLIC_IP:/home/ec2-user/kite_collector/
        ```
        (Replace paths and IP. `ec2-user` for Amazon Linux, `ubuntu` for Ubuntu AMIs).

### Step 3: Configure `systemd` Service on EC2

This ensures your script runs automatically whenever the EC2 instance starts.

1.  **SSH into your EC2 Instance.**
    ```bash
    ssh -i /path/to/your-key.pem ec2-user@YOUR_EC2_PUBLIC_IP
    ```
2.  **Create Service File:**
    ```bash
    sudo nano /etc/systemd/system/kite_data_collector.service
    ```
    Paste the following content. **Remember to update `User`, `WorkingDirectory`, `ExecStart` paths, and `Environment` variables for your S3 bucket name.**
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
3.  **Save the file** (Ctrl+X, Y, Enter for Nano).
4.  **Reload systemd and Enable the Service:**
    ```bash
    sudo systemctl daemon-reload
    sudo systemctl enable kite_data_collector.service
    ```
    *If you previously masked the service, you might need to run `sudo systemctl unmask kite_data_collector.service` first.*
5.  **Start and Monitor the Service:**
    ```bash
    sudo systemctl start kite_data_collector.service
    sudo systemctl status kite_data_collector.service
    tail -f /var/log/kite_collector.log
    ```

---

## Part 4: Common Errors and Resolutions (For your README)

This section summarizes the errors we encountered during setup and their fixes, which you can use for your project's README or internal documentation.

### Troubleshooting: Common Errors

Here are the issues you might encounter during setup and execution, along with their resolutions.

#### 1. `Failed to enable unit: Unit file /etc/systemd/system/kite_data_collector.service is masked.`

*   **Cause:** The `systemd` service was forcefully prevented from being enabled or started.
*   **Resolution:** Unmask the service, then reload `systemd` daemon, then enable.
    ```bash
    sudo systemctl unmask kite_data_collector.service
    sudo systemctl daemon-reload
    sudo systemctl enable kite_data_collector.service
    ```

#### 2. `Failed to enable unit: Unit file kite_data_collector.service does not exist.`

*   **Cause:** The `systemd` service file is not found at the expected path (`/etc/systemd/system/`). This indicates a typo in the filename or that it wasn't saved correctly.
*   **Resolution:** Verify the file's exact name and location (`ls -l /etc/systemd/system/`). If it's missing or misnamed, recreate/rename it ensuring the name is precisely `kite_data_collector.service`.

#### 3. `status=217/USER` in `systemctl status` output

*   **Cause:** `systemd` failed to set up the execution environment for the user specified in the service file's `User=` directive. This can happen if the user doesn't exist, or if there are fundamental path/permission issues preventing the script from launching under that user's context.
*   **Resolution:**
    1.  **Verify User:** Confirm the actual username on your EC2 instance (`whoami`).
    2.  **Edit Service File:** Ensure `User=` in `/etc/systemd/system/kite_data_collector.service` matches this user (e.g., `User=ec2-user` or `User=ubuntu`). Also, check `WorkingDirectory` and `ExecStart` paths are correct.
    3.  **Get Detailed Logs (Manual Run):** Run the script manually as the target user to capture Python-specific errors that `systemd` might not show directly:
        ```bash
        cd /home/ec2-user/kite_collector/ # Or your WorkingDirectory
        sudo -u ec2-user /usr/bin/python3 /home/ec2-user/kite_collector/ec2_kite_collector.py > test_run.log 2>&1
        cat test_run.log # Inspect the content of this log file
        ```
        The `test_run.log` will reveal the specific Python traceback or environmental error.

#### 4. `ValueError: signal only works in main thread of the main interpreter`

*   **Cause:** The `kiteconnect` library's underlying dependencies attempt to install signal handlers from a non-main thread when `kws.connect()` is called in a separate thread.
*   **Resolution:** The `kws.connect()` call *must* be executed directly in the **main thread** of your `ec2_kite_collector.py` script. Other long-running tasks (periodic saving, market session management) should be launched in separate background threads *before* `kws.connect()`. The provided `ec2_kite_collector.py` in this guide includes this fix.

#### 5. `ERROR - Error retrieving Kite credentials from Secrets Manager: An error occurred (AccessDeniedException) when calling the GetSecretValue operation: User: ... is not authorized...`

*   **Cause:** The IAM user/role trying to access Secrets Manager (either your `KiteDataUploaderLocal` user locally or your `KiteDataCollectorEC2Role` on EC2) does not have the necessary `secretsmanager:GetSecretValue` or `secretsmanager:PutSecretValue` permissions for your specific secret.
*   **Resolution:**
    1.  Go to **IAM** in AWS Console.
    2.  Navigate to **Users** (for local script's IAM user) or **Roles** (for EC2 instance's IAM role).
    3.  Select the relevant identity (`KiteDataUploaderLocal` or `KiteDataCollectorEC2Role`).
    4.  Go to the **"Permissions" tab**.
    5.  **Add/Edit an inline policy** (or attach a managed policy) to grant the required permissions. The policy JSON **must** explicitly allow the actions on the correct resource. For example:
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
        *   For the EC2 role, only `secretsmanager:GetSecretValue` is needed.
    6.  Save changes.

#### 6. `ERROR - Error fetching secret 'KiteConnectBankniftyData': An error occurred (ResourceNotFoundException) when calling the PutSecretValue operation: Secrets Manager can't find the specified secret.`

*   **Cause:** The secret named `KiteConnectBankniftyData` does not exist in AWS Secrets Manager in the specified region, or the script/policy has a case-sensitive typo in the secret name. `PutSecretValue` updates an existing secret, it doesn't create one if not found.
*   **Resolution:**
    1.  Go to **Secrets Manager** in the AWS Console.
    2.  **Manually create the secret** named `KiteConnectBankniftyData` (ensuring **exact case matching**). Populate it with initial dummy JSON values for `API_KEY`, `API_SECRET`, and `ACCESS_TOKEN`.
    3.  Ensure the `SECRETS_MANAGER_SECRET_NAME` variable in both your Python scripts (`local_token_updater.py` and `ec2_kite_collector.py`) matches this exact name.

#### 7. `Lambda was unable to configure your environment variables because the environment variables you have provided contains reserved keys that are currently not supported for modification. Reserved keys used in this request: AWS_REGION`

*   **Cause:** `AWS_REGION` is a reserved environment variable in Lambda and is automatically set by AWS based on the function's deployment region. You cannot manually define it.
*   **Resolution:**
    1.  Go to the Lambda function's **"Configuration" tab**, then **"Environment variables"**.
    2.  **Remove** the `AWS_REGION` entry from the environment variables list.
    3.  Ensure only `EC2_INSTANCE_ID` remains (and is correct).
    4.  Save changes.

#### 8. `KeyError: 'i-0f0bb17f687a74fc9'` in Lambda logs

*   **Cause:** The Python code within your Lambda function (`lambda_function.py`) has been incorrectly modified to use the EC2 Instance ID as the *key* when trying to retrieve an environment variable (e.g., `os.environ['i-0f0bb17f687a74fc9']`), instead of using the intended key name (`os.environ['EC2_INSTANCE_ID']`).
*   **Resolution:**
    1.  Go to the Lambda function's **"Code" tab**.
    2.  **Correct the Python code:** Change the line `instance_id = os.environ['i-0f0bb17f687a74fc9']` back to `instance_id = os.environ['EC2_INSTANCE_ID']`. Do similarly for `region = os.environ['AWS_REGION']`.
    3.  Click **"Deploy"** to save the code changes.

#### 9. Lambda `Status: timeout`

*   **Cause:** The Lambda function's default timeout (often 3 seconds) is too short for the EC2 API call to complete.
*   **Resolution:**
    1.  Go to the Lambda function's **"Configuration" tab**, then **"General configuration"**.
    2.  Click "Edit".
    3.  Increase the **"Timeout"** value to `10 sec` or `30 sec`.
    4.  Click "Save".

#### 10. `WebSocket connection upgrade failed (403 - Forbidden)` in EC2 script logs

*   **Cause:** The Kite Connect WebSocket server is rejecting the connection.
    *   **Most Common:** Attempting to connect outside live Indian market hours (9:15 AM - 3:30 PM IST).
    *   Expired or invalid `ACCESS_TOKEN` in Secrets Manager.
    *   EC2 instance's Public IP address is not whitelisted in your Kite Connect app settings on Zerodha's developer portal.
*   **Resolution:**
    1.  **Test During Market Hours:** Run the script ONLY during Indian market hours on a trading day.
    2.  **Fresh Access Token:** Ensure you've run your `local_token_updater.py` script on the current day to refresh the `ACCESS_TOKEN` in Secrets Manager.
    3.  **IP Whitelisting:** Log in to your Kite Connect Developer Console, go to your API App settings, and add the Public IP address of your EC2 instance to the whitelist.

---

## Part 5: Testing the Automation (EventBridge Triggers)

It's crucial to test your EC2 start and stop automation without waiting for the daily schedule.

### Step 1: Create a Temporary EventBridge Rule for Starting EC2

1.  **Check Current UTC Time:** Get the precise current UTC time (e.g., from a quick web search).
2.  Go to **Amazon EventBridge Console** > **Rules**.
3.  Click **"Create rule"**.
    *   **Name:** `TEST-StartKiteCollectorNow` (clear test name).
    *   **Description:** "Temporary rule for immediate testing of EC2 start."
    *   **Schedule pattern:** Choose `Cron expression`. Enter a cron expression for a time **2-3 minutes from the current UTC time**. (e.g., if it's 08:30 AM UTC, set `cron(33 08 * * ? *)`).
    *   **Target:** Select `Lambda function` and choose your `start_kite_ec2` Lambda function.
    *   Click **"Create rule"**.

### Step 2: Monitor and Verify EC2 Startup

1.  **Monitor Lambda Logs:**
    *   Immediately go to the **Lambda console** > `start_kite_ec2` function > **"Monitor" tab** > **"View CloudWatch logs"**.
    *   Wait for the scheduled time. A new log stream should appear, and you should see `INFO - Successfully sent start command for EC2 instance: i-xxxxxxxxxxxxxxxxx`.
2.  **Check EC2 State:**
    *   Go to the **EC2 console** > **"Instances"**.
    *   Your EC2 instance should change from "stopped" to "pending" and then "running".
3.  **Monitor EC2 Script Logs (Once EC2 is running):**
    *   SSH into the EC2 instance.
    *   Check your script's logs: `tail -f /var/log/kite_collector.log`. You should see it initializing.

### Step 3: Create a Temporary EventBridge Rule for Stopping EC2

1.  **Ensure your EC2 instance is currently running.**
2.  **Check Current UTC Time.**
3.  Go to **Amazon EventBridge Console** > **Rules**.
4.  Click **"Create rule"**.
    *   **Name:** `TEST-StopKiteCollectorNow`.
    *   **Description:** "Temporary rule for immediate testing of EC2 stop."
    *   **Schedule pattern:** Choose `Cron expression`. Set it for a time **2-3 minutes from the current UTC time**.
    *   **Target:** Select `Lambda function` and choose your `stop_kite_ec2` Lambda function.
    *   Click **"Create rule"**.

### Step 4: Monitor and Verify EC2 Shutdown

1.  **Monitor Lambda Logs:**
    *   Immediately go to the **Lambda console** > `stop_kite_ec2` function > **"Monitor" tab** > **"View CloudWatch logs"**.
    *   At the scheduled time, you should see `INFO - Successfully sent stop command for EC2 instance: i-xxxxxxxxxxxxxxxxx`.
2.  **Check EC2 State:**
    *   Go to the **EC2 console** > **"Instances"**.
    *   Your EC2 instance should change from "running" to "stopping" and then "stopped".

### Step 5: Clean Up Temporary Test Rules (CRITICAL!)

Once you've confirmed both start and stop functionalities work, **immediately delete the temporary EventBridge rules** to prevent unexpected instance actions later.

1.  Go to **Amazon EventBridge Console** > **Rules**.
2.  Select `TEST-StartKiteCollectorNow`.
3.  Click **"Delete"** and confirm.
4.  Select `TEST-StopKiteCollectorNow`.
5.  Click **"Delete"** and confirm.

---

Congratulations! You now have a complete guide to deploy and maintain your automated Kite Connect data collector on AWS. 
