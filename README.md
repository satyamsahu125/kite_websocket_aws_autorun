let's go through the step-by-step AWS configuration. This guide assumes you have an AWS account and basic familiarity with the AWS Management Console.

**Before you start:**
*   Have your Kite Connect API Key and API Secret ready.
*   Decide on a unique name for your S3 bucket (e.g., `my-kite-banknifty-data-2025`).
*   Choose an AWS region (e.g., `ap-south-1` for Mumbai, which is often preferred for Indian market data due to lower latency, but any region works). Consistency is key!

---

### Phase 1: Core AWS Service Setup (IAM, S3, Secrets Manager)

#### Step 1: Create an IAM Role for your EC2 Instance

This role grants your EC2 instance permissions to interact with S3 and Secrets Manager.

1.  **Go to IAM:** In the AWS Management Console, search for "IAM" and click on it.
2.  **Navigate to Roles:** In the left-hand navigation, click on "Roles".
3.  **Create Role:** Click the "Create role" button.
    *   **Trusted entity type:** Select `AWS service`.
    *   **Use case:** Select `EC2`.
    *   Click "Next".
4.  **Add Permissions:**
    *   In the "Add permissions" page, search for and select these policies:
        *   `AmazonS3FullAccess` (For simplicity. In a production environment, you'd create a custom policy allowing `s3:PutObject`, `s3:ListBucket` only for your specific bucket).
        *   `SecretsManagerReadWrite` (Again, for simplicity. Custom policy: `secretsmanager:GetSecretValue` and `secretsmanager:PutSecretValue` for your specific secret).
    *   Click "Next".
5.  **Name and Create Role:**
    *   **Role name:** Enter `KiteDataCollectorEC2Role` (or a name you prefer).
    *   (Optional) Add a description.
    *   Click "Create role".

#### Step 2: Create an S3 Bucket for Your Data

This is where your final Parquet files will be stored.

1.  **Go to S3:** In the AWS Management Console, search for "S3" and click on it.
2.  **Create bucket:** Click the "Create bucket" button.
    *   **Bucket name:** Enter a globally unique name (e.g., `your-kite-banknifty-data-YYYYMMDD`). **Make sure it's unique.**
    *   **AWS Region:** Select the same region you plan to launch your EC2 instance in (e.g., `Asia Pacific (Mumbai) ap-south-1`).
    *   **Object Ownership:** Keep `ACLs enabled` and `Recommended: Bucket owner preferred`.
    *   **Block Public Access settings for this bucket:** Keep all options **checked** (recommended for security).
    *   Keep other settings as default for now.
    *   Click "Create bucket".

#### Step 3: Create a Secret in AWS Secrets Manager

This will securely store your Kite Connect API credentials and the daily-updated Access Token.

1.  **Go to Secrets Manager:** In the AWS Management Console, search for "Secrets Manager" and click on it.
2.  **Store a new secret:** Click the "Store a new secret" button.
    *   **Secret type:** Select `Other type of secret`.
    *   **Key/value pairs:**
        *   Add a key `API_KEY` with your actual Kite Connect API Key as its value.
        *   Add a key `API_SECRET` with your actual Kite Connect API Secret as its value.
        *   Add a key `ACCESS_TOKEN` with a dummy value for now (e.g., `DUMMY_TOKEN`). Your local script will update this daily.
        *   Example:
            ```json
            {
              "API_KEY": "your_actual_kite_api_key",
              "API_SECRET": "your_actual_kite_api_secret",
              "ACCESS_TOKEN": "DUMMY_TOKEN"
            }
            ```
    *   Click "Next".
3.  **Configure secret:**
    *   **Secret name:** Enter `KiteConnectBankNiftyData` (must match the name in your scripts).
    *   (Optional) Add a description.
    *   Click "Next".
4.  **Review and Store:**
    *   Review the settings.
    *   Click "Store".

---

### Phase 2: EC2 Instance Setup and Script Deployment

#### Step 4: Launch and Configure Your EC2 Instance

This is where your Python data collection script will run.

1.  **Go to EC2:** In the AWS Management Console, search for "EC2" and click on it.
2.  **Launch instances:** Click the "Launch instances" button.
3.  **Step 1: Choose an Amazon Machine Image (AMI):**
    *   Select `Amazon Linux 2 AMI` (HVM) or `Ubuntu Server`. Amazon Linux 2 is generally lightweight and good for this purpose.
4.  **Step 2: Choose an Instance Type:**
    *   Select `t2.micro` (free tier eligible) or `t3.micro`. These are usually sufficient for data collection.
5.  **Step 3: Configure Instance Details:**
    *   **IAM role:** Select the `KiteDataCollectorEC2Role` you created earlier. This is crucial for permissions!
    *   **User data:** This script will run automatically *once* when the instance is launched (or on subsequent reboots if configured to do so). Select "As text" and paste the following. **Remember to replace placeholders** like `your-bucket-name-unique-name` and `your-github-repo-url`.
        ```bash
        #!/bin/bash
        # Update system
        sudo yum update -y

        # Install Python 3 and pip (Amazon Linux 2 comes with Python 3, just ensure pip is there)
        sudo yum install -y python3 python3-pip git

        # Create script directory and navigate into it
        mkdir -p /home/ec2-user/kite_collector
        cd /home/ec2-user/kite_collector

        # Clone your script repository (replace with your actual Git URL)
        # IMPORTANT: Ensure your repository is public or you configure SSH keys/credentials.
        # For simplicity, if your script is small, you could use 'curl' to download directly
        # or upload it via SCP/SFTP after launch.
        # If using Git, ensure it's a public repo, or clone using token if private.
        git clone https://github.com/your-username/your-kite-repo.git . # Replace with your repo URL if using Git
        # Or, if manually uploading later: just create the directory and skip git clone here.

        # Install Python dependencies
        pip3 install kiteconnect pandas pyarrow boto3 pytz

        # Create data directories
        mkdir -p temp_kite_data
        mkdir -p final_kite_data

        # Configure environment variables for the S3 bucket (optional, can be in systemd service too)
        # export S3_BUCKET_NAME="your-bucket-name-unique-name" # Set this to your S3 bucket name
        # export S3_PREFIX="banknifty_data/"
        # export SAVE_TO_S3="True"

        # Create systemd service file (this will run your script on boot)
        # We'll create a full systemd service in the next step, but you can
        # put a basic start command here if you don't use a dedicated service file yet.
        # This user data is primarily for initial setup, not the daily run.
        ```
    *   **Ensure the `ec2_kite_collector.py` script is present on the EC2 instance in `/home/ec2-user/kite_collector` (or the directory you choose in `WorkingDirectory`).** You can:
        *   Include it in the `git clone` command in User Data.
        *   Manually `scp` it after the instance launches for the first time.
6.  **Step 4: Add Storage:**
    *   Default 8GB is usually fine for `t2.micro`. If you expect massive data volumes, increase it.
7.  **Step 5: Configure Security Group:**
    *   **Create a new security group.**
    *   **Type:** `SSH` (Port 22), **Source:** `My IP` (for initial setup and troubleshooting). This allows you to SSH into the instance.
    *   **Important:** Your script needs outbound access to Kite Connect and AWS services. EC2 instances by default allow all outbound traffic, which is fine.
    *   Click "Review and Launch".
8.  **Step 6: Review and Launch:**
    *   Review your settings.
    *   Click "Launch".
    *   **Key Pair:** Choose an existing key pair or create a new one. You'll need this `.pem` file to SSH into your instance. Download and store it securely.
    *   Click "Launch Instances".

#### Step 5: Configure `systemd` Service on EC2

This makes your script run automatically every time the EC2 instance starts.

1.  **SSH into your EC2 Instance:**
    ```bash
    ssh -i /path/to/your-key.pem ec2-user@YOUR_EC2_PUBLIC_IP
    ```
    (Replace `/path/to/your-key.pem` and `YOUR_EC2_PUBLIC_IP`).
2.  **Create Service File:**
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
    WorkingDirectory=/home/ec2-user/kite_collector # Path where your ec2_kite_collector.py is
    ExecStart=/usr/bin/python3 /home/ec2-user/kite_collector/ec2_kite_collector.py
    Restart=on-failure # Automatically restart if the script exits with an error
    StandardOutput=append:/var/log/kite_collector.log # Redirect stdout to this log file
    StandardError=append:/var/log/kite_collector.log # Redirect stderr to the same log file

    # Environment variables for your S3 bucket (can be overridden by script or set here)
    Environment="S3_BUCKET_NAME=your-bucket-name-unique-name" # YOUR S3 BUCKET NAME
    Environment="S3_PREFIX=banknifty_data/"
    Environment="SAVE_TO_S3=True" # Set to "False" if you only want local storage

    [Install]
    WantedBy=multi-user.target
    ```
3.  **Save and Exit:** (Ctrl+X, Y, Enter for Nano).
4.  **Reload systemd and Enable the Service:**
    ```bash
    sudo systemctl daemon-reload
    sudo systemctl enable kite_data_collector.service
    ```
    This makes the service start on boot.
5.  **Test Start the Service (Optional, but Recommended):**
    ```bash
    sudo systemctl start kite_data_collector.service
    ```
    Check its status and logs:
    ```bash
    sudo systemctl status kite_data_collector.service
    tail -f /var/log/kite_collector.log
    ```
    You should see your script's logs appearing. If there are errors, check the logs for clues.

---

### Phase 3: Automation of EC2 Instance (CloudWatch Events & Lambda)

#### Step 6: Create IAM Roles for Lambda Functions

Each Lambda function needs a role to perform its action (start/stop EC2).

1.  **Go to IAM > Roles > Create role.**
2.  **Trusted entity type:** `AWS service`, **Use case:** `Lambda`. Click "Next".
3.  **Permissions for `start_kite_ec2_lambda_role`:**
    *   Search for `AmazonEC2FullAccess` (for simplicity; a custom policy with `ec2:StartInstances` and `ec2:DescribeInstances` is better for production).
    *   Click "Next".
    *   **Role name:** `start_kite_ec2_lambda_role`. Create role.
4.  **Permissions for `stop_kite_ec2_lambda_role`:**
    *   Repeat steps for a new role.
    *   Search for `AmazonEC2FullAccess` (or custom policy with `ec2:StopInstances` and `ec2:DescribeInstances`).
    *   Click "Next".
    *   **Role name:** `stop_kite_ec2_lambda_role`. Create role.

#### Step 7: Create AWS Lambda Functions

These functions will be triggered by schedules to start/stop your EC2 instance.

1.  **Go to Lambda:** In the AWS Management Console, search for "Lambda" and click on it.
2.  **Create Function 1: `start_kite_ec2`**
    *   Click "Create function".
    *   **Author from scratch:**
        *   **Function name:** `start_kite_ec2`
        *   **Runtime:** `Python 3.9` (or latest stable)
        *   **Architecture:** `x86_64`
        *   **Execution role:** Choose `Use an existing role` and select `start_kite_ec2_lambda_role`.
    *   Click "Create function".
    *   **Function code:** In the "Code source" section, replace the default code with:
        ```python
        import boto3
        import os
        import logging

        logger = logging.getLogger()
        logger.setLevel(logging.INFO)

        def lambda_handler(event, context):
            # Get EC2 instance ID and AWS region from environment variables
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
                # You might want to raise the exception to see it in CloudWatch logs
                raise e
        ```
    *   **Environment variables:**
        *   Add `EC2_INSTANCE_ID` with the actual ID of your EC2 instance (e.g., `i-0abcdef1234567890`). Find this in the EC2 dashboard.
        *   Add `AWS_REGION` with your region (e.g., `ap-south-1`).
    *   Click "Deploy" to save changes.

3.  **Create Function 2: `stop_kite_ec2`**
    *   Repeat the process for a new function.
    *   **Function name:** `stop_kite_ec2`
    *   **Runtime:** `Python 3.9`
    *   **Execution role:** Choose `Use an existing role` and select `stop_kite_ec2_lambda_role`.
    *   Click "Create function".
    *   **Function code:** Replace the default code with:
        ```python
        import boto3
        import os
        import logging

        logger = logging.getLogger()
        logger.setLevel(logging.INFO)

        def lambda_handler(event, context):
            # Get EC2 instance ID and AWS region from environment variables
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
    *   **Environment variables:**
        *   Add `EC2_INSTANCE_ID` with the actual ID of your EC2 instance.
        *   Add `AWS_REGION` with your region.
    *   Click "Deploy" to save changes.

#### Step 8: Create EventBridge (CloudWatch Events) Rules

These rules will trigger your Lambda functions on a schedule. **Times are in UTC.**

**Convert IST to UTC:**
*   9:15 AM IST (Market Open) = 3:45 AM UTC
*   3:45 PM IST (After EOD processing) = 10:15 AM UTC

1.  **Go to Amazon EventBridge:** In the AWS Management Console, search for "EventBridge" (or "CloudWatch Events" if you're on an older console version) and click on it.
2.  **Create Rule 1: `StartKiteCollectorDaily`**
    *   In the left navigation, click "Rules".
    *   Click "Create rule".
    *   **Name:** `StartKiteCollectorDaily`
    *   **Description:** "Starts the EC2 instance for Kite data collection daily."
    *   **Define pattern:**
        *   **Event pattern:** Select `Schedule`.
        *   **Schedule pattern:** Choose `Cron expression`.
        *   Enter the cron expression for your market open time (e.g., `cron(0 3 * * ? *)` for 8:30 AM IST / 3:00 AM UTC, giving 45 mins to boot before 9:15 AM IST). Adjust this for a comfortable buffer time for your EC2 to boot and run the script.
            *   **Cron format:** `minutes hours day-of-month month day-of-week year` (year is optional)
            *   `cron(MM HH * * ? *)` where MM is minutes (0-59), HH is hours (0-23 UTC).
    *   **Select targets:**
        *   **Target:** `Lambda function`.
        *   **Function:** Select `start_kite_ec2`.
    *   Click "Create".

3.  **Create Rule 2: `StopKiteCollectorDaily`**
    *   Repeat the process for a new rule.
    *   **Name:** `StopKiteCollectorDaily`
    *   **Description:** "Stops the EC2 instance for Kite data collection daily after market close."
    *   **Define pattern:**
        *   **Event pattern:** `Schedule`.
        *   **Schedule pattern:** `Cron expression`.
        *   Enter the cron expression for after your EOD processing (e.g., `cron(30 10 * * ? *)` for 4:00 PM IST / 10:30 AM UTC, allowing for your 3:45 PM IST EOD processing to complete).
    *   **Select targets:**
        *   **Target:** `Lambda function`.
        *   **Function:** Select `stop_kite_ec2`.
    *   Click "Create".

---

### Final Steps and Testing

1.  **Test Local Token Updater:** On your local machine, run `python3 local_token_updater.py` daily. Follow the prompts to get the `request_token` and update the `KiteConnectBankNiftyData` secret in AWS Secrets Manager.
2.  **Verify Secrets Manager:** After running the local script, go to AWS Secrets Manager and check the `KiteConnectBankNiftyData` secret. The `ACCESS_TOKEN` value should be updated.
3.  **Test EC2 Start/Stop (Manual):**
    *   Go to EC2 Console, select your instance, and click "Instance state > Start instance".
    *   Wait a few minutes. Check the `/var/log/kite_collector.log` via SSH. You should see your script running.
    *   After some time, manually stop the instance: "Instance state > Stop instance".
    *   Verify that `process_eod_data()` was called and a Parquet file appeared in your S3 bucket.
4.  **Monitor CloudWatch Logs:**
    *   For Lambda functions: Go to Lambda, select your function, then "Monitor" tab, and click "View CloudWatch logs".
    *   For EC2 script: The logs will be in `/var/log/kite_collector.log` on the EC2 instance. You can also send EC2 logs to CloudWatch Logs for centralized monitoring.

This completes the setup for automated daily data collection using AWS services. Remember to monitor your costs and logs regularly.
