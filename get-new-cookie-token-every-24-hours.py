import os
import requests
import uuid
import schedule
import time
from datetime import datetime, timedelta
import logging

# Set up logging to file
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('cookie-token-24-hour—new-cookie-log.txt')
    ]
)
logger = logging.getLogger(__name__)

# Global variables
# Note: Ensure YOUR_SECRET_KEY is exported in your .bashrc file, e.g.,
# export YOUR_SECRET_KEY="your-api-key-here"
YOUR_SECRET_KEY = "YOUR_SECRET_KEY"
secret = None
last_runtime = None

def get_next_run_time():
    """Calculate the next run time at 00:01 tomorrow."""
    now = datetime.now()
    next_run = (now + timedelta(days=1)).replace(hour=0, minute=1, second=0, microsecond=0)
    return next_run

def job():
    """Main job function to execute the API calls."""
    global last_runtime
    try:
        # Record the current runtime
        current_runtime = datetime.now()
        logger.info("Program started running.")

        # Core API code (must match exactly)
        secret = os.getenv("YOUR_SECRET_KEY")

        # Authorization
        url = "https://api.public.com/userapiauthservice/personal/access-tokens"
        headers = {
            "Content-Type": "application/json"
        }

        request_body = {
            "validityInMinutes": 1440,
            "secret": secret
        }

        response = requests.post(url, headers=headers, json=request_body)
        access = response.json()["accessToken"]

        # Account Information
        url = "https://api.public.com/userapigateway/trading/account"
        headers = {
            "Authorization": f"Bearer {access}",
            "Content-Type": "application/json"
        }

        response = requests.get(url, headers=headers)
        data = response.json()
        print(data)
        print(f"Current date and time: {datetime.now()}")
        print()  # Empty print statement for spacing
        next_run = get_next_run_time()
        print(f"Next run time: {next_run}")

        accountId = data["accounts"][0]["accountId"]

        # Owned Stocks
        url = "https://api.public.com/userapigateway/trading/{accountId}/portfolio/v2"
        headers = {
            "Authorization": f"Bearer {access}",
            "Content-Type": "application/json"
        }

        response = requests.get(url.format(accountId=accountId), headers=headers)
        data = response.json()
        #print(data)

        # Log the data for consistency
        logger.info(f"Account data: {data}")
        logger.info(f"Portfolio data: {data}")

        # Update last runtime
        last_runtime = current_runtime
        logger.info(f"Program completed successfully. Last runtime: {last_runtime}")

        # Log next run time
        logger.info(f"Next scheduled run: {next_run}")

    except Exception as e:
        logger.error(f"Error in job: {str(e)}")
        logger.info("Restarting job in 2 minutes...")
        time.sleep(120)  # Wait 2 minutes before retrying
        job()  # Retry the job

def main():
    """Main function to set up and run the scheduler."""
    global secret
    while True:
        try:
            # Clear any existing scheduled jobs to prevent duplicates
            schedule.clear()

            # Retrieve secret from environment variable (sourced from .bashrc)
            secret = os.getenv(YOUR_SECRET_KEY)
            if not secret:
                logger.error(f"Failed to retrieve {YOUR_SECRET_KEY} from environment (ensure it is exported in .bashrc)")
                raise ValueError(f"Environment variable {YOUR_SECRET_KEY} is not set")
            logger.info(f"Successfully retrieved {YOUR_SECRET_KEY} from environment")

            # Run the job immediately on first run or after restart
            logger.info("Running job immediately on first run or restart.")
            job()

            # Schedule the job to run every day at 00:01
            schedule.every().day.at("00:01").do(job)

            # Main loop for scheduling
            while True:
                schedule.run_pending()
                # Display next run time when sleeping
                next_run = get_next_run_time()
                logger.info(f"Waiting for next run at: {next_run}")
                if last_runtime:
                    logger.info(f"Previous runtime: {last_runtime}")
                time.sleep(60)  # Check every minute

        except Exception as e:
            logger.error(f"Error in main loop: {str(e)}")
            logger.info("Restarting main loop in 2 minutes...")
            time.sleep(120)  # Wait 2 minutes before restarting the main loop

if __name__ == "__main__":
    main()
